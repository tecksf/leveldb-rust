use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::rc::{Rc, Weak};
use crate::core::format::InternalKey;
use crate::core::iterator::LevelIterator;
use crate::Options;
use crate::logs::{file, filename};
use crate::table::table::Table;
use crate::utils::coding;
use crate::utils::common::hash;

struct Node<T> {
    value: Option<T>,
    next: Option<Rc<RefCell<Node<T>>>>,
    prev: Weak<RefCell<Node<T>>>,
}

impl<T> Node<T> {
    fn new() -> Self {
        Self {
            value: None,
            next: None,
            prev: Weak::new(),
        }
    }
}

type LinkedNode<T> = Rc<RefCell<Node<T>>>;

struct LinkedList<T> {
    head: LinkedNode<T>,
    tail: LinkedNode<T>,
    size: usize,
}

impl<T> LinkedList<T> {
    fn new() -> Self {
        let head = Rc::new(RefCell::new(Node::new()));
        let tail = Rc::new(RefCell::new(Node::new()));
        head.borrow_mut().next = Some(tail.clone());
        tail.borrow_mut().prev = Rc::downgrade(&head);
        Self {
            head,
            tail,
            size: 0,
        }
    }

    fn len(&self) -> usize {
        self.size
    }

    fn push_front(&mut self, value: T) -> LinkedNode<T> {
        let node = Rc::new(RefCell::new(Node {
            value: Some(value),
            next: None,
            prev: Weak::new(),
        }));

        self.size += 1;
        self.push_node::<true>(node.clone());
        node
    }

    fn push_back(&mut self, value: T) -> LinkedNode<T> {
        let node = Rc::new(RefCell::new(Node {
            value: Some(value),
            next: None,
            prev: Weak::new(),
        }));

        self.size += 1;
        self.push_node::<false>(node.clone());
        node
    }

    fn pop_front(&mut self) -> Option<LinkedNode<T>> {
        if self.size == 0 {
            return None;
        }

        self.size -= 1;
        let node = self.head.borrow().next.clone().expect("no node");
        LinkedList::remove_node(node.clone());

        Some(node)
    }

    fn pop_back(&mut self) -> Option<LinkedNode<T>> {
        if self.size == 0 {
            return None;
        }

        self.size -= 1;
        let node = self.tail.borrow().prev.clone().upgrade().expect("no node");
        LinkedList::remove_node(node.clone());

        Some(node)
    }

    fn remove_node(node: LinkedNode<T>) {
        let prev: LinkedNode<T>;
        let next: LinkedNode<T>;
        if let Some(ptr) = node.borrow().prev.clone().upgrade() {
            prev = ptr;
        } else {
            return;
        }

        if let Some(ptr) = node.borrow().next.clone() {
            next = ptr;
        } else {
            return;
        }

        prev.borrow_mut().next = Some(next.clone());
        next.borrow_mut().prev = Rc::downgrade(&prev);
    }

    fn push_node<const FRONT: bool>(&mut self, node: LinkedNode<T>) {
        if FRONT {
            let succeed_node = self.head.borrow().next.clone().expect("no succeed node");
            self.head.borrow_mut().next = Some(node.clone());
            node.borrow_mut().next = Some(succeed_node.clone());
            succeed_node.borrow_mut().prev = Rc::downgrade(&node);
            node.borrow_mut().prev = Rc::downgrade(&self.head);
        } else {
            let precursor_node = self.tail.borrow().prev.clone().upgrade().expect("no precursor node");
            precursor_node.borrow_mut().next = Some(node.clone());
            node.borrow_mut().next = Some(self.tail.clone());
            self.tail.borrow_mut().prev = Rc::downgrade(&node);
            node.borrow_mut().prev = Rc::downgrade(&precursor_node);
        }
    }
}

struct LinkedListIterator<T> {
    begin: LinkedNode<T>,
    end: LinkedNode<T>,
}

impl<T: Clone> IntoIterator for LinkedList<T> {
    type Item = T;
    type IntoIter = LinkedListIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            begin: self.head.clone(),
            end: self.tail.clone(),
        }
    }
}

impl<T: Clone> Iterator for LinkedListIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.begin.borrow().next.clone();
        self.begin = next.unwrap_or(self.end.clone());
        if Rc::ptr_eq(&self.begin, &self.end) {
            return None;
        }

        self.begin.borrow().value.clone()
    }
}

struct LRUCache<K, V> {
    table: HashMap<K, LinkedNode<V>>,
    lru: LinkedList<V>,
}

impl<K, V> LRUCache<K, V> where K: Eq + Hash {
    fn new() -> Self {
        Self {
            table: HashMap::new(),
            lru: LinkedList::new(),
        }
    }

    fn lookup(&mut self, key: &K) -> Option<&LinkedNode<V>> {
        let node = self.table.get(key);
        if let Some(l) = node {
            LinkedList::remove_node(l.clone());
            self.lru.push_node::<true>(l.clone());
        }

        node
    }

    fn add(&mut self, key: K, value: V) -> LinkedNode<V> {
        let node = self.lru.push_front(value);
        self.table.insert(key, node.clone());
        node
    }

    fn value<E: FnMut(&V)>(node: LinkedNode<V>, mut execute: E) {
        let value = &node.borrow().value;
        if let Some(v) = value {
            execute(v);
        }
    }
}

impl<K, V> Default for LRUCache<K, V> where K: Eq + Hash {
    fn default() -> Self {
        LRUCache::new()
    }
}

type SSTable = Table<file::ReadableFile>;
type TableLRUCache = LRUCache<u64, SSTable>;

struct ShardedLRUCache {
    shard: [TableLRUCache; ShardedLRUCache::NUM_SHARDS],
}

impl ShardedLRUCache {
    const NUM_SHARD_BITS: usize = 4;
    const NUM_SHARDS: usize = 1 << ShardedLRUCache::NUM_SHARD_BITS;

    fn new() -> Self {
        Self {
            shard: Default::default(),
        }
    }

    fn shard(hash: u32) -> usize {
        (hash as usize) >> (32 - ShardedLRUCache::NUM_SHARD_BITS)
    }

    fn lookup(&mut self, key: u64) -> Option<&LinkedNode<SSTable>> {
        let k = coding::encode_fixed64(key);
        let slot = hash(&k, 0);
        self.shard[Self::shard(slot)].lookup(&key)
    }

    fn add(&mut self, key: u64, table: SSTable) -> LinkedNode<SSTable> {
        let k = coding::encode_fixed64(key);
        let slot = hash(&k, 0);
        self.shard[Self::shard(slot)].add(key, table)
    }
}

pub struct TableCache {
    db_name: String,
    options: Options,
    cache: ShardedLRUCache,
}

impl TableCache {
    pub fn new(db_name: &str, options: Options) -> Self {
        Self {
            options,
            db_name: String::from(db_name),
            cache: ShardedLRUCache::new(),
        }
    }

    pub fn get(&mut self, file_number: u64, file_size: u64, key: &InternalKey) -> io::Result<Option<Vec<u8>>> {
        let mut value: io::Result<Option<Vec<u8>>> = Ok(None);
        if let Ok(sst) = self.find_table(file_number, file_size) {
            TableLRUCache::value(sst, |table| {
                value = table.internal_get(key);
            });
        }
        value
    }

    fn find_table(&mut self, file_number: u64, file_size: u64) -> io::Result<LinkedNode<SSTable>> {
        if let Some(sst) = self.cache.lookup(file_number) {
            Ok(sst.clone())
        } else {
            let table_file_name = filename::make_table_file_name(self.db_name.as_str(), file_number);
            let file = file::ReadableFile::open(table_file_name)?;
            let table = Table::open(self.options, file, file_size)?;
            let sst = self.cache.add(file_number, table);
            Ok(sst)
        }
    }

    pub fn iter(&mut self, file_number: u64, file_size: u64) -> Option<Box<dyn LevelIterator>> {
        let mut iter: Option<Box<dyn LevelIterator>> = None;
        if let Ok(sst) = self.find_table(file_number, file_size) {
            TableLRUCache::value(sst, |table| {
                iter = Some(table.iter());
            });
        }
        iter
    }
}

#[cfg(test)]
mod tests {
    use super::{LinkedList, LRUCache};

    #[test]
    fn test_linked_list() {
        let mut list = LinkedList::<i32>::new();
        for i in 1..5 {
            list.push_back(i);
        }

        for i in 6..10 {
            list.push_front(i);
        }
        list.pop_front();
        list.pop_back();

        assert_eq!(list.len(), 6);
        assert_eq!(list.into_iter().collect::<Vec<_>>(), vec![8, 7, 6, 1, 2, 3]);
    }

    #[test]
    fn test_lru_cache() {
        let mut cache = LRUCache::<i32, i32>::new();
        cache.add(400, 499);
        cache.add(300, 399);
        cache.add(200, 299);
        cache.add(100, 199);

        let ans = cache.lookup(&200);
        assert!(ans.is_some());
        if let Some(node) = ans {
            LRUCache::<i32, i32>::value(node.clone(), |v| {
                assert_eq!(*v, 299);
            });
        }
        assert_eq!(cache.lru.into_iter().collect::<Vec<_>>(), vec![299, 199, 399, 499]);
    }
}
