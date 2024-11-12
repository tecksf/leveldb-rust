use std::collections::HashMap;
use std::hash::Hash;
use std::{io, ptr};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use crate::core::format::InternalKey;
use crate::core::iterator::LevelIterator;
use crate::Options;
use crate::logs::{file, filename};
use crate::table::table::Table;
use crate::utils::coding;
use crate::utils::common::hash;

type Link<T> = *mut Node<T>;

struct Node<T> {
    value: T,
    prev: Link<T>,
    next: Link<T>,
}

impl<T> Node<T> {
    fn new_link(value: T) -> Link<T> {
        let link = Box::into_raw(Box::new(Node {
            value,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));

        unsafe {
            (*link).prev = link;
            (*link).next = link;
        }
        link
    }

    fn release(link: Link<T>) {
        unsafe {
            let _ = Box::from_raw(link);
        }
    }
}

struct LinkedList<T> {
    head: Link<T>,
    len: usize,
}

unsafe impl<T> Send for LinkedList<T> {}

impl<T> LinkedList<T> {
    fn new() -> Self {
        LinkedList {
            head: ptr::null_mut(),
            len: 0,
        }
    }

    fn push_back(&mut self, link: Link<T>) {
        self.len += 1;
        unsafe {
            if self.head.is_null() {
                self.head = link;
            } else {
                let tail = (*self.head).prev;
                (*link).next = (*tail).next;
                (*tail).next = link;
                (*(*link).next).prev = link;
                (*link).prev = tail;
            }
        }
    }

    fn pop_front(&mut self) -> Option<Link<T>> {
        if self.head.is_null() {
            return None;
        }

        unsafe {
            let link = self.head;
            self.pop(self.head);
            Some(link)
        }
    }

    fn pop(&mut self, link: Link<T>) {
        self.len -= 1;
        unsafe {
            if self.head == link {
                self.head = (*link).next;
            }

            (*(*link).next).prev = (*link).prev;
            (*(*link).prev).next = (*link).next;

            if self.len == 0 {
                self.head = ptr::null_mut();
            }
        }
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        while let Some(link) = self.pop_front() {
            Node::release(link);
        }
    }
}

struct LinkedListIterator<'a, T> {
    start: Link<T>,
    len: usize,
    marker: PhantomData<&'a T>,
}

impl<'a, T> Iterator for LinkedListIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }

        unsafe {
            let link = self.start;
            self.start = (*self.start).prev;
            self.len -= 1;
            Some(&(*link).value)
        }
    }
}

impl<'a, T> IntoIterator for &'a LinkedList<T> {
    type Item = &'a T;
    type IntoIter = LinkedListIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let mut start = self.head;
            if !start.is_null() {
                start = (*start).prev;
            }
            LinkedListIterator {
                start,
                len: self.len,
                marker: PhantomData,
            }
        }
    }
}

struct LRUCache<K, V> {
    table: HashMap<K, Link<V>>,
    lru: LinkedList<V>,
    capacity: usize,
}

unsafe impl<K, V> Send for LRUCache<K, V> {}

impl<K: Eq + Hash, V> LRUCache<K, V> {
    fn new(cap: usize) -> Self {
        let capacity = if cap < 8 { 8 } else { cap };
        Self {
            table: HashMap::with_capacity(capacity),
            lru: LinkedList::new(),
            capacity,
        }
    }

    fn lookup(&mut self, key: &K) -> Option<&V> {
        let &link = self.table.get(key)?;
        unsafe {
            self.lru.pop(link);
            self.lru.push_back(link);
            Some(&(*link).value)
        }
    }

    fn add(&mut self, key: K, value: V) {
        if let Some(&link) = self.table.get(&key) {
            unsafe {
                (*link).value = value;
                self.lru.pop(link);
                self.lru.push_back(link);
            }
            return;
        }

        if self.table.len() == self.capacity {
            let link = self.lru.pop_front().unwrap();
            self.table.retain(|_, v| *v != link);
            Node::release(link);
        }

        let link = Node::new_link(value);
        self.table.insert(key, link);
        self.lru.push_back(link);
    }
}

impl<K, V> Default for LRUCache<K, V> where K: Eq + Hash {
    fn default() -> Self {
        LRUCache::new(16)
    }
}

type SSTable = Arc<Table<file::ReadableFile>>;
type TableLRUCache = LRUCache<u64, SSTable>;

struct ShardedLRUCache {
    shard: [Mutex<TableLRUCache>; ShardedLRUCache::NUM_SHARDS],
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

    fn lookup(&self, key: u64) -> Option<SSTable> {
        let k = coding::encode_fixed64(key);
        let slot = hash(&k, 0);
        self.shard[Self::shard(slot)].lock().unwrap().lookup(&key).map(|t| t.clone())
    }

    fn add(&self, key: u64, table: SSTable) {
        let k = coding::encode_fixed64(key);
        let slot = hash(&k, 0);
        self.shard[Self::shard(slot)].lock().unwrap().add(key, table);
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

    pub fn get(&self, file_number: u64, file_size: u64, key: &InternalKey) -> io::Result<Option<Vec<u8>>> {
        let mut value: io::Result<Option<Vec<u8>>> = Ok(None);
        if let Ok(table) = self.find_table(file_number, file_size) {
            value = table.internal_get(key);
        }
        value
    }

    fn find_table(&self, file_number: u64, file_size: u64) -> io::Result<SSTable> {
        if let Some(sst) = self.cache.lookup(file_number) {
            Ok(sst.clone())
        } else {
            let table_file_name = filename::make_table_file_name(self.db_name.as_str(), file_number);
            let file = file::ReadableFile::open(table_file_name)?;
            let table = Arc::new(Table::open(self.options, file, file_size)?);
            self.cache.add(file_number, table.clone());
            Ok(table)
        }
    }

    pub fn iter(&self, file_number: u64, file_size: u64) -> Option<Box<dyn LevelIterator>> {
        let mut iter: Option<Box<dyn LevelIterator>> = None;
        if let Ok(table) = self.find_table(file_number, file_size) {
            iter = Some(table.iter());
        }
        iter
    }
}

#[cfg(test)]
mod tests {
    use super::LRUCache;

    #[test]
    fn test_lru_cache() {
        let mut cache = LRUCache::<i32, i32>::new(16);
        for n in 1..=16 {
            cache.add(n, n);
        }
        assert_eq!(cache.lru.into_iter().map(|x| *x).collect::<Vec<_>>(), (1..=16).rev().collect::<Vec<i32>>());

        cache.add(17, 17);
        cache.add(18, 18);
        assert_eq!(cache.lru.into_iter().map(|x| *x).collect::<Vec<_>>(), (3..=18).rev().collect::<Vec<i32>>());

        cache.lookup(&5);
        cache.lookup(&7);
        cache.lookup(&9);
        assert_eq!(cache.lru.into_iter().map(|x| *x).take(3).collect::<Vec<i32>>(), vec![9, 7, 5]);
    }
}
