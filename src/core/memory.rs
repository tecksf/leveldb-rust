use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use rand::Rng;
use crate::core::batch::WriteBatch;
use crate::core::format;
use crate::core::format::{LookupKey, ValueType};
use crate::utils::coding;

const MAX_HEIGHT: usize = 12;

enum Link<T> {
    Ptr(Rc<RefCell<T>>),
    Nil,
}

struct Node<T> {
    key: T,
    next: Vec<Link<Node<T>>>,
}

type LinkedNode<T> = Link<Node<T>>;

impl<T> Clone for LinkedNode<T> {
    fn clone(&self) -> Self {
        match self {
            Link::Ptr(p) => Link::Ptr(p.clone()),
            _ => Link::Nil
        }
    }
}

impl<T> Node<T> {
    fn new(key: T, height: usize) -> Self {
        Self { key, next: vec![Link::Nil; height] }
    }

    fn set_next(&mut self, level: usize, node: LinkedNode<T>) {
        self.next[level] = node;
    }

    fn get_next(&self, level: usize) -> LinkedNode<T> {
        self.next[level].clone()
    }
}

struct SkipList<T> {
    head: LinkedNode<T>,
}

impl<T> SkipList<T> where T: Default + Ord {
    pub fn new() -> Self {
        let root = Rc::new(RefCell::new(Node::new(T::default(), MAX_HEIGHT)));
        for i in 0..MAX_HEIGHT {
            root.borrow_mut().set_next(i, Link::Nil);
        }

        Self { head: Link::Ptr(root) }
    }

    pub fn insert(&self, key: T) {
        let mut prev = vec![Link::Nil; MAX_HEIGHT];
        if let Link::Ptr(_) = self.find(&key, prev.as_mut_slice()) {
            return;
        }

        let height = SkipList::<T>::get_random_height();
        let new_node = Rc::new(RefCell::new(Node::<T>::new(key, height)));
        for i in 0..height {
            if let Link::Ptr(ptr) = &prev[i] {
                new_node.borrow_mut().set_next(i, ptr.borrow().get_next(i));
                ptr.borrow_mut().set_next(i, Link::Ptr(new_node.clone()));
            }
        }
    }

    pub fn contains(&self, key: &T) -> bool {
        if let Link::Ptr(_) = self.find(key, &mut []) {
            return true;
        }
        false
    }

    fn get_random_height() -> usize {
        let mut random = rand::thread_rng();
        let mut height: usize = 1;
        while height < MAX_HEIGHT && random.gen::<f64>() < 0.5 {
            height += 1;
        }
        height
    }

    fn find(&self, key: &T, prev: &mut [LinkedNode<T>]) -> LinkedNode<T> {
        let mut current: LinkedNode<T> = self.head.clone();

        for index in (0..MAX_HEIGHT).rev() {
            while let Link::Ptr(rc1) = &current {
                let target = rc1.borrow().get_next(index);
                match &target {
                    Link::Ptr(rc2)
                    if rc2.borrow().key.cmp(key).is_le() => {
                        current = target;
                    }
                    _ => break
                }
            }
            if index < prev.len() {
                prev[index] = current.clone();
            }
        }

        if let Link::Ptr(rc) = &current {
            if key.cmp(&rc.borrow().key) == Ordering::Equal {
                return current.clone();
            }
        }
        Link::Nil
    }
}

pub struct SkipListIterator<'a, T> {
    current: LinkedNode<T>,
    marker: PhantomData<&'a T>,
}

impl<'a, T> Iterator for SkipListIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current.clone();
        if let Link::Ptr(rc) = current {
            let next = rc.borrow().get_next(0);
            self.current = next;
            unsafe {
                let node = &*rc.as_ptr();
                return Some(&node.key);
            }
        }
        None
    }
}

impl<'a, T> IntoIterator for &'a SkipList<T> {
    type Item = &'a T;
    type IntoIter = SkipListIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        let mut start: LinkedNode<T> = Link::Nil;
        if let Link::Ptr(rc) = &self.head {
            start = rc.borrow().get_next(0);
        }

        SkipListIterator {
            current: start,
            marker: Default::default(),
        }
    }
}

pub type MemoryTableIterator<'a> = SkipListIterator<'a, LookupKey>;

pub struct MemoryTable {
    table: SkipList<LookupKey>,
    usage: Cell<usize>,
    refs: Cell<u32>,
}

unsafe impl Send for MemoryTable {}
unsafe impl Sync for MemoryTable {}

impl MemoryTable {
    pub fn new() -> Self {
        Self {
            table: SkipList::new(),
            usage: Cell::new(0),
            refs: Cell::new(0),
        }
    }

    pub fn insert(&self, batch: &WriteBatch) {
        for (sequence_number, key, value) in batch {
            let value_type = if value.len() == 0 { ValueType::Deletion } else { ValueType::Insertion };
            self.add(sequence_number, value_type, key, value);
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.usage.get()
    }

    pub fn iter(&self) -> MemoryTableIterator {
        self.table.into_iter()
    }

    fn add(&self, sequence_number: u64, value_type: ValueType, key: &[u8], value: &[u8]) {
        let internal_key_size = key.len() + 8;
        let encoded_length = internal_key_size
            + coding::get_variant_length(internal_key_size as u64)
            + coding::get_variant_length(value.len() as u64)
            + value.len();
        let mut buf = Vec::<u8>::with_capacity(encoded_length);

        coding::put_variant32_into_vec(&mut buf, internal_key_size as u32);
        buf.extend_from_slice(key);

        coding::put_fixed64_into_vec(&mut buf, format::pack_sequence_and_type(sequence_number, value_type));

        coding::put_variant32_into_vec(&mut buf, value.len() as u32);
        buf.extend_from_slice(value);

        self.table.insert(LookupKey::from(buf));
        self.usage.set(self.usage.get() + encoded_length);
    }

    pub fn get(&self, lookup_key: &LookupKey) -> io::Result<Option<Vec<u8>>> {
        let mut prev = vec![Link::Nil; MAX_HEIGHT];
        let link_node = match self.table.find(&lookup_key, prev.as_mut_slice()) {
            Link::Ptr(p) => Link::Ptr(p),
            Link::Nil => {
                if let Link::Ptr(p1) = &prev[0] {
                    if let Link::Ptr(p2) = p1.borrow().get_next(0) {
                        Link::Ptr(p2)
                    } else {
                        Link::Nil
                    }
                } else {
                    Link::Nil
                }
            }
        };

        if let Link::Ptr(ptr) = link_node {
            let seek_key = &ptr.borrow().key;
            let key1 = seek_key.extract_internal_key();
            let key2 = lookup_key.extract_internal_key();
            if key1.extract_user_key() == key2.extract_user_key() {
                return if key1.extract_value_type() == ValueType::Insertion {
                    Ok(Some(Vec::from(seek_key.get_raw_value())))
                } else {
                    Ok(None)
                };
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, ""))
    }
}

#[cfg(test)]
mod tests {
    use crate::core::format::{LookupKey, ValueType};
    use crate::core::memory::{MemoryTable, SkipList};

    #[test]
    fn test_skip_list_insert_and_delete() {
        let list = SkipList::<i32>::new();
        let data = [1, 34, 21, 6, 13, 4, 11, 39, 25];
        for n in data {
            list.insert(n);
        }
        assert_eq!(list.contains(&11), true);
        assert_eq!(list.contains(&25), true);
        assert_eq!(list.contains(&19), false);

        let mut result = Vec::with_capacity(data.len());
        for &n in &list {
            result.push(n);
        }
        assert_eq!(result, vec![1, 4, 6, 11, 13, 21, 25, 34, 39]);
    }

    #[test]
    fn test_memory_table_set_and_get() {
        let data: [(u64, ValueType, &str, &str); 6] = [
            (1, ValueType::Insertion, "C++", "100"),
            (2, ValueType::Insertion, "Rust", "200"),
            (3, ValueType::Insertion, "Java", "300"),
            (4, ValueType::Insertion, "Python", "400"),
            (5, ValueType::Insertion, "Python", "500"),
            (6, ValueType::Deletion, "Java", ""),
        ];

        let table = MemoryTable::new();
        for (sequence, value_type, key, value) in data {
            table.add(sequence, value_type, key.as_bytes(), value.as_bytes());
        }

        let result = table.iter().map(|lookup_key| {
            let key = String::from_utf8_lossy(lookup_key.get_raw_key()).to_string();
            let value = String::from_utf8_lossy(lookup_key.get_raw_value()).to_string();
            (key, value)
        }).collect::<Vec<(String, String)>>();

        let expected = vec![
            ("C++", "100"),
            ("Java", ""),
            ("Java", "300"),
            ("Python", "500"),
            ("Python", "400"),
            ("Rust", "200"),
        ];

        for i in 0..expected.len() {
            assert_eq!(result[i].0, expected[i].0);
            assert_eq!(result[i].1, expected[i].1);
        }

        for (exist_key, value) in [("C++", "100"), ("Rust", "200"), ("Python", "500")] {
            let lookup_key = LookupKey::new(exist_key, 99);
            let result = table.get(&lookup_key);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(Vec::from(value)));
        }

        for not_exist_key in ["Kotlin", "Scala"] {
            let lookup_key = LookupKey::new(not_exist_key, 99);
            let result = table.get(&lookup_key);
            assert!(result.is_err());
        }

        for deleted_key in ["Java"] {
            let lookup_key = LookupKey::new(deleted_key, 6);
            let result = table.get(&lookup_key);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), None);
        }
    }
}
