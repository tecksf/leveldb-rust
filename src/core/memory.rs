use std::io;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use rand::Rng;
use typed_arena::Arena;
use crate::core::batch::WriteBatch;
use crate::core::format;
use crate::core::format::{LookupKey, ValueType};
use crate::utils::coding;

const MAX_HEIGHT: usize = 12;

struct Node<T> {
    key: T,
    next: [AtomicPtr<Node<T>>; MAX_HEIGHT],
}

impl<T> Node<T> {
    fn new(key: T) -> Self {
        Self {
            key,
            next: Default::default(),
        }
    }

    fn get_next(&self, level: usize) -> *mut Node<T> {
        self.next[level].load(std::sync::atomic::Ordering::Acquire)
    }

    fn set_next(&mut self, level: usize, node: *mut Node<T>) {
        self.next[level].store(node, std::sync::atomic::Ordering::Release);
    }

    fn get_next_without_barrier(&self, level: usize) -> *mut Node<T> {
        self.next[level].load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_next_without_barrier(&mut self, level: usize, node: *mut Node<T>) {
        self.next[level].store(node, std::sync::atomic::Ordering::Relaxed);
    }
}

struct SkipList<T> {
    head: *mut Node<T>,
    arena: Arena<Node<T>>,
}

impl<T> SkipList<T> where T: Default + Ord {
    fn new() -> Self {
        let arena = Arena::new();
        let head = arena.alloc(Node::new(T::default()));
        Self {
            head,
            arena,
        }
    }

    fn new_node(&self, key: T) -> &mut Node<T> {
        self.arena.alloc(Node::new(key))
    }

    fn insert(&self, key: T) {
        let mut prev: [*mut Node<T>; MAX_HEIGHT] = [std::ptr::null_mut(); MAX_HEIGHT];
        if self.find(&key, &mut prev) {
            return;
        }

        let height = Self::get_random_height();
        let node = self.new_node(key);
        for level in 0..height {
            unsafe {
                node.set_next_without_barrier(level, (*prev[level]).get_next_without_barrier(level));
                (*prev[level]).set_next(level, node);
            }
        }
    }

    pub fn contains(&self, key: &T) -> bool {
        let mut prev: [*mut Node<T>; MAX_HEIGHT] = [std::ptr::null_mut(); MAX_HEIGHT];
        self.find(key, &mut prev)
    }

    fn find(&self, key: &T, prev: &mut [*mut Node<T>; MAX_HEIGHT]) -> bool {
        let mut node = self.head;
        let mut level = MAX_HEIGHT - 1;
        unsafe {
            loop {
                let next = (*node).get_next(level);
                if !next.is_null() && key.cmp(&(*next).key).is_ge() {
                    node = next;
                } else {
                    prev[level] = node;
                    if level == 0 {
                        break;
                    }
                    level -= 1;
                }
            }
            !prev[0].is_null() && key.cmp(&(*prev[0]).key).is_eq()
        }
    }

    fn get_random_height() -> usize {
        let mut random = rand::thread_rng();
        let mut height: usize = 1;
        while height < MAX_HEIGHT && random.gen::<f64>() < 0.5 {
            height += 1;
        }
        height
    }
}

pub struct SkipListIterator<'a, T> {
    next: *mut Node<T>,
    marker: PhantomData<&'a T>,
}

impl<'a, T> Iterator for SkipListIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next;
        if !current.is_null() {
            unsafe {
                self.next = (*current).get_next(0);
                return Some(&(*current).key);
            }
        }
        None
    }
}

impl<'a, T> IntoIterator for &'a SkipList<T> {
    type Item = &'a T;
    type IntoIter = SkipListIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        let mut next: *mut Node<T> = std::ptr::null_mut();
        if !self.head.is_null() {
            unsafe {
                next = (*self.head).get_next(0);
            }
        }
        Self::IntoIter {
            next,
            marker: PhantomData,
        }
    }
}

pub type MemoryTableIterator<'a> = SkipListIterator<'a, LookupKey>;

pub struct MemoryTable {
    table: SkipList<LookupKey>,
    usage: AtomicUsize,
}

unsafe impl Send for MemoryTable {}
unsafe impl Sync for MemoryTable {}

impl MemoryTable {
    pub fn new() -> Self {
        Self {
            table: SkipList::new(),
            usage: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, batch: &WriteBatch) {
        for (sequence_number, key, value) in batch {
            let value_type = if value.len() == 0 { ValueType::Deletion } else { ValueType::Insertion };
            self.add(sequence_number, value_type, key, value);
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.usage.load(std::sync::atomic::Ordering::Relaxed)
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
        self.usage.fetch_add(encoded_length, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get(&self, lookup_key: &LookupKey) -> io::Result<Option<Vec<u8>>> {
        let mut prev = [std::ptr::null_mut(); MAX_HEIGHT];
        let mut ptr: *mut Node<LookupKey> = std::ptr::null_mut();
        if self.table.find(&lookup_key, &mut prev) {
            ptr = prev[0];
        } else if !prev[0].is_null() {
            unsafe { ptr = (*prev[0]).get_next(0); }
        }

        if !ptr.is_null() {
            let seek_key: &LookupKey;
            unsafe { seek_key = &(*ptr).key; }
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
