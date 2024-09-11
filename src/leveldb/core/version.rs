use std::cell::Cell;
use std::cmp::Ordering;
use crate::leveldb::core::format::InternalKey;

#[derive(Clone, Eq)]
pub struct FileMetaData {
    refs: Cell<u32>,
    pub allowed_seeks: Cell<i32>,
    pub number: u64,
    pub file_size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
}

impl FileMetaData {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for FileMetaData {
    fn default() -> Self {
        Self {
            refs: Cell::new(0),
            allowed_seeks: Cell::new(1 << 30),
            number: 0,
            file_size: 0,
            smallest: InternalKey::default(),
            largest: InternalKey::default(),
        }
    }
}

impl PartialEq for FileMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number && self.smallest == other.smallest
    }
}

impl PartialOrd for FileMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let result = self.smallest.partial_cmp(&other.smallest);
        if result.is_some() && result.unwrap().is_eq() {
            return self.number.partial_cmp(&other.number);
        }
        result
    }
}

impl Ord for FileMetaData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
