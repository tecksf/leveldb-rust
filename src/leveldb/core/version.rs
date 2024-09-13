use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::rc::Rc;
use crate::leveldb::core::format;
use crate::leveldb::core::format::{InternalKey, UserKey};
use crate::leveldb::logs;

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
        if result?.is_eq() {
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

fn total_file_size(files: &Vec<Rc<FileMetaData>>) -> u64 {
    let mut sum: u64 = 0;
    for file in files {
        sum += file.file_size;
    }
    sum
}

pub struct Version {
    refs: Cell<i32>,
    files: [Vec<Rc<FileMetaData>>; logs::NUM_LEVELS],

    // seek compaction
    file_to_compact: RefCell<Option<Rc<FileMetaData>>>,
    file_to_compact_level: Cell<usize>,

    // size compaction
    compaction_score: f64,
    compaction_level: usize,
}

impl Version {
    pub fn new() -> Self {
        Self {
            refs: Cell::new(0),
            files: Default::default(),
            file_to_compact: RefCell::new(None),
            file_to_compact_level: Cell::new(0),
            compaction_level: 0,
            compaction_score: -1.0,
        }
    }

    fn after_file(user_key: &UserKey, meta: &FileMetaData) -> bool {
        user_key.cmp(&meta.largest.extract_user_key()) == Ordering::Greater
    }

    fn before_file(user_key: &UserKey, meta: &FileMetaData) -> bool {
        user_key.cmp(&meta.smallest.extract_user_key()) == Ordering::Less
    }

    pub fn pick_level_for_memory_table(&self, smallest: &UserKey, largest: &UserKey) -> usize {
        let mut level = 0;
        if !self.overlap_in_level(0, smallest, largest) {
            let small_key = InternalKey::restore(smallest, format::MAX_SEQUENCE_NUMBER, format::INSERTION_TYPE_FOR_SEEK);
            let large_key = InternalKey::restore(smallest, 0, format::ValueType::Insertion);

            while level < logs::MAX_MEMORY_COMPACT_LEVEL {
                if self.overlap_in_level(level + 1, smallest, largest) {
                    break;
                }

                if level + 2 < logs::NUM_LEVELS {
                    let overlaps = self.get_over_lapping_inputs(level + 2, &small_key, &large_key);
                    if total_file_size(&overlaps) > 2 * 1024 * 1024 {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    fn overlap_in_level(&self, level: usize, smallest: &UserKey, largest: &UserKey) -> bool {
        let files = &self.files[level];
        if level == 0 {
            let disjoint = files.iter().all(|f| {
                Self::after_file(smallest, f) || Self::before_file(largest, f)
            });
            return !disjoint;
        }

        let small_key = InternalKey::restore(smallest, format::MAX_SEQUENCE_NUMBER, format::INSERTION_TYPE_FOR_SEEK);
        let index = files.binary_search_by(
            |meta| meta.largest.cmp(&small_key)).unwrap_or_else(|index| index);

        if index >= files.len() {
            return false;
        }

        !Self::before_file(largest, &files[index])
    }

    fn get_over_lapping_inputs(&self, level: usize, smallest: &InternalKey, largest: &InternalKey) -> Vec<Rc<FileMetaData>> {
        let mut overlaps = Vec::<Rc<FileMetaData>>::new();
        let mut begin = smallest.extract_user_key();
        let mut end = largest.extract_user_key();

        let mut i = 0;
        while i < self.files[level].len() {
            let file = &self.files[level][i];
            let start = file.smallest.extract_user_key();
            let finish = file.largest.extract_user_key();
            i += 1;
            if finish < begin || start > end {
                // no overlap, skip it
            } else {
                overlaps.push(file.clone());
                if level == 0 {
                    if start < begin {
                        begin = start;
                        overlaps.clear();
                        i = 0;
                    } else if finish > end {
                        end = finish;
                        overlaps.clear();
                        i = 0;
                    }
                }
            }
        }
        overlaps
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::leveldb::core::format::{InternalKey, UserKey, ValueType};
    use super::{FileMetaData, Version};

    fn create_file_meta<T: AsRef<[u8]>>(number: u64, key1: T, key2: T) -> FileMetaData {
        let mut meta = FileMetaData::new();
        meta.number = number;
        meta.file_size = 10 << 8;
        meta.smallest = InternalKey::restore(key1, 9, ValueType::Insertion);
        meta.largest = InternalKey::restore(key2, 10, ValueType::Insertion);
        meta
    }

    fn assert_file_range<T: AsRef<[u8]>>(file: &Rc<FileMetaData>, begin: T, end: T) {
        assert_eq!(file.smallest.extract_user_key().as_ref(), begin.as_ref());
        assert_eq!(file.largest.extract_user_key().as_ref(), end.as_ref());
    }

    #[test]
    fn test_file_level_seek() {
        let keys: [(&str, &str); 9] = [
            ("000", "100"),
            ("200", "300"),
            ("260", "320"),
            ("400", "500"),
            ("100", "360"),
            ("400", "550"),
            ("600", "700"),
            ("200", "400"),
            ("500", "800"),
        ];
        let mut version = Version::new();
        for i in 0..4usize {
            version.files[0].push(Rc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
        }

        for i in 4..7usize {
            version.files[1].push(Rc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
        }

        for i in 7..9usize {
            version.files[2].push(Rc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
        }

        let key1 = InternalKey::restore("210", 9, ValueType::Insertion);
        let key2 = InternalKey::restore("230", 9, ValueType::Insertion);
        let files = version.get_over_lapping_inputs(0, &key1, &key2);
        assert_eq!(files.len(), 2);
        assert_file_range(&files[0], "200", "300");
        assert_file_range(&files[1], "260", "320");

        assert_eq!(version.pick_level_for_memory_table(&UserKey::new("350"), &UserKey::new("450")), 0);
        assert_eq!(version.pick_level_for_memory_table(&UserKey::new("120"), &UserKey::new("150")), 0);
        assert_eq!(version.pick_level_for_memory_table(&UserKey::new("370"), &UserKey::new("380")), 1);
    }
}
