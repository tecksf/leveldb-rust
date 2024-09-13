use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::rc::Rc;
use crate::leveldb::core::format;
use crate::leveldb::core::format::{InternalKey, UserKey};
use crate::leveldb::logs;
use crate::leveldb::utils::coding;

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

#[repr(u32)]
enum Tag {
    Comparator,
    LogNumber,
    NextFileNumber,
    LastSequence,
    CompactPointer,
    DeletedFile,
    NewFile,
    PrevLogNumber,
    Unknown,
}

impl From<u32> for Tag {
    fn from(num: u32) -> Self {
        match num {
            0 => Tag::Comparator,
            1 => Tag::LogNumber,
            2 => Tag::NextFileNumber,
            3 => Tag::LastSequence,
            4 => Tag::CompactPointer,
            5 => Tag::DeletedFile,
            6 => Tag::NewFile,
            7 => Tag::PrevLogNumber,
            _ => Tag::Unknown,
        }
    }
}

pub struct VersionEdit {
    comparator: Option<String>,
    log_number: Option<u64>,
    prev_log_number: Option<u64>,
    next_file_number: Option<u64>,
    last_sequence: Option<u64>,
    compact_pointers: Vec<(u8, Vec<u8>)>,
    deleted_files: BTreeSet<(u8, u64)>,
    new_files: Vec<(usize, FileMetaData)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            comparator: None,
            log_number: None,
            prev_log_number: None,
            next_file_number: None,
            last_sequence: None,
            compact_pointers: Vec::new(),
            deleted_files: BTreeSet::new(),
            new_files: Vec::new(),
        }
    }

    pub fn make_from<T: AsRef<[u8]>>(record: T) -> Result<Self, String> {
        let mut edit = VersionEdit::new();
        edit.decode_from(record.as_ref())?;
        Ok(edit)
    }

    pub fn add_file(&mut self, level: usize, meta: FileMetaData) {
        self.new_files.push((level, meta));
    }

    pub fn remove_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files.insert((level as u8, file_number));
    }

    pub fn set_comparator_name(&mut self, name: String) {
        self.comparator = Some(name);
    }

    pub fn get_log_number(&self) -> Option<u64> {
        self.log_number
    }

    pub fn set_log_number(&mut self, number: u64) {
        self.log_number = Some(number);
    }

    pub fn set_prev_log_number(&mut self, number: u64) {
        self.prev_log_number = Some(number);
    }

    pub fn set_next_file_number(&mut self, number: u64) {
        self.next_file_number = Some(number);
    }

    pub fn set_last_sequence(&mut self, sequence: u64) {
        self.last_sequence = Some(sequence);
    }

    pub fn set_compact_pointer(&mut self, level: usize, internal_key: &[u8]) {
        self.compact_pointers.push((level as u8, Vec::from(internal_key)));
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut result = Vec::<u8>::new();

        if let Some(comparator) = &self.comparator {
            coding::put_variant32_into_vec(&mut result, Tag::Comparator as u32);
            coding::put_variant32_into_vec(&mut result, comparator.len() as u32);
            result.extend_from_slice(comparator.as_bytes());
        }

        if let Some(log_number) = self.log_number {
            coding::put_variant32_into_vec(&mut result, Tag::LogNumber as u32);
            coding::put_variant64_into_vec(&mut result, log_number);
        }

        if let Some(prev_log_number) = self.prev_log_number {
            coding::put_variant32_into_vec(&mut result, Tag::PrevLogNumber as u32);
            coding::put_variant64_into_vec(&mut result, prev_log_number);
        }

        if let Some(next_file_number) = self.next_file_number {
            coding::put_variant32_into_vec(&mut result, Tag::NextFileNumber as u32);
            coding::put_variant64_into_vec(&mut result, next_file_number);
        }

        if let Some(last_sequence) = self.last_sequence {
            coding::put_variant32_into_vec(&mut result, Tag::LastSequence as u32);
            coding::put_variant64_into_vec(&mut result, last_sequence);
        }

        for (level, internal_key) in &self.compact_pointers {
            coding::put_variant32_into_vec(&mut result, Tag::CompactPointer as u32);
            coding::put_variant32_into_vec(&mut result, *level as u32);
            coding::put_variant32_into_vec(&mut result, internal_key.len() as u32);
            result.extend_from_slice(internal_key.as_slice());
        }

        for (level, file_number) in &self.deleted_files {
            coding::put_variant32_into_vec(&mut result, Tag::DeletedFile as u32);
            coding::put_variant32_into_vec(&mut result, *level as u32);
            coding::put_variant64_into_vec(&mut result, *file_number);
        }

        for (level, meta) in &self.new_files {
            coding::put_variant32_into_vec(&mut result, Tag::NewFile as u32);
            coding::put_variant32_into_vec(&mut result, *level as u32);
            coding::put_variant64_into_vec(&mut result, meta.number);
            coding::put_variant64_into_vec(&mut result, meta.file_size);

            coding::put_variant32_into_vec(&mut result, meta.smallest.len() as u32);
            result.extend_from_slice(meta.smallest.as_ref());

            coding::put_variant32_into_vec(&mut result, meta.largest.len() as u32);
            result.extend_from_slice(meta.largest.as_ref());
        }
        result
    }

    pub fn decode_from(&mut self, record: &[u8]) -> Result<(), String> {
        let mut offset: usize = 0;
        while offset < record.len() {
            let (num, w) = coding::decode_variant32(&record[offset..]);
            let tag: Tag = num.into();
            offset += w as usize;
            match tag {
                Tag::Comparator => {
                    let (comparator, size_width) = coding::get_length_prefixed_slice(&record[offset..]);
                    self.comparator = Some(String::from_utf8_lossy(comparator).to_string());
                    offset += comparator.len() + size_width as usize;
                }
                Tag::LogNumber => {
                    let (log_number, size_width) = coding::decode_variant64(&record[offset..]);
                    self.log_number = Some(log_number);
                    offset += size_width as usize;
                }
                Tag::NextFileNumber => {
                    let (next_file_number, size_width) = coding::decode_variant64(&record[offset..]);
                    self.next_file_number = Some(next_file_number);
                    offset += size_width as usize;
                }
                Tag::LastSequence => {
                    let (sequence, size_width) = coding::decode_variant64(&record[offset..]);
                    self.last_sequence = Some(sequence);
                    offset += size_width as usize;
                }
                Tag::CompactPointer => {
                    let (level, size_width) = coding::decode_variant32(&record[offset..]);
                    offset += size_width as usize;
                    let (internal_key, size_width) = coding::get_length_prefixed_slice(&record[offset..]);
                    offset += internal_key.len() + size_width as usize;
                    self.compact_pointers.push((level as u8, internal_key.to_vec()));
                }
                Tag::DeletedFile => {
                    let (level, size_width) = coding::decode_variant32(&record[offset..]);
                    offset += size_width as usize;
                    let (file_number, size_width) = coding::decode_variant64(&record[offset..]);
                    offset += size_width as usize;
                    self.deleted_files.insert((level as u8, file_number));
                }
                Tag::NewFile => {
                    let mut meta = FileMetaData::new();
                    let mut size_width: u8;
                    let level: u32;

                    (level, size_width) = coding::decode_variant32(&record[offset..]);
                    offset += size_width as usize;

                    (meta.number, size_width) = coding::decode_variant64(&record[offset..]);
                    offset += size_width as usize;

                    (meta.file_size, size_width) = coding::decode_variant64(&record[offset..]);
                    offset += size_width as usize;

                    let mut key: &[u8];
                    (key, size_width) = coding::get_length_prefixed_slice(&record[offset..]);
                    offset += key.len() + size_width as usize;
                    meta.smallest = InternalKey::new(key);

                    (key, size_width) = coding::get_length_prefixed_slice(&record[offset..]);
                    offset += key.len() + size_width as usize;
                    meta.largest = InternalKey::new(key);

                    self.new_files.push((level as usize, meta));
                }
                Tag::PrevLogNumber => {
                    let (log_number, size_width) = coding::decode_variant64(&record[offset..]);
                    self.prev_log_number = Some(log_number);
                    offset += size_width as usize;
                }
                Tag::Unknown => {
                    return Err(String::from("unknown tag"));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::leveldb::core::format::{InternalKey, UserKey, ValueType};
    use super::{FileMetaData, Version, VersionEdit};

    fn create_file_meta<T: AsRef<[u8]>>(number: u64, key1: T, key2: T) -> FileMetaData {
        let mut meta = FileMetaData::new();
        meta.number = number;
        meta.file_size = 10 << 8;
        meta.smallest = InternalKey::restore(key1, 9, ValueType::Insertion);
        meta.largest = InternalKey::restore(key2, 10, ValueType::Insertion);
        meta
    }

    fn create_version_edit() -> VersionEdit {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(String::from("InternalKeyComparator"));
        edit.set_last_sequence(123);
        edit.set_prev_log_number(99);
        edit.set_log_number(100);
        edit.set_next_file_number(101);

        let mut f1 = FileMetaData::new();
        f1.number = 1;
        f1.file_size = 10 << 8;
        f1.smallest = InternalKey::restore("abc", 123, ValueType::Insertion);
        f1.largest = InternalKey::restore("abl", 124, ValueType::Insertion);

        let mut f2 = FileMetaData::new();
        f2.number = 2;
        f2.file_size = 10 << 8;
        f2.smallest = InternalKey::restore("adg", 240, ValueType::Insertion);
        f2.largest = InternalKey::restore("adz", 241, ValueType::Insertion);

        edit.add_file(0, f1);
        edit.add_file(1, f2);
        edit.remove_file(3, 99);
        edit.remove_file(4, 98);
        edit.remove_file(5, 97);
        edit
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

    #[test]
    fn test_version_edit_encoded() {
        let edit1 = create_version_edit();
        let result = edit1.encode();
        let new_edit = VersionEdit::make_from(result);
        assert!(new_edit.is_ok());

        let edit2 = new_edit.unwrap();
        assert_eq!(edit2.comparator, Some(String::from("InternalKeyComparator")));
        assert_eq!(edit2.last_sequence, Some(123));
        assert_eq!(edit2.prev_log_number, Some(99));
        assert_eq!(edit2.log_number, Some(100));
        assert_eq!(edit2.next_file_number, Some(101));
        assert_eq!(edit2.new_files.len(), 2);
        assert_eq!(edit2.deleted_files.len(), 3);
    }
}
