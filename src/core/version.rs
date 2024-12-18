use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::{fs, io};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicI32, AtomicU32};
use crate::core::format;
use crate::core::format::{Comparator, InternalKey, LookupKey, UserKey};
use crate::{logs, Options};
use crate::core::cache::TableCache;
use crate::core::compaction::{threshold, Compaction};
use crate::core::iterator::{IteratorGen, LevelIterator, MergingIterator, TwoLevelIterator};
use crate::logs::{file, filename, wal};
use crate::logs::filename::FileType;
use crate::utils::coding;


pub struct FileMetaData {
    refs: AtomicU32,
    pub allowed_seeks: AtomicI32,
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
            refs: AtomicU32::new(0),
            allowed_seeks: AtomicI32::new(1 << 30),
            number: 0,
            file_size: 0,
            smallest: InternalKey::default(),
            largest: InternalKey::default(),
        }
    }
}

impl Clone for FileMetaData {
    fn clone(&self) -> Self {
        Self {
            refs: AtomicU32::new(self.refs.load(std::sync::atomic::Ordering::Relaxed)),
            allowed_seeks: AtomicI32::new(self.allowed_seeks.load(std::sync::atomic::Ordering::Relaxed)),
            number: self.number,
            file_size: self.file_size,
            smallest: self.smallest.clone(),
            largest: self.largest.clone(),
        }
    }
}

impl PartialEq for FileMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number && self.smallest == other.smallest
    }
}

impl Eq for FileMetaData {}

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

#[derive(Default)]
pub struct GetStatistics {
    seek_file: Option<Arc<FileMetaData>>,
    seek_file_level: usize,
}

struct SeekCompactionInfo {
    level: usize,
    file: Arc<FileMetaData>,
}

pub struct Version {
    files: [Vec<Arc<FileMetaData>>; logs::NUM_LEVELS],

    // seek compaction
    file_to_compact: RwLock<Option<SeekCompactionInfo>>,

    // size compaction
    compaction_score: f64,
    compaction_level: usize,
    table_cache: Arc<TableCache>,
}

impl Version {
    pub fn new(version_set: &VersionSet) -> Self {
        Self {
            files: Default::default(),
            file_to_compact: RwLock::new(None),
            compaction_level: 0,
            compaction_score: -1.0,
            table_cache: version_set.table_cache.clone(),
        }
    }

    fn after_file(user_key: &UserKey, meta: &FileMetaData) -> bool {
        user_key.cmp(&meta.largest.extract_user_key()) == Ordering::Greater
    }

    fn before_file(user_key: &UserKey, meta: &FileMetaData) -> bool {
        user_key.cmp(&meta.smallest.extract_user_key()) == Ordering::Less
    }

    pub fn update_statistics(&self, stats: &GetStatistics) -> bool {
        if let Some(seek_file) = &stats.seek_file {
            let count = seek_file.allowed_seeks.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            if count - 1 <= 0 {
                if let Ok(mut guard) = self.file_to_compact.write() {
                    if guard.is_none() {
                        let seek = SeekCompactionInfo {
                            file: seek_file.clone(),
                            level: stats.seek_file_level,
                        };
                        *guard = Some(seek);
                    }
                }
            }
            return true;
        }
        false
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (io::Result<Vec<u8>>, GetStatistics) {
        let internal_key = lookup_key.extract_internal_key();
        let mut result: io::Result<Vec<u8>> = Err(io::Error::new(io::ErrorKind::NotFound, ""));
        let mut stats = GetStatistics::default();
        let mut last_file_read: Option<Arc<FileMetaData>> = None;
        let mut last_file_read_level: usize = 0;

        let seek_wrapper = |level: usize, file: Arc<FileMetaData>| -> bool {
            if stats.seek_file.is_none() && last_file_read.is_some() {
                stats.seek_file = last_file_read.clone();
                stats.seek_file_level = last_file_read_level;
            }

            last_file_read = Some(file.clone());
            last_file_read_level = level;

            return match self.table_cache.get(file.number, file.file_size, &internal_key) {
                Ok(table_result) => {
                    if let Some(value) = table_result {
                        result = Ok(value);
                    };
                    true
                }
                Err(err) => {
                    result = Err(err);
                    false
                }
            };
        };
        self.for_each_overlapping(&internal_key, seek_wrapper);
        (result, stats)
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
                    if threshold::total_file_size(&overlaps) > 2 * 1024 * 1024 {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    pub fn exists_in_nonzero_level(&self, level: usize, user_key: &UserKey) -> bool {
        let files = &self.files[level];
        let seek_key = InternalKey::restore(user_key, format::MAX_SEQUENCE_NUMBER, format::INSERTION_TYPE_FOR_SEEK);
        let index = files.binary_search_by(
            |meta| meta.largest.cmp(&seek_key)).unwrap_or_else(|index| index);

        if index >= files.len() {
            return false;
        }

        user_key.cmp(&files[index].smallest.extract_user_key()).is_ge()
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

    fn get_over_lapping_inputs(&self, level: usize, smallest: &InternalKey, largest: &InternalKey) -> Vec<Arc<FileMetaData>> {
        let mut overlaps = Vec::<Arc<FileMetaData>>::new();
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

    fn for_each_overlapping<T>(&self, internal_key: &InternalKey, mut seek: T)
        where T: FnMut(usize, Arc<FileMetaData>) -> bool
    {
        let user_key = internal_key.extract_user_key();
        let mut tmp = Vec::<Arc<FileMetaData>>::new();
        self.files[0].iter().for_each(|f| {
            if user_key >= f.smallest.extract_user_key() &&
                user_key <= f.largest.extract_user_key() {
                tmp.push(f.clone());
            }
        });

        if !tmp.is_empty() {
            tmp.sort_by(|f1, f2| f2.number.cmp(&f1.number));
            for f in tmp {
                if seek(0, f) {
                    return;
                }
            }
        }

        for level in 1..logs::NUM_LEVELS {
            let files = &self.files[level];
            if files.len() == 0 {
                continue;
            }

            let index = files.binary_search_by(
                |meta| meta.largest.cmp(&internal_key)).unwrap_or_else(|index| index);
            if index < files.len() {
                let f = self.files[level][index].clone();
                if user_key >= f.smallest.extract_user_key() && seek(level, f) {
                    return;
                }
            }
        }
    }
}

pub struct VersionLevelFileIterator {
    index: Cell<usize>,
    files: Vec<Arc<FileMetaData>>,
}

impl VersionLevelFileIterator {
    pub fn new(files: Vec<Arc<FileMetaData>>) -> Self {
        Self {
            index: Cell::new(files.len()),
            files,
        }
    }
}

impl LevelIterator for VersionLevelFileIterator {
    fn is_valid(&self) -> bool {
        self.index.get() < self.files.len()
    }

    fn key(&self) -> Vec<u8> {
        Vec::from(self.files[self.index.get()].largest.as_ref())
    }

    fn value(&self) -> Vec<u8> {
        let mut result = Vec::new();
        coding::put_fixed64_into_vec(&mut result, self.files[self.index.get()].number);
        coding::put_fixed64_into_vec(&mut result, self.files[self.index.get()].file_size);
        result
    }

    fn next(&self) -> bool {
        self.index.set(self.index.get() + 1);
        self.is_valid()
    }

    fn seek(&self, target: &[u8]) -> bool {
        let result = self.files.binary_search_by(|file| {
            file.largest.as_ref().cmp(target)
        });

        self.index.set(result.unwrap_or_else(|n| n));
        self.is_valid()
    }

    fn seek_to_first(&self) {
        self.index.set(0);
    }

    fn seek_to_last(&self) {
        self.index.set(self.files.len());
    }
}

struct FileIteratorGen {
    table_cache: Arc<TableCache>,
}

impl FileIteratorGen {
    fn new(table_cache: Arc<TableCache>) -> Self {
        Self {
            table_cache
        }
    }
}

impl IteratorGen for FileIteratorGen {
    fn gen(&self, data: &[u8]) -> Option<Box<dyn LevelIterator>> {
        if data.len() != 16 {
            return None;
        }

        let file_number = coding::decode_fixed64(data);
        let file_size = coding::decode_fixed64(&data[8..]);
        self.table_cache.iter(file_number, file_size)
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
    compact_pointers: Vec<(u8, InternalKey)>,
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

    pub fn has_updated(&self) -> bool {
        self.deleted_files.len() > 0 || self.new_files.len() > 0
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

    pub fn set_compact_pointer(&mut self, level: usize, key: InternalKey) {
        self.compact_pointers.push((level as u8, key));
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
            result.extend_from_slice(internal_key.as_ref());
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
                    let (key, size_width) = coding::get_length_prefixed_slice(&record[offset..]);
                    offset += key.len() + size_width as usize;
                    self.compact_pointers.push((level as u8, InternalKey::new(key)));
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

#[derive(Default)]
struct LevelFileState {
    deleted_files: BTreeSet<u64>,
    added_files: BTreeSet<Arc<FileMetaData>>,
}

struct VersionBuilder<'a> {
    version: Arc<Version>,
    version_set: &'a mut VersionSet,
    levels: [LevelFileState; logs::NUM_LEVELS],
}

impl<'a> VersionBuilder<'a> {
    fn new(version_set: &'a mut VersionSet, version: Arc<Version>) -> Self {
        Self {
            version,
            version_set,
            levels: Default::default(),
        }
    }

    fn apply(&mut self, edit: &VersionEdit) {
        for (level, internal_key) in &edit.compact_pointers {
            self.version_set.compact_pointers[*level as usize] = internal_key.clone();
        }

        for &(level, file_number) in &edit.deleted_files {
            self.levels[level as usize].deleted_files.insert(file_number);
        }

        for (level, meta) in &edit.new_files {
            let mut file_meta = meta.clone();
            file_meta.refs = AtomicU32::new(1);

            let mut count = (file_meta.file_size / 16384) as i32;
            if count < 100 {
                count = 100;
            }
            file_meta.allowed_seeks.store(count, std::sync::atomic::Ordering::Relaxed);
            self.levels[*level].deleted_files.remove(&file_meta.number);
            self.levels[*level].added_files.insert(Arc::new(file_meta));
        }
    }

    fn generate_new_version(&self) -> Version {
        let mut new_version = Version::new(self.version_set);

        for (level, base_file_metas) in self.version.files.iter().enumerate() {
            let ref added_file_metas = self.levels[level].added_files;
            new_version.files[level].reserve(base_file_metas.len() + added_file_metas.len());

            // base_file_metas are in ascending order,
            // since version.files are from VersionEdit.levels.added_files which is ordered in each generation
            let mut start: usize = 0;
            for added_file in added_file_metas {
                let base_file = &base_file_metas[start..];
                let index = base_file.partition_point(|file| file < added_file);
                for base in &base_file[..index] {
                    self.maybe_add_file(&mut new_version, level, base.clone())
                }
                start += index;
                self.maybe_add_file(&mut new_version, level, added_file.clone());
            }
            (&base_file_metas[start..]).iter().for_each(|file| self.maybe_add_file(&mut new_version, level, file.clone()));
        }

        new_version
    }

    fn maybe_add_file(&self, version: &mut Version, level: usize, meta: Arc<FileMetaData>) {
        if self.levels[level].deleted_files.contains(&meta.number) {
            log::debug!("{} file is deleted: do nothing", meta.number);
        } else {
            let ref mut file_metas = version.files[level];
            if level > 0 && !file_metas.is_empty() {}

            meta.refs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            file_metas.push(meta);
        }
    }
}

pub struct VersionSet {
    db_name: String,
    options: Options,
    versions: VecDeque<Arc<Version>>,
    next_file_number: u64,
    last_sequence: u64,
    log_number: u64,
    prev_log_number: u64,
    manifest_file_number: u64,
    manifest_logger: Option<wal::Writer<file::WritableFile>>,
    compact_pointers: [InternalKey; logs::NUM_LEVELS],
    table_cache: Arc<TableCache>,
}

impl VersionSet {
    pub fn new(db_name: &str, options: Options) -> Self {
        let mut set = VersionSet {
            db_name: String::from(db_name),
            options,
            versions: VecDeque::new(),
            next_file_number: 2,
            last_sequence: 0,
            log_number: 0,
            prev_log_number: 0,
            manifest_file_number: 0,
            manifest_logger: None,
            compact_pointers: Default::default(),
            table_cache: Arc::new(TableCache::new(db_name, options)),
        };
        set.versions.push_back(Arc::new(Version::new(&set)));
        set
    }

    pub fn latest_version(&self) -> Arc<Version> {
        self.versions.back().unwrap().clone()
    }

    pub fn num_level_files(&self, level: u8) -> usize {
        self.latest_version().files[level as usize].len()
    }

    pub fn get_new_file_number(&mut self) -> u64 {
        let number = self.next_file_number;
        self.next_file_number = number + 1;
        number
    }

    pub fn get_last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, sequence: u64) {
        self.last_sequence = sequence
    }

    pub fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub fn get_prev_log_number(&self) -> u64 {
        self.prev_log_number
    }

    pub fn get_manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }

    pub fn mark_file_number_used(&mut self, number: u64) {
        if self.next_file_number <= number {
            self.next_file_number = number + 1;
        }
    }

    pub fn reuse_file_number(&mut self, number: u64) {
        if self.next_file_number == number + 1 {
            self.next_file_number = number;
        }
    }

    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) -> io::Result<()> {
        if edit.log_number.is_none() {
            edit.set_log_number(self.log_number);
        }

        if edit.prev_log_number.is_none() {
            edit.set_prev_log_number(self.prev_log_number);
        }

        edit.set_next_file_number(self.next_file_number);
        edit.set_last_sequence(self.last_sequence);

        // let handle_compact_pointers = |level: usize, internal_key: &Vec<u8>| {
        //     self.compact_pointers[level] = internal_key.clone();
        // };
        let mut builder = VersionBuilder::new(self, self.latest_version());
        builder.apply(&edit);
        let mut version = builder.generate_new_version();
        self.finalize(&mut version);

        if self.manifest_logger.is_none() {
            let file = file::WritableFile::open(filename::make_manifest_file_name(self.db_name.as_str(), self.manifest_file_number))?;
            self.manifest_logger = Some(wal::Writer::new(file));
        }

        let record = edit.encode();
        if let Some(logger) = &mut self.manifest_logger {
            logger.add_record(record)?;
            self.append_version(version);
            self.log_number = edit.log_number.unwrap();
            self.prev_log_number = edit.prev_log_number.unwrap();
        }

        Ok(())
    }

    pub fn add_live_files(&self) -> BTreeSet<u64> {
        let mut live = BTreeSet::<u64>::new();
        for version in &self.versions {
            for level in 0..logs::NUM_LEVELS {
                let files = &version.files[level];
                for f in files {
                    live.insert(f.number);
                }
            }
        }
        live
    }

    pub fn recover(&mut self) -> io::Result<()> {
        let manifest_name = fs::read_to_string(filename::make_current_file_name(&self.db_name))?;
        let file = file::ReadableFile::open(Path::new(&self.db_name).join(manifest_name.as_str()))?;

        let mut next_file_number: Option<u64> = None;
        let mut last_sequence: Option<u64> = None;
        let mut log_number: Option<u64> = None;
        let mut prev_log_numer: Option<u64> = None;

        let mut builder = VersionBuilder::new(self, self.latest_version());

        let mut reader = wal::Reader::new(file);
        while let Some(record) = reader.read_record() {
            let edit = VersionEdit::make_from(record).map_err(|err| {
                io::Error::new(io::ErrorKind::InvalidData, err)
            })?;
            builder.apply(&edit);

            if edit.log_number.is_some() {
                log_number = edit.log_number;
            }

            if edit.prev_log_number.is_some() {
                prev_log_numer = edit.prev_log_number;
            }

            if edit.next_file_number.is_some() {
                next_file_number = edit.next_file_number;
            }

            if edit.last_sequence.is_some() {
                last_sequence = edit.last_sequence;
            }
        }

        if next_file_number.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no meta next file entry in descriptor"));
        }

        if log_number.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no meta log number entry in descriptor"));
        }

        if last_sequence.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no meta last sequence entry in descriptor"));
        }

        let mut version = builder.generate_new_version();
        self.finalize(&mut version);
        self.append_version(version);

        self.manifest_file_number = next_file_number.unwrap_or(0);
        self.next_file_number = self.manifest_file_number + 1;
        self.last_sequence = last_sequence.unwrap_or(0);
        self.log_number = log_number.unwrap_or(0);
        self.prev_log_number = prev_log_numer.unwrap_or(0);

        self.mark_file_number_used(self.prev_log_number);
        self.mark_file_number_used(self.log_number);

        let mut need_new_manifest = true;
        if let Some(number) = self.reuse_manifest(manifest_name.as_str()) {
            log::info!("Reuse Manifest {}", manifest_name);
            self.manifest_file_number = number;
            need_new_manifest = false;
        }

        let manifest_path = filename::make_manifest_file_name(self.db_name.as_str(), self.manifest_file_number);
        let manifest_logger = match file::WritableFile::open(&manifest_path) {
            Ok(file) => wal::Writer::new(file),
            _ => return Err(io::Error::new(io::ErrorKind::NotFound, format!("cannot open manifest {:?}", manifest_path)))
        };
        self.manifest_logger = Some(manifest_logger);

        if need_new_manifest {
            self.write_snapshot()?;
            if file::set_current_file(self.db_name.as_str(), self.manifest_file_number).is_err() {
                return Err(io::Error::new(io::ErrorKind::Other, "cannot write current file"));
            }
        }

        Ok(())
    }

    pub fn level_summary(&self) -> String {
        let current = self.latest_version();
        format!("file[ {0} {1} {2} {3} {4} {5} {6} ]",
                current.files[0].len(), current.files[1].len(),
                current.files[2].len(), current.files[3].len(),
                current.files[4].len(), current.files[5].len(),
                current.files[6].len())
    }

    fn append_version(&mut self, version: Version) {
        let latest_version = Arc::new(version);
        self.versions.push_back(latest_version.clone());
    }

    fn reuse_manifest(&self, manifest_name: &str) -> Option<u64> {
        if !self.options.reuse_logs {
            return None;
        }

        let (file_type, number) = filename::parse_file_name(manifest_name)?;
        let manifest_path = Path::new(&self.db_name).join(manifest_name);
        let manifest_file = fs::metadata(&manifest_path);
        if file_type != FileType::ManifestFile || manifest_file.is_err() || manifest_file.unwrap().len() >= self.options.max_file_size {
            return None;
        }

        Some(number)
    }

    fn finalize(&self, version: &mut Version) {
        let mut best_level: usize = 0;
        let mut best_score: f64 = -1.0;

        for level in 0..logs::NUM_LEVELS {
            let score: f64 = if level == 0 {
                version.files[level].len() as f64 / logs::L0_COMPACTION_TRIGGER as f64
            } else {
                threshold::total_file_size(&version.files[level]) as f64 / threshold::max_bytes_for_level(level)
            };

            if score > best_score {
                best_score = score;
                best_level = level;
            }
        }

        version.compaction_level = best_level;
        version.compaction_score = best_score;
    }

    fn write_snapshot(&mut self) -> io::Result<()> {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(String::from(""));

        for level in 0..logs::NUM_LEVELS {
            let internal_key = &self.compact_pointers[level];
            edit.set_compact_pointer(level, internal_key.clone());
        }

        for level in 0..logs::NUM_LEVELS {
            let files = &self.latest_version().files[level];
            for file in files {
                edit.add_file(level, file.as_ref().clone());
            }
        }

        let record = edit.encode();
        if let Some(logger) = &mut self.manifest_logger {
            logger.add_record(record)?;
        }

        Ok(())
    }

    fn get_range(inputs: &Vec<Arc<FileMetaData>>) -> (InternalKey, InternalKey) {
        let mut smallest = InternalKey::default();
        let mut largest = InternalKey::default();

        if inputs.len() > 0 {
            smallest.assign(&inputs[0].smallest);
            largest.assign(&inputs[0].largest);

            for file in &inputs[1..] {
                if file.smallest <= smallest {
                    smallest.assign(&file.smallest);
                }

                if file.largest >= largest {
                    largest.assign(&file.largest);
                }
            }
        }

        (smallest, largest)
    }

    fn get_range2(inputs1: &Vec<Arc<FileMetaData>>, inputs2: &Vec<Arc<FileMetaData>>) -> (InternalKey, InternalKey) {
        let mut all = Vec::with_capacity(inputs1.len() + inputs2.len());
        all.extend_from_slice(inputs1);
        all.extend_from_slice(inputs2);
        Self::get_range(&all)
    }

    fn get_other_inputs(&mut self, compaction: &mut Compaction) {
        let current = self.latest_version();
        let level = compaction.level;
        let (mut start, mut limit) = Self::get_range(&compaction.inputs[0]);
        compaction.inputs[1] = current.get_over_lapping_inputs(level + 1, &start, &limit);

        let (mut smallest, mut largest) = Self::get_range2(&compaction.inputs[0], &compaction.inputs[1]);
        if !compaction.inputs[1].is_empty() {
            let expanded0 = current.get_over_lapping_inputs(level, &smallest, &largest);
            let inputs0_size = threshold::total_file_size(&compaction.inputs[0]);
            let inputs1_size = threshold::total_file_size(&compaction.inputs[1]);
            let expanded0_size = threshold::total_file_size(&expanded0);
            if expanded0.len() > compaction.inputs[0].len() && inputs1_size + expanded0_size < threshold::expanded_compaction_byte_size_limit(&self.options) {
                let (new_start, new_limit) = Self::get_range(&expanded0);
                let expanded1 = current.get_over_lapping_inputs(level + 1, &new_start, &new_limit);
                if expanded1.len() == compaction.inputs[1].len() {
                    log::info!("Expanding level[{0}] {1}+{2} ({3}+{4} bytes) to {5}+{6} ({7}+{8} bytes)", level,
                        compaction.inputs[0].len(), compaction.inputs[1].len(), inputs0_size, inputs1_size,
                        expanded0.len(), expanded1.len(), expanded0_size, inputs1_size
                    );
                    start = new_start;
                    limit = new_limit;
                    compaction.inputs[0] = expanded0;
                    compaction.inputs[1] = expanded1;
                    (smallest, largest) = Self::get_range2(&compaction.inputs[0], &compaction.inputs[1]);
                }
            }
        }

        if level + 2 < logs::NUM_LEVELS {
            compaction.grandparents = current.get_over_lapping_inputs(level + 2, &smallest, &largest);
        }

        compaction.edit.set_compact_pointer(level, limit.clone());
        self.compact_pointers[level] = limit;
    }

    pub fn pick_compaction(&mut self) -> Option<Compaction> {
        let current = self.latest_version();
        let mut compaction: Compaction;
        let level: usize;
        let size_compaction = current.compaction_score >= 1.0;

        if size_compaction {
            level = current.compaction_level;
            compaction = Compaction::new(self.options, current.clone(), level);
            for file in &current.files[level] {
                if file.largest > self.compact_pointers[level] {
                    compaction.inputs[0].push(file.clone());
                    break;
                }
            }
            if compaction.inputs[0].is_empty() {
                compaction.inputs[0].push(current.files[level][0].clone());
            }
        } else if let Some(seek_compaction) = current.file_to_compact.read().unwrap().as_ref() {
            level = seek_compaction.level;
            compaction = Compaction::new(self.options, current.clone(), seek_compaction.level);
            compaction.inputs[0].push(seek_compaction.file.clone());
        } else {
            return None;
        }

        if level == 0 {
            let (smallest, largest) = Self::get_range(&compaction.inputs[0]);
            compaction.inputs[0] = current.get_over_lapping_inputs(0, &smallest, &largest);
        }

        self.get_other_inputs(&mut compaction);

        Some(compaction)
    }

    pub fn make_input_iterator(&self, compaction: &Compaction) -> MergingIterator {
        let space = if compaction.level == 0 { compaction.inputs[0].len() + 1 } else { 2 };
        let mut iterator_list = Vec::with_capacity(space);
        for level in 0..2 {
            if compaction.inputs[level].is_empty() {
                continue;
            }

            if compaction.level + level == 0 {
                for file in &compaction.inputs[level] {
                    let table_iter = self.table_cache.iter(file.number, file.file_size);
                    if let Some(iter) = table_iter {
                        iterator_list.push(iter);
                    }
                }
            } else {
                let iter = TwoLevelIterator::new(
                    VersionLevelFileIterator::new(compaction.inputs[level].clone()),
                    FileIteratorGen::new(self.table_cache.clone()),
                );
                iterator_list.push(Box::new(iter));
            }
        }

        MergingIterator::new(InternalKey::compare, iterator_list)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::core::format::{InternalKey, UserKey, ValueType};
    use crate::Options;
    use super::{FileMetaData, Version, VersionBuilder, VersionEdit, VersionSet};

    fn create_file_meta<T: AsRef<[u8]>>(number: u64, key1: T, key2: T) -> FileMetaData {
        let mut meta = FileMetaData::new();
        meta.number = number;
        meta.file_size = 10 << 8;
        meta.smallest = InternalKey::restore(key1, 9, ValueType::Insertion);
        meta.largest = InternalKey::restore(key2, 10, ValueType::Insertion);
        meta
    }

    fn create_version_edit1() -> VersionEdit {
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

    fn create_version_edit2() -> VersionEdit {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(String::from("InternalKeyComparator"));
        edit.set_last_sequence(130);
        edit.set_prev_log_number(100);
        edit.set_log_number(101);
        edit.set_next_file_number(102);

        let mut f3 = FileMetaData::new();
        f3.number = 3;
        f3.file_size = 10 << 8;
        f3.smallest = InternalKey::restore("abc", 362, ValueType::Insertion);
        f3.largest = InternalKey::restore("adz", 363, ValueType::Insertion);

        edit.add_file(1, f3);
        edit.remove_file(0, 1);
        edit.remove_file(1, 2);
        edit
    }

    fn create_version_edit3() -> VersionEdit {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(String::from("InternalKeyComparator"));
        edit.set_last_sequence(140);
        edit.set_prev_log_number(101);
        edit.set_log_number(102);
        edit.set_next_file_number(103);

        let mut f4 = FileMetaData::new();
        f4.number = 4;
        f4.file_size = 10 << 8;
        f4.smallest = InternalKey::restore("fgh", 320, ValueType::Insertion);
        f4.largest = InternalKey::restore("jkl", 321, ValueType::Insertion);

        edit.add_file(0, f4);
        edit
    }

    fn assert_file_range<T: AsRef<[u8]>>(file: &Arc<FileMetaData>, begin: T, end: T) {
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
        let version_set = VersionSet::new("test", Options::default());
        let mut version = Version::new(&version_set);
        for i in 0..4usize {
            version.files[0].push(Arc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
        }

        for i in 4..7usize {
            version.files[1].push(Arc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
        }

        for i in 7..9usize {
            version.files[2].push(Arc::new(create_file_meta(i as u64, keys[i].0, keys[i].1)));
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

        assert!(version.exists_in_nonzero_level(1, &UserKey::new("200")));
        assert!(version.exists_in_nonzero_level(1, &UserKey::new("650")));
        assert!(!version.exists_in_nonzero_level(1, &UserKey::new("380")));
        assert!(!version.exists_in_nonzero_level(1, &UserKey::new("800")));
        assert!(version.exists_in_nonzero_level(2, &UserKey::new("800")));

        version.compaction_level = 0;
        version.compaction_score = 1.2;
        let mut version_set = VersionSet::new("test", Options::default());
        version_set.compact_pointers[0] = InternalKey::restore("280", 9, ValueType::Insertion);
        version_set.append_version(version);
        let compaction = version_set.pick_compaction().unwrap();
        assert_eq!(version_set.compact_pointers[0].extract_user_key().as_ref(), "320".as_bytes());
        assert_file_range(&compaction.inputs[0][0], "000", "100");
        assert_file_range(&compaction.inputs[0][1], "200", "300");
        assert_file_range(&compaction.inputs[0][2], "260", "320");
        assert_file_range(&compaction.inputs[1][0], "100", "360");
    }

    #[test]
    fn test_version_edit_encoded() {
        let edit1 = create_version_edit1();
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

    #[test]
    fn test_merge_multi_version_edit() {
        let mut version_set = VersionSet::new("test", Options::default());
        let base_version = Arc::new(Version::new(&version_set));
        let mut builder = VersionBuilder::new(&mut version_set, base_version);

        let edit1 = create_version_edit1();
        let edit2 = create_version_edit2();
        let edit3 = create_version_edit3();
        builder.apply(&edit1);
        builder.apply(&edit2);
        builder.apply(&edit3);

        let new_version = builder.generate_new_version();
        assert_eq!(new_version.files[0].len(), 1);
        assert_eq!(new_version.files[1].len(), 1);
    }
}
