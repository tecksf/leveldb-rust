use std::io;
use std::sync::Arc;
use crate::core::format::{InternalKey, UserKey};
use crate::core::version::{FileMetaData, Version, VersionEdit};
use crate::logs::file::WritableFile;
use crate::{logs, Options};
use crate::table::builder::TableBuilder;

pub mod threshold {
    use super::*;

    pub fn total_file_size(files: &Vec<Arc<FileMetaData>>) -> u64 {
        let mut sum: u64 = 0;
        for file in files {
            sum += file.file_size;
        }
        sum
    }

    pub fn max_bytes_for_level(level: usize) -> f64 {
        let mut level = level;
        let mut result: f64 = 10.0 * 1048576.0;
        while level > 1 {
            result *= 10.0;
            level -= 1;
        }
        result
    }

    pub fn target_file_size(options: &Options) -> u64 {
        options.max_file_size
    }

    pub fn max_grand_parent_overlap_bytes(options: &Options) -> u64 {
        10 * target_file_size(&options)
    }

    pub fn expanded_compaction_byte_size_limit(options: &Options) -> u64 {
        25 * target_file_size(&options)
    }
}

pub struct Compaction {
    options: Options,
    pub level: usize,
    pub edit: VersionEdit,
    pub inputs: [Vec<Arc<FileMetaData>>; 2],
    pub grandparents: Vec<Arc<FileMetaData>>,
    input_version: Arc<Version>,
    grandparent_index: usize,
    seen_key: bool,
    overlapped_bytes: u64,
}

impl Compaction {
    pub fn new(options: Options, version: Arc<Version>, level: usize) -> Self {
        Self {
            level,
            options,
            edit: VersionEdit::new(),
            inputs: Default::default(),
            grandparents: Default::default(),
            grandparent_index: 0,
            seen_key: false,
            overlapped_bytes: 0,
            input_version: version,
        }
    }

    pub fn is_trivial_move(&self) -> bool {
        self.inputs[0].len() == 1 &&
            self.inputs[1].len() == 0 &&
            threshold::total_file_size(&self.grandparents) <= threshold::max_grand_parent_overlap_bytes(&self.options)
    }

    pub fn get_max_output_file_size(&self) -> u64 {
        threshold::target_file_size(&self.options)
    }

    pub fn should_stop_before(&mut self, internal_key: &InternalKey) -> bool {
        while self.grandparent_index < self.grandparents.len() &&
            internal_key > &self.grandparents[self.grandparent_index].largest {
            if self.seen_key {
                self.overlapped_bytes += self.grandparents[self.grandparent_index].file_size;
            }
            self.grandparent_index += 1;
        }
        self.seen_key = true;

        if self.overlapped_bytes > threshold::max_grand_parent_overlap_bytes(&self.options) {
            self.overlapped_bytes = 0;
            return true;
        }

        false
    }

    pub fn is_base_level_for_key(&self, user_key: &UserKey) -> bool {
        for level in self.level + 2..logs::NUM_LEVELS {
            if self.input_version.exists_in_nonzero_level(level, user_key) {
                return true;
            }
        }

        false
    }
}

pub struct CompactionState<'a> {
    pub compaction: &'a mut Compaction,
    pub table_builder: Option<TableBuilder<WritableFile>>,
    pub smallest_snapshot: u64,
    pub outputs: Vec<FileMetaData>,
    pub stats: CompactionStatistics,
    pub total_bytes: u64,
}

impl<'a> CompactionState<'a> {
    pub fn new(compaction: &'a mut Compaction) -> Self {
        Self {
            compaction,
            table_builder: None,
            smallest_snapshot: 0,
            outputs: Default::default(),
            stats: Default::default(),
            total_bytes: 0,
        }
    }

    pub fn finish_compaction_output_file(&mut self) -> io::Result<()> {
        if self.table_builder.is_none() || self.outputs.is_empty() {
            return Ok(());
        }

        let mut builder = self.table_builder.take().unwrap();
        let result = builder.finish();

        let current_bytes = builder.file_size();
        self.total_bytes += current_bytes;
        self.outputs.last_mut().unwrap().file_size = current_bytes;

        result
    }

    pub fn add_key<T: AsRef<[u8]>>(&mut self, internal_key: &InternalKey, value: T) -> u64 {
        if self.table_builder.is_none() || self.outputs.is_empty() {
            return 0;
        }

        if let Some(builder) = &mut self.table_builder {
            if builder.get_num_entries() == 0 {
                self.outputs.last_mut().unwrap().smallest.assign(internal_key);
            }
            self.outputs.last_mut().unwrap().largest.assign(&internal_key);
            builder.add(&internal_key, value);
            return builder.file_size();
        }
        0
    }
}

pub struct CompactionStatistics {
    microsecond: u64,
    bytes_read: u64,
    bytes_written: u64,
}

impl CompactionStatistics {
    pub fn new() -> Self {
        Self {
            microsecond: 0,
            bytes_read: 0,
            bytes_written: 0,
        }
    }

    pub fn add(&mut self, microsecond: u64, bytes_read: u64, bytes_written: u64) {
        self.microsecond += microsecond;
        self.bytes_read += bytes_read;
        self.bytes_written += bytes_written;
    }

    pub fn add_by(&mut self, other: &Self) {
        self.microsecond += other.microsecond;
        self.bytes_read += other.bytes_read;
        self.bytes_written += other.bytes_written;
    }
}

impl Default for CompactionStatistics {
    fn default() -> Self {
        Self::new()
    }
}
