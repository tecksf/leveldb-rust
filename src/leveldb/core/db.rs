use std::{fs, io, thread, time};
use std::ffi::OsString;
use std::path::Path;
use fslock::LockFile;
use crate::leveldb::{logs, Options, WriteOptions};
use crate::leveldb::core::batch::WriteBatch;
use crate::leveldb::core::format::{Comparator, InternalKey};
use crate::leveldb::core::memory::MemoryTable;
use crate::leveldb::core::sst::build_table;
use crate::leveldb::core::version::{FileMetaData, Version, VersionEdit, VersionSet};
use crate::leveldb::logs::file::WritableFile;
use crate::leveldb::logs::{file, filename, wal};
use crate::leveldb::logs::filename::FileType;

pub struct Database {
    name: String,
    options: Options,
    log_file_number: u64,
    write_ahead_logger: Option<wal::Writer<WritableFile>>,
    mutable: MemoryTable,
    immutable: Option<MemoryTable>,
    versions: VersionSet,
}

impl Database {
    pub fn open<T: AsRef<str>>(path: T, options: Options) -> io::Result<Self> {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        log::info!("Opening database at path: {}", path.as_ref());

        let mut db = Database {
            name: String::from(path.as_ref()),
            options,
            log_file_number: 0,
            write_ahead_logger: None,
            mutable: MemoryTable::new(),
            immutable: None,
            versions: VersionSet::new(path.as_ref(), options),
        };

        db.init()?;
        let mut edit = db.recover()?;

        // has log number indicate that reuse log file
        db.log_file_number = if let Some(number) = edit.get_log_number() {
            number
        } else {
            db.versions.get_new_file_number()
        };

        if db.write_ahead_logger.is_none() {
            let file = WritableFile::open(filename::make_log_file_name(path.as_ref(), db.log_file_number))?;
            db.write_ahead_logger = Some(wal::Writer::new(file));
        }

        if edit.has_updated() || edit.get_log_number().is_none() {
            edit.set_prev_log_number(0);
            edit.set_log_number(db.log_file_number);
            db.versions.log_and_apply(edit)?;
        }

        db.remove_obsolete_files();

        Ok(db)
    }

    pub fn put<T: AsRef<str>>(&mut self, options: WriteOptions, key: T, value: T) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(options, batch)
    }

    pub fn write(&mut self, options: WriteOptions, batch: WriteBatch) -> io::Result<()> {
        let mut last_sequence = self.versions.get_last_sequence();
        let mut write_batch = batch;

        let status = self.make_room_for_write(false);
        if status.is_ok() {
            write_batch.set_sequence(last_sequence + 1);
            last_sequence += write_batch.get_count() as u64;

            if let Some(logger) = &mut self.write_ahead_logger {
                logger.add_record(write_batch.get_payload())?;
                if options.sync {
                    logger.sync()?;
                }
                self.mutable.insert(&write_batch);
            }
            self.versions.set_last_sequence(last_sequence);
        }

        Ok(())
    }

    fn make_room_for_write(&mut self, force: bool) -> io::Result<()> {
        let mut allow_delay = !force;
        loop {
            if allow_delay && self.versions.num_level_files(0) >= logs::L0_SLOW_DOWN_WRITES_TRIGGER {
                thread::sleep(time::Duration::from_millis(1));
                allow_delay = false;
            } else if self.mutable.memory_usage() <= self.options.write_buffer_size {
                break;
            } else if self.immutable.is_some() {
                log::info!("current memory table is full, waiting...\n");
            } else if self.versions.num_level_files(0) >= logs::L0_STOP_WRITES_TRIGGER {
                log::info!("too many L0 files, waiting...\n");
            } else {
                let new_log_number = self.versions.get_new_file_number();
                let status = WritableFile::open(filename::make_log_file_name(self.name.as_str(), new_log_number));
                match status {
                    Ok(file) => {
                        self.log_file_number = new_log_number;
                        self.write_ahead_logger = Some(wal::Writer::new(file));
                    }
                    Err(reason) => {
                        log::error!("cannot create new WAL: {}", reason);
                        self.versions.reuse_file_number(new_log_number);
                        return Err(reason);
                    }
                }

                let immutable = std::mem::replace(&mut self.mutable, MemoryTable::new());
                self.immutable = Some(immutable);
                self.maybe_schedule_compaction();
            }
        }
        Ok(())
    }

    fn init(&self) -> io::Result<()> {
        fs::create_dir(&self.name).unwrap_or_default();

        let lock_file_name = filename::make_lock_file_name(self.name.as_str());
        LockFile::open(&lock_file_name)?;

        if fs::metadata(filename::make_current_file_name(self.name.as_str())).is_err() {
            if self.options.create_if_missing {
                log::info!("Creating DB {} since it was missing", self.name);
                Self::create_manifest(self.name.as_str(), InternalKey::name().as_str())?;
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, format!("InvalidArgument:{} does not exist(create_if_missing is false)", self.name)));
            }
        } else {
            if self.options.error_if_exists {
                return Err(io::Error::new(io::ErrorKind::Other, format!("InvalidArgument:{} does not exist(create_if_missing is false)", self.name)));
            }
        }

        Ok(())
    }

    fn create_manifest(db_name: &str, comparator_name: &str) -> io::Result<()> {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(String::from(comparator_name));
        edit.set_log_number(0);
        edit.set_next_file_number(2);
        edit.set_last_sequence(0);

        let result: io::Result<()>;
        let manifest = filename::make_manifest_file_name(db_name, 1);
        {
            let mut manifest_logger = wal::Writer::new(WritableFile::open(&manifest)?);
            let record = edit.encode();
            result = manifest_logger.add_record(record);
        }

        if result.is_ok() {
            file::set_current_file(db_name, 1)?;
        } else {
            fs::remove_file(manifest)?;
        }

        result
    }

    fn recover(&mut self) -> io::Result<VersionEdit> {
        self.versions.recover()?;
        let min_log = self.versions.get_log_number();
        let prev_log = self.versions.get_prev_log_number();

        let mut expected_files = self.versions.add_live_files();
        let mut log_numbers = Vec::<u64>::new();
        let filenames = file::get_all_filenames(self.name.as_str());
        for filename in filenames {
            if let Some((file_type, number)) = filename::parse_file_name(filename.to_str().unwrap_or("unknown")) {
                expected_files.remove(&number);
                match file_type {
                    FileType::LogFile if number >= min_log || number == prev_log => {
                        log_numbers.push(number);
                    }
                    _ => {}
                }
            }
        }

        if !expected_files.is_empty() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{} missing files; e.g.", expected_files.len())));
        }

        let mut max_sequence: u64 = 0;
        let mut edit = VersionEdit::new();
        log_numbers.sort();
        for (index, &number) in log_numbers.iter().enumerate() {
            let sequence = self.recover_log_file(number, &mut edit, index == (log_numbers.len() - 1))?;
            if sequence > max_sequence {
                max_sequence = sequence;
            }
            self.versions.mark_file_number_used(number);
        }

        if self.versions.get_last_sequence() < max_sequence {
            self.versions.set_last_sequence(max_sequence);
        }

        Ok(edit)
    }

    fn recover_log_file(&mut self, log_number: u64, edit: &mut VersionEdit, last_file: bool) -> io::Result<u64> {
        let log_file_name = filename::make_log_file_name(self.name.as_ref(), log_number);
        let reader = wal::Reader::new(file::ReadableFile::open(&log_file_name)?);
        let mut max_sequence: u64 = 0;
        let mut compactions = 0;
        let mut new_table = MemoryTable::new();

        while let Some(record) = reader.read_record() {
            if record.len() < 12 {
                log::error!("log record too small");
                continue;
            }
            let batch = WriteBatch::make_from(record);
            new_table.insert(&batch);

            let last_sequence = batch.get_sequence() + (batch.get_count() - 1) as u64;
            if last_sequence > max_sequence {
                max_sequence = last_sequence;
            }

            if new_table.memory_usage() > self.options.write_buffer_size {
                compactions += 1;
                let new_number = self.versions.get_new_file_number();
                let (level, file_meta) = self.write_level0_table(None, new_number, &new_table)?;
                edit.add_file(level, file_meta);

                new_table = MemoryTable::new();
            }
        }

        if self.options.reuse_logs && last_file && compactions == 0 {
            edit.set_log_number(log_number);
            new_table = std::mem::replace(&mut self.mutable, new_table);
        }

        if new_table.memory_usage() > 0 {
            let new_number = self.versions.get_new_file_number();
            let (level, file_meta) = self.write_level0_table(None, new_number, &new_table)?;
            edit.add_file(level, file_meta);
        }

        Ok(max_sequence)
    }

    fn maybe_schedule_compaction(&mut self) {
        self.compact_memory_table();
    }

    fn compact_memory_table(&mut self) {
        let base = self.versions.latest_version();
        let file_number = self.versions.get_new_file_number();

        if let Some(table) = &self.immutable {
            let result = self.write_level0_table(Some(&base), file_number, table);
            if result.is_err() {
                return;
            }

            let (level, meta) = result.unwrap();
            let mut edit = VersionEdit::new();
            edit.set_prev_log_number(0);
            edit.set_log_number(self.log_file_number);
            edit.add_file(level, meta);
            let status = self.versions.log_and_apply(edit);
            if status.is_ok() {
                self.immutable = None;
                self.remove_obsolete_files();
            }
        }
    }

    fn write_level0_table(&self, base: Option<&Version>, file_number: u64, table: &MemoryTable) -> io::Result<(usize, FileMetaData)> {
        log::info!("level-0 table {}:started", file_number);

        let meta = build_table(self.options, self.name.as_str(), table.iter(), file_number)?;

        log::info!("level-0 table {}: {} bytes", file_number, meta.file_size);

        let mut level = 0;
        if meta.file_size > 0 {
            let min_user_key = meta.smallest.extract_user_key();
            let max_user_key = meta.largest.extract_user_key();

            if let Some(version) = base {
                level = version.pick_level_for_memory_table(&min_user_key, &max_user_key);
            }
        }
        Ok((level, meta))
    }

    fn remove_obsolete_files(&self) {
        let live_files = self.versions.add_live_files();
        let mut files_to_delete = Vec::<OsString>::new();
        let filenames = file::get_all_filenames(self.name.as_str());
        for filename in filenames {
            if let Some((file_type, number)) = filename::parse_file_name(filename.to_str().unwrap_or("unknown")) {
                let keep = match file_type {
                    FileType::LogFile => number >= self.versions.get_log_number() || number == self.versions.get_prev_log_number(),
                    FileType::ManifestFile => number >= self.versions.get_manifest_file_number(),
                    FileType::TableFile | FileType::TempFile => live_files.contains(&number),
                    _ => true,
                };

                if !keep {
                    files_to_delete.push(filename);
                    log::info!("Delete type={:?}, number={}", file_type, number);
                }
            }
        }

        for filename in files_to_delete {
            fs::remove_file(Path::new(&self.name).join(filename)).unwrap();
        }
    }
}
