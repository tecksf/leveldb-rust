use std::{fs, io, thread, time};
use fslock::LockFile;
use crate::leveldb::{logs, Options, WriteOptions};
use crate::leveldb::core::batch::WriteBatch;
use crate::leveldb::core::memory::MemoryTable;
use crate::leveldb::core::sst::build_table;
use crate::leveldb::core::version::{FileMetaData, Version, VersionEdit, VersionSet};
use crate::leveldb::logs::file::WritableFile;
use crate::leveldb::logs::{filename, wal};

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

        if db.write_ahead_logger.is_none() {
            let file = WritableFile::open(filename::make_log_file_name(path.as_ref(), db.log_file_number))?;
            db.write_ahead_logger = Some(wal::Writer::new(file));
        }

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
}
