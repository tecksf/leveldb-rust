use std::{fs, io, time};
use std::collections::LinkedList;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use crossbeam_channel::internal::SelectHandle;
use parking_lot::{Mutex, Condvar, MutexGuard};
use fslock::LockFile;
use crate::{logs, Options, WriteOptions};
use crate::core::batch::WriteBatch;
use crate::core::format::{Comparator, InternalKey, LookupKey};
use crate::core::memory::MemoryTable;
use crate::core::schedule;
use crate::core::sst::build_table;
use crate::core::version::{FileMetaData, Version, VersionEdit, VersionSet};
use crate::logs::file::WritableFile;
use crate::logs::{file, filename, wal};
use crate::logs::filename::FileType;

struct Agent {
    batch: WriteBatch,
    sync: bool,
    cond: Condvar,
    result_sender: crossbeam_channel::Sender<io::Result<()>>,
    result_receiver: crossbeam_channel::Receiver<io::Result<()>>,
}

#[derive(Clone)]
pub struct Database {
    db_impl: Arc<Mutex<DatabaseImpl>>,
    dispatcher: schedule::Dispatcher,
    background_work_finished_signal: Arc<Condvar>,
}

impl Database {
    pub fn open<T: AsRef<str>>(path: T, options: Options) -> io::Result<Self> {
        Ok(Self {
            db_impl: Arc::new(Mutex::new(DatabaseImpl::new(path.as_ref(), options)?)),
            dispatcher: schedule::Dispatcher::new(),
            background_work_finished_signal: Arc::new(Condvar::new()),
        })
    }

    pub fn put<T: AsRef<str>>(&self, options: WriteOptions, key: T, value: T) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(options, batch)
    }

    pub fn delete<T: AsRef<str>>(&self, options: WriteOptions, key: T) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(options, batch)
    }

    pub fn write(&self, options: WriteOptions, batch: WriteBatch) -> io::Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let current_agent = Arc::new(Agent {
            batch,
            sync: options.sync,
            cond: Condvar::new(),
            result_sender: tx,
            result_receiver: rx,
        });

        let mut db_impl = self.db_impl.lock();
        db_impl.agents.push_back(current_agent.clone());
        while current_agent.result_receiver.is_empty() && !Arc::ptr_eq(&current_agent, &db_impl.agents.front().unwrap().clone()) {
            current_agent.cond.wait(&mut db_impl);
        }

        if current_agent.result_receiver.is_ready() {
            return current_agent.result_receiver.recv().unwrap();
        }

        let mut write_batch = WriteBatch::new();
        let last_agent = Self::build_batch_group(&db_impl.agents, &mut write_batch);

        let mut write_ahead_logger = db_impl.write_ahead_logger.take();
        let mut last_sequence = db_impl.versions.get_last_sequence();
        let mutable = db_impl.mutable.clone();
        let mut result = self.make_room_for_write(&mut db_impl, false);

        drop(db_impl);

        if result.is_ok() {
            write_batch.set_sequence(last_sequence + 1);
            last_sequence += write_batch.get_count() as u64;

            result = match &mut write_ahead_logger {
                Some(logger) => {
                    logger.add_record(write_batch.get_payload())?;
                    if options.sync {
                        logger.sync()?;
                    }
                    mutable.insert(&write_batch);
                    Ok(())
                }
                _ => Ok(())
            };
        }

        let mut db_impl = self.db_impl.lock();
        db_impl.versions.set_last_sequence(last_sequence);
        db_impl.write_ahead_logger = write_ahead_logger;
        while let Some(ready_agent) = db_impl.agents.pop_front() {
            if !Arc::ptr_eq(&ready_agent, &current_agent) {
                let agent_result = match &result {
                    Ok(()) => Ok(()),
                    Err(err) => Err(io::Error::new(err.kind(), err.to_string()))
                };
                ready_agent.result_sender.send(agent_result).unwrap();
                ready_agent.cond.notify_one();
            }

            if Arc::ptr_eq(&ready_agent, &last_agent) {
                break;
            }
        }

        if let Some(agent) = db_impl.agents.front() {
            agent.cond.notify_one();
        }

        result
    }

    pub fn get<T: AsRef<str>>(&self, key: T) -> io::Result<Vec<u8>> {
        let db_impl = self.db_impl.lock();
        let sequence = db_impl.versions.get_last_sequence();
        let version = db_impl.versions.latest_version();
        let mutable = db_impl.mutable.clone();
        let immutable = db_impl.immutable.clone();

        drop(db_impl);

        let lookup_key = LookupKey::new(key.as_ref(), sequence);
        let not_found: io::Result<Vec<u8>> = Err(io::Error::new(io::ErrorKind::NotFound, ""));

        if let Ok(result) = mutable.get(&lookup_key) {
            return match result {
                Some(value) => Ok(value),
                _ => not_found,
            };
        }

        if let Some(table) = &immutable {
            if let Ok(result) = table.get(&lookup_key) {
                return match result {
                    Some(value) => Ok(value),
                    _ => not_found,
                };
            }
        }

        let (result, statistics) = version.get(&lookup_key);
        if version.update_statistics(&statistics) {
            self.maybe_schedule_compaction();
        }

        result
    }

    fn build_batch_group(agents: &LinkedList<Arc<Agent>>, result: &mut WriteBatch) -> Arc<Agent> {
        let first = agents.front().unwrap().clone();
        let mut last = first.clone();

        result.extend(&first.batch);

        let mut size = first.batch.size();
        let mut max_size: usize = 1 << 20;
        if size <= (128 << 10) {
            max_size = size + (128 << 10);
        }

        let mut it = agents.iter();
        it.next();
        while let Some(p) = it.next() {
            if p.sync && !first.sync {
                break;
            }

            size += p.batch.size();
            if size > max_size {
                break;
            }

            last = p.clone();
            result.extend(&p.batch);
        }

        last
    }

    fn make_room_for_write(&self, mut database: &mut MutexGuard<DatabaseImpl>, force: bool) -> io::Result<()> {
        let mut allow_delay = !force;
        loop {
            if allow_delay && database.versions.num_level_files(0) >= logs::L0_SLOW_DOWN_WRITES_TRIGGER {
                allow_delay = false;
                self.background_work_finished_signal.wait_for(&mut database, time::Duration::from_millis(1));
            } else if database.mutable.memory_usage() <= database.options.write_buffer_size {
                break;
            } else if database.immutable.is_some() {
                log::info!("current memory table is full, waiting...\n");
                self.background_work_finished_signal.wait(&mut database);
            } else if database.versions.num_level_files(0) >= logs::L0_STOP_WRITES_TRIGGER {
                log::info!("too many L0 files, waiting...\n");
                self.background_work_finished_signal.wait(&mut database);
            } else {
                let new_log_number = database.versions.get_new_file_number();
                let status = WritableFile::open(filename::make_log_file_name(database.name.as_str(), new_log_number));
                match status {
                    Ok(file) => {
                        database.log_file_number = new_log_number;
                        database.write_ahead_logger = Some(wal::Writer::new(file));
                    }
                    Err(reason) => {
                        log::error!("cannot create new WAL: {}", reason);
                        database.versions.reuse_file_number(new_log_number);
                        return Err(reason);
                    }
                }

                database.immutable = Some(database.mutable.clone());
                database.mutable = Arc::new(MemoryTable::new());
                self.maybe_schedule_compaction();
            }
        }
        Ok(())
    }

    fn maybe_schedule_compaction(&self) {
        let database = self.db_impl.clone();
        let background_work_finished_signal = self.background_work_finished_signal.clone();
        self.dispatcher.dispatch(Box::new(move || {
            DatabaseImpl::background_compaction(database);
            background_work_finished_signal.notify_all();
        }));
    }
}

struct DatabaseImpl {
    name: String,
    options: Options,
    log_file_number: u64,
    write_ahead_logger: Option<wal::Writer<WritableFile>>,
    mutable: Arc<MemoryTable>,
    immutable: Option<Arc<MemoryTable>>,
    versions: VersionSet,
    agents: LinkedList<Arc<Agent>>,
}

impl DatabaseImpl {
    pub fn new(path: &str, options: Options) -> io::Result<Self> {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        log::info!("Opening database at path: {}", path);

        let mut db = DatabaseImpl {
            name: String::from(path),
            options,
            write_ahead_logger: None,
            log_file_number: 0,
            mutable: Arc::new(MemoryTable::new()),
            immutable: None,
            versions: VersionSet::new(path, options),
            agents: LinkedList::new(),
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
        let mut reader = wal::Reader::new(file::ReadableFile::open(&log_file_name)?);
        let mut max_sequence: u64 = 0;
        let mut compactions = 0;
        let mut new_table = Arc::new(MemoryTable::new());

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
                let (level, file_meta) = Self::write_level0_table(self.options, self.name.as_str(), None, new_number, &new_table)?;
                edit.add_file(level, file_meta);

                new_table = Arc::new(MemoryTable::new());
            }
        }

        if self.options.reuse_logs && last_file && compactions == 0 {
            edit.set_log_number(log_number);
            new_table = std::mem::replace(&mut self.mutable, new_table);
        }

        if new_table.memory_usage() > 0 {
            let new_number = self.versions.get_new_file_number();
            let (level, file_meta) = Self::write_level0_table(self.options, self.name.as_str(), None, new_number, &new_table)?;
            edit.add_file(level, file_meta);
        }

        Ok(max_sequence)
    }

    fn background_compaction(database: Arc<Mutex<Self>>) {
        if database.lock().immutable.is_some() {
            Self::compact_memory_table(database);
            return;
        }

        let mut db = database.lock();
        if let Some(mut compaction) = db.versions.pick_compaction() {
            if compaction.is_trivial_move() {
                let level = compaction.level;
                let file = compaction.inputs[0][0].clone();
                compaction.edit.remove_file(level, file.number);
                compaction.edit.add_file(level + 1, file.as_ref().clone());
                db.versions.log_and_apply(compaction.edit).unwrap();
                log::info!("Moved {0} to level{1} {2} bytes: {3}", file.number, level + 1, file.file_size, db.versions.level_summary());
            } else {
                db.do_compaction_work();
            }
        }
    }

    fn do_compaction_work(&self) {}

    fn compact_memory_table(database: Arc<Mutex<Self>>) {
        let mut db = database.lock();
        let table = db.immutable.as_ref().unwrap().clone();
        let base = db.versions.latest_version();
        let file_number = db.versions.get_new_file_number();
        let options = db.options;
        let db_name = String::from(db.name.as_str());
        let log_file_number = db.log_file_number;

        drop(db);

        let result = Self::write_level0_table(options, db_name.as_str(), Some(&base), file_number, &table);
        if result.is_err() {
            return;
        }

        let (level, meta) = result.unwrap();
        let mut edit = VersionEdit::new();
        edit.set_prev_log_number(0);
        edit.set_log_number(log_file_number);
        edit.add_file(level, meta);

        let mut db = database.lock();
        let status = db.versions.log_and_apply(edit);
        if status.is_ok() {
            db.immutable = None;
            db.remove_obsolete_files();
        }
    }

    fn write_level0_table(options: Options, db_name: &str, base: Option<&Version>, file_number: u64, table: &MemoryTable) -> io::Result<(usize, FileMetaData)> {
        log::info!("level-0 table {}:started", file_number);

        let meta = build_table(options, db_name, table.iter(), file_number)?;

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
