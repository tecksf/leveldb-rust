use std::{fs, io, time};
use std::collections::LinkedList;
use std::ffi::OsString;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crossbeam_channel::internal::SelectHandle;
use parking_lot::{Mutex, Condvar, MutexGuard};
use fslock::LockFile;
use crate::{logs, Options, WriteOptions};
use crate::core::batch::WriteBatch;
use crate::core::compaction::{CompactionState, CompactionStatistics, ManualCompaction};
use crate::core::format::{Comparator, InternalKey, LookupKey, UserKey, ValueType, MAX_SEQUENCE_NUMBER};
use crate::core::iterator::{LevelIterator, MergingIterator};
use crate::core::memory::MemoryTable;
use crate::core::schedule;
use crate::core::sst::build_table;
use crate::core::version::{FileMetaData, Version, VersionEdit, VersionSet};
use crate::logs::file::WritableFile;
use crate::logs::{file, filename, wal};
use crate::logs::filename::FileType;
use crate::table::builder::TableBuilder;
use crate::utils::common::now_micros;

struct Agent {
    batch: WriteBatch,
    sync: bool,
    cond: Condvar,
    result_sender: crossbeam_channel::Sender<io::Result<()>>,
    result_receiver: crossbeam_channel::Receiver<io::Result<()>>,
}

#[derive(Clone)]
pub struct Database {
    dispatcher: schedule::Dispatcher,
    db_wrap: DatabaseWrap,
}

impl Deref for Database {
    type Target = DatabaseWrap;
    fn deref(&self) -> &Self::Target {
        &self.db_wrap
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        log::info!("Terminates background thread, waiting all tasks finish...");
        self.dispatcher.terminate();
        log::info!("Database Closed {:?}", self.name);
    }
}

impl Database {
    pub fn open<T: AsRef<str>>(path: T, options: Options) -> io::Result<Arc<Self>> {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        log::info!("Opening database at path: {}", path.as_ref());

        let db_impl = Arc::new(Mutex::new(DatabaseImpl::new(path.as_ref(), options)));
        let mut db = db_impl.lock();
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
            db.versions.log_and_apply(&mut edit)?;
        }

        db.remove_obsolete_files();

        drop(db);

        let database = Database {
            dispatcher: schedule::Dispatcher::new(),
            db_wrap: DatabaseWrap::new(String::from(path.as_ref()), options, db_impl),
        };
        database.maybe_schedule_compaction();

        Ok(Arc::new(database))
    }

    pub fn put<T: AsRef<str>>(self: &Arc<Self>, options: WriteOptions, key: T, value: T) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(options, batch)
    }

    pub fn delete<T: AsRef<str>>(self: &Arc<Self>, options: WriteOptions, key: T) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(options, batch)
    }

    pub fn write(self: &Arc<Self>, options: WriteOptions, batch: WriteBatch) -> io::Result<()> {
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

        let mut last_sequence = db_impl.versions.get_last_sequence();
        let mutable = db_impl.mutable.clone();
        let mut result = self.make_room_for_write(&mut db_impl, false);
        let mut write_ahead_logger = db_impl.write_ahead_logger.take();

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

    pub fn get<T: AsRef<str>>(self: &Arc<Self>, key: T) -> io::Result<Vec<u8>> {
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

    pub fn compact_range<T: AsRef<str>>(self: &Arc<Self>, begin: T, end: T) {
        let mut max_level_with_files = 1;
        {
            let db_impl = self.db_impl.lock();
            let current = db_impl.versions.latest_version();
            for level in 1..logs::NUM_LEVELS {
                if current.overlap_in_level(level, &UserKey::new(begin.as_ref()), &UserKey::new(end.as_ref())) {
                    max_level_with_files = level;
                }
            }
        }

        for level in 0..max_level_with_files {
            self.compact_range_for_test(level, begin.as_ref(), end.as_ref());
        }
    }

    fn compact_range_for_test(self: &Arc<Self>, level: usize, begin: &str, end: &str) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let manual = Arc::new(ManualCompaction {
            level,
            begin: InternalKey::restore(begin, MAX_SEQUENCE_NUMBER, ValueType::Insertion),
            end: InternalKey::restore(end, 0, ValueType::Deletion),
            done: tx,
        });

        let mut db_impl = self.db_impl.lock();
        while !rx.is_empty() {
            if db_impl.manual_compaction.is_none() {
                db_impl.manual_compaction = Some(manual.clone());
                self.maybe_schedule_compaction();
            } else {
                self.background_work_finished_signal.wait(&mut db_impl)
            }
        }
        db_impl.manual_compaction = None;
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
                log::info!("current memory table is full, waiting...");
                self.background_work_finished_signal.wait(&mut database);
                log::info!("resume memory table operation...");
            } else if database.versions.num_level_files(0) >= logs::L0_STOP_WRITES_TRIGGER {
                log::info!("too many L0 files, waiting...");
                self.background_work_finished_signal.wait(&mut database);
            } else {
                let new_log_number = database.versions.get_new_file_number();
                let status = WritableFile::open(filename::make_log_file_name(self.name.as_str(), new_log_number));
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

                self.has_imm.store(true, Ordering::Release);
                database.immutable = Some(database.mutable.clone());
                database.mutable = Arc::new(MemoryTable::new());
                self.maybe_schedule_compaction();
            }
        }
        Ok(())
    }

    fn maybe_schedule_compaction(&self) {
        let database = self.db_wrap.clone();
        if database.match_compaction_condition() {
            Self::background_call(database, self.dispatcher.clone());
        }
    }

    fn background_call(database: DatabaseWrap, dispatcher: schedule::Dispatcher) {
        let dispatcher_ = dispatcher.clone();
        dispatcher_.dispatch(Box::new(move || {
            database.background_compaction();

            if database.match_compaction_condition() {
                Self::background_call(database.clone(), dispatcher);
            }
            database.background_work_finished_signal.notify_all();
        }));
    }
}

#[derive(Clone)]
pub struct DatabaseWrap {
    name: String,
    options: Options,
    db_impl: Arc<Mutex<DatabaseImpl>>,
    has_imm: Arc<AtomicBool>,
    background_work_finished_signal: Arc<Condvar>,
    background_compaction_scheduled: Arc<AtomicBool>,
}

impl DatabaseWrap {
    fn new(name: String, options: Options, db_impl: Arc<Mutex<DatabaseImpl>>) -> Self {
        Self {
            name,
            options,
            db_impl,
            has_imm: Arc::new(AtomicBool::new(false)),
            background_work_finished_signal: Arc::new(Condvar::new()),
            background_compaction_scheduled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn match_compaction_condition(&self) -> bool {
        if self.background_compaction_scheduled.load(Ordering::Relaxed) {
            return false;
        } else if !self.has_imm.load(Ordering::Acquire) {
            let db = self.db_impl.lock();
            if !db.versions.needs_compaction() {
                return false;
            }
        }
        self.background_compaction_scheduled.store(true, Ordering::Relaxed);
        true
    }

    fn background_compaction(&self) {
        if self.has_imm.load(Ordering::Acquire) {
            self.compact_memory_table();
            self.background_compaction_scheduled.store(false, Ordering::Relaxed);
            return;
        }

        let mut db = self.db_impl.lock();
        let manual_compaction = db.manual_compaction.clone();
        let compaction_proposal = if let Some(manual) = manual_compaction.clone() {
            db.versions.compact_range(manual.level, &manual.begin, &manual.end)
        } else {
            db.versions.pick_compaction()
        };

        if let Some(mut compaction) = compaction_proposal {
            if db.manual_compaction.is_none() && compaction.is_trivial_move() {
                let level = compaction.level;
                let file = compaction.inputs[0][0].clone();
                compaction.edit.remove_file(level, file.number);
                compaction.edit.add_file(level + 1, file.as_ref().clone());
                db.versions.log_and_apply(&mut compaction.edit).unwrap();
                log::info!("Moved {0} to level{1} {2} bytes: {3}", file.number, level + 1, file.file_size, db.versions.level_summary());
            } else {
                let mut compact = CompactionState::new(&mut compaction);
                compact.smallest_snapshot = db.versions.get_last_sequence();
                let input = db.versions.make_input_iterator(compact.compaction);

                drop(db);
                let _ = self.do_compaction_work(&mut compact, &input);
            }
        }
        self.background_compaction_scheduled.store(false, Ordering::Relaxed);

        if let Some(manual) = manual_compaction {
            manual.done.send(true).unwrap();
        }
    }

    fn compact_memory_table(&self) {
        let mut db = self.db_impl.lock();
        if db.immutable.is_none() {
            return;
        }

        let table = db.immutable.as_ref().unwrap().clone();
        let base = db.versions.latest_version();
        let file_number = db.versions.get_new_file_number();
        let log_file_number = db.log_file_number;

        drop(db);

        let result = DatabaseImpl::write_level0_table(self.options, self.name.as_str(), Some(&base), file_number, &table);
        if result.is_err() {
            return;
        }

        let (level, meta, spend_time) = result.unwrap();
        let file_size = meta.file_size;
        let mut edit = VersionEdit::new();
        edit.set_prev_log_number(0);
        edit.set_log_number(log_file_number);
        edit.add_file(level, meta);

        let mut db = self.db_impl.lock();
        db.stats[level].add(spend_time, 0, file_size);

        let status = db.versions.log_and_apply(&mut edit);
        if status.is_ok() {
            self.has_imm.store(false, Ordering::Release);
            db.immutable = None;
            db.remove_obsolete_files();
        }
    }

    fn do_compaction_work(&self, compact: &mut CompactionState, input: &MergingIterator) -> io::Result<()> {
        let start_micros = now_micros();
        let mut imm_micros = 0;
        let mut result = Ok(());
        let mut last_sequence_for_key = MAX_SEQUENCE_NUMBER;
        let mut current_user_key: Option<Vec<u8>> = None;

        input.seek_to_first();
        while input.is_valid() {
            if self.has_imm.load(Ordering::Relaxed) {
                let imm_start = now_micros();
                self.compact_memory_table();
                self.background_work_finished_signal.notify_all();
                imm_micros += now_micros() - imm_start;
            }

            let internal_key = InternalKey::from(input.key());
            if compact.compaction.should_stop_before(&internal_key) && compact.table_builder.is_some() {
                result = compact.finish_compaction_output_file();
                if result.is_err() { break; }
            }

            let mut drop = false;
            let first_occurrence = match &current_user_key {
                Some(key) => UserKey::compare(key, &internal_key.extract_user_key()).is_ne(),
                None => true
            };
            if first_occurrence {
                current_user_key = Some(internal_key.extract_user_key().to_vec());
                last_sequence_for_key = MAX_SEQUENCE_NUMBER;
            }

            if last_sequence_for_key <= compact.smallest_snapshot {
                drop = true;
            } else if internal_key.extract_value_type() == ValueType::Deletion &&
                internal_key.extract_sequence() <= compact.smallest_snapshot &&
                compact.compaction.is_base_level_for_key(&internal_key.extract_user_key())
            {
                drop = true;
            }

            last_sequence_for_key = internal_key.extract_sequence();

            if !drop {
                if compact.table_builder.is_none() {
                    result = self.open_compaction_output_file(compact);
                    if result.is_err() { break; }
                }

                if compact.add_key(&internal_key, input.value()) >= compact.compaction.get_max_output_file_size() {
                    result = compact.finish_compaction_output_file();
                    if result.is_err() { break; }
                }
            }
            input.next();
        }

        if result.is_ok() && compact.table_builder.is_some() {
            result = compact.finish_compaction_output_file();
        }

        let spend_time = now_micros() - imm_micros - start_micros;
        let mut bytes_read = 0;
        let mut bytes_written = 0;
        for which in 0..2 {
            for i in 0..compact.compaction.inputs[which].len() {
                bytes_read += compact.compaction.inputs[which][i].file_size;
            }
        }
        for i in 0..compact.outputs.len() {
            bytes_written += compact.outputs[i].file_size;
        }
        compact.stats.add(spend_time, bytes_read, bytes_written);

        if result.is_ok() {
            result = self.install_compaction_results(compact);
        }

        result
    }

    fn open_compaction_output_file(&self, compact: &mut CompactionState) -> io::Result<()> {
        let file_number;
        {
            let mut db = self.db_impl.lock();
            file_number = db.versions.get_new_file_number();
        }

        let file_name = filename::make_table_file_name(self.name.as_str(), file_number);
        let mut file_meta = FileMetaData::new();
        file_meta.number = file_number;
        compact.outputs.push(file_meta);
        compact.table_builder = Some(TableBuilder::new(self.options, WritableFile::open(&file_name)?));

        Ok(())
    }

    fn install_compaction_results(&self, compact: &mut CompactionState) -> io::Result<()> {
        log::info!("Compacted {}@{} + {}@{} files => {} bytes",
            compact.compaction.inputs[0].len(), compact.compaction.level,
            compact.compaction.inputs[1].len(), compact.compaction.level + 1,
            compact.total_bytes
        );

        let level = compact.compaction.level;
        for which in 0..2 {
            for input in &compact.compaction.inputs[which] {
                compact.compaction.edit.remove_file(compact.compaction.level + which, input.number);
            }
        }

        for output in &compact.outputs {
            compact.compaction.edit.add_file(level + 1, output.clone());
        }

        let mut db = self.db_impl.lock();
        db.stats[compact.compaction.level + 1].add_by(&compact.stats);
        db.versions.log_and_apply(&mut compact.compaction.edit)
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
    stats: [CompactionStatistics; logs::NUM_LEVELS],
    agents: LinkedList<Arc<Agent>>,
    manual_compaction: Option<Arc<ManualCompaction>>,
}

impl DatabaseImpl {
    pub fn new(path: &str, options: Options) -> Self {
        Self {
            name: String::from(path),
            options,
            write_ahead_logger: None,
            log_file_number: 0,
            mutable: Arc::new(MemoryTable::new()),
            immutable: None,
            versions: VersionSet::new(path, options),
            stats: Default::default(),
            agents: LinkedList::new(),
            manual_compaction: None,
        }
    }

    fn create_manifest(&self) -> io::Result<()> {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name(InternalKey::name());
        edit.set_log_number(0);
        edit.set_next_file_number(2);
        edit.set_last_sequence(0);

        let result: io::Result<()>;
        let manifest = filename::make_manifest_file_name(self.name.as_str(), 1);
        {
            let mut manifest_logger = wal::Writer::new(WritableFile::open(&manifest)?);
            let record = edit.encode();
            result = manifest_logger.add_record(record);
        }

        if result.is_ok() {
            file::set_current_file(self.name.as_str(), 1)?;
        } else {
            fs::remove_file(manifest)?;
        }

        result
    }

    fn recover(&mut self) -> io::Result<VersionEdit> {
        fs::create_dir(&self.name).unwrap_or_default();

        let lock_file_name = filename::make_lock_file_name(self.name.as_str());
        LockFile::open(&lock_file_name)?;

        if fs::metadata(filename::make_current_file_name(self.name.as_str())).is_err() {
            if self.options.create_if_missing {
                log::info!("Creating DB {} since it was missing", self.name);
                self.create_manifest()?;
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, format!("InvalidArgument:{} does not exist(create_if_missing is false)", self.name)));
            }
        } else {
            if self.options.error_if_exists {
                return Err(io::Error::new(io::ErrorKind::Other, format!("InvalidArgument:{} does not exist(create_if_missing is false)", self.name)));
            }
        }

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
                let (level, file_meta, spend_time) = Self::write_level0_table(self.options, self.name.as_str(), None, new_number, &new_table)?;
                self.stats[level].add(spend_time, 0, file_meta.file_size);
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
            let (level, file_meta, spend_time) = Self::write_level0_table(self.options, self.name.as_str(), None, new_number, &new_table)?;
            self.stats[level].add(spend_time, 0, file_meta.file_size);
            edit.add_file(level, file_meta);
        }

        Ok(max_sequence)
    }

    fn write_level0_table(options: Options, db_name: &str, base: Option<&Version>, file_number: u64, table: &MemoryTable) -> io::Result<(usize, FileMetaData, u64)> {
        let start_micros = now_micros();
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

        Ok((level, meta, now_micros() - start_micros))
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

#[cfg(test)]
mod tests {
    use std::{env, fs};
    use std::sync::Arc;
    use parking_lot::Mutex;
    use tempfile::tempdir;
    use crate::core::compaction::{Compaction, CompactionState};
    use crate::core::db::{DatabaseImpl, DatabaseWrap};
    use crate::core::format::{InternalKey, Comparator, UserKey};
    use crate::core::iterator::{LevelIterator, MergingIterator};
    use crate::logs::file;
    use crate::Options;
    use crate::table::table::Table;

    #[test]
    fn test_db_size_compaction() {
        let root_path = env::current_dir().expect("Failed to get root dir");
        let options = Options::default();

        let mut iterator_list: Vec<Box<dyn LevelIterator>> = Vec::new();
        for i in [3, 6, 7] {
            let file_path = root_path.join("data").join(format!("{:06}.ldb", i));
            let file_size = fs::metadata(&file_path).unwrap().len();
            let file = file::ReadableFile::open(file_path).unwrap();
            let table = Table::open(options, file, file_size, None).unwrap();
            iterator_list.push(table.iter());
        }
        let iterator = MergingIterator::new(InternalKey::compare, iterator_list);

        let tmp_dir = tempdir().unwrap();
        let db_path = tmp_dir.path().join("leveldb");
        fs::create_dir(&db_path).unwrap_or_default();

        let db_impl = DatabaseImpl::new(&db_path.to_str().unwrap(), options);
        let version = db_impl.versions.latest_version();
        let db_wrap = DatabaseWrap::new(db_impl.name.clone(), options, Arc::new(Mutex::new(db_impl)));
        let mut compaction = Compaction::new(options, version, 1);
        let mut compaction_state = CompactionState::new(&mut compaction);
        compaction_state.smallest_snapshot = 5000;
        let _ = db_wrap.do_compaction_work(&mut compaction_state, &iterator);

        let file_path = db_path.join("000002.ldb");
        let file_size = fs::metadata(&file_path).unwrap().len();
        let file = file::ReadableFile::open(file_path).unwrap();
        let table = Table::open(Options::default(), file, file_size, None).unwrap();
        let iter = table.iter();
        let mut number = 0;
        let mut last_user_key = Vec::new();
        iter.seek_to_first();
        while iter.is_valid() {
            let (key, value) = (iter.key(), iter.value());
            let internal_key = InternalKey::from(key.clone());
            let user_key = internal_key.extract_user_key();

            assert!(user_key > UserKey::new(&last_user_key));

            let raw_key = String::from_utf8(user_key.to_vec()).unwrap();
            let raw_value = String::from_utf8(value).unwrap();

            if (user_key >= UserKey::new("00200") && user_key <= UserKey::new("00300")) ||
                (user_key >= UserKey::new("00400") && user_key <= UserKey::new("00500")) {
                assert_eq!(format!("l1-{}", raw_key), raw_value);
            } else {
                assert_eq!(format!("l2-{}", raw_key), raw_value);
            }

            last_user_key = user_key.to_vec();

            iter.next();
            number += 1;
        }
        assert_eq!(number, 401);
    }
}
