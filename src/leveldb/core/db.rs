use std::{fs, io};
use fslock::LockFile;
use crate::leveldb::{Options, WriteOptions};
use crate::leveldb::core::batch::WriteBatch;
use crate::leveldb::core::memory::MemoryTable;
use crate::leveldb::logs::file::WritableFile;
use crate::leveldb::logs::{filename, wal};

pub struct Database {
    name: String,
    options: Options,
    log_file_number: u64,
    write_ahead_logger: Option<wal::Writer<WritableFile>>,
    mutable: MemoryTable,
    immutable: Option<MemoryTable>,
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
        if let Some(logger) = &mut self.write_ahead_logger {
            logger.add_record(batch.get_payload())?;
            if options.sync {
                logger.sync()?;
            }
            self.mutable.insert(&batch);
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
}
