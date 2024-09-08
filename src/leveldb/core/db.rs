use crate::leveldb::{Options, WriteOptions};
use crate::leveldb::core::batch::WriteBatch;

pub struct Database {
    name: String,
}

impl Database {
    pub fn open<T: AsRef<str>>(path: T, options: Options) -> std::io::Result<Self> {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        log::info!("Opening database at path: {}", path.as_ref());

        let db = Database {
            name: String::new(),
        };

        Ok(db)
    }

    pub fn put<T: AsRef<str>>(&mut self, options: WriteOptions, key: T, value: T) -> std::io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(options, batch)
    }

    pub fn write(&mut self, options: WriteOptions, batch: WriteBatch) -> std::io::Result<()> {
        todo!()
    }
}
