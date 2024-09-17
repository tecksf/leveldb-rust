use crate::leveldb::{Options, WriteOptions};

mod leveldb;

fn main() {
    let mut options = Options::default();
    options.create_if_missing = true;

    let mut db = match leveldb::DB::open("/tmp/leveldb", options) {
        Ok(db) => db,
        Err(reason) => {
            println!("{}", reason);
            return;
        }
    };

    db.put(WriteOptions::default(), "hello", "world").unwrap();
}
