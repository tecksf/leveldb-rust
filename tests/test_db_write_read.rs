use std::fs;
use leveldb;
use leveldb::{Options, WriteOptions};
use tempfile::tempdir;


#[test]
fn test_db_write_and_read() {
    let tmp_dir = tempdir().unwrap();
    let db_path = tmp_dir.path().join("leveldb");

    let mut options = Options::default();
    options.create_if_missing = true;
    options.reuse_logs = true;
    options.write_buffer_size = 128;
    options.enable_filter_policy = true;

    let db = leveldb::DB::open(db_path.to_str().unwrap(), options).unwrap();
    let data = [
        ("lang009", "Ruby"),
        ("lang002", "Rust"),
        ("lang001", "C++"),
        ("lang003", "Golang"),
        ("lang004", "Python"),
        ("lang005", "Typescript"),
        ("lang007", "Clojure"),
        ("lang008", "Scala"),
        ("lang006", "Kotlin"),
        ("db02", "Mysql"),
        ("db01", "Oracle"),
        ("db04", "Redis"),
        ("db03", "Sybase"),
        ("db05", "TiDB"),
        ("db05", "MongoDB"),
    ];

    for (key, value) in data {
        db.put(WriteOptions::default(), key, value).unwrap();
    }
    db.delete(WriteOptions::default(), "db01").unwrap();
    db.delete(WriteOptions::default(), "lang005").unwrap();

    assert!(db.get("lang005").is_err());
    assert!(db.get("db01").is_err());
    assert!(db.get("network").is_err());
    assert_eq!(db.get("db05").unwrap(), "MongoDB".as_bytes());
    assert_eq!(db.get("lang002").unwrap(), "Rust".as_bytes());
    assert_eq!(db.get("lang006").unwrap(), "Kotlin".as_bytes());

    let current = db_path.join("CURRENT");
    assert!(current.exists());
    assert_eq!(fs::read_to_string(&current).unwrap(), "MANIFEST-000001");

    let manifest = db_path.join("MANIFEST-000001");
    assert!(manifest.exists());
}
