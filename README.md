# leveldb Rust

#### Implementation of LevelDB in Rust.

```rust
use leveldb::{Options, WriteOptions};

fn main() {
    let mut options = Options::default();
    options.create_if_missing = true;
    options.reuse_logs = true;
    options.enable_block_cache = true;
    options.enable_filter_policy = true;

    let db = leveldb::DB::open("/tmp/leveldb", options).unwrap();
    db.put(WriteOptions::default(), "hello", "world").unwrap();
}
```
