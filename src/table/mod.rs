pub mod builder;
pub mod table;
pub mod block;
pub mod cache;

const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;
const FILTER_BASE_LOG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LOG;
const BLOCK_TRAILER_SIZE: usize = 5;
const MAX_ENCODED_LENGTH: usize = 10 + 10;
