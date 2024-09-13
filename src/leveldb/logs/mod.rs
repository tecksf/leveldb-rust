pub mod filename;
pub mod file;
pub mod wal;

pub const NUM_LEVELS: usize = 7;
pub const MAX_MEMORY_COMPACT_LEVEL: usize = 2;