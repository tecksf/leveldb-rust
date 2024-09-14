pub mod filename;
pub mod file;
pub mod wal;

pub const NUM_LEVELS: usize = 7;
pub const MAX_MEMORY_COMPACT_LEVEL: usize = 2;
pub const L0_COMPACTION_TRIGGER: usize = 4;
pub const L0_SLOW_DOWN_WRITES_TRIGGER: usize = 8;
pub const L0_STOP_WRITES_TRIGGER: usize = 12;
