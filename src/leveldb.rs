mod core;
mod utils;

pub type DB = core::db::Database;

#[derive(Copy, Clone)]
pub struct Options {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    pub write_buffer_size: usize,
    pub max_open_files: u32,
    pub block_size: usize,
    pub block_restart_interval: u32,
    pub max_file_size: u64,
    pub reuse_logs: bool,
    pub enable_filter_policy: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            create_if_missing: false,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: 4 * 1024 * 1024,
            max_open_files: 1000,
            block_size: 4 * 1024,
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024,
            reuse_logs: false,
            enable_filter_policy: false,
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct WriteOptions {
    pub sync: bool,
}
