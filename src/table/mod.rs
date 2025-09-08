use std::hash::Hash;
use std::sync::Arc;

pub mod builder;
pub mod table;
pub mod block;
pub mod cache;

const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;
const FILTER_BASE_LOG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LOG;
const BLOCK_TRAILER_SIZE: usize = 5;
const MAX_ENCODED_LENGTH: usize = 10 + 10;

pub trait HashKey: Hash {
    fn hash_key(&self) -> u32;
}

pub trait Usage {
    fn usage(&self) -> usize;
}

impl<T: Usage> Usage for Arc<T> {
    fn usage(&self) -> usize {
        self.as_ref().usage()
    }
}

macro_rules! derive_usage_for_primitive {
    ($($t:ty),*) => {
        $(
            impl Usage for $t {
                fn usage(&self) -> usize {
                    1
                }
            }
        )*
    };
}

derive_usage_for_primitive!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, bool, String);
