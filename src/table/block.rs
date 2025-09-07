use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;
use crate::{table, FilterPolicy};
use crate::core::iterator::LevelIterator;
use crate::utils::coding;

#[derive(Copy, Clone, Default, Eq, PartialEq, Debug)]
pub struct BlockHandle {
    pub offset: usize,
    pub size: usize,
}

impl BlockHandle {
    pub fn decode_from<T: AsRef<[u8]>>(data: T) -> Result<Self, String> {
        let data = data.as_ref();
        let (offset, offset_width) = coding::decode_variant64(data);
        if offset_width == 0 {
            return Err("bad block handle".to_string());
        }

        let (size, size_width) = coding::decode_variant64(&data[offset_width as usize..]);
        if size_width == 0 {
            return Err("bad block handle".to_string());
        }

        let handle = Self {
            offset: offset as usize,
            size: size as usize,
        };

        Ok(handle)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut result = Vec::<u8>::with_capacity(table::MAX_ENCODED_LENGTH);
        coding::put_variant64_into_vec(&mut result, self.offset as u64);
        coding::put_variant64_into_vec(&mut result, self.size as u64);

        result
    }
}

#[derive(Clone)]
pub struct Block {
    data: Arc<Vec<u8>>,
    restart_offset: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> io::Result<Self> {
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "block size is less than 4 bytes"));
        }
        let mut block = Block { data: Arc::new(data), restart_offset: 0 };
        block.restart_offset = block.data.len() - ((1 + block.num_restarts()) * 4) as usize;
        Ok(block)
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn num_restarts(&self) -> u32 {
        coding::decode_fixed32(&self.data[self.data.len() - 4..])
    }

    pub fn iter(&self, compare: fn(&[u8], &[u8]) -> Ordering) -> BlockIterator {
        BlockIterator::new(self.data.clone(), self.restart_offset, self.num_restarts() as usize, compare)
    }
}

pub struct BlockIterator {
    data: Arc<Vec<u8>>,
    restart_offset: usize,
    num_restarts: usize,
    restart_index: Cell<usize>,
    entry_offset: Cell<usize>,
    key: RefCell<Vec<u8>>,
    value: RefCell<Vec<u8>>,
    compare: fn(&[u8], &[u8]) -> Ordering,
}

impl BlockIterator {
    fn new(data: Arc<Vec<u8>>, restart_offset: usize, num_restarts: usize, compare: fn(&[u8], &[u8]) -> Ordering) -> Self {
        Self {
            data,
            restart_offset,
            num_restarts,
            restart_index: Cell::new(0),
            entry_offset: Cell::new(0),
            key: RefCell::new(Vec::new()),
            value: RefCell::new(Vec::new()),
            compare,
        }
    }

    pub fn check_key<V>(&self, mut verify: V) -> bool
        where V: FnMut(&[u8]) -> bool
    {
        let key = self.key.borrow();
        verify(key.as_slice())
    }

    fn get_restart_point(&self, index: usize) -> usize {
        if index >= self.num_restarts {
            return self.restart_offset;
        }
        coding::decode_fixed32(&self.data[(self.restart_offset + index * 4)..]) as usize
    }

    fn seek_to_restart_point(&self, index: usize) {
        self.restart_index.set(index);
        self.entry_offset.set(self.get_restart_point(index));
        self.key.borrow_mut().clear();
        self.value.borrow_mut().clear();
    }

    fn decode_entry(data: &[u8]) -> Option<(usize, usize, usize, &[u8])> {
        if data.len() < 3 {
            return None;
        }

        let mut offset: usize = 0;
        let (mut shared_len, mut non_shared_len, mut value_len) = (data[0] as u32, data[1] as u32, data[2] as u32);
        if (shared_len | non_shared_len | value_len) < 128 {
            offset = 3;
        } else {
            let mut w: u8;

            (shared_len, w) = coding::decode_variant32(data);
            if w == 0 { return None; }
            offset += w as usize;

            (non_shared_len, w) = coding::decode_variant32(&data[offset..]);
            if w == 0 { return None; }
            offset += w as usize;

            (value_len, w) = coding::decode_variant32(&data[offset..]);
            if w == 0 { return None; }
            offset += w as usize;
        }

        if data.len() - offset < (non_shared_len + value_len) as usize {
            return None;
        }

        Some((shared_len as usize, non_shared_len as usize, value_len as usize, &data[offset..]))
    }

    fn parse_next_key(&self) -> bool {
        self.entry_offset.set(self.entry_offset.get() + self.value.borrow().len());

        if !self.is_valid() {
            self.seek_to_restart_point(self.num_restarts);
            return false;
        }

        let current_offset = self.entry_offset.get();
        let entry = &self.data[current_offset..];
        if let Some((shared_len, non_shared_len, value_len, rest)) = Self::decode_entry(entry) {
            let mut key = self.key.borrow_mut();
            key.truncate(shared_len);
            key.extend_from_slice(&rest[..non_shared_len]);
            let mut value = self.value.borrow_mut();
            value.clear();
            value.extend_from_slice(&rest[non_shared_len..non_shared_len + value_len]);

            while self.restart_index.get() + 1 < self.num_restarts {
                let restart_point = self.get_restart_point(self.restart_index.get() + 1);
                if restart_point >= current_offset {
                    break;
                }
                self.restart_index.set(self.restart_index.get() + 1);
            }

            self.entry_offset.set(current_offset + entry.len() - rest.len() + non_shared_len);
            return true;
        }

        false
    }
}

impl LevelIterator for BlockIterator {
    fn is_valid(&self) -> bool {
        self.entry_offset.get() < self.restart_offset
    }

    fn key(&self) -> Vec<u8> {
        Vec::from(self.key.borrow().as_slice())
    }

    fn value(&self) -> Vec<u8> {
        Vec::from(self.value.borrow().as_slice())
    }

    fn next(&self) -> bool {
        self.parse_next_key()
    }

    fn seek(&self, target: &[u8]) -> bool {
        let mut left = 0;
        let mut right = self.num_restarts - 1;

        let mut result = Ordering::Equal;
        if self.is_valid() {
            result = (self.compare)(self.key.borrow().as_slice(), target);
            if result.is_lt() {
                left = self.restart_index.get();
            } else if result.is_gt() {
                right = self.restart_index.get();
            } else {
                return true;
            }
        }

        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
            let entry = &self.data[region_offset..];

            if let Some((shared_len, non_shared_len, _, rest)) = Self::decode_entry(entry) {
                if shared_len != 0 {
                    return false;
                }

                let mid_key = &rest[..non_shared_len];
                if (self.compare)(mid_key, target).is_lt() {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            }
        }

        let skip_seek = left == self.restart_index.get() && result.is_lt();
        if !skip_seek {
            self.seek_to_restart_point(left);
        }

        while self.parse_next_key() {
            if (self.compare)(self.key.borrow().as_slice(), target).is_ge() {
                return true;
            }
        }
        false
    }

    fn seek_to_first(&self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    fn seek_to_last(&self) {
        todo!()
    }
}

pub struct Filter {
    data: Vec<u8>,
    policy: Box<dyn FilterPolicy>,
    filter_offset_start: usize,
    filter_num: usize,
    base_log: u8,
}

impl Filter {
    pub fn new(data: Vec<u8>, policy: Box<dyn FilterPolicy>) -> io::Result<Self> {
        let length = data.len();
        if length < 5 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "block size is less than 5 bytes"));
        }

        let base_log = data[length - 1];
        let filter_offset_start = coding::decode_fixed32(&data[length - 5..]) as usize;
        if filter_offset_start > length - 5 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "block structure is unbalanced"));
        }
        let filter_num = (length - 5 - filter_offset_start) / 4;

        Ok(Self { data, policy, filter_offset_start, filter_num, base_log })
    }

    pub fn key_may_match(&self, block_offset: usize, key: &[u8]) -> bool {
        let index = block_offset >> self.base_log;
        if index < self.filter_num {
            let start = coding::decode_fixed32(&self.data[self.filter_offset_start + index * 4..]) as usize;
            let limit = coding::decode_fixed32(&self.data[self.filter_offset_start + index * 4 + 4..]) as usize;
            if start <= limit && limit <= self.filter_offset_start {
                let filter_data = &self.data[start..limit];
                return self.policy.key_may_match(filter_data, key);
            } else if start == limit {
                return false;
            }
        }
        true
    }
}

pub struct Footer {
    pub meta_index_block_handle: BlockHandle,
    pub index_block_handle: BlockHandle,
}

impl Footer {
    pub const ENCODED_LENGTH: usize = 2 * table::MAX_ENCODED_LENGTH + 8;

    pub fn decode_from(data: &[u8]) -> Result<Self, String> {
        if data.len() < Self::ENCODED_LENGTH {
            return Err("not an sstable (footer too short)".to_string());
        }

        let magic = &data[Self::ENCODED_LENGTH - 8..];
        let magic_low = coding::decode_fixed32(&magic[0..4]);
        let magic_high = coding::decode_fixed32(&magic[4..8]);
        if ((magic_high as u64) << 32 | magic_low as u64) != table::TABLE_MAGIC_NUMBER {
            return Err("not an sstable (bad magic number)".to_string());
        }

        let meta_index_block_handle = BlockHandle::decode_from(data)?;
        let start = coding::get_variant_length(meta_index_block_handle.offset as u64)
            + coding::get_variant_length(meta_index_block_handle.size as u64);
        let index_block_handle = BlockHandle::decode_from(&data[start..])?;

        Ok(Self { meta_index_block_handle, index_block_handle })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut result = Vec::<u8>::with_capacity(48);
        result.extend(self.meta_index_block_handle.encode());
        result.extend(self.index_block_handle.encode());

        // padding
        result.resize(table::MAX_ENCODED_LENGTH * 2, 0);

        coding::put_fixed32_into_vec(&mut result, (table::TABLE_MAGIC_NUMBER & 0xffffffff) as u32);
        coding::put_fixed32_into_vec(&mut result, (table::TABLE_MAGIC_NUMBER >> 32) as u32);

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::core::format::{Comparator, UserKey};
    use crate::core::iterator::LevelIterator;
    use crate::table::builder::BlockBuilder;
    use super::{Block, BlockHandle, Footer};

    fn create_block_data() -> Vec<u8> {
        let data: [(&str, &str); 8] = [
            ("123", "Data001"),
            ("1234", "Data002"),
            ("12345", "Data003"),
            ("123456", "Data004"),
            ("123abc", "Data005"),
            ("123bcd", "Data006"),
            ("123efg", "Data007"),
            ("123efh", "Data008"),
        ];

        let mut builder = BlockBuilder::new(3);
        for (key, value) in data {
            builder.add(key.as_bytes(), value.as_bytes());
        }
        Vec::from(builder.finish())
    }

    #[test]
    fn test_footer_block() {
        let meta_index_block_handle = BlockHandle { offset: 1234, size: 12800 };
        let index_block_handle = BlockHandle { offset: 2048, size: 65535 };
        let footer = Footer { meta_index_block_handle, index_block_handle };
        let ans = footer.encode();

        let result = Footer::decode_from(&ans);
        assert!(result.is_ok());

        let f = result.unwrap();
        assert_eq!(f.meta_index_block_handle, meta_index_block_handle);
        assert_eq!(f.index_block_handle, index_block_handle);
    }

    #[test]
    fn test_block_retrieve_key() {
        let block = Block::new(create_block_data()).unwrap();
        let iter = block.iter(UserKey::compare);
        let mut index = 0;
        let expected = ["Data001", "Data002", "Data003", "Data004", "Data005", "Data006", "Data007", "Data008"];

        while iter.next() {
            assert_eq!(iter.value(), expected[index].as_bytes());
            index += 1;
        }
        assert_eq!(index, expected.len());

        assert!(!iter.seek("abc".as_bytes()));
        assert!(iter.seek("1234".as_bytes()));
        assert_eq!(iter.value(), "Data002".as_bytes());

        index = 0;
        iter.seek_to_first();
        while iter.is_valid() {
            assert_eq!(iter.value(), expected[index].as_bytes());
            iter.next();
            index += 1;
        }
        assert_eq!(index, expected.len());

        assert!(iter.seek("123bcd".as_bytes()));
        assert_eq!(iter.value(), "Data006".as_bytes());

        assert!(iter.seek("123efg".as_bytes()));
        assert_eq!(iter.value(), "Data007".as_bytes());
    }
}
