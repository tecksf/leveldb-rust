use std::io;
use std::rc::Rc;
use crate::leveldb::{table, FilterPolicy};
use crate::leveldb::utils::coding;

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

pub struct Block {
    data: Rc<Vec<u8>>,
    restart_offset: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> io::Result<Self> {
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "block size is less than 4 bytes"));
        }
        let mut block = Block { data: Rc::new(data), restart_offset: 0 };
        block.restart_offset = block.data.len() - ((1 + block.num_restarts()) * 4) as usize;
        Ok(block)
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn num_restarts(&self) -> u32 {
        coding::decode_fixed32(&self.data[self.data.len() - 4..])
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
