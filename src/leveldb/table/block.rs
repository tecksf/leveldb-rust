use crate::leveldb::table;
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
