use std::io;
use std::rc::Rc;
use crate::leveldb::logs::file::RandomReaderView;
use crate::leveldb::{CompressionType, Options};
use crate::leveldb::table::block::{Block, BlockHandle, Footer};
use crate::leveldb::utils::coding;

pub struct Table<T> {
    options: Options,
    file: Rc<T>,
    index_block: Block,
}

impl<T: RandomReaderView> Table<T> {
    pub fn open(options: Options, file: T, size: u64) -> io::Result<Self> {
        let mut buffer = [0; Footer::ENCODED_LENGTH];
        file.read(size - Footer::ENCODED_LENGTH as u64, Footer::ENCODED_LENGTH, &mut buffer[..])?;

        let footer = Footer::decode_from(&buffer).map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?;
        let result = Self::read_block(options.paranoid_checks, &file, &footer.index_block_handle)?;
        let index_block = Block::new(result)?;

        Ok(Self {
            options,
            file: Rc::new(file),
            index_block,
        })
    }

    fn read_data_block(&self, index_block_handle: &BlockHandle) -> io::Result<Block> {
        let result = Self::read_block(self.options.paranoid_checks, &self.file, index_block_handle)?;
        Block::new(result)
    }

    fn read_block(verify_checksum: bool, file: &T, block_handle: &BlockHandle) -> io::Result<Vec<u8>> {
        let target_size = block_handle.size + super::BLOCK_TRAILER_SIZE;
        let mut result = vec![0; target_size];
        file.read(block_handle.offset as u64, target_size, result.as_mut_slice())?;

        if result.len() != target_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated block read"));
        }

        if verify_checksum {
            let actual_crc = crc32c::crc32c(result.as_slice());
            let expected_crc = coding::decode_fixed32(&result[block_handle.size + 1..]);
            if expected_crc != actual_crc {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "block checksum mismatch"));
            }
        }

        let compression_type: CompressionType = result[block_handle.size].into();
        match compression_type {
            CompressionType::NoCompression => {}
            CompressionType::SnappyCompression => {}
        }
        result.truncate(block_handle.size);

        Ok(result)
    }
}
