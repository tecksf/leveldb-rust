use std::io;
use std::rc::Rc;
use crate::logs::file::RandomReaderView;
use crate::{CompressionType, FilterPolicy, Options};
use crate::core::format::{Comparator, InternalKey, UserKey, ValueType};
use crate::core::iterator::{IteratorGen, LevelIterator, TwoLevelIterator};
use crate::table::block::{Block, BlockHandle, BlockIterator, Filter, Footer};
use crate::utils::bloom::{BloomFilterPolicy, InternalFilterPolicy};
use crate::utils::coding;

pub struct Table<T> {
    options: Options,
    file: Rc<T>,
    index_block: Block,
    filter: Option<Filter>,
}

pub struct BlockIteratorGen<T> {
    file: Rc<T>,
    paranoid_checks: bool,
}

impl<T: RandomReaderView> BlockIteratorGen<T> {
    fn new(file: Rc<T>, paranoid_checks: bool) -> Self {
        Self {
            file,
            paranoid_checks,
        }
    }
}

impl<T: RandomReaderView> IteratorGen for BlockIteratorGen<T> {
    fn gen(&self, data: &[u8]) -> Option<Box<dyn LevelIterator>> {
        let data_block_handle = BlockHandle::decode_from(data).ok()?;
        let result = Table::<T>::read_block(self.paranoid_checks, &self.file, &data_block_handle).ok()?;
        let block = Block::new(result).ok()?;
        Some(Box::new(block.iter(InternalKey::compare)))
    }
}

impl<T: RandomReaderView> Table<T> {
    pub fn open(options: Options, file: T, size: u64) -> io::Result<Self> {
        let mut buffer = [0; Footer::ENCODED_LENGTH];
        file.read(size - Footer::ENCODED_LENGTH as u64, Footer::ENCODED_LENGTH, &mut buffer[..])?;

        let footer = Footer::decode_from(&buffer).map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?;
        let result = Self::read_block(options.paranoid_checks, &file, &footer.index_block_handle)?;
        let index_block = Block::new(result)?;

        let mut filter = None;
        if options.enable_filter_policy && footer.meta_index_block_handle.size > 0 {
            match Self::read_filter_block(options, &file, &footer.meta_index_block_handle) {
                Ok(f) => filter = Some(f),
                Err(err) => {
                    log::info!("cannot load the filter block: {}", err);
                }
            }
        }

        Ok(Self {
            options,
            file: Rc::new(file),
            index_block,
            filter,
        })
    }

    pub fn internal_get(&self, internal_key: &InternalKey) -> io::Result<Option<Vec<u8>>> {
        let index_iter = self.index_block.iter(InternalKey::compare);
        if index_iter.seek(internal_key.as_ref()) {
            let value = index_iter.value();
            let data_block_handle = BlockHandle::decode_from(value).map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?;
            let data_block = self.read_data_block(&data_block_handle)?;

            if let Some(filter) = &self.filter {
                if !filter.key_may_match(data_block_handle.offset, internal_key.as_ref()) {
                    return Err(io::Error::new(io::ErrorKind::NotFound, ""));
                }
            }

            let data_iter = data_block.iter(InternalKey::compare);
            if data_iter.seek(internal_key.as_ref()) {
                let user_key = internal_key.extract_user_key();
                let mut value_type = ValueType::Unknown;
                let verify = |key: &[u8]| -> bool {
                    if key.len() >= 8 {
                        let ik = InternalKey::new(key);
                        let uk = ik.extract_user_key();
                        value_type = ik.extract_value_type();
                        return uk.cmp(&user_key).is_eq();
                    }
                    false
                };

                if data_iter.check_key(verify) {
                    if value_type == ValueType::Insertion {
                        return Ok(Some(data_iter.value()));
                    } else if value_type == ValueType::Deletion {
                        return Ok(None);
                    }
                }
            }
        }

        Err(io::Error::new(io::ErrorKind::NotFound, ""))
    }

    pub fn iter(&self) -> Box<TwoLevelIterator<BlockIterator, BlockIteratorGen<T>>> {
        let generator = BlockIteratorGen::<T>::new(self.file.clone(), self.options.paranoid_checks);
        Box::new(TwoLevelIterator::new(self.index_block.iter(InternalKey::compare), generator))
    }

    fn read_data_block(&self, index_block_handle: &BlockHandle) -> io::Result<Block> {
        let result = Self::read_block(self.options.paranoid_checks, &self.file, index_block_handle)?;
        Block::new(result)
    }

    fn read_filter_block(options: Options, file: &T, meta_index_block_handle: &BlockHandle) -> io::Result<Filter> {
        let meta_index_result = Self::read_block(options.paranoid_checks, &file, &meta_index_block_handle)?;
        let meta_index_block = Block::new(meta_index_result)?;
        let meta_index_iter = meta_index_block.iter(UserKey::compare);

        let policy = Box::new(InternalFilterPolicy::new(BloomFilterPolicy::new(10)));
        let filter_name = format!("filter.{}", policy.name());
        if meta_index_iter.seek(filter_name.as_bytes()) && meta_index_iter.check_key(|k| UserKey::compare(k, filter_name.as_bytes()).is_eq()) {
            let value = meta_index_iter.value();
            let filter_block_handle = BlockHandle::decode_from(value).map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?;
            let filter_result = Self::read_block(options.paranoid_checks, &file, &filter_block_handle)?;
            return Filter::new(filter_result, policy);
        }

        let error_msg = format!("cannot find filter block, offset={}, size={}", meta_index_block_handle.offset, meta_index_block_handle.size);
        Err(io::Error::new(io::ErrorKind::NotFound, error_msg))
    }

    fn read_block(verify_checksum: bool, file: &T, block_handle: &BlockHandle) -> io::Result<Vec<u8>> {
        let target_size = block_handle.size + super::BLOCK_TRAILER_SIZE;
        let mut result = vec![0; target_size];
        let read_len = file.read(block_handle.offset as u64, target_size, result.as_mut_slice())?;

        if read_len != target_size {
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
