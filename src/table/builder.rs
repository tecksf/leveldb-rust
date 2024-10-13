use std::io;
use crate::logs::file::WriterView;
use crate::{CompressionType, Options, table, FilterPolicy};
use crate::core::format::{Comparator, InternalKey};
use crate::table::block::{BlockHandle, Footer};
use crate::utils::bloom::{BloomFilterPolicy, InternalFilterPolicy};
use crate::utils::coding;

struct BlockChunk {
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    filter_block: Option<FilterBlockBuilder>,
}

pub struct TableBuilder<T: WriterView> {
    file: T,
    options: Options,
    offset: usize,
    block_trunk: BlockChunk,
    num_entries: u64,
    closed: bool,
    status: io::Result<()>,
    last_key: InternalKey,
    pending_index_handle: Option<BlockHandle>,
}

impl<T: WriterView> TableBuilder<T> {
    pub fn new(options: Options, file: T) -> Self {
        let mut block_trunk = BlockChunk {
            data_block: BlockBuilder::new(options.block_restart_interval),
            index_block: BlockBuilder::new(1),
            filter_block: None,
        };

        if options.enable_filter_policy {
            let filter = InternalFilterPolicy::new(BloomFilterPolicy::new(10));
            block_trunk.filter_block = Some(FilterBlockBuilder::new(Box::new(filter)));
        }

        Self {
            file,
            options,
            block_trunk,
            offset: 0,
            num_entries: 0,
            closed: false,
            status: Ok(()),
            last_key: InternalKey::default(),
            pending_index_handle: None,
        }
    }

    pub fn add<Value: AsRef<[u8]>>(&mut self, internal_key: &InternalKey, value: Value) {
        if !self.is_ok() {
            return;
        }

        // insert one record into index block when write a data block in TableBuilder::flush
        if let Some(index_handle) = self.pending_index_handle {
            let tmp = self.last_key.find_shortest_separator(internal_key);
            let value = index_handle.encode();
            self.block_trunk.index_block.add(tmp.as_slice(), value.as_slice());
            self.pending_index_handle = None;
        }

        if let Some(filter_block) = &mut self.block_trunk.filter_block {
            filter_block.add_key(internal_key.as_ref());
        }

        self.block_trunk.data_block.add(internal_key.as_ref(), value.as_ref());
        self.last_key.assign(internal_key);
        self.num_entries += 1;

        let estimated_lock_size = self.block_trunk.data_block.estimate_current_size();
        if estimated_lock_size >= self.options.block_size {
            self.flush();
        }
    }

    pub fn finish(&mut self) {
        let mut meta_index_block_handle = BlockHandle::default();
        let mut index_block_handle = BlockHandle::default();

        self.flush();
        self.closed = true;

        // filter(meta) block
        if self.block_trunk.filter_block.is_some() {
            let mut filter_block = std::mem::replace(&mut self.block_trunk.filter_block, None).unwrap();
            let filter_block_handle = self.write_raw_block(filter_block.finish(), CompressionType::NoCompression);

            // meta index block
            if self.is_ok() {
                let mut meta_index_block = BlockBuilder::new(self.options.block_restart_interval);
                let key = format!("filter.{}", filter_block.get_policy_name());
                let value = filter_block_handle.encode();
                meta_index_block.add(key.as_bytes(), value.as_slice());

                let block_contents = Vec::from(meta_index_block.finish());
                meta_index_block_handle = self.write_block_contents(block_contents);
            }
        }

        // index block
        if self.is_ok() {
            if let Some(index_handle) = self.pending_index_handle {
                let tmp = self.last_key.find_shortest_successor();
                let value = index_handle.encode();
                self.block_trunk.index_block.add(tmp.as_slice(), value.as_slice());
                self.pending_index_handle = None;
            }

            let block_contents = Vec::from(self.block_trunk.index_block.finish());
            index_block_handle = self.write_block_contents(block_contents);
            self.block_trunk.index_block.reset();
        }

        // footer
        if self.is_ok() {
            let footer = Footer {
                meta_index_block_handle,
                index_block_handle,
            };

            let data = footer.encode();
            self.status = self.file.append(&data).map(|_| ());
            if self.is_ok() {
                self.offset += data.len();
            }
        }

        if self.is_ok() {
            self.status = self.file.sync();
        }
    }

    pub fn is_ok(&self) -> bool {
        self.status.is_ok()
    }

    pub fn file_size(&self) -> u64 {
        self.offset as u64
    }

    fn abandon(&mut self) {
        self.closed = true;
    }

    fn flush(&mut self) {
        if self.block_trunk.data_block.is_empty() {
            return;
        }

        let block_contents = Vec::from(self.block_trunk.data_block.finish());
        self.pending_index_handle = Some(self.write_block_contents(block_contents));
        self.block_trunk.data_block.reset();

        if self.is_ok() {
            self.status = self.file.flush();
        } else {
            self.pending_index_handle = None;
        }

        if let Some(filter_block) = &mut self.block_trunk.filter_block {
            filter_block.start_block(self.offset);
        }
    }

    fn write_block_contents(&mut self, block_contents: Vec<u8>) -> BlockHandle {
        let raw_contents = match self.options.compression {
            CompressionType::NoCompression => block_contents,
            CompressionType::SnappyCompression => block_contents,
        };

        self.write_raw_block(raw_contents.as_slice(), self.options.compression)
    }

    fn write_raw_block(&mut self, block_contents: &[u8], compression: CompressionType) -> BlockHandle {
        let handle = BlockHandle {
            offset: self.offset,
            size: block_contents.len(),
        };

        self.status = self.file.append(block_contents).map(|_| ());
        if self.is_ok() {
            let mut trailer: [u8; table::BLOCK_TRAILER_SIZE] = [0; table::BLOCK_TRAILER_SIZE];
            trailer[0] = compression as u8;
            let crc = crc32c::crc32c(block_contents);
            coding::put_fixed32(&mut trailer[1..], crc);

            self.status = self.file.append(&trailer[..]).map(|_| ());
            if self.is_ok() {
                self.offset += table::BLOCK_TRAILER_SIZE + block_contents.len();
            }
        }

        handle
    }
}

pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<usize>,
    counter: u32,
    finished: bool,
    last_key: Vec<u8>,
    restart_interval: u32,
}

impl BlockBuilder {
    pub fn new(restart_interval: u32) -> Self {
        let mut restarts = Vec::<usize>::with_capacity(8);
        restarts.push(0);

        Self {
            buffer: Vec::with_capacity(128),
            restarts,
            counter: 0,
            finished: false,
            last_key: Vec::with_capacity(16),
            restart_interval,
        }
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
        self.restarts.clear();
        self.restarts.push(0);
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let last_key_piece = self.last_key.as_slice();
        let mut shared_length = 0;

        if self.counter < self.restart_interval {
            let min_length = std::cmp::min(last_key_piece.len(), key.len());
            while shared_length < min_length && last_key_piece[shared_length] == key[shared_length] {
                shared_length += 1;
            }
        } else {
            self.counter = 0;
            self.restarts.push(self.buffer.len());
        }
        let non_shared = key.len() - shared_length;

        coding::put_variant32_into_vec(&mut self.buffer, shared_length as u32);
        coding::put_variant32_into_vec(&mut self.buffer, non_shared as u32);
        coding::put_variant32_into_vec(&mut self.buffer, value.len() as u32);
        self.buffer.extend_from_slice(&key[shared_length..]);
        self.buffer.extend_from_slice(value);

        self.last_key.truncate(shared_length);
        self.last_key.extend_from_slice(&key[shared_length..]);
        self.counter += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        self.buffer.reserve(self.estimate_current_size());
        for &n in &self.restarts {
            coding::put_fixed32_into_vec(&mut self.buffer, n as u32);
        }
        coding::put_fixed32_into_vec(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        &self.buffer[..]
    }

    pub fn estimate_current_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

pub struct FilterBlockBuilder {
    keys: Vec<u8>,
    key_offsets: Vec<usize>,
    bloom_filter_results: Vec<u8>,
    filter_offsets: Vec<usize>,
    policy: Box<dyn FilterPolicy>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Box<dyn FilterPolicy>) -> Self {
        Self {
            keys: Vec::with_capacity(128),
            key_offsets: Vec::with_capacity(16),
            bloom_filter_results: Vec::with_capacity(128),
            filter_offsets: Vec::with_capacity(16),
            policy,
        }
    }

    pub fn start_block(&mut self, block_offset: usize) {
        // block_offset represents data block size, one filter index per 2KB block
        let filter_index_count = block_offset / table::FILTER_BASE;
        while filter_index_count > self.filter_offsets.len() {
            self.generate_filter();
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.key_offsets.push(self.keys.len());
        self.keys.extend_from_slice(key);
    }

    pub fn finish(&mut self) -> &[u8] {
        if !self.key_offsets.is_empty() {
            self.generate_filter();
        }

        let filter_offset_in_block = self.bloom_filter_results.len() as u32;
        self.filter_offsets.iter().for_each(|&offset| coding::put_fixed32_into_vec(&mut self.bloom_filter_results, offset as u32));
        coding::put_fixed32_into_vec(&mut self.bloom_filter_results, filter_offset_in_block);
        self.bloom_filter_results.push(table::FILTER_BASE_LOG as u8);

        &self.bloom_filter_results[..]
    }

    pub fn get_policy_name(&self) -> String {
        self.policy.name()
    }

    fn generate_filter(&mut self) {
        let num_keys = self.key_offsets.len();
        if num_keys == 0 {
            self.filter_offsets.push(self.bloom_filter_results.len());
            return;
        }

        self.key_offsets.push(self.keys.len());
        let mut tmp_keys: Vec<&[u8]> = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let (begin, end) = (self.key_offsets[i], self.key_offsets[i + 1]);
            tmp_keys.push(&self.keys[begin..end]);
        }

        let result = self.policy.create_filter(tmp_keys);
        self.filter_offsets.push(self.bloom_filter_results.len());
        self.bloom_filter_results.extend(result);

        self.keys.clear();
        self.key_offsets.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::core::format::{InternalKey, ValueType};
    use super::{BlockBuilder, TableBuilder};
    use crate::Options;
    use crate::logs::file::tests::WritableMemory;
    use crate::table::block::Footer;
    use crate::utils::coding;

    #[test]
    fn test_data_block_constructor() {
        let expected_results: [(u32, u32, u32, &str, &str); 8] = [
            (0, 3, 3, "abr", "100"),
            (3, 3, 3, "ade", "200"),
            (4, 3, 3, "ham", "300"),
            (7, 2, 3, "ic", "400"),
            (0, 6, 3, "abrash", "500"),
            (5, 3, 3, "ion", "600"),
            (6, 2, 3, "ve", "700"),
            (3, 3, 3, "oad", "800"),
        ];
        let mut ans = Vec::<u8>::with_capacity(128);
        let mut offset = 0;
        let mut restarts = Vec::<usize>::with_capacity(4);
        for (index, res) in expected_results.iter().enumerate() {
            coding::put_variant32_into_vec(&mut ans, res.0);
            coding::put_variant32_into_vec(&mut ans, res.1);
            coding::put_variant32_into_vec(&mut ans, res.2);
            ans.extend_from_slice(res.3.as_bytes());
            ans.extend_from_slice(res.4.as_bytes());
            if index % 4 == 0 {
                restarts.push(offset as usize);
            }
            offset += 3 + res.1 + res.2;
        }
        restarts.iter().for_each(|&n| coding::put_fixed32_into_vec(&mut ans, n as u32));
        coding::put_fixed32_into_vec(&mut ans, restarts.len() as u32);
        assert_eq!(restarts.len(), 2);

        let data: [(&str, &str); 8] = [
            ("abr", "100"),
            ("abrade", "200"),
            ("abraham", "300"),
            ("abrahamic", "400"),
            ("abrash", "500"),
            ("abrasion", "600"),
            ("abrasive", "700"),
            ("abroad", "800"),
        ];
        let mut data_builder = BlockBuilder::new(4);
        for (key, value) in data {
            data_builder.add(key.as_bytes(), value.as_bytes());
        }
        data_builder.finish();

        assert_eq!(data_builder.buffer, ans.as_slice());
        assert_eq!(data_builder.counter, 4);
        assert_eq!(data_builder.last_key, data[data.len() - 1].0.as_bytes());
        assert_eq!(data_builder.restarts, restarts);
        assert_eq!(data_builder.finished, true);
    }

    #[test]
    fn test_index_block_constructor() {
        let data = [
            ("data_block1", "123"),
            ("data_block2", "456"),
            ("data_block3", "789"),
        ];

        let mut index_builder = BlockBuilder::new(1);
        for (key, value) in data {
            index_builder.add(key.as_bytes(), value.as_bytes());
        }
        let index_block = index_builder.finish();

        let num_restarts = coding::decode_fixed32(&index_block[index_block.len() - 4..]);
        assert_eq!(num_restarts, 3);
        let mut restart_offset = index_block.len() - 4 * 4;

        let mut offset = 0;
        for i in 0..num_restarts as usize {
            let restart = coding::decode_fixed32(&index_block[restart_offset..restart_offset + 4]);
            let entry = &index_block[restart as usize..];
            assert_eq!(restart, offset);
            let (shared_len, non_shared_len, value_len) = (entry[0], entry[1], entry[2]);
            assert_eq!(shared_len, 0);
            assert_eq!(non_shared_len as usize, data[i].0.len());
            assert_eq!(value_len as usize, data[i].1.len());
            assert_eq!(&entry[3..(non_shared_len + 3) as usize], data[i].0.as_bytes());
            assert_eq!(&entry[(non_shared_len + 3) as usize..(non_shared_len + value_len + 3) as usize], data[i].1.as_bytes());

            restart_offset += 4;
            offset += (3 + non_shared_len + value_len) as u32;
        }

        assert_eq!(index_block.len() - 4 * 4, offset as usize);
    }

    #[test]
    fn test_sst_file_builder() {
        let data = [
            ("all", "100"),
            ("alliance", "101"),
            ("allow", "102"),
            ("ally", "103"),
            ("almost", "104"),
            ("design", "105"),
            ("designer", "106"),
            ("desire", "107"),
            ("desk", "108"),
            ("develop", "109"),
            ("development", "110"),
        ];

        let mut result = Vec::<u8>::with_capacity(256);
        let mut options = Options::default();
        options.block_restart_interval = 4;
        let file = WritableMemory::new(&mut result);
        let mut builder = TableBuilder::new(options, file);
        for (key, value) in data {
            let internal_key = InternalKey::restore(key, 8, ValueType::Insertion);
            builder.add(&internal_key, value);
        }
        builder.finish();

        let size = result.len();
        let footer = Footer::decode_from(&result[size - Footer::ENCODED_LENGTH..]).unwrap();
        assert_eq!(footer.meta_index_block_handle.offset, 0);
        assert_eq!(footer.meta_index_block_handle.size, 0);

        let index_block = &result[footer.index_block_handle.offset..footer.index_block_handle.offset + footer.index_block_handle.size];
        assert_eq!(coding::decode_fixed32(&index_block[index_block.len() - 4..]), 1);
        assert_eq!(coding::decode_fixed32(&index_block[index_block.len() - 8..index_block.len() - 4]), 0);
    }
}
