use crate::core::format::ValueType;
use crate::utils::coding;

const PAYLOAD_HEADER_SIZE: usize = 12;
const PAYLOAD_SEQUENCE_OFFSET: usize = 0;
const PAYLOAD_COUNT_OFFSET: usize = 8;


pub struct WriteBatch {
    payload: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            payload: vec![0; PAYLOAD_HEADER_SIZE]
        }
    }

    pub fn make_from(payload: Vec<u8>) -> Self {
        Self {
            payload
        }
    }

    pub fn get_sequence(&self) -> u64 {
        let sequence_offset: &[u8] = &self.payload[PAYLOAD_SEQUENCE_OFFSET..PAYLOAD_SEQUENCE_OFFSET + 8];
        coding::decode_fixed64(sequence_offset)
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        let sequence_offset: &mut [u8] = &mut self.payload[PAYLOAD_SEQUENCE_OFFSET..PAYLOAD_SEQUENCE_OFFSET + 8];
        coding::put_fixed64(sequence_offset, sequence);
    }

    pub fn put<T: AsRef<str>>(&mut self, key: T, value: T) {
        self.increase_count(1);
        self.payload.push(ValueType::Insertion as u8);
        self.put_slice(key.as_ref());
        self.put_slice(value.as_ref());
    }

    pub fn delete<T: AsRef<str>>(&mut self, key: T) {
        self.increase_count(1);
        self.payload.push(ValueType::Deletion as u8);
        self.put_slice(key.as_ref());
    }

    pub fn extend(&mut self, other: &WriteBatch) {
        let count = other.get_count();
        self.increase_count(count);
        self.payload.extend_from_slice(&other.payload[PAYLOAD_HEADER_SIZE..]);
    }

    pub fn size(&self) -> usize {
        self.payload.len()
    }

    pub fn get_count(&self) -> u32 {
        let count_offset: &[u8] = &self.payload[PAYLOAD_COUNT_OFFSET..PAYLOAD_COUNT_OFFSET + 4];
        coding::decode_fixed32(count_offset)
    }

    pub fn get_payload(&self) -> &[u8] {
        &self.payload[..]
    }

    pub fn set_payload(&mut self, record: &[u8]) {
        self.payload.clear();
        self.payload.extend_from_slice(record);
    }

    fn put_slice(&mut self, slice: &str) {
        let (size, w) = coding::encode_variant32(slice.len() as u32);
        self.payload.extend_from_slice(&size[..w as usize]);
        self.payload.extend_from_slice(slice.as_bytes());
    }

    fn increase_count(&mut self, num: u32) {
        let count_offset: &mut [u8] = &mut self.payload[PAYLOAD_COUNT_OFFSET..PAYLOAD_COUNT_OFFSET + 4];
        let count = coding::decode_fixed32(count_offset);
        coding::put_fixed32(count_offset, count + num);
    }
}

pub struct WriteBatchIterator<'a> {
    data: &'a [u8],
    sequence: u64,
}

impl<'a> Iterator for WriteBatchIterator<'a> {
    type Item = (u64, &'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.data.len() > 0 {
            let tag: ValueType = self.data[0].into();
            let mut offset: usize = 1;
            let s = self.sequence;
            match tag {
                ValueType::Insertion => {
                    let (key, b1) = coding::get_length_prefixed_slice(&self.data[offset..]);
                    offset += key.len() + b1 as usize;
                    let (value, b2) = coding::get_length_prefixed_slice(&self.data[offset..]);
                    offset += value.len() + b2 as usize;

                    self.sequence += 1;
                    self.data = &self.data[offset..];
                    return Some((s, key, value));
                }
                ValueType::Deletion => {
                    let (key, b) = coding::get_length_prefixed_slice(&self.data[offset..]);
                    offset += key.len() + b as usize;

                    self.sequence += 1;
                    self.data = &self.data[offset..];
                    return Some((s, key, &[]));
                }
                _ => break
            };
        }
        None
    }
}

impl<'a> IntoIterator for &'a WriteBatch {
    type Item = (u64, &'a [u8], &'a [u8]);
    type IntoIter = WriteBatchIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            data: &self.payload[PAYLOAD_HEADER_SIZE..],
            sequence: self.get_sequence(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WriteBatch;

    #[test]
    fn test_write_batch_put_and_delete() {
        let data = [
            ("db001", "001"),
            ("db002", "002"),
            ("db003", "003"),
            ("db004", "004"),
            ("db005", "005"),
        ];

        let mut wb = WriteBatch::new();
        wb.set_sequence(100);

        for (key, value) in data {
            wb.put(key, value);
        }
        assert_eq!(data.len() as u32, wb.get_count());

        for (sequence, key, value) in &wb {
            assert_eq!(String::from_utf8_lossy(key).as_ref(), data[(sequence as usize) - 100].0);
            assert_eq!(String::from_utf8_lossy(value).as_ref(), data[(sequence as usize) - 100].1);
        }
    }
}
