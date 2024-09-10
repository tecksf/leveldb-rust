use std::cmp::Ordering;
use crate::leveldb::utils::coding;

#[repr(u8)]
#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum ValueType {
    Deletion = 0,
    Insertion = 1,
    Unknown = 2,
}

impl From<u8> for ValueType {
    fn from(n: u8) -> Self {
        return if n == 0 {
            ValueType::Deletion
        } else if n == 1 {
            ValueType::Insertion
        } else {
            ValueType::Unknown
        };
    }
}

pub const INSERTION_TYPE_FOR_SEEK: ValueType = ValueType::Insertion;
pub const MAX_SEQUENCE_NUMBER: u64 = (0x1 << 56) - 1;

pub fn pack_sequence_and_type(sequence_number: u64, value_type: ValueType) -> u64 {
    (sequence_number << 8) | value_type as u64
}

pub trait Comparator {
    fn name() -> String;
    fn find_shortest_separator(&self, other: &Self) -> Vec<u8>;
    fn find_shortest_successor(&self) -> Vec<u8>;
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct UserKey<'a> {
    payload: &'a [u8],
}

impl<'a> UserKey<'a> {
    pub fn new<T: AsRef<[u8]> + ?Sized>(text: &'a T) -> Self {
        Self {
            payload: text.as_ref()
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }
}

impl<'a> AsRef<[u8]> for UserKey<'a> {
    fn as_ref(&self) -> &[u8] {
        self.payload
    }
}

impl Comparator for UserKey<'_> {
    fn name() -> String {
        String::from("leveldb.BytewiseComparator")
    }

    fn find_shortest_separator(&self, other: &Self) -> Vec<u8> {
        let min_length = std::cmp::min(self.len(), other.len());
        let mut diff_index = 0;
        while diff_index < min_length && self.payload[diff_index] == other.payload[diff_index] {
            diff_index += 1;
        }

        let mut ans = Vec::from(&self.payload[..diff_index]);
        if diff_index < min_length {
            let diff_byte = self.payload[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < other.payload[diff_index] {
                ans.push(diff_byte + 1);
            }
        }
        ans
    }

    fn find_shortest_successor(&self) -> Vec<u8> {
        let mut ans = Vec::from(self.payload);
        for (i, &n) in self.payload.iter().enumerate() {
            if n != 0xff {
                ans[i] = n + 1;
                ans.truncate(i + 1);
                break;
            }
        }
        ans
    }
}


#[derive(Eq, PartialEq, Clone)]
pub struct InternalKey {
    payload: Vec<u8>,
}

impl InternalKey {
    const MIN_LEN: usize = 8;

    pub fn new<T: AsRef<[u8]> + ?Sized>(text: &T) -> Self {
        let mut payload = Vec::<u8>::with_capacity(16);
        payload.extend_from_slice(text.as_ref());
        if payload.len() < Self::MIN_LEN {
            payload.resize(Self::MIN_LEN, 0);
        }

        Self { payload }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn assign(&mut self, other: &Self) {
        self.payload.clear();
        self.payload.extend_from_slice(&other.payload);
    }

    pub fn restore<T: AsRef<[u8]>>(user_key: T, sequence_number: u64, value_type: ValueType) -> InternalKey {
        let mut payload = Vec::<u8>::with_capacity(user_key.as_ref().len() + 8);
        payload.extend_from_slice(user_key.as_ref());
        coding::put_fixed64_into_vec(&mut payload, pack_sequence_and_type(sequence_number, value_type));
        Self { payload }
    }

    pub fn extract_user_key(&self) -> UserKey {
        UserKey::new(&self.payload[..self.payload.len() - Self::MIN_LEN])
    }

    pub fn extract_sequence(&self) -> u64 {
        let info = &self.payload[self.payload.len() - Self::MIN_LEN..];
        let mut sequence = coding::decode_fixed64(info);
        sequence = sequence >> 8;
        sequence
    }

    pub fn extract_value_type(&self) -> ValueType {
        let info = &self.payload[self.payload.len() - Self::MIN_LEN..];
        let sequence = coding::decode_fixed64(info);
        let value_type = (sequence & 0xff) as u8;
        value_type.into()
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let k1 = self.extract_user_key();
        let k2 = other.extract_user_key();
        let rc = k1.cmp(&k2);
        if rc == Ordering::Equal {
            let n1 = self.extract_sequence();
            let n2 = other.extract_sequence();
            return Some(n2.cmp(&n1));
        }
        return Some(rc);
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl AsRef<[u8]> for InternalKey {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

impl Default for InternalKey {
    fn default() -> Self {
        Self::new("")
    }
}

impl Comparator for InternalKey {
    fn name() -> String {
        String::from("leveldb.InternalKeyComparator")
    }

    fn find_shortest_separator(&self, other: &Self) -> Vec<u8> {
        let start = self.extract_user_key();
        let limit = other.extract_user_key();

        // tmp: ad
        // start: abc
        let mut tmp = start.find_shortest_separator(&limit);
        let tmp_user_key = UserKey::new(&tmp);
        if tmp.len() < start.len() && start.cmp(&tmp_user_key) == Ordering::Less {
            coding::put_fixed64_into_vec(&mut tmp, pack_sequence_and_type(MAX_SEQUENCE_NUMBER, INSERTION_TYPE_FOR_SEEK));
        }
        tmp
    }

    fn find_shortest_successor(&self) -> Vec<u8> {
        let user_key = self.extract_user_key();
        let mut tmp = user_key.find_shortest_successor();
        let tmp_user_key = UserKey::new(&tmp);
        if tmp_user_key.len() < user_key.len() && user_key.cmp(&tmp_user_key).is_lt() {
            coding::put_fixed64_into_vec(&mut tmp, pack_sequence_and_type(MAX_SEQUENCE_NUMBER, INSERTION_TYPE_FOR_SEEK));
        }
        tmp
    }
}

#[derive(Eq, PartialEq)]
pub struct LookupKey {
    payload: Vec<u8>,
}

impl LookupKey {
    pub fn new<T: AsRef<[u8]>>(text: T, sequence_number: u64) -> Self {
        let key_size = text.as_ref().len() + 8;
        let size = key_size + coding::get_variant_length(key_size as u64);

        let mut payload = Vec::<u8>::with_capacity(size);
        coding::put_variant32_into_vec(&mut payload, key_size as u32);
        payload.extend_from_slice(text.as_ref());
        coding::put_fixed64_into_vec(&mut payload, pack_sequence_and_type(sequence_number, ValueType::Insertion));
        Self { payload }
    }

    pub fn extract_internal_key(&self) -> InternalKey {
        let (key, _) = coding::get_length_prefixed_slice(self.payload.as_slice());
        InternalKey::new(key)
    }

    pub fn get_raw_key(&self) -> &[u8] {
        let (internal_key, _) = coding::get_length_prefixed_slice(self.payload.as_slice());
        &internal_key[..internal_key.len() - InternalKey::MIN_LEN]
    }

    pub fn get_raw_value(&self) -> &[u8] {
        let (internal_key, w) = coding::get_length_prefixed_slice(self.payload.as_slice());
        let value_offset = internal_key.len() + w as usize;
        let (value, _) = coding::get_length_prefixed_slice(&self.payload[value_offset..]);
        value
    }
}

impl PartialOrd for LookupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let key1 = self.extract_internal_key();
        let key2 = other.extract_internal_key();
        key1.partial_cmp(&key2)
    }
}

impl Ord for LookupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl AsRef<[u8]> for LookupKey {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

impl Default for LookupKey {
    fn default() -> Self {
        Self { payload: vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] }
    }
}

impl From<Vec<u8>> for LookupKey {
    fn from(value: Vec<u8>) -> Self {
        if value.len() < InternalKey::MIN_LEN + 2 {
            Self::default()
        } else {
            Self { payload: value }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{UserKey, InternalKey, Comparator, ValueType};

    #[test]
    fn test_user_key_shortest_separator() {
        let cases: [(&str, &str, &str); 3] = [
            ("abc", "abk", "abd"),
            ("1234", "123978", "1235"),
            ("java", "javascript", "java"),
        ];

        for (key1, key2, expected) in cases {
            let user_key1 = UserKey::new(key1);
            let user_key2 = UserKey::new(key2);
            assert_eq!(user_key1.find_shortest_separator(&user_key2), expected.as_bytes());
        }
    }

    #[test]
    fn test_user_key_shortest_successor() {
        let cases: [(&str, &str); 2] = [
            ("abc", "b"),
            ("123", "2"),
        ];

        for (key, expected) in cases {
            let user_key = UserKey::new(key);
            assert_eq!(user_key.find_shortest_successor(), expected.as_bytes());
        }

        let user_key = UserKey::new([0xff, 0x01].as_slice());
        assert_eq!(user_key.find_shortest_successor(), [0xff, 0x02]);
    }

    #[test]
    fn test_internal_key_convert_to_user_key() {
        let internal_key = InternalKey::restore("123", 99, ValueType::Insertion);
        let user_key = internal_key.extract_user_key();
        assert_eq!(user_key.as_ref(), "123".as_bytes());
        assert_eq!(internal_key.extract_value_type(), ValueType::Insertion);
        assert_eq!(internal_key.extract_sequence(), 99);
    }

    #[test]
    fn test_internal_key_comparative() {
        let k1 = InternalKey::restore("abc", 1, ValueType::Insertion);
        let k2 = InternalKey::restore("abc", 1, ValueType::Insertion);
        assert!(k1 == k2);

        let k3 = InternalKey::restore("123456789", 1, ValueType::Insertion);
        let k4 = InternalKey::restore("123", 1, ValueType::Insertion);
        assert!(k3 > k4);

        let k5 = InternalKey::restore("123", 1, ValueType::Insertion);
        let k6 = InternalKey::restore("123", 2, ValueType::Insertion);
        assert!(k6 < k5);
    }
}
