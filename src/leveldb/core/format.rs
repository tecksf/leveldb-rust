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

pub fn pack_sequence_and_type(sequence_number: u64, value_type: ValueType) -> u64 {
    (sequence_number << 8) | value_type as u64
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct UserKey<'a> {
    key: &'a [u8],
}

impl<'a> UserKey<'a> {
    pub fn new<T: AsRef<[u8]> + ?Sized>(text: &'a T) -> Self {
        Self {
            key: text.as_ref()
        }
    }
}

impl<'a> AsRef<[u8]> for UserKey<'a> {
    fn as_ref(&self) -> &[u8] {
        self.key
    }
}


#[derive(Eq, PartialEq)]
pub struct InternalKey {
    key: Vec<u8>,
}

impl InternalKey {
    const MIN_LEN: usize = 8;

    pub fn new<T: AsRef<[u8]> + ?Sized>(text: &T) -> Self {
        let mut key = Vec::<u8>::with_capacity(16);
        key.extend_from_slice(text.as_ref());
        if key.len() < Self::MIN_LEN {
            key.resize(Self::MIN_LEN, 0);
        }

        Self { key }
    }

    pub fn restore<T: AsRef<[u8]>>(user_key: T, sequence_number: u64, value_type: ValueType) -> InternalKey {
        let mut key = Vec::<u8>::with_capacity(user_key.as_ref().len() + 8);
        key.extend_from_slice(user_key.as_ref());
        coding::put_fixed64_into_vec(&mut key, pack_sequence_and_type(sequence_number, value_type));
        Self { key }
    }

    pub fn extract_user_key(&self) -> UserKey {
        UserKey::new(&self.key[..self.key.len() - Self::MIN_LEN])
    }

    pub fn extract_sequence(&self) -> u64 {
        let info = &self.key[self.key.len() - Self::MIN_LEN..];
        let mut sequence = coding::decode_fixed64(info);
        sequence = sequence >> 8;
        sequence
    }

    pub fn extract_value_type(&self) -> ValueType {
        let info = &self.key[self.key.len() - Self::MIN_LEN..];
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
        self.key.as_ref()
    }
}

#[derive(Eq, PartialEq)]
pub struct LookupKey {
    key: Vec<u8>,
}

impl LookupKey {
    pub fn new<T: AsRef<[u8]>>(text: T, sequence_number: u64) -> Self {
        let key_size = text.as_ref().len() + 8;
        let size = key_size + coding::get_variant_length(key_size as u64);

        let mut key = Vec::<u8>::with_capacity(size);
        coding::put_variant32_into_vec(&mut key, key_size as u32);
        key.extend_from_slice(text.as_ref());
        coding::put_fixed64_into_vec(&mut key, pack_sequence_and_type(sequence_number, ValueType::Insertion));
        Self { key }
    }

    pub fn extract_internal_key(&self) -> InternalKey {
        let (key, _) = coding::get_length_prefixed_slice(self.key.as_slice());
        InternalKey::new(key)
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
        self.key.as_ref()
    }
}
