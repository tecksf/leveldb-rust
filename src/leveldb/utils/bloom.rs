use crate::leveldb::FilterPolicy;
use crate::leveldb::utils::common::hash;

pub struct BloomFilterPolicy {
    // k: num of hash
    k: u32,
    bits_per_key: u32,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: u32) -> Self {
        let mut k: u32 = 1;
        let f = bits_per_key as f32 * 0.69;

        if f < 1.0 {
            k = 1;
        }
        if f > 30.0 {
            k = 30;
        }

        Self {
            k,
            bits_per_key,
        }
    }

    pub fn bloom_hash(key: &[u8]) -> u32 {
        hash(key, 0xbc9f1d34)
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> String {
        String::from("leveldb.BuiltinBloomFilter2")
    }

    fn key_may_match(&self, filter: &[u8], key: &[u8]) -> bool {
        let length = filter.len();

        if length < 2 {
            return false;
        }

        let bits = (length - 1) * 8;
        let k = filter[length - 1];
        if k > 30 {
            return true;
        }

        let mut h = Self::bloom_hash(key) as usize;
        let delta = (h >> 17) | (h << 15);

        for _ in 0..k {
            let bit_pos = h % bits;
            if filter[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h += delta;
        }
        true
    }

    fn create_filter(&self, keys: Vec<&[u8]>) -> Vec<u8> {
        let mut bits = keys.len() * self.bits_per_key as usize;
        let mut ans: Vec<u8> = vec![];

        if bits < 64 {
            bits = 64;
        }
        bits = ((bits + 7) / 8) * 8;
        ans.resize(bits / 8, 0);
        ans.push(self.k as u8);

        for key in keys {
            let mut h = Self::bloom_hash(key) as usize;
            let delta: usize = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_ops = h % bits;
                ans[bit_ops / 8] |= 1 << (bit_ops % 8);
                h += delta;
            }
        }

        ans
    }
}

#[cfg(test)]
mod tests {
    use crate::leveldb::FilterPolicy;
    use super::BloomFilterPolicy;

    #[test]
    fn test_bloom_filter() {
        let policy = BloomFilterPolicy::new(10);
        let data = ["Leveldb", "Redis", "Mysql", "Sqlite", "HBase"];
        let bloom = policy.create_filter(data.iter().map(|&x| x.as_bytes()).collect());

        assert_eq!(policy.key_may_match(bloom.as_slice(), "Leveldb".as_bytes()), true);
        assert_eq!(policy.key_may_match(bloom.as_slice(), "Oracle".as_bytes()), false);
    }
}
