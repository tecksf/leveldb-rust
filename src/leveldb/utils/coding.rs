pub fn put_fixed32(dest: &mut [u8], value: u32) {
    let numerical = encode_fixed32(value);
    for i in 0..numerical.len() {
        dest[i] = numerical[i]
    }
}

pub fn put_fixed64(dest: &mut [u8], value: u64) {
    let numerical = encode_fixed64(value);
    for i in 0..numerical.len() {
        dest[i] = numerical[i]
    }
}

pub fn put_variant32(dest: &mut [u8], value: u32) {
    let (numerical, size) = encode_variant32(value);
    for i in 0..size as usize {
        dest[i] = numerical[i]
    }
}

pub fn put_variant64(dest: &mut [u8], value: u64) {
    let (numerical, size) = encode_variant64(value);
    for i in 0..size as usize {
        dest[i] = numerical[i]
    }
}

pub fn put_fixed32_into_vec(dest: &mut Vec<u8>, value: u32) {
    let numerical = encode_fixed32(value);
    dest.extend_from_slice(&numerical[..]);
}

pub fn put_fixed64_into_vec(dest: &mut Vec<u8>, value: u64) {
    let numerical = encode_fixed64(value);
    dest.extend_from_slice(&numerical[..]);
}

pub fn put_variant32_into_vec(dest: &mut Vec<u8>, value: u32) {
    let (numerical, size) = encode_variant32(value);
    dest.extend_from_slice(&numerical[0..size as usize]);
}

pub fn put_variant64_into_vec(dest: &mut Vec<u8>, value: u64) {
    let (numerical, size) = encode_variant64(value);
    dest.extend_from_slice(&numerical[0..size as usize]);
}

pub fn encode_fixed32(value: u32) -> [u8; 4] {
    let mut ans: [u8; 4] = [0; 4];
    ans[0] = (value & 0xff) as u8;
    ans[1] = ((value >> 8) & 0xff) as u8;
    ans[2] = ((value >> 16) & 0xff) as u8;
    ans[3] = ((value >> 24) & 0xff) as u8;
    ans
}

pub fn encode_fixed64(value: u64) -> [u8; 8] {
    let mut ans: [u8; 8] = [0; 8];
    ans[0] = (value & 0xff) as u8;
    ans[1] = ((value >> 8) & 0xff) as u8;
    ans[2] = ((value >> 16) & 0xff) as u8;
    ans[3] = ((value >> 24) & 0xff) as u8;
    ans[4] = ((value >> 32) & 0xff) as u8;
    ans[5] = ((value >> 40) & 0xff) as u8;
    ans[6] = ((value >> 48) & 0xff) as u8;
    ans[7] = ((value >> 56) & 0xff) as u8;
    ans
}

pub fn decode_fixed32(src: &[u8]) -> u32 {
    let mut ans: u32 = 0;

    for i in 0..4 {
        ans = (ans << 8) | (src[3 - i] as u32);
    }
    ans
}

pub fn decode_fixed64(src: &[u8]) -> u64 {
    let mut ans: u64 = 0;

    for i in 0..8 {
        ans = (ans << 8) | (src[7 - i] as u64);
    }
    ans
}

pub fn encode_variant32(value: u32) -> ([u8; 5], u8) {
    const MSB: u8 = 128;

    let mut ans: [u8; 5] = [0; 5];
    let mut i = 0;

    if value < (1 << 7) {
        ans[i] = value as u8;
        i += 1;
    } else if value < (1 << 14) {
        ans[i] = MSB | value as u8;
        i += 1;
        ans[i] = (value >> 7) as u8;
        i += 1;
    } else if value < (1 << 21) {
        ans[i] = MSB | value as u8;
        i += 1;
        ans[i] = MSB | (value >> 7) as u8;
        i += 1;
        ans[i] = (value >> 14) as u8;
        i += 1;
    } else if value < (1 << 28) {
        ans[i] = MSB | value as u8;
        i += 1;
        ans[i] = MSB | (value >> 7) as u8;
        i += 1;
        ans[i] = MSB | (value >> 14) as u8;
        i += 1;
        ans[i] = (value >> 21) as u8;
        i += 1;
    } else {
        ans[i] = MSB | value as u8;
        i += 1;
        ans[i] = MSB | (value >> 7) as u8;
        i += 1;
        ans[i] = MSB | (value >> 14) as u8;
        i += 1;
        ans[i] = MSB | (value >> 21) as u8;
        i += 1;
        ans[i] = (value >> 28) as u8;
        i += 1;
    }

    (ans, i as u8)
}

pub fn encode_variant64(value: u64) -> ([u8; 10], u8) {
    const MSB: u64 = 128;
    let mut val = value;
    let mut ans: [u8; 10] = [0; 10];
    let mut i = 0;

    while val >= MSB {
        ans[i] = (val | MSB) as u8;
        val >>= 7;
        i += 1;
    }
    ans[i] = val as u8;

    (ans, (i + 1) as u8)
}

pub fn decode_variant32(src: &[u8]) -> (u32, u8) {
    let mut result: u32 = 0;
    let (mut shift, mut index): (u8, usize) = (0, 0);
    while shift <= 28 && index < src.len() {
        let byte = src[index];
        if byte & 128 > 0 {
            result |= ((byte & 0x7f) as u32) << shift;
        } else {
            result |= (byte as u32) << shift;
            return (result, (index + 1) as u8);
        }
        index += 1;
        shift += 7;
    }
    return (0, 0);
}

pub fn decode_variant64(src: &[u8]) -> (u64, u8) {
    let mut result: u64 = 0;
    let (mut shift, mut index): (u8, usize) = (0, 0);
    while shift <= 63 && index < src.len() {
        let byte = src[index];
        if byte & 128 > 0 {
            result |= ((byte & 0x7f) as u64) << shift;
        } else {
            result |= (byte as u64) << shift;
            return (result, (index + 1) as u8);
        }
        index += 1;
        shift += 7;
    }
    return (0, 0);
}

pub fn get_variant_length(val: u64) -> usize {
    let mut value = val;
    let mut length = 1;
    while value >= 128 {
        value >>= 7;
        length += 1;
    }
    length
}

pub fn get_length_prefixed_slice(input: &[u8]) -> (&[u8], u8) {
    let (num, width) = decode_variant32(input);
    let (num, width) = (num as usize, width as usize);
    (&input[width..num + width], width as u8)
}

#[cfg(test)]
mod tests {
    use super::{
        put_fixed32, put_fixed64, put_variant32, put_variant64,
        decode_fixed32, decode_fixed64, decode_variant32, decode_variant64,
    };

    #[test]
    fn test_fixed32() {
        let mut dest: [u8; 4] = [0; 4];
        put_fixed32(dest.as_mut_slice(), 0x12fe34dc);
        assert_eq!(dest, [0xdc, 0x34, 0xfe, 0x12]);

        let ans = decode_fixed32(dest.as_slice());
        assert_eq!(ans, 0x12fe34dc);
    }

    #[test]
    fn test_fixed64() {
        let mut dest: [u8; 8] = [0; 8];
        put_fixed64(dest.as_mut_slice(), 0x12fe34dc56ba78ff);
        assert_eq!(dest, [0xff, 0x78, 0xba, 0x56, 0xdc, 0x34, 0xfe, 0x12]);

        let ans = decode_fixed64(dest.as_slice());
        assert_eq!(ans, 0x12fe34dc56ba78ff);
    }

    #[test]
    fn test_variant32() {
        let mut dest: [u8; 5] = [0; 5];

        put_variant32(dest.as_mut_slice(), 1);
        assert_eq!(dest, [0x01, 0x00, 0x00, 0x00, 0x00]);
        let ans = decode_variant32(dest.as_slice());
        assert_eq!(ans, (1, 1));

        put_variant32(dest.as_mut_slice(), 128);
        assert_eq!(dest, [0x80, 0x01, 0x00, 0x00, 0x00]);
        let ans = decode_variant32(dest.as_slice());
        assert_eq!(ans, (128, 2));

        put_variant32(dest.as_mut_slice(), 123456);
        assert_eq!(dest, [0xc0, 0xc4, 0x07, 0x00, 0x00]);
        let ans = decode_variant32(dest.as_slice());
        assert_eq!(ans, (123456, 3));
    }

    #[test]
    fn test_variant64() {
        let mut dest: [u8; 10] = [0; 10];

        put_variant64(dest.as_mut_slice(), 1);
        assert_eq!(dest, [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let ans = decode_variant64(dest.as_slice());
        assert_eq!(ans, (1, 1));

        put_variant64(dest.as_mut_slice(), 128);
        assert_eq!(dest, [0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let ans = decode_variant64(dest.as_slice());
        assert_eq!(ans, (128, 2));

        put_variant64(dest.as_mut_slice(), 1234567890);
        assert_eq!(dest, [0xd2, 0x85, 0xd8, 0xcc, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let ans = decode_variant64(dest.as_slice());
        assert_eq!(ans, (1234567890, 5));
    }
}
