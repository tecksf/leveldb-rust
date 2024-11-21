use std::cell::Cell;
use std::time::SystemTime;
use crate::utils::coding;

#[derive(Clone, Default)]
pub struct Interval {
    begin: Cell<usize>,
    end: Cell<usize>,
}

impl Interval {
    pub fn new(x: usize, y: usize) -> Self {
        Self {
            begin: Cell::new(x),
            end: Cell::new(y),
        }
    }

    pub fn get(&self) -> (usize, usize) {
        (self.begin.get(), self.end.get())
    }

    pub fn set(&self, x: usize, y: usize) {
        self.begin.set(x);
        self.end.set(y);
    }

    pub fn size(&self) -> usize {
        self.end.get() - self.begin.get()
    }

    pub fn clear(&self) {
        self.set(0, 0);
    }

    pub fn skip(&self, n: usize) {
        if self.size() > n {
            self.begin.set(self.begin.get() + n);
        } else {
            self.begin.set(self.end.get());
        }
    }
}

pub fn hash(data: &[u8], seed: u32) -> u32 {
    let mut slice = data;
    let m: u32 = 0xc6a4a793;
    let r: u32 = 24;
    let mut h: u32 = seed ^ m.wrapping_mul(slice.len() as u32);

    while slice.len() >= 4 {
        let w = coding::decode_fixed32(slice);
        slice = &slice[4..];
        h = h.wrapping_add(w);
        h = h.wrapping_mul(m);
        h ^= h >> 16;
    }

    match slice.len() {
        3 => h += (slice[2] as u32) << 16,
        2 => h += (slice[1] as u32) << 8,
        1 => {
            h += slice[0] as u32;
            h = h.wrapping_mul(m);
            h ^= h >> r;
        }
        _ => {}
    };
    h
}

pub fn now_micros() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
}