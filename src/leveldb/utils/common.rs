use std::cell::Cell;

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