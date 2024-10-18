use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

pub trait LevelIterator {
    fn is_valid(&self) -> bool;
    fn key(&self) -> Vec<u8>;
    fn value(&self) -> Vec<u8>;
    fn next(&self) -> bool;
    fn seek(&self, target: &[u8]) -> bool;
    fn seek_to_first(&self);
    fn seek_to_last(&self);
}

pub trait IteratorGen {
    fn gen(&self, data: &[u8]) -> Option<Box<dyn LevelIterator>>;
}

pub struct TwoLevelIterator<Iter, Gen> {
    index_iter: Iter,
    iterator_function: Gen,
    data_iter: RefCell<Option<Box<dyn LevelIterator>>>,
    data_block_handle: RefCell<Vec<u8>>,
}

impl<Iter, Gen> TwoLevelIterator<Iter, Gen>
    where Iter: LevelIterator, Gen: IteratorGen
{
    pub fn new(index_iter: Iter, iterator_function: Gen) -> Self {
        Self {
            index_iter,
            iterator_function,
            data_iter: RefCell::new(None),
            data_block_handle: RefCell::new(Vec::new()),
        }
    }

    fn init_data_block(&self) {
        if !self.index_iter.is_valid() {
            self.data_iter.replace(None);
        } else {
            let value = self.index_iter.value();
            let has_data_iter = self.data_iter.borrow().is_some();
            if !has_data_iter || !self.data_block_handle.borrow().cmp(&value).is_eq() {
                let iter = self.iterator_function.gen(value.as_slice());
                self.data_block_handle.borrow_mut().clear();
                self.data_block_handle.borrow_mut().extend(value);
                self.data_iter.replace(iter);
            }
        }
    }

    fn skip_empty_data_blocks_forward(&self) {
        while self.data_iter.borrow().is_none() || !self.data_iter.borrow().as_ref().unwrap().is_valid() {
            if !self.index_iter.is_valid() {
                self.data_iter.replace(None);
                return;
            }

            self.index_iter.next();
            self.init_data_block();
            if let Some(iter) = self.data_iter.borrow().as_ref() {
                iter.seek_to_first();
            }
        }
    }
}

impl<Iter, Gen> LevelIterator for TwoLevelIterator<Iter, Gen>
    where Iter: LevelIterator, Gen: IteratorGen
{
    fn is_valid(&self) -> bool {
        if let Some(iter) = self.data_iter.borrow().as_ref() {
            return iter.is_valid();
        }
        false
    }

    fn key(&self) -> Vec<u8> {
        if let Some(iter) = self.data_iter.borrow().as_ref() {
            return iter.key();
        }
        Vec::new()
    }

    fn value(&self) -> Vec<u8> {
        if let Some(iter) = self.data_iter.borrow().as_ref() {
            return iter.value();
        }
        Vec::new()
    }

    fn next(&self) -> bool {
        if let Some(iter) = self.data_iter.borrow().as_ref() {
            iter.next();
        }
        self.skip_empty_data_blocks_forward();
        self.is_valid()
    }

    fn seek(&self, target: &[u8]) -> bool {
        if self.index_iter.seek(target) {
            self.init_data_block();
            if let Some(iter) = self.data_iter.borrow().as_ref() {
                iter.seek(target);
            }
        }
        false
    }

    fn seek_to_first(&self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if let Some(iter) = self.data_iter.borrow().as_ref() {
            iter.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&self) {
        todo!()
    }
}

struct IteratorWrapper<'a> {
    iterators: &'a Vec<Box<dyn LevelIterator>>,
    this: Option<&'a Box<dyn LevelIterator>>,
}

impl<'a> IteratorWrapper<'a> {
    fn new(iterators: &'a Vec<Box<dyn LevelIterator>>) -> Self {
        Self {
            iterators,
            this: None,
        }
    }
}


pub struct MergingIterator {
    compare: fn(&[u8], &[u8]) -> Ordering,
    iterators: Vec<Rc<dyn LevelIterator>>,
    current: RefCell<Option<Rc<dyn LevelIterator>>>,
}

impl MergingIterator {
    pub fn new(compare: fn(&[u8], &[u8]) -> Ordering, iterators_list: Vec<Box<dyn LevelIterator>>) -> Self {
        let iterators = iterators_list
            .into_iter()
            .map(Rc::from)
            .collect();

        Self {
            compare,
            iterators,
            current: RefCell::new(None),
        }
    }

    fn find_smallest(&self) {
        let mut smallest: Option<Rc<dyn LevelIterator>> = None;
        for iter in &self.iterators {
            if iter.is_valid() {
                match smallest {
                    Some(smallest_iter)
                    if (self.compare)(iter.key().as_slice(), smallest_iter.key().as_slice()).is_lt() => {
                        smallest = Some(iter.clone());
                    }
                    None => {
                        smallest = Some(iter.clone());
                    }
                    _ => {}
                }
            }
        }
        *self.current.borrow_mut() = smallest;
    }
}

impl LevelIterator for MergingIterator {
    fn is_valid(&self) -> bool {
        self.current.borrow().is_some()
    }

    fn key(&self) -> Vec<u8> {
        if let Some(iter) = self.current.borrow().as_ref() {
            iter.key()
        } else {
            Vec::new()
        }
    }

    fn value(&self) -> Vec<u8> {
        if let Some(iter) = self.current.borrow().as_ref() {
            iter.value()
        } else {
            Vec::new()
        }
    }

    fn next(&self) -> bool {
        if !self.is_valid() {
            return false;
        }

        let current = self.current.borrow().clone().unwrap();
        for iter in &self.iterators {
            if !Rc::ptr_eq(&current, iter) {
                if iter.seek(self.key().as_slice()) &&
                    (self.compare)(self.key().as_slice(), iter.key().as_slice()).is_eq()
                {
                    iter.next();
                }
            }
        }

        current.next();
        self.find_smallest();
        self.is_valid()
    }

    fn seek(&self, target: &[u8]) -> bool {
        for iter in &self.iterators {
            iter.seek(target);
        }
        self.find_smallest();
        self.is_valid()
    }

    fn seek_to_first(&self) {
        for iter in &self.iterators {
            iter.seek_to_first();
        }
        self.find_smallest();
    }

    fn seek_to_last(&self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use super::{IteratorGen, LevelIterator, MergingIterator, TwoLevelIterator};

    struct DataIterator {
        data: Vec<&'static str>,
        index: Cell<usize>,
    }

    impl DataIterator {
        fn new(data: Vec<&'static str>) -> Self {
            Self {
                data,
                index: Cell::new(0),
            }
        }
    }

    impl LevelIterator for DataIterator {
        fn is_valid(&self) -> bool {
            self.index.get() < self.data.len()
        }

        fn key(&self) -> Vec<u8> {
            Vec::from(self.data[self.index.get()].as_bytes())
        }

        fn value(&self) -> Vec<u8> {
            self.key()
        }

        fn next(&self) -> bool {
            self.index.set(self.index.get() + 1);
            self.is_valid()
        }

        fn seek(&self, target: &[u8]) -> bool {
            let result = self.data.binary_search_by(|key| {
                key.as_bytes().cmp(target)
            });

            self.index.set(result.unwrap_or_else(|n| n));
            self.is_valid()
        }

        fn seek_to_first(&self) {
            self.index.set(0);
        }

        fn seek_to_last(&self) {
            self.index.set(self.data.len());
        }
    }

    struct DataIteratorGen {
        data: Vec<&'static str>,
    }

    impl DataIteratorGen {
        fn new(data: Vec<&'static str>) -> Self {
            Self {
                data,
            }
        }
    }

    impl IteratorGen for DataIteratorGen {
        fn gen(&self, data: &[u8]) -> Option<Box<dyn LevelIterator>> {
            let index = std::str::from_utf8(data).unwrap().parse().ok()?;
            if index + 3 > self.data.len() {
                return None;
            }
            Some(Box::new(DataIterator::new(self.data[index..index + 3].to_vec())))
        }
    }

    #[test]
    fn test_multi_iterators_merged() {
        let d1 = Box::new(DataIterator::new(vec!["123", "abc", "opq"]));
        let d2 = Box::new(DataIterator::new(vec!["789", "efg", "lmn"]));
        let d3 = Box::new(DataIterator::new(vec!["145", "189", "def"]));
        let d4 = Box::new(DataIterator::new(vec!["123", "456", "789"]));

        let mut result = Vec::new();
        let iterator = MergingIterator::new(|x, y| { x.cmp(y) }, vec![d1, d2, d3, d4]);
        iterator.seek_to_first();
        while iterator.is_valid() {
            let key = iterator.key();
            result.push(String::from_utf8(key).unwrap());
            iterator.next();
        }
        assert_eq!(result, vec!["123", "145", "189", "456", "789", "abc", "def", "efg", "lmn", "opq"]);
    }

    #[test]
    fn test_two_level_iterator_seek() {
        let index_iterator = DataIterator::new(vec!["3", "0", "6"]);
        let generator = DataIteratorGen::new(vec!["100", "200", "300", "400", "500", "600", "700", "800", "900"]);
        let iterator = TwoLevelIterator::new(index_iterator, generator);
        let mut result = Vec::new();
        iterator.seek_to_first();
        while iterator.is_valid() {
            let key = iterator.key();
            result.push(String::from_utf8(key).unwrap());
            iterator.next();
        }
        assert_eq!(result, vec!["400", "500", "600", "100", "200", "300", "700", "800", "900"]);
    }
}
