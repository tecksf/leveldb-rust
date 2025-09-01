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
        self.skip_empty_data_blocks_forward();
        self.is_valid()
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

        fn decode_data(&self, text: &[u8]) -> usize {
            text.iter().position(|&x| x == b'-').unwrap()
        }
    }

    impl LevelIterator for DataIterator {
        fn is_valid(&self) -> bool {
            self.index.get() < self.data.len()
        }

        fn key(&self) -> Vec<u8> {
            let text = self.data[self.index.get()].as_bytes();
            let sep = self.decode_data(text);
            Vec::from(&text[..sep])
        }

        fn value(&self) -> Vec<u8> {
            let text = self.data[self.index.get()].as_bytes();
            let sep = self.decode_data(text);
            Vec::from(&text[sep + 1..])
        }

        fn next(&self) -> bool {
            self.index.set(self.index.get() + 1);
            self.is_valid()
        }

        fn seek(&self, target: &[u8]) -> bool {
            let result = self.data.binary_search_by(|text| {
                let sep = self.decode_data(text.as_bytes());
                let key = &text.as_bytes()[..sep];
                key.cmp(target)
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
    fn test_two_level_iterator_seek() {
        let index_iterator = DataIterator::new(vec!["400-3", "100-0", "700-6"]);
        let generator = DataIteratorGen::new(
            vec!["100-v100", "200-v200", "300-v300", "400-v400",
                 "500-v500", "600-v600", "700-v700", "800-v800", "900-v900"]
        );
        let iterator = TwoLevelIterator::new(index_iterator, generator);
        let mut result = Vec::new();
        iterator.seek_to_first();
        while iterator.is_valid() {
            result.push(String::from_utf8(iterator.value()).unwrap());
            iterator.next();
        }
        assert_eq!(result, vec!["v400", "v500", "v600", "v100", "v200", "v300", "v700", "v800", "v900"]);
    }

    #[test]
    fn test_multi_iterators_merged() {
        let it1 = Box::new(DataIterator::new(vec!["100-v100", "200-v200", "300-v300"]));
        let it2 = Box::new(DataIterator::new(vec!["150-v150", "250-v250", "350-v350"]));
        let it3 = Box::new(DataIterator::new(vec!["180-v180", "280-v280", "380-v380"]));

        let d4 = DataIterator::new(vec!["500-0", "503-3", "506-6"]);
        let g4 = DataIteratorGen::new(
            vec!["500-v500", "501-v501", "502-v502", "503-v503", "504-v504", "505-v505", "506-v506", "507-v507", "508-v508"]
        );
        let it4 = Box::new(TwoLevelIterator::new(d4, g4));

        let d5 = DataIterator::new(vec!["600-0", "603-3", "606-6"]);
        let g5 = DataIteratorGen::new(
            vec!["600-v600", "601-v601", "602-v602", "603-v603", "604-v604", "605-v605", "606-v606", "607-v607", "608-v608"]
        );
        let it5 = Box::new(TwoLevelIterator::new(d5, g5));

        let expected = [
            100, 150, 180, 200, 250, 280, 300, 350, 380,
            500, 501, 502, 503, 504, 505, 506, 507, 508,
            600, 601, 602, 603, 604, 605, 606, 607, 608
        ];
        let mut index = 0;

        let iterator = MergingIterator::new(|x, y| { x.cmp(y) }, vec![it1, it2, it3, it4, it5]);
        iterator.seek_to_first();
        while iterator.is_valid() {
            let value = String::from_utf8(iterator.value()).unwrap();
            assert_eq!(value, format!("v{}", expected[index]));
            iterator.next();
            index += 1;
        }
        assert_eq!(index, 27);
    }
}
