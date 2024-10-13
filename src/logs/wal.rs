use std::cell::{Cell, RefCell};
use std::io;
use crate::logs::file::{ReaderView, WriterView};
use crate::utils::coding;
use crate::utils::common::Interval;

#[repr(u8)]
#[derive(Eq, PartialEq, Debug)]
enum RecordType {
    Zero,
    Full,
    First,
    Middle,
    Last,
}

enum AdditionRecordType {
    RecordType(RecordType),
    Eof,
    BadRecord,
}

impl From<u8> for RecordType {
    fn from(num: u8) -> Self {
        match num {
            0 => RecordType::Zero,
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            _ => RecordType::Last,
        }
    }
}


#[cfg(not(test))]
const BLOCK_SIZE: usize = 32 * 1024;

#[cfg(test)]
const BLOCK_SIZE: usize = 32;

const RECORD_HEADER_SIZE: usize = 4 + 2 + 1;

pub struct Writer<T: WriterView> {
    block_offset: usize,
    file: T,
}

impl<T: WriterView> Writer<T> {
    pub fn new(file: T) -> Self {
        Self {
            block_offset: 0,
            file,
        }
    }

    pub fn add_record<S: AsRef<[u8]>>(&mut self, slice: S) -> io::Result<()> {
        let mut data = slice.as_ref();
        let mut begin: bool = true;

        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < RECORD_HEADER_SIZE {
                if leftover > 0 {
                    self.file.append(&[0; 6][..leftover])?;
                }
                self.block_offset = 0;
            }

            let data_len = data.len();
            let avail = BLOCK_SIZE - self.block_offset - RECORD_HEADER_SIZE;
            let fragment_length = if data_len < avail { data_len } else { avail };

            let end: bool = data_len == fragment_length;
            let record_type = if begin && end {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if end {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            self.emit_physical_record(record_type, &data[..fragment_length])?;
            data = &data[fragment_length..];
            begin = false;

            if data.len() <= 0 {
                break;
            }
        };

        Ok(())
    }

    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> io::Result<()> {
        let mut record_header: [u8; 7] = [0; 7];
        let len = data.len();
        record_header[4] = (len & 0xff) as u8;
        record_header[5] = (len >> 8) as u8;
        record_header[6] = record_type as u8;

        let crc = crc32c::crc32c(data);
        coding::put_fixed32(&mut record_header, crc);

        self.file.append(&record_header)?;
        self.file.append(data)?;
        self.file.flush()?;
        self.block_offset += RECORD_HEADER_SIZE + len;

        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}


pub struct Reader<T: ReaderView> {
    file: T,
    buffer: RefCell<[u8; BLOCK_SIZE]>,
    buffer_room: Interval,
    eof: Cell<bool>,
}

impl<T: ReaderView> Reader<T> {
    pub fn new(file: T) -> Self {
        Self {
            file,
            buffer: RefCell::new([0; BLOCK_SIZE]),
            buffer_room: Interval::new(0, 0),
            eof: Cell::new(false),
        }
    }

    pub fn read_record(&self) -> Option<Vec<u8>> {
        let mut in_fragmented_record: bool = false;
        let mut full_record = Vec::<u8>::new();

        loop {
            let (record_type, interval) = self.read_physical_record();
            let (begin, end) = interval.get();
            let fragment = &self.buffer.borrow()[begin..end];

            match record_type {
                AdditionRecordType::RecordType(RecordType::Full) => {
                    if in_fragmented_record {
                        log::error!("partial record without end(1)");
                        return None;
                    }
                    full_record.extend_from_slice(fragment);
                    break;
                }

                AdditionRecordType::RecordType(RecordType::First) => {
                    if in_fragmented_record {
                        log::error!("partial record without end(2)");
                        return None;
                    }
                    full_record.extend_from_slice(fragment);
                    in_fragmented_record = true;
                }

                AdditionRecordType::RecordType(RecordType::Middle) => {
                    if !in_fragmented_record {
                        log::error!("missing start of fragmented record(1)");
                        return None;
                    }
                    full_record.extend_from_slice(fragment);
                }

                AdditionRecordType::RecordType(RecordType::Last) => {
                    if !in_fragmented_record {
                        log::error!("missing start of fragmented record(2)");
                        return None;
                    }
                    full_record.extend_from_slice(fragment);
                    break;
                }

                _ => return None
            };
        }

        Some(full_record)
    }

    fn read_physical_record(&self) -> (AdditionRecordType, Interval) {
        while self.buffer_room.size() < RECORD_HEADER_SIZE {
            if !self.eof.get() {
                // last read was a full record, skip the rest trailer(0, 0,...)
                match self.file.read(BLOCK_SIZE, self.buffer.borrow_mut().as_mut()) {
                    Ok(count) => {
                        self.buffer_room.set(0, count);
                        if count < BLOCK_SIZE {
                            self.eof.set(true);
                        }
                    }
                    _ => {
                        self.eof.set(true);
                        return (AdditionRecordType::Eof, Interval::default());
                    }
                };
            } else {
                return (AdditionRecordType::Eof, Interval::default());
            }
        };

        let (begin, end) = self.buffer_room.get();
        let record_header = &self.buffer.borrow()[begin..end][..RECORD_HEADER_SIZE];
        let record_length = ((record_header[4] & 0xff) as usize) | ((record_header[5] & 0xff) as usize) << 8;
        let record_type: RecordType = record_header[6].into();
        if RECORD_HEADER_SIZE + record_length > BLOCK_SIZE {
            self.buffer_room.clear();
            let result = if self.eof.get() { AdditionRecordType::Eof } else { AdditionRecordType::BadRecord };
            return (result, Interval::default());
        }

        if record_type == RecordType::Zero && record_length == 0 {
            self.buffer_room.clear();
            return (AdditionRecordType::BadRecord, Interval::default());
        }

        let expected_crc = coding::decode_fixed32(&record_header[..4]);
        let actual_crc = crc32c::crc32c(&self.buffer.borrow()[begin + RECORD_HEADER_SIZE..begin + RECORD_HEADER_SIZE + record_length]);
        if expected_crc != actual_crc {
            self.buffer_room.clear();
            return (AdditionRecordType::BadRecord, Interval::default());
        }

        if RECORD_HEADER_SIZE + record_length > self.buffer_room.size() {
            self.buffer_room.clear();
            return (AdditionRecordType::BadRecord, Interval::default());
        }

        let (begin, _) = self.buffer_room.get();
        let record_interval = Interval::new(begin + RECORD_HEADER_SIZE, begin + record_length + RECORD_HEADER_SIZE);
        self.buffer_room.skip(RECORD_HEADER_SIZE + record_length);

        (AdditionRecordType::RecordType(record_type), record_interval)
    }
}


#[cfg(test)]
mod tests {
    use super::{BLOCK_SIZE, Writer, Reader, RecordType};
    use crate::logs::file::tests::{WritableMemory, ReadableMemory};

    #[test]
    fn test_wal_write_and_read_record() {
        let mut data = Vec::<u8>::with_capacity(BLOCK_SIZE * 3);
        let mut writer = Writer::new(WritableMemory::new(&mut data));
        let message = ["0123456789", "Rust build reliable and efficient software"];
        for msg in message {
            assert!(writer.add_record(msg).is_ok());
        }

        let r1 = &data[..17];
        let length_high: u16 = r1[5] as u16;
        assert_eq!((length_high << 8) | r1[4] as u16, message[0].len() as u16);
        assert_eq!(RecordType::from(r1[6]), RecordType::Full);

        let r2 = &data[17..32];
        let length_high: u16 = r2[5] as u16;
        assert_eq!((length_high << 8) | r2[4] as u16, 8);
        assert_eq!(RecordType::from(r2[6]), RecordType::First);

        let r3 = &data[32..64];
        let length_high: u16 = r3[5] as u16;
        assert_eq!((length_high << 8) | r3[4] as u16, 25);
        assert_eq!(RecordType::from(r3[6]), RecordType::Middle);

        let r4 = &data[64..80];
        let length_high: u16 = r4[5] as u16;
        assert_eq!((length_high << 8) | r4[4] as u16, 9);
        assert_eq!(RecordType::from(r4[6]), RecordType::Last);

        let reader = Reader::new(ReadableMemory::new(data.as_slice(), 0));
        assert_eq!(reader.read_record(), Some(Vec::from(message[0])));
        assert_eq!(reader.read_record(), Some(Vec::from(message[1])));
    }
}
