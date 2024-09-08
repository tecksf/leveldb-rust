use std::{fs, io};
use std::cell::RefCell;
use std::ffi::OsStr;
use std::io::{Read, Seek, SeekFrom, Write};

pub trait WriterView {
    fn append(&mut self, slice: &[u8]) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
}

pub trait ReaderView {
    fn read(&self, count: usize, buffer: &mut [u8]) -> io::Result<usize>;
}

pub trait RandomReaderView {
    fn read(&self, offset: u64, count: usize, buffer: &mut [u8]) -> io::Result<()>;
}

pub struct WritableFile {
    file_handle: fs::File,
}

impl WritableFile {
    pub fn open<T: AsRef<OsStr>>(path: T) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path.as_ref())?;

        Ok(WritableFile { file_handle: file })
    }
}

impl WriterView for WritableFile {
    fn append(&mut self, slice: &[u8]) -> io::Result<usize> {
        self.file_handle.write(slice)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file_handle.sync_data()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_handle.flush()
    }
}

pub struct ReadableFile {
    file_handle: RefCell<fs::File>,
}

impl ReadableFile {
    pub fn open<T: AsRef<OsStr>>(path: T) -> io::Result<Self> {
        let file = fs::File::open(path.as_ref())?;
        Ok(Self { file_handle: RefCell::new(file) })
    }
}

impl ReaderView for ReadableFile {
    fn read(&self, count: usize, buffer: &mut [u8]) -> io::Result<usize> {
        let buf = &mut buffer[..count];
        return self.file_handle.borrow_mut().read(buf);
    }
}

impl RandomReaderView for ReadableFile {
    fn read(&self, offset: u64, count: usize, buffer: &mut [u8]) -> io::Result<()> {
        self.file_handle.borrow_mut().seek(SeekFrom::Start(offset))?;
        let buf = &mut buffer[..count];
        self.file_handle.borrow_mut().read_exact(buf)
    }
}

#[cfg(test)]
pub mod tests {
    use std::io;
    use std::cell::Cell;
    use super::{ReaderView, WriterView};

    pub struct WritableMemory<'a> {
        data: &'a mut Vec<u8>,
    }

    impl<'a> WritableMemory<'a> {
        pub fn new(data: &'a mut Vec<u8>) -> Self {
            Self {
                data
            }
        }
    }

    impl<'a> WriterView for WritableMemory<'a> {
        fn append(&mut self, slice: &[u8]) -> io::Result<usize> {
            self.data.extend_from_slice(slice);
            Ok(slice.len())
        }

        fn sync(&mut self) -> io::Result<()> { Ok(()) }

        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    pub struct ReadableMemory<'a> {
        source: &'a [u8],
        offset: Cell<usize>,
    }

    impl<'a> ReadableMemory<'a> {
        pub fn new(data: &'a [u8], offset: usize) -> Self {
            Self {
                source: data,
                offset: Cell::new(offset),
            }
        }
    }

    impl<'a> ReaderView for ReadableMemory<'a> {
        fn read(&self, count: usize, buffer: &mut [u8]) -> io::Result<usize> {
            let rest = self.source.len() - self.offset.get();
            let bytes = if count > rest { rest } else { count };

            (&mut buffer[..bytes]).copy_from_slice(&self.source[self.offset.get()..self.offset.get() + bytes]);
            self.offset.set(self.offset.get() + bytes);

            Ok(bytes)
        }
    }
}
