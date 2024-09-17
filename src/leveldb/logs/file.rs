use std::{fs, io};
use std::cell::RefCell;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Seek, SeekFrom, Write};
use crate::leveldb::logs::filename;

pub trait WriterView {
    fn append<T: AsRef<[u8]>>(&mut self, slice: T) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
}

pub trait ReaderView {
    fn read(&self, count: usize, buffer: &mut [u8]) -> io::Result<usize>;
}

pub trait RandomReaderView {
    fn read(&self, offset: u64, count: usize, buffer: &mut [u8]) -> io::Result<()>;
}

pub fn get_all_filenames(dir: &str) -> Vec<OsString> {
    if let Ok(paths) = fs::read_dir(dir) {
        paths.map(|path| { path.unwrap().file_name() }).collect::<Vec<_>>()
    } else {
        vec![]
    }
}

pub fn set_current_file(db_name: &str, manifest_number: u64) -> io::Result<()> {
    let temp_path = filename::make_temp_file_name(db_name, manifest_number);
    fs::write(&temp_path, format!("MANIFEST-{:06}", manifest_number))?;
    let result = fs::rename(&temp_path, filename::make_current_file_name(db_name));
    if result.is_err() {
        fs::remove_file(&temp_path)?;
    }
    result
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
    fn append<T: AsRef<[u8]>>(&mut self, slice: T) -> io::Result<usize> {
        self.file_handle.write(slice.as_ref())
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
        self.file_handle.borrow_mut().read(buf)
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
        fn append<T: AsRef<[u8]>>(&mut self, slice: T) -> io::Result<usize> {
            self.data.extend_from_slice(slice.as_ref());
            Ok(slice.as_ref().len())
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
