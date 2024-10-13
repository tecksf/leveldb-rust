use std::{fs, io};
use crate::core::format::InternalKey;
use crate::core::memory::MemoryTableIterator;
use crate::core::version::FileMetaData;
use crate::logs::file::WritableFile;
use crate::logs::filename;
use crate::Options;
use crate::table::builder::TableBuilder;

pub fn build_table(options: Options, path: &str, mut iter: MemoryTableIterator, file_number: u64) -> io::Result<FileMetaData> {
    let file_name = filename::make_table_file_name(path, file_number);
    let mut builder = TableBuilder::new(options, WritableFile::open(&file_name)?);

    let mut meta = FileMetaData::new();
    meta.number = file_number;

    if let Some(lookup_key) = iter.next() {
        meta.smallest = lookup_key.extract_internal_key();
        builder.add(&meta.smallest, lookup_key.get_raw_value());
    }

    let mut internal_key = InternalKey::default();
    for lookup_key in iter {
        internal_key = lookup_key.extract_internal_key();
        builder.add(&internal_key, lookup_key.get_raw_value());
    }
    meta.largest = internal_key;

    builder.finish();
    if builder.is_ok() {
        meta.file_size = builder.file_size();
    }

    if meta.file_size <= 0 {
        fs::remove_file(file_name).unwrap_or_default();
        return Err(io::Error::new(io::ErrorKind::InvalidData, "new sst file is empty"));
    }

    Ok(meta)
}
