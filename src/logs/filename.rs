use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Eq, PartialEq, Debug)]
pub enum FileType {
    LogFile,
    LockFile,
    TableFile,
    ManifestFile,
    CurrentFile,
    TempFile,
    InfoLogFile,
}

fn make_file_name(db_path: &str, number: u64, suffix: &str) -> OsString {
    let mut path = PathBuf::from(db_path);
    path.push(format!("{:06}", number));
    path.set_extension(suffix);
    path.into_os_string()
}

pub fn make_log_file_name(db_path: &str, number: u64) -> OsString {
    make_file_name(db_path, number, "log")
}

pub fn make_table_file_name(db_path: &str, number: u64) -> OsString {
    make_file_name(db_path, number, "ldb")
}

pub fn make_temp_file_name(db_path: &str, number: u64) -> OsString {
    make_file_name(db_path, number, "dbtmp")
}

pub fn make_manifest_file_name(db_path: &str, number: u64) -> OsString {
    let mut path = PathBuf::from(db_path);
    path.push(format!("MANIFEST-{:06}", number));
    path.into_os_string()
}

pub fn make_current_file_name(db_path: &str) -> OsString {
    let mut path = PathBuf::from(db_path);
    path.push("CURRENT");
    path.into_os_string()
}

pub fn make_lock_file_name(db_path: &str) -> OsString {
    let mut path = PathBuf::from(db_path);
    path.push("LOCK");
    path.into_os_string()
}

pub fn parse_file_name(filename: &str) -> Option<(FileType, u64)> {
    if filename == "CURRENT" {
        Some((FileType::CurrentFile, 0))
    } else if filename == "LOCK" {
        Some((FileType::LockFile, 0))
    } else if filename == "LOG" || filename == "LOG.old" {
        Some((FileType::InfoLogFile, 0))
    } else if filename.starts_with("MANIFEST-") {
        let res = filename["MANIFEST-".len()..].parse::<u64>();
        if let Ok(num) = res {
            Some((FileType::ManifestFile, num))
        } else {
            None
        }
    } else {
        let filepath = Path::new(filename);
        let num = filepath.file_stem()?.to_str()?.parse::<u64>();
        let extension = filepath.extension()?.to_str()?;
        if extension == "log" {
            Some((FileType::LogFile, num.unwrap()))
        } else if extension == "sst" || extension == "ldb" {
            Some((FileType::TableFile, num.unwrap()))
        } else if extension == "dbtmp" {
            Some((FileType::TempFile, num.unwrap()))
        } else {
            None
        }
    }
}
