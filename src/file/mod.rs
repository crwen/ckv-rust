mod log_reader;
mod log_writer;
mod readable;
mod writeable;

use std::io::Error;
use std::path::{Path, PathBuf};

pub use log_reader::*;
pub use log_writer::*;
pub use readable::*;
pub use writeable::*;

// A file abstraction for reading sequentially through a file
pub trait SequentialAccess {
    // read n bytes
    fn read(&mut self, buf: &mut [u8]) -> Result<(), Error>;
}

pub trait RandomAccess {
    // read n bytes
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<(), Error>;

    fn size(&self) -> Result<u64, Error>;
}

pub trait Writable: Sync + Send + 'static {
    // apend data to file
    fn append(&mut self, data: &[u8]) -> Result<(), Error>;
    fn flush(&mut self) -> Result<(), Error>;

    /// Attempts to sync all OS-internal metadata to disk.
    /// This function will attempt to ensure that all in-memory data reaches the
    /// filesystem before returning.
    fn sync(&mut self) -> Result<(), Error>;

    fn size(&self) -> Result<u64, Error>;
}

pub enum Ext {
    WAL,
    SST,
    VLOG,
    MANIFEST,
}

pub fn path_of_file(work_dir: &str, id: u64, ext: Ext) -> PathBuf {
    let file_ext = match ext {
        Ext::WAL => ".wal",
        Ext::SST => ".sst",
        Ext::VLOG => ".vlog",
        Ext::MANIFEST => "",
    };
    if file_ext.is_empty() {
        return Path::new(work_dir).join("MANIFEST");
    }
    Path::new(work_dir).join(format!("{:05}{}", id, file_ext))
}

pub enum ReadableFile {
    Sequential(SequentialFileImpl),
    Random(RandomAccessFileImpl),
}

pub enum WritableFile {
    Sequential(WritableFileImpl),
}
