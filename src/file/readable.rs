use std::fs::File;
use std::io::Error;
use std::os::unix::prelude::FileExt;
use std::path::Path;

use super::{RandomAccess, SequentialAccess};

pub struct RandomAccessFileImpl {
    // filename: String,
    // path: Path,
    file: std::fs::File,
}

impl RandomAccessFileImpl {
    pub fn open(path: &Path) -> Self {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(err) => panic!("open {} fail: {}", path.as_os_str().to_str().unwrap(), err),
        };
        Self { file }
    }
}

impl RandomAccess for RandomAccessFileImpl {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<(), Error> {
        self.file.read_exact_at(buf, offset)
    }

    fn size(&self) -> Result<u64, Error> {
        let meta = self.file.metadata()?;
        Ok(meta.len())
    }
}

pub struct SequentialFileImpl {
    file: std::fs::File,
    offset: u64,
    // file_sz: u64,
}

impl SequentialFileImpl {
    // Open a file in read-only mode
    pub fn new(path: &Path) -> Self {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(err) => panic!("{}", err),
        };
        // let file_sz = file.metadata().unwrap().len();
        Self {
            file,
            offset: 0,
            // file_sz,
        }
    }
}

impl SequentialAccess for SequentialFileImpl {
    fn read(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        self.file.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}
