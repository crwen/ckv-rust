use std::fs::File;
use std::io::{Error, Write};
use std::os::unix::prelude::MetadataExt;
use std::path::Path;

use super::Writable;

pub struct WritableFileImpl {
    // filename: String,
    // path: Path,
    file: std::fs::File,
}

impl Writable for WritableFileImpl {
    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        self.file.write_all(data)
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()
    }

    fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_all()
    }

    fn size(&self) -> Result<u64, Error> {
        Ok(self.file.metadata()?.size())
    }
}
impl WritableFileImpl {
    pub fn new(path: &Path) -> Self {
        // Open a file in write-only mode, returns `io::Result<File>`
        let file = match File::options().append(true).create(true).open(path) {
            Ok(f) => f,
            Err(err) => panic!("{}", err),
        };
        Self {
            // filename: String::from(""),
            // path,
            file,
        }
    }
}

#[cfg(test)]
mod file_test {

    use super::*;
    use std::{os::unix::prelude::FileExt, path::Path};

    #[test]
    fn write_file_test() {
        let path = Path::new("hello.txt");
        if std::fs::metadata(path).is_ok() {
            std::fs::remove_file(path).unwrap()
        };
        let mut f = WritableFileImpl::new(path);
        f.append(b"hello ").unwrap();
        f.append(b"world!\n").unwrap();
        f.append(b"hello rust").unwrap();
        f.flush().unwrap();

        let f = File::open(Path::new("hello.txt")).unwrap();
        let mut buf: Vec<u8> = vec![0_u8; 10];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf[..3], b"hel");
    }
}
