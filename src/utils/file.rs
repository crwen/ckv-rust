use std::fs::File;
use std::io::{Error, Write};
use std::os::unix::prelude::FileExt;
use std::path::Path;

pub trait WriteableFile {
    // apend data to file
    fn append(&mut self, data: &[u8]) -> Result<(), Error>;
    fn flush(&mut self) -> Result<(), Error>;
    fn sync(&mut self) -> Result<(), Error>;
}

// A file abstraction for reading sequentially through a file
pub trait SequentialFile {
    // read n bytes
    fn read(&mut self, buf: &mut [u8]) -> Result<(), Error>;
}

pub trait RandomAccessFile {
    // read n bytes
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<(), Error>;

    fn size(&self) -> Result<u64, Error>;
}

pub struct WritableFileImpl {
    // filename: String,
    // path: Path,
    file: std::fs::File,
}

impl WriteableFile for WritableFileImpl {
    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        self.file.write_all(data)
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()
    }

    fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_all()
    }
}
impl WritableFileImpl {
    pub fn new(path: &Path) -> Self {
        // let paths = Path::new("lorem_ipsum.txt");
        // let display = paths.display();

        // Open a file in write-only mode, returns `io::Result<File>`
        let file = match File::create(path) {
            Ok(f) => f,
            Err(err) => panic!("{}", err),
        };
        // let mut file = match File::create(&path) {
        //     Err(why) => panic!(why,
        //     Ok(file) => file,
        // };
        Self {
            // filename: String::from(""),
            // path,
            file,
        }
    }
}

pub struct RandomAccessFileImpl {
    // filename: String,
    // path: Path,
    file: std::fs::File,
}

impl RandomAccessFileImpl {
    pub fn open(path: &Path) -> Self {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(err) => panic!("{}", err),
        };
        Self { file }
    }
}

impl RandomAccessFile for RandomAccessFileImpl {
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

impl SequentialFile for SequentialFileImpl {
    fn read(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        self.file.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod file_test {
    use std::path::Path;

    use super::{RandomAccessFile, RandomAccessFileImpl, WritableFileImpl, WriteableFile};

    #[test]
    fn write_file_test() {
        let mut f = WritableFileImpl::new(Path::new("hello.txt"));
        f.append(b"hello ").unwrap();
        f.append(b"world!\n").unwrap();
        f.append(b"hello rust").unwrap();
        f.flush().unwrap();

        let f = RandomAccessFileImpl::open(Path::new("hello.txt"));
        let mut buf: Vec<u8> = vec![0_u8; 10];
        f.read(&mut buf[..3], 0).unwrap();
        assert_eq!(&buf[..3], b"hel");
    }
}
