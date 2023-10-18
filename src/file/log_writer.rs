use std::io::Error;

use bytes::BufMut;
use parking_lot::Mutex;

use crate::utils::codec::calculate_checksum;

use super::{writeable::WritableFileImpl, Writable};

pub struct Writer {
    inner: Mutex<WriterInner>,
}

struct WriterInner {
    file: WritableFileImpl,
    offset: u64,
}
impl WriterInner {
    pub fn new(file: WritableFileImpl) -> Self {
        Self { file, offset: 0 }
    }
}

impl Writer {
    pub fn new(file: WritableFileImpl) -> Self {
        Self {
            inner: Mutex::new(WriterInner::new(file)),
        }
    }

    pub fn add_recore(&self, data: &[u8]) -> Result<(), anyhow::Error> {
        let checksum = calculate_checksum(data);
        let mut buf = Vec::new();
        buf.put_u64(checksum);
        // let mut buf = checksum.to_le_bytes().to_vec();
        buf.put_u32(data.len() as u32);
        buf.put(data);
        let mut inner = self.inner.lock();
        inner.file.append(&buf)?;

        // self.file.append(data)?;

        inner.offset += data.len() as u64 + 12;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let mut inner = self.inner.lock();
        inner.file.flush()?;
        Ok(())
    }
}
