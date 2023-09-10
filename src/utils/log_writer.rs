use std::io::Error;

use bytes::BufMut;

use super::{codec::calculate_checksum, file::WriteableFile};

pub struct Writer {
    file: Box<dyn WriteableFile>,
    offset: u64,
}

impl Writer {
    pub fn new(file: Box<dyn WriteableFile>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn add_recore(&mut self, data: &[u8]) -> Result<(), Error> {
        let checksum = calculate_checksum(data);
        let mut buf = Vec::new();
        buf.put_u64(checksum);
        // let mut buf = checksum.to_le_bytes().to_vec();
        buf.put_u32(data.len() as u32);
        self.file.append(&buf)?;

        self.file.append(data)?;

        self.offset += data.len() as u64 + 12;
        Ok(())
    }
}
