use std::io::Error;

use bytes::Buf;

use crate::utils::codec::verify_checksum;

use super::{RandomAccess, SequentialAccess};

pub struct Reader {
    file: Box<dyn SequentialAccess>,
    offset: u64,
}

impl Reader {
    pub fn new(file: Box<dyn SequentialAccess>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn read_record(&mut self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0_u8; 12];
        self.file.read(&mut buf)?;
        let checksum = (&buf[..]).get_u64();
        let len = (&buf[8..]).get_u32();
        let mut data = vec![0_u8; len as usize];
        self.file.read(&mut data)?;
        let data = lz4_flex::decompress_size_prepended(&data).unwrap();
        verify_checksum(&data, checksum).unwrap();
        self.offset += 12 + data.len() as u64;
        Ok(data)
    }
}

pub struct RandomReader {
    file: Box<dyn RandomAccess>,
}

impl RandomReader {
    pub fn new(file: Box<dyn RandomAccess>) -> Self {
        Self { file }
    }

    pub fn read_record(&mut self, offset: u64) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0_u8; 12];
        self.file.read(&mut buf, offset)?;
        let checksum = (&buf[..]).get_u64();
        let len = (&buf[8..]).get_u32();

        let mut data = vec![0_u8; len as usize];
        self.file.read(&mut data, offset + 12)?;

        let data = lz4_flex::decompress_size_prepended(&data).unwrap();
        verify_checksum(&data, checksum).unwrap();
        Ok(data)
    }
}
