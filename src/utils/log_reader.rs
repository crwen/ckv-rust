use std::io::Error;

use bytes::Buf;

use crate::utils::codec::verify_checksum;

use super::file::SequentialFile;

pub struct Reader {
    file: Box<dyn SequentialFile>,
    offset: u64,
}

impl Reader {
    pub fn new(file: Box<dyn SequentialFile>) -> Self {
        Self { file, offset: 0 }
    }

    pub fn read_record(&mut self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0_u8; 12];
        self.file.read(&mut buf)?;
        println!("header .........{:?}", buf);
        let checksum = (&buf[..]).get_u64();
        let len = (&buf[8..]).get_u32();
        println!("len .........{:?}", len);
        let mut data = vec![0_u8; len as usize];
        self.file.read(&mut data)?;
        println!("data .........{:?}", data);
        verify_checksum(&data, checksum).unwrap();
        self.offset += 12 + data.len() as u64;
        Ok(data)
    }
}

#[cfg(test)]
mod log_reader_test {
    use core::panic;
    use std::{io::ErrorKind, path::Path};

    use crate::{
        lsm::Lsm,
        utils::{
            codec::{decode_varintu32, varintu32_length},
            file::SequentialFileImpl,
            log_reader::Reader,
            Entry,
        },
        Options,
    };

    #[test]
    fn log_reader_test() {
        {
            let mut lsm = Lsm::new(Options { block_size: 4096 });
            for i in 0..100 {
                let e = Entry::new(
                    (i as u32).to_be_bytes().to_vec(),
                    (i as u32).to_be_bytes().to_vec(),
                    i,
                );
                lsm.set(&e.key, &e.value);
            }
        }

        let mut log = Reader::new(Box::new(SequentialFileImpl::new(Path::new("0.wal"))));
        let mut i: u32 = 0;
        let mut end = false;
        while !end {
            let record = log.read_record();
            match record {
                Ok(record) => {
                    let data = &record[8..];
                    let key_sz = decode_varintu32(data).unwrap();
                    let var_key_sz = varintu32_length(key_sz) as usize;
                    let key = &data[var_key_sz..var_key_sz + key_sz as usize];
                    let expected_key = i.to_be_bytes();
                    println!("{:?}", key);
                    assert_eq!(key, expected_key);
                }
                Err(err) => match err.kind() {
                    ErrorKind::UnexpectedEof => end = true,
                    err => panic!("{:?}", err),
                },
            };
            i += 1;
        }
    }
}
