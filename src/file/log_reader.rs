use std::io::Error;

use bytes::Buf;

use crate::utils::codec::verify_checksum;

use super::SequentialAccess;

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
        verify_checksum(&data, checksum).unwrap();
        self.offset += 12 + data.len() as u64;
        Ok(data)
    }
}

#[cfg(test)]
mod log_reader_test {
    use core::panic;
    use std::io::ErrorKind;

    use crate::{
        file::{path_of_file, readable::SequentialFileImpl, Ext},
        lsm::Lsm,
        utils::{
            codec::{decode_varintu32, varintu32_length},
            Entry,
        },
    };

    use super::Reader;

    #[test]
    fn log_reader_test() {
        let opt = crate::Options::default_opt().work_dir("work_dir/log");

        let path = path_of_file(&opt.work_dir, 1, Ext::WAL);

        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap()
        };
        {
            let lsm = Lsm::open(opt);
            for i in 0..100 {
                let e = Entry::new(
                    (i as u32).to_be_bytes().to_vec(),
                    (i as u32).to_be_bytes().to_vec(),
                    i,
                );
                lsm.put(&e.key, &e.value).unwrap();
            }
        }

        let mut log = Reader::new(Box::new(SequentialFileImpl::new(path.as_path())));
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
