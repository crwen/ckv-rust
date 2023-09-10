use std::{io::Error, path::Path};

use bytes::BufMut;

use crate::{
    mem_table::{MemTable, MemTableIterator},
    sstable::table_builder::TableBuilder,
    utils::{codec::encode_varintu32, file::WritableFileImpl, log_writer::Writer, Entry},
    version::version_set::VersionSet,
    Options,
};

pub struct Lsm {
    opt: Options,
    version: VersionSet,
    mem: MemTable,
    imm: Vec<MemTable>,
    wal: Writer,
}

impl Lsm {
    pub fn new(opt: Options) -> Self {
        let mem = MemTable::new();
        let mut version = VersionSet::new();
        let new_fid = version.new_file_number();

        let wal_name = new_fid.to_string() + ".wal";
        Self {
            opt,
            mem,
            imm: Vec::new(),
            version,
            wal: Writer::new(Box::new(WritableFileImpl::new(Path::new(&wal_name)))),
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let seq = self.version.add_last_sequence(1);
        // write wal first
        self.write_wal(key, value, seq).unwrap();

        // write
        let e = Entry::new(key.to_vec(), value.to_vec(), seq);
        self.mem.set(e);

        if self.mem.approximate_memory_usage() > 4096 {
            let imm = std::mem::replace(&mut self.mem, MemTable::new());
            self.imm.push(imm);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let seq = self.version.last_sequence();
        let mut result = self.mem.get(key, seq);
        if result.is_some() {
            return result.map(|val| val.to_vec());
        }
        for m in self.imm.iter() {
            result = m.get(key, seq);
            if result.is_some() {
                return result.map(|val| val.to_vec());
            }
        }
        // TODO: search sst
        None
    }

    fn write_level0_table(opt: Options, imm: MemTable) {
        TableBuilder::build_table("", opt, MemTableIterator::new(&imm));

        // TODO: pick level to push
    }

    fn write_wal(&mut self, key: &[u8], value: &[u8], seq: u64) -> Result<(), Error> {
        let mut data = Vec::new();
        data.put_u64(seq);
        encode_varintu32(&mut data, key.len() as u32);
        data.put(key);
        encode_varintu32(&mut data, value.len() as u32);
        data.put(value);
        self.wal.add_recore(&data)
    }
}

#[cfg(test)]
mod lsm_test {
    use crate::Options;

    use super::Lsm;

    #[test]
    fn lsm_crud_test() {
        let mut lsm = Lsm::new(Options { block_size: 4096 });
        for i in 0..1000 {
            let n = i as u32;
            lsm.set(&n.to_be_bytes(), &n.to_be_bytes());
        }

        for i in 0..1000 {
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes());
            assert_ne!(res, None);
            assert_eq!(res.unwrap(), n.to_be_bytes());
        }
    }
}
