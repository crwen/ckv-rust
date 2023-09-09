use crate::{mem_table::MemTable, utils::Entry, version::Version};

#[derive(Default)]
pub struct Lsm {
    version: Version,
    mem: MemTable,
    imm: Vec<MemTable>,
}

impl Lsm {
    pub fn new() -> Self {
        let mem = MemTable::new();
        Self {
            mem,
            imm: Vec::new(),
            version: Version::new(),
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let seq = self.version.add_last_sequence(1);
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
}

#[cfg(test)]
mod lsm_test {
    use super::Lsm;

    #[test]
    fn lsm_crud_test() {
        let mut lsm = Lsm::new();
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
