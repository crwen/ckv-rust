pub mod bloom;
pub mod codec;
pub mod convert;

pub const OP_TYPE_DELETE: u8 = 0;
pub const OP_TYPE_PUT: u8 = 1;

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>, seq: u64) -> Self {
        Self { key, value, seq }
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}

pub trait FilterPolicy: Send + Sync {
    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
}
