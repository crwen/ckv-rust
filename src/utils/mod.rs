use bytes::Bytes;

pub mod bloom;
pub mod codec;
pub mod convert;

pub const OP_TYPE_DELETE: u8 = 0;
pub const OP_TYPE_PUT: u8 = 1;

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub seq: u64,
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes, seq: u64) -> Self {
        Self { key, value, seq }
    }

    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}

pub trait FilterPolicy: Send + Sync {
    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
}
