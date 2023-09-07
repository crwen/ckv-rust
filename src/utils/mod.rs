pub mod codec;
pub mod convert;
pub mod file;

#[derive(Debug)]
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
