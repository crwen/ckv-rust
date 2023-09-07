use bytes::BufMut;

use crate::utils::codec::calculate_checksum;

pub const SIZEOF_U32: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug)]
pub struct Block {
    data: Vec<u8>,
    // entry_offsets: Vec<u16>,
}

impl Block {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            // entry_offsets: Vec::new(),
        }
    }

    pub fn append(&mut self, data: &[u8]) {
        self.data.put(data);
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }
    pub fn calculate_checksum(&self) -> u64 {
        calculate_checksum(&self.data)
    }
}
