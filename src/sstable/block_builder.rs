use bytes::BufMut;

use crate::utils::{
    codec::{calculate_checksum, encode_varintu32},
    convert::u32vec_to_bytes,
};

use super::block::SIZEOF_U32;

/// BlockBuilder write data to Blockm
///
/// +--------------------- --------------------------+
/// |  data | entryOffsets | entryOff len | checksum |
/// +------------------------------------------------+
///
#[derive(Clone)]
pub struct BlockBuilder {
    data: Vec<u8>,
    entry_offsets: Vec<u32>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            entry_offsets: vec![0],
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        // encode key
        encode_varintu32(&mut self.data, key.len() as u32);
        self.data.put(key);

        // encode value
        encode_varintu32(&mut self.data, value.len() as u32);
        self.data.put(value);

        self.entry_offsets.push(self.data.len() as u32);
        // self.offset += entry_data.len() as u32;
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.entry_offsets.len() * SIZEOF_U32
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.entry_offsets.clear();
        self.entry_offsets.push(0);
    }

    pub fn finish(&mut self) -> &[u8] {
        self.data.put(&u32vec_to_bytes(&self.entry_offsets)[..]);
        self.data.put_u32(self.entry_offsets.len() as u32);
        let checksum = calculate_checksum(&self.data);
        self.data.put_u64(checksum);
        self.data = lz4_flex::compress_prepend_size(&self.data);
        &self.data
    }
}
