use std::sync::Arc;

use bytes::{Buf, BufMut};

use crate::utils::{
    codec::{decode_varintu32, varintu32_length, verify_checksum},
    Entry,
};

use super::{Result, TableError};

pub const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub const SIZEOF_U64: usize = std::mem::size_of::<u64>();
pub const BLOCK_TRAILER_SIZE_: usize = 8;

#[derive(Clone, Debug)]
pub struct BlockHandler {
    offset: u32,
    block_size: u32,
}

impl BlockHandler {
    pub fn new() -> Self {
        Self {
            offset: 0,
            block_size: 0,
        }
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(TableError::DecodeBlockHandlerError);
        }
        let offset = (&data[..4]).get_u32();
        let block_size = (&data[4..]).get_u32();
        Ok(Self { offset, block_size })
    }

    // pub fn offset(&self) -> u32 {
    //     self.offset
    // }
    pub fn set_offset(&mut self, offset: u32) {
        self.offset = offset;
    }
    // pub fn block_size(&self) -> u32 {
    //     self.block_size
    // }
    pub fn set_block_size(&mut self, block_size: u32) {
        self.block_size = block_size;
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u32(self.offset);
        buf.put_u32(self.block_size);
        buf
    }
}

#[derive(Clone, Debug)]
pub struct Block {
    data: Vec<u8>,
    entry_offsets: Vec<u32>,
}

impl Block {
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let checksum = (&data[len - SIZEOF_U64..]).get_u64();
        verify_checksum(&data[..len - SIZEOF_U64], checksum).unwrap();

        let offset_end = data.len() - SIZEOF_U64 - SIZEOF_U32;
        let num_offset = (&data[offset_end..]).get_u32();
        let data_end = offset_end - num_offset as usize * SIZEOF_U32;
        Self {
            data: data[..data_end].to_vec(),
            entry_offsets: data[data_end..offset_end]
                .chunks(SIZEOF_U32)
                .map(|mut x| x.get_u32())
                .collect(),
        }
    }

    pub fn read_entry_at(&self, offset: usize) -> Option<Entry> {
        if offset >= self.data.len() {
            return None;
        }
        let e = Self::decode_entry(&self.data[offset..]);
        Some(e)
    }

    fn decode_entry(data: &[u8]) -> Entry {
        // decode key
        let key_sz = decode_varintu32(data).unwrap();
        let varint_key_sz = varintu32_length(key_sz) as usize;
        let key = data[varint_key_sz..varint_key_sz + key_sz as usize].to_vec();

        // decode value
        let value_data = &data[varint_key_sz + key_sz as usize..];
        let value_sz = decode_varintu32(value_data).unwrap();
        let varint_value_sz = varintu32_length(key_sz) as usize;
        let value = value_data[varint_value_sz..varint_value_sz + value_sz as usize].to_vec();
        Entry::new(key, value, 0)
    }

    // pub fn append(&mut self, data: &[u8]) {
    //     self.data.put(data);
    // }
}

impl IntoIterator for Block {
    type Item = Entry;

    type IntoIter = BlockIterator;

    fn into_iter(self) -> Self::IntoIter {
        BlockIterator::new(Arc::new(self))
    }
}

pub struct BlockIterator {
    block: Arc<Block>,
    idx: usize,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self { block, idx: 0 }
    }

    fn seek_to(&self, idx: usize) -> Option<Entry> {
        if idx >= self.block.entry_offsets.len() {
            return None;
        }
        let offset = self.block.entry_offsets[idx];
        self.block.read_entry_at(offset as usize)
    }

    pub fn seek(&mut self, key: &[u8]) -> Option<Entry> {
        // self.block.
        let (mut low, mut high) = (0, self.block.entry_offsets.len() - 1);
        // let target_key = Key::new(key.to_vec());
        while low < high {
            let mid = ((high - low) >> 1) + low;
            let offset = self.block.entry_offsets[mid];
            let entry = self.block.read_entry_at(offset as usize).unwrap();
            // println!("ekey{}, target {}, low {}, mid {}, high {}", e.key, )
            // TODO: compare

            if BlockIterator::greater_or_equal(&entry.key, key) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        self.idx = low;
        self.seek_to(low)
    }

    // fn less_or_equal(key: &[u8], target: &[u8]) -> bool {
    //     let user_key1 = &key[..key.len() - 8];
    //     let user_key2 = &target[..target.len() - 8];
    //     // user_key1.
    //     match user_key1.cmp(user_key2) {
    //         std::cmp::Ordering::Less => true,
    //         std::cmp::Ordering::Greater => false,
    //         std::cmp::Ordering::Equal => {
    //             let seq1 = (&key[key.len() - 8..]).get_u64() >> 8;
    //             let seq2 = (&target[target.len() - 8..]).get_u64() >> 8;
    //             seq1 <= seq2
    //         }
    //     }
    // }

    fn greater_or_equal(key: &[u8], target: &[u8]) -> bool {
        let user_key1 = &key[..key.len() - 8];
        let user_key2 = &target[..target.len() - 8];
        // user_key1.
        match user_key1.cmp(user_key2) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => {
                let seq1 = (&key[key.len() - 8..]).get_u64() >> 8;
                let seq2 = (&target[target.len() - 8..]).get_u64() >> 8;
                seq1 >= seq2
            }
        }
    }
}

impl Iterator for BlockIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.seek_to(self.idx);
        self.idx += 1;
        res
    }
}

#[cfg(test)]
mod block_test {
    use std::{io::Read, path::Path, sync::Arc};

    use bytes::Buf;

    use crate::{
        mem_table::{MemTable, MemTableIterator},
        sstable::table_builder::TableBuilder,
        utils::{file::FileOptions, Entry},
    };

    use super::{Block, BlockIterator};

    #[test]
    fn block_test() {
        let mem = MemTable::new();
        for i in 0..300 {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            mem.set(e);
        }

        TableBuilder::build_table(
            "block.sst",
            FileOptions {
                block_size: 4096 * 2,
            },
            MemTableIterator::new(&mem),
        );
        let mut mem_iter = MemTableIterator::new(&mem);

        let mut file = std::fs::File::open(Path::new("block.sst")).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let len = buf.len();
        (&buf[len - 4..]).get_u32();
        let index_offset: u32 = (&buf[len - 8..]).get_u32();
        // let checksum = buf[len-8..]

        let block = Block::decode(&buf[..index_offset as usize]);
        let iter = BlockIterator::new(Arc::new(block));
        for (_, ele) in iter.enumerate() {
            let e = mem_iter.next().unwrap();
            // let e = Entry::new(
            //     (i as u32).to_be_bytes().to_vec(),
            //     (i as u32).to_be_bytes().to_vec(),
            //     i as u64,
            // );

            // let expected_key = build_internal_key(&e, 0);
            assert_eq!(ele.key, e.key);
            assert_eq!(ele.value, e.value);
        }
    }
}
