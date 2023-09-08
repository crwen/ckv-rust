use bytes::{Buf, BufMut};

use crate::utils::{
    codec::{decode_varintu32, varintu32_length, verify_checksum},
    Entry,
};

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
        let mut entry = &self.data[offset..];
        let key_sz = decode_varintu32(entry).unwrap();
        let varint_key_sz = varintu32_length(key_sz) as usize;
        let key = entry[varint_key_sz..varint_key_sz + key_sz as usize].to_vec();

        entry = &entry[varint_key_sz + key_sz as usize..];
        let value_sz = decode_varintu32(entry).unwrap();
        let varint_value_sz = varintu32_length(key_sz) as usize;
        let value = entry[varint_value_sz..varint_value_sz + value_sz as usize].to_vec();
        Some(Entry::new(key, value, 0))
    }

    // pub fn append(&mut self, data: &[u8]) {
    //     self.data.put(data);
    // }
}

pub struct BlockIterator<'a> {
    block: &'a Block,
    idx: usize,
}

impl<'a> BlockIterator<'a> {
    pub fn new(block: &'a Block) -> Self {
        Self { block, idx: 0 }
    }

    fn seek_to(&self, idx: usize) -> Option<Entry> {
        if idx >= self.block.entry_offsets.len() {
            return None;
        }
        let offset = self.block.entry_offsets[idx];
        self.block.read_entry_at(offset as usize)
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.seek_to(self.idx);
        self.idx += 1;
        res
    }
}

#[cfg(test)]
mod block_test {
    use std::{io::Read, path::Path};

    use bytes::Buf;

    use crate::{
        sstable::table_builder::TableBuilder,
        utils::{
            file::{FileOptions, WritableFileImpl},
            Entry,
        },
    };

    use super::{Block, BlockIterator};

    #[test]
    fn block_test() {
        let mut tb = TableBuilder::new(
            FileOptions { block_size: 4096 },
            Box::new(WritableFileImpl::new(Path::new("0001.sst"))),
        );
        for i in 0..260 {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            tb.add(&e.key, &e.value);
        }
        tb.finish();

        let mut file = std::fs::File::open(Path::new("0001.sst")).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let len = buf.len();
        (&buf[len - 4..]).get_u32();
        let index_offset: u32 = (&buf[len - 8..]).get_u32();
        // let checksum = buf[len-8..]

        let block = Block::decode(&buf[..index_offset as usize]);
        let iter = BlockIterator::new(&block);
        for (i, ele) in iter.enumerate() {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i as u64,
            );
            assert_eq!(ele.key, e.key);
            assert_eq!(ele.value, e.value);
        }
    }
}
