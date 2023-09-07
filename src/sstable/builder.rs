use bytes::BufMut;

use crate::utils::{
    codec::encode_varintu32,
    convert::u32vec_to_bytes,
    file::{FileOptions, WriteableFile},
    Entry,
};

use super::block::{Block, SIZEOF_U32};

/// A block builder
pub struct TableBuilder {
    blocks: Vec<Block>,
    curr_block: Block,
    entry_offsets: Vec<u32>,
    offset: u32,
    file_opt: FileOptions,
    file: Box<dyn WriteableFile>,
}

impl TableBuilder {
    pub fn new(file_opt: FileOptions, file: Box<dyn WriteableFile>) -> Self {
        TableBuilder {
            blocks: Vec::new(),
            curr_block: Block::new(),
            entry_offsets: Vec::new(),
            offset: 0,
            file_opt,
            file,
        }
    }

    /// TODO: prefix compaction
    pub fn add(&mut self, entry: Entry) {
        let key = entry.key();
        let value = entry.value();
        let seq = entry.seq();
        let mut entry_data = vec![];

        // encode key
        encode_varintu32(&mut entry_data, key.len() as u32);
        entry_data.put(&key[..]);
        entry_data.put_u64(seq);

        // encode value
        encode_varintu32(&mut entry_data, value.len() as u32);
        entry_data.put(&value[..]);

        self.try_finish_block(entry_data.len());

        self.curr_block.append(&entry_data);
        self.entry_offsets.push(self.offset);
        self.offset += entry_data.len() as u32;
    }

    fn try_finish_block(&mut self, expected_len: usize) {
        // checksum(8) + entry_offsets_len(4)
        if self.curr_block.estimated_size()
            + self.entry_offsets.len() * SIZEOF_U32
            + expected_len
            + 12
            < self.file_opt.block_size()
        {
            return;
        }

        self.finish_block();
        self.file.flush().unwrap();
    }

    /// finishBlock write other info to Block, e.g. entry offsets, checksum
    ///
    /// +--------------------- --------------------------+
    /// |  data | entryOffsets | entryOff len | checksum |
    /// +------------------------------------------------+
    fn finish_block(&mut self) {
        // write entry offsets
        self.curr_block
            .append(&u32vec_to_bytes(&self.entry_offsets));
        self.curr_block
            .append(&self.entry_offsets.len().to_be_bytes());
        // write checksum
        self.curr_block
            .append(&self.curr_block.calculate_checksum().to_be_bytes());
        // switch blocks
        let block = std::mem::replace(&mut self.curr_block, Block::new());
        self.file.append(block.data()).unwrap();
        self.blocks.push(block);
        self.entry_offsets = Vec::new();
    }
}

#[cfg(test)]
mod buildtest {
    use std::path::Path;

    use crate::utils::{
        file::{FileOptions, WritableFileImpl},
        Entry,
    };

    use super::TableBuilder;

    #[test]
    fn test_write() {
        let mut tb = TableBuilder::new(
            FileOptions { block_size: 4096 },
            Box::new(WritableFileImpl::new(Path::new("0001.sst"))),
        );
        for i in 0..1000 {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            tb.add(e);
        }
        tb.finish_block();
    }
}
