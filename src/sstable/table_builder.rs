use std::path::Path;

use crate::utils::{
    file::{FileOptions, WritableFileImpl, WriteableFile},
    Entry,
};

use super::{block::BlockHandler, block_builder::BlockBuilder};

enum BlockType {
    DataBlock,
    IndexBlock,
}
/// A block builder
pub struct TableBuilder {
    file_opt: FileOptions,
    file: Box<dyn WriteableFile>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    offset: u32,
    pending_handler: BlockHandler,
    last_key: Vec<u8>,
    pending_index_entry: bool,
}

impl TableBuilder {
    pub fn new(file_opt: FileOptions, file: Box<dyn WriteableFile>) -> Self {
        TableBuilder {
            file_opt,
            pending_handler: BlockHandler::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            offset: 0,
            file,
            last_key: Vec::new(),
            pending_index_entry: false,
        }
    }

    pub fn build_table<T>(dbname: &str, opt: FileOptions, iter: T)
    where
        T: Iterator<Item = Entry>,
    {
        let mut tb = TableBuilder::new(opt, Box::new(WritableFileImpl::new(Path::new(dbname))));

        iter.for_each(|e| tb.add(&e.key, &e.value));

        tb.finish();
        tb.file.sync().unwrap();
    }

    /// TODO: prefix compaction
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.pending_index_entry {
            self.index_block
                .add(&self.last_key, &self.pending_handler.to_vec());
            self.pending_index_entry = false;
        }

        self.last_key = key.to_vec();
        self.data_block.add(key, value);

        let estimated_size = self.data_block.estimated_size();
        if estimated_size > self.file_opt.block_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.write_block(BlockType::DataBlock);
        self.pending_index_entry = true;
        self.file.flush().unwrap();
    }

    fn write_block(&mut self, block_type: BlockType) {
        let content = match block_type {
            BlockType::DataBlock => self.data_block.finish(),
            BlockType::IndexBlock => self.index_block.finish(),
        };

        self.pending_handler.set_offset(self.offset);
        self.pending_handler
            .set_block_size(content.len() as u32 - 8);

        self.offset += content.len() as u32;
        self.file.append(content).unwrap();
        match block_type {
            BlockType::DataBlock => self.data_block.reset(),
            BlockType::IndexBlock => self.index_block.reset(),
        };
    }

    pub fn finish(&mut self) {
        // write last data block
        self.flush();

        // TODO: write filter block

        // write index block
        if self.pending_index_entry {
            let handler = self.pending_handler.to_vec();
            self.index_block.add(&self.last_key, &handler);
            self.pending_index_entry = false;
        }
        self.write_block(BlockType::IndexBlock);

        // write footer
        self.file.append(&self.pending_handler.to_vec()).unwrap();
    }
}

#[cfg(test)]
mod builder_test {
    use std::{io::Read, path::Path};

    use bytes::Buf;

    use crate::{
        mem_table::{MemTable, MemTableIterator},
        sstable::block::{Block, BLOCK_TRAILER_SIZE_},
        utils::{file::FileOptions, Entry},
    };

    use super::TableBuilder;

    #[test]
    fn builder_test() {
        let mem = MemTable::new();
        for i in 0..1000 {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            mem.set(e);
        }
        TableBuilder::build_table(
            "builder.sst",
            FileOptions { block_size: 4096 },
            MemTableIterator::new(&mem),
        );
        let mut mem_iter = MemTableIterator::new(&mem);

        let mut file = std::fs::File::open(Path::new("builder.sst")).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let len = buf.len();
        let index_sz = (&buf[len - 4..]).get_u32() as usize;
        let index_offset = (&buf[len - 8..]).get_u32() as usize;
        let index_end = index_sz + index_offset + BLOCK_TRAILER_SIZE_;

        let index = &buf[index_offset..index_end];
        let index_block = Block::decode(index);

        let index_iter = index_block.into_iter();
        let mut i: u32 = 0;
        index_iter.for_each(|e| {
            let last_key = e.key;
            let handler = e.value;
            let offset = (&handler[..4]).get_u32() as usize;
            let block_sz = (&handler[4..]).get_u32() as usize;

            let data = &buf[offset..offset + block_sz + BLOCK_TRAILER_SIZE_];
            let data_block = Block::decode(data);
            // Block::decode(data);
            let mut lkey: Vec<u8> = Vec::new();
            let iter = data_block.into_iter();
            iter.for_each(|e| {
                let mem_entry = mem_iter.next().unwrap();
                let expected_key = mem_entry.key;
                let expected_value = i.to_be_bytes();
                assert_eq!(e.key, expected_key);
                assert_eq!(e.value, expected_value);
                i += 1;
                lkey = expected_key.to_vec();
            });
            assert_eq!(lkey, last_key);
        })
    }
}
