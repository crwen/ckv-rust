use std::sync::Arc;

use bytes::Buf;

use crate::utils::{
    file::{FileOptions, RandomAccessFile},
    Entry,
};

use super::{
    block::{Block, BlockHandler, BlockIterator, BLOCK_TRAILER_SIZE_},
    Result,
};

pub struct Table {
    #[allow(unused)]
    file_opt: FileOptions,
    file: Box<dyn RandomAccessFile>,
    index_block: Block,
}

impl Table {
    pub fn new(file_opt: FileOptions, file: Box<dyn RandomAccessFile>) -> Result<Self> {
        let mut footer = vec![0_u8; 8];
        let sz = file.size().unwrap();
        file.read(&mut footer, sz - 8).unwrap();
        let index_offset = (&footer[..4]).get_u32();
        let index_sz = (&footer[4..]).get_u32();

        let mut index_data = vec![1_u8; index_sz as usize + BLOCK_TRAILER_SIZE_];
        file.read(&mut index_data, index_offset as u64).unwrap();
        let index_block = Block::decode(&index_data);
        Ok(Self {
            file_opt,
            file,
            index_block,
        })
    }

    pub fn internal_get(&self, internal_key: &[u8]) -> Option<Entry> {
        // find data block first
        let mut index_iter = BlockIterator::new(Arc::new(self.index_block.clone()));
        let res = index_iter.seek(internal_key);
        let e = res.as_ref()?;
        let handler = BlockHandler::decode(e.value()).unwrap();

        // find in data block
        let mut data = vec![0_u8; handler.block_size() as usize + BLOCK_TRAILER_SIZE_];
        self.file.read(&mut data, handler.offset() as u64).unwrap();
        let data_block = Arc::new(Block::decode(&data));
        let mut data_iter = BlockIterator::new(data_block);
        data_iter.seek(internal_key)
    }

    fn read_block(&self, handler: BlockHandler) -> Block {
        let mut data = vec![0_u8; handler.block_size() as usize + BLOCK_TRAILER_SIZE_];
        self.file.read(&mut data, handler.offset() as u64).unwrap();
        Block::decode(&data)
    }
}

pub struct TableIterator {
    table: Arc<Table>,
    index_iter: BlockIterator,
    block_iter: BlockIterator,
    idx_block: usize,
}

impl TableIterator {
    pub fn new(table: Arc<Table>) -> Result<Self> {
        // table.index_block
        let mut index_iter = BlockIterator::new(Arc::new(table.index_block.clone()));
        let e = index_iter.next().unwrap();
        let handler = e.value();
        let offset = (&handler[..4]).get_u32();
        let block_size = (&handler[4..]).get_u32();
        // let block = Block::decode(data)
        let mut data = vec![0_u8; block_size as usize + BLOCK_TRAILER_SIZE_];

        if table.file.read(&mut data, offset as u64).is_err() {
            return Err(super::TableError::DecodeTableError);
        }
        let data_block = Block::decode(&data);
        let data_iter = data_block.into_iter();

        let it = Self {
            table,
            index_iter,
            block_iter: data_iter,
            idx_block: 0,
        };
        Ok(it)
    }
}
impl Iterator for TableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // self.index_block

        let mut res = self.block_iter.next();
        if res.is_none() {
            self.idx_block += 1;
            let e = self.index_iter.next().unwrap();
            let handler = BlockHandler::decode(&e.value).unwrap();
            self.table.read_block(handler);

            // TODO: next block
            res = self.block_iter.next();
        }
        res
    }
}

#[cfg(test)]
mod table_test {
    use std::path::Path;

    use crate::{
        mem_table::{MemTable, MemTableIterator},
        sstable::{table::Table, table_builder::TableBuilder},
        utils::{
            file::{FileOptions, RandomAccessFileImpl},
            Entry,
        },
    };

    #[test]
    fn table_seek_test() {
        let mem = MemTable::new();
        for i in 0..1000 {
            let e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            mem.set(e);
        }
        let mut mem_iter = MemTableIterator::new(&mem);
        TableBuilder::build_table(
            "table.sst",
            FileOptions { block_size: 4096 },
            MemTableIterator::new(&mem),
        );
        let t = Table::new(
            FileOptions { block_size: 4096 },
            Box::new(RandomAccessFileImpl::open(Path::new("table.sst"))),
        )
        .unwrap();

        for _ in 0..300 {
            let e = mem_iter.next().unwrap();
            let ikey = e.key;
            println!("{:?}", ikey);
            let res = t.internal_get(&ikey);
            assert!(res.is_some());
            assert_eq!(res.unwrap().key(), &ikey.to_vec());
        }
    }
}
