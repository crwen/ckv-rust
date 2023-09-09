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
        let e = index_iter.seek(internal_key).unwrap();
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

    use bytes::BufMut;

    use crate::{
        sstable::{table::Table, table_builder::TableBuilder},
        utils::{
            codec::encode_varintu32,
            file::{FileOptions, RandomAccessFileImpl, WritableFileImpl},
            Entry,
        },
    };

    fn build_internal_key(entry: &Entry, typ: u8) -> Vec<u8> {
        let key = entry.key();
        let seq = entry.seq();
        let key_sz = key.len() as u32;
        let mut internal_key = vec![];

        encode_varintu32(&mut internal_key, key_sz);

        internal_key.put_slice(key);
        internal_key.put_u64((seq << 8) | typ as u64);

        internal_key
    }
    #[test]
    fn table_seek_test() {
        let mut tb = TableBuilder::new(
            FileOptions { block_size: 4096 },
            Box::new(WritableFileImpl::new(Path::new("table.sst"))),
        );
        for i in 0..1000 {
            let mut e = Entry::new(
                (i as u32).to_be_bytes().to_vec(),
                (i as u32).to_be_bytes().to_vec(),
                i,
            );
            e.key = build_internal_key(&e, 0);
            tb.add(&e.key, &e.value);
        }
        tb.finish();
        let t = Table::new(
            FileOptions { block_size: 4096 },
            Box::new(RandomAccessFileImpl::open(Path::new("table.sst"))),
        )
        .unwrap();

        for i in 0..300 {
            let k: u32 = i as u32;
            let e = Entry::new(k.to_be_bytes().to_vec(), k.to_be_bytes().to_vec(), i);

            let ikey = build_internal_key(&e, 0);
            let res = t.internal_get(&ikey).unwrap();
            assert_eq!(res.key(), &ikey.to_vec());
        }
    }
}
