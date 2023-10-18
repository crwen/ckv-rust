use std::{io::Error, path::Path};

use crate::{
    file::{writeable::WritableFileImpl, Writable},
    utils::Entry,
    version::{FileMetaData, InternalKey},
    Options,
};

use super::{block::BlockHandler, block_builder::BlockBuilder};

enum BlockType {
    DataBlock,
    IndexBlock,
}
/// A block builder
pub struct TableBuilder {
    file_opt: Options,
    file: Box<dyn Writable>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    offset: u32,
    pending_handler: BlockHandler,
    last_key: Vec<u8>,
    pending_index_entry: bool,
    largest: InternalKey,
    smallest: InternalKey,
}

impl TableBuilder {
    pub fn new(file_opt: Options, file: Box<dyn Writable>) -> Self {
        TableBuilder {
            file_opt,
            pending_handler: BlockHandler::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            offset: 0,
            file,
            last_key: Vec::new(),
            pending_index_entry: false,
            largest: InternalKey::new(vec![]),
            smallest: InternalKey::new(vec![]),
        }
    }

    pub fn build_table<T>(
        path: &Path,
        opt: Options,
        iter: T,
        meta: &mut FileMetaData,
    ) -> Result<(), Error>
    where
        T: Iterator<Item = Entry>,
    {
        // let (mut largest, mut smallest) = (InternalKey::new(vec![]), InternalKey::new(vec![]));
        let mut tb = TableBuilder::new(opt, Box::new(WritableFileImpl::new(path)));

        iter.for_each(|e| {
            // if smallest.is_empty() {
            //     smallest = InternalKey::new(e.key.to_vec());
            // }
            // largest = InternalKey::new(e.key.to_vec());
            tb.add(&e.key, &e.value);
        });

        tb.finish();
        tb.file.sync()?;

        meta.set_file_size(tb.file.size()?);
        meta.set_smallest(tb.smallest.clone());
        meta.set_largest(tb.largest.clone());
        Ok(())
    }

    /// TODO: prefix compaction
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.smallest.is_empty() {
            self.smallest = InternalKey::new(key.to_vec());
        }
        self.largest = InternalKey::new(key.to_vec());

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

    pub fn finish_builder(&mut self, meta: &mut FileMetaData) -> Result<(), Error> {
        self.finish();
        self.file.sync()?;
        meta.set_file_size(self.file.size()?);
        meta.set_smallest(self.smallest.clone());
        meta.set_largest(self.largest.clone());
        Ok(())
    }

    fn finish(&mut self) {
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
    use std::io::Read;

    use bytes::Buf;

    use crate::{
        file::{path_of_file, Ext},
        mem_table::{MemTable, MemTableIterator},
        sstable::block::{Block, BLOCK_TRAILER_SIZE_},
        utils::Entry,
        version::FileMetaData,
        Options,
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
            mem.put(e);
        }

        let opt = Options {
            block_size: 4096,
            work_dir: "work_dir/table_builder".to_string(),
            mem_size: 4096,
        };
        let path = path_of_file(&opt.work_dir, 10, Ext::SST);

        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap();
        };
        std::fs::create_dir(&opt.work_dir).expect("create work direction fail!");
        let mut file_meta = FileMetaData::new(10);
        TableBuilder::build_table(
            path.as_path(),
            opt,
            MemTableIterator::new(&mem),
            &mut file_meta,
        )
        .unwrap();
        let mut mem_iter = MemTableIterator::new(&mem);

        let mut file = std::fs::File::open(path).unwrap();
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
