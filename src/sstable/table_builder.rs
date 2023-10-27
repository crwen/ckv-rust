use std::{io::Error, path::Path};

use bytes::BufMut;

use crate::{
    file::{log_writer::Writer, path_of_file, writeable::WritableFileImpl, Ext, Writable},
    utils::{bloom::BloomFilter, Entry, FilterPolicy},
    version::{FileMetaData, InternalKey},
    Options,
};

use super::{block::BlockHandler, block_builder::BlockBuilder};

enum BlockType {
    Data,
    Index,
    Filter,
}
/// A block builder
pub struct TableBuilder {
    file_opt: Options,
    #[allow(unused)]
    fid: u64,
    file: Box<dyn Writable>,
    vlog: Option<Writer>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    offset: u32,
    pending_handler: BlockHandler,
    last_key: Vec<u8>,
    pending_index_entry: bool,
    largest: InternalKey,
    smallest: InternalKey,
    filters_keys: Vec<Vec<u8>>,
    filters: Vec<u8>,
}

impl TableBuilder {
    pub fn new(file_opt: Options, file: Box<dyn Writable>, fid: u64) -> Self {
        TableBuilder {
            pending_handler: BlockHandler::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            offset: 0,
            file,
            vlog: None,
            fid,
            last_key: Vec::new(),
            pending_index_entry: false,
            largest: InternalKey::new(vec![]),
            smallest: InternalKey::new(vec![]),
            filters_keys: Vec::new(),
            filters: Vec::new(),
            file_opt,
        }
    }

    pub fn build_table<T>(
        path: &Path,
        opt: Options,
        iter: T,
        meta: &mut FileMetaData,
    ) -> Result<(), anyhow::Error>
    where
        T: Iterator<Item = Entry>,
    {
        // let (mut largest, mut smallest) = (InternalKey::new(vec![]), InternalKey::new(vec![]));
        let fid = meta.number;
        let mut tb = TableBuilder::new(opt, Box::new(WritableFileImpl::new(path)), fid);

        iter.for_each(|e| {
            let mut value_wrapper = vec![];
            if !e.value.is_empty() && e.value.len() >= tb.file_opt.kv_separate_threshold {
                if tb.vlog.is_none() {
                    tb.vlog = Some(Writer::new(WritableFileImpl::new(&path_of_file(
                        &tb.file_opt.work_dir.clone(),
                        fid,
                        Ext::VLOG,
                    ))));
                    meta.vlogs.push(fid);
                }
                let off = tb.vlog.as_ref().unwrap().offset();
                tb.vlog
                    .as_ref()
                    .unwrap()
                    .add_recore(&e.value)
                    .expect("write vlog failed!");
                value_wrapper.put_u8(1);
                value_wrapper.put_u64(fid);
                value_wrapper.put_u64(off);
            } else {
                value_wrapper.put_u8(0);
                value_wrapper.put_slice(&e.value);
            }
            tb.add(&e.key, &value_wrapper);
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

        let internal_key = InternalKey::new(key.to_vec());
        self.filters_keys.push(internal_key.user_key().to_vec());

        self.last_key = key.to_vec();
        self.data_block.add(key, value);

        let estimated_size = self.data_block.estimated_size();
        if estimated_size >= self.file_opt.block_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.write_block(BlockType::Data);
        self.pending_index_entry = true;
        self.file.flush().unwrap();
    }

    fn write_block(&mut self, block_type: BlockType) {
        let content = match block_type {
            BlockType::Data => self.data_block.finish(),
            BlockType::Index => self.index_block.finish(),
            BlockType::Filter => &self.filters,
        };

        self.pending_handler.set_offset(self.offset);
        self.pending_handler
            .set_block_size(content.len() as u32 - 8);

        self.offset += content.len() as u32;
        self.file.append(content).unwrap();
        match block_type {
            BlockType::Data => self.data_block.reset(),
            BlockType::Index => self.index_block.reset(),
            BlockType::Filter => {
                self.filters_keys = vec![];
                self.filters = vec![];
            }
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

        // write index block
        if self.pending_index_entry {
            let handler = self.pending_handler.to_vec();
            self.index_block.add(&self.last_key, &handler);
            self.pending_index_entry = false;
        }

        let bloom = BloomFilter::new(BloomFilter::bits_per_key(
            self.filters_keys.len() as u32,
            0.1,
        ));

        // write filter block
        self.filters = bloom.create_filter(&self.filters_keys);
        let mut filter_handler = BlockHandler::new();
        filter_handler.set_offset(self.offset);
        filter_handler.set_block_size(self.filters.len() as u32);

        self.write_block(BlockType::Filter);

        // write index block
        self.write_block(BlockType::Index);

        // write footer
        self.file.append(&filter_handler.to_vec()).unwrap();
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

        let opt = Options::default_opt().work_dir("work_dir/table_builder");
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
                assert_eq!(e.value[1..], expected_value);
                i += 1;
                lkey = expected_key.to_vec();
            });
            assert_eq!(lkey, last_key);
        })
    }
}
