use std::sync::Arc;

use bytes::{Buf, Bytes};

use crate::{
    file::{path_of_file, RandomAccess, RandomAccessFileImpl, RandomReader},
    utils::{bloom::BloomFilter, Entry, FilterPolicy},
    version::InternalKey,
    Options,
};

use super::{
    block::{Block, BlockHandler, BlockIterator, BLOCK_TRAILER_SIZE_},
    Result,
};

struct Footer {
    filter_handler: BlockHandler,
    index_handler: BlockHandler,
}

impl Footer {
    fn decode(data: &[u8]) -> Self {
        let mut filter_handler = BlockHandler::new();
        filter_handler.set_offset((&data[0..4]).get_u32());
        filter_handler.set_block_size((&data[4..8]).get_u32());

        let mut index_handler = BlockHandler::new();
        index_handler.set_offset((&data[8..12]).get_u32());
        index_handler.set_block_size((&data[12..]).get_u32());
        Self {
            filter_handler,
            index_handler,
        }
    }
}

pub struct Table {
    // #[allow(unused)]
    // file_opt: Options,
    file: Box<dyn RandomAccess>,
    index_block: Block,
    #[allow(dead_code)]
    smallest: InternalKey,
    #[allow(dead_code)]
    largest: InternalKey,
    file_sz: u64,
    bloom: BloomFilter,
    filter_data: Vec<u8>,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    pub fn new(file: Box<dyn RandomAccess>) -> anyhow::Result<Self, anyhow::Error> {
        // read footer
        let mut footer = vec![0_u8; 16];
        let sz = file.size().unwrap();
        file.read(&mut footer, sz - 16).unwrap();
        let footer = Footer::decode(&footer);

        // read index
        let mut index_data =
            vec![0_u8; footer.index_handler.block_size() as usize + BLOCK_TRAILER_SIZE_];
        file.read(&mut index_data, footer.index_handler.offset() as u64)
            .unwrap();
        let index_block = Block::decode(&index_data);
        let file_sz = file.size()?;

        // read filter
        let mut filter_data = vec![0_u8; footer.filter_handler.block_size() as usize];
        file.read(&mut filter_data, footer.filter_handler.offset() as u64)
            .unwrap();

        Ok(Self {
            // file_opt,
            file,
            index_block,
            smallest: InternalKey::new(Bytes::new()),
            largest: InternalKey::new(Bytes::new()),
            file_sz,
            bloom: BloomFilter::new(BloomFilter::bits_per_key(1999, 0.1)),
            filter_data,
        })
    }

    pub fn size(&self) -> u64 {
        self.file_sz
    }

    pub fn internal_get(&self, opt: &Options, internal_key: &[u8]) -> Option<Entry> {
        let target = InternalKey::new(Bytes::from(internal_key.to_vec()));
        if !self.bloom.may_contain(&self.filter_data, target.user_key()) {
            return None;
        }
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

        if let Some(mut e) = data_iter.seek(internal_key) {
            let found = InternalKey::new(e.clone().key);
            let target = InternalKey::new(Bytes::from(internal_key.to_vec()));
            if found.user_key() == target.user_key() {
                let v = e.value.clone();
                if !v.is_empty() && v[0] == 0 {
                    e.value = Bytes::from(v[1..].to_vec());
                } else if !v.is_empty() {
                    let fid = (&e.value[1..9]).get_u64();
                    let offset = (&e.value[9..17]).get_u64();
                    let path = path_of_file(&opt.work_dir, fid, crate::file::Ext::VLOG);
                    let mut vlog =
                        RandomReader::new(Box::new(RandomAccessFileImpl::open(path.as_path())));
                    e.value = Bytes::from(vlog.read_record(offset).unwrap());
                }

                Some(e)
            } else {
                None
            }
        } else {
            None
        }
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
    curr: Option<Entry>,
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
            curr: None,
        };
        Ok(it)
    }

    pub fn key(&self) -> Option<InternalKey> {
        let entry = self.curr.clone()?;
        Some(InternalKey::new(entry.key))
    }

    pub fn item(&self) -> Option<Entry> {
        self.curr.clone()
    }
}
impl Iterator for TableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // self.index_block

        let mut res = self.block_iter.next();
        if res.is_none() {
            self.idx_block += 1;
            if let Some(e) = self.index_iter.next() {
                // let e = self.index_iter.next()?;
                // let handler = BlockHandler::decode(&e.value).expect("Decode block fail!");
                let handler = BlockHandler::decode(e.value).expect("Decode block fail!");

                let data_block = self.table.read_block(handler);
                self.block_iter = data_block.into_iter();

                res = self.block_iter.next();
            }
        }
        self.curr = res.clone();
        res
    }
}

#[cfg(test)]
mod table_test {

    use std::sync::Arc;

    use bytes::Bytes;

    use crate::{
        file::{path_of_file, Ext, RandomAccessFileImpl},
        mem_table::{MemTable, MemTableIterator},
        sstable::{table::Table, table_builder::TableBuilder},
        utils::Entry,
        version::FileMetaData,
        Options,
    };

    use super::TableIterator;

    #[test]
    fn table_seek_test() {
        let mem = MemTable::new();
        for i in 0..1000 {
            let e = Entry::new(
                Bytes::from((i as u32).to_be_bytes().to_vec()),
                Bytes::from((i as u32).to_be_bytes().to_vec()),
                i,
            );
            mem.put(e);
        }

        let opt = Options::default_opt()
            .work_dir("work_dir/table")
            .mem_size(4096 * 2);
        let path = path_of_file(&opt.work_dir, 1, Ext::SST);
        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap();
        };
        std::fs::create_dir(&opt.work_dir).expect("create work direction fail!");

        let mut mem_iter = MemTableIterator::new(&mem);
        let mut file_meta = FileMetaData::new(1);
        TableBuilder::build_table(
            path.as_path(),
            opt.clone(),
            MemTableIterator::new(&mem),
            &mut file_meta,
        )
        .unwrap();
        // let t = Table::new(opt, Box::new(RandomAccessFileImpl::open(path.as_path()))).unwrap();
        let t = Table::new(Box::new(RandomAccessFileImpl::open(path.as_path()))).unwrap();

        for _ in 0..300 {
            let e = mem_iter.next().unwrap();
            let ikey = e.key;
            let res = t.internal_get(&opt, &ikey);
            assert!(res.is_some());
            assert_eq!(res.clone().unwrap().key(), &ikey.to_vec());
            assert_eq!(res.unwrap().value(), &ikey[..4].to_vec());
        }

        let iter = TableIterator::new(Arc::new(t)).unwrap();
        let mut count = 0;
        iter.for_each(|_| count += 1);
        assert_eq!(count, 1000)
    }
}
