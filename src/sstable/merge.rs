use crate::utils::Entry;

use super::table::TableIterator;

pub struct MergeIterator {
    iters: Vec<TableIterator>,
    idx: usize,
    current: Option<Entry>,
}

impl MergeIterator {
    pub fn new(iters: Vec<TableIterator>) -> Self {
        Self {
            iters,
            idx: 0,
            current: None,
        }
    }
}

impl Iterator for MergeIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut smallest = None;
        let mut idx = self.idx;

        for (i, it) in self.iters.iter_mut().enumerate() {
            if self.current.is_none() {
                // first iter
                it.next();
            }
            let internal_key = it.key();
            if internal_key.is_none() {
                continue;
            }
            if smallest.is_none() || internal_key.clone()? < smallest.clone()? {
                smallest = internal_key;
                idx = i;
            }
        }
        self.current = self.iters[idx].item();
        self.iters[idx].next();
        self.idx = idx;
        self.current.clone()
    }
}

#[cfg(test)]
mod merge_test {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::{
        file::{path_of_file, Ext, RandomAccessFileImpl},
        mem_table::{MemTable, MemTableIterator},
        sstable::{
            table::{Table, TableIterator},
            table_builder::TableBuilder,
        },
        utils::Entry,
        version::{FileMetaData, InternalKey},
        Options,
    };

    use super::MergeIterator;

    #[test]
    fn seq_merge_test() {
        let opt = Options::default_opt().work_dir("work_dir/merge");
        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap()
        };
        std::fs::create_dir(&opt.work_dir).expect("create work direction fail!");

        // create table
        for i in 0..3 {
            let path = path_of_file(&opt.clone().work_dir, i, Ext::SST);
            let mem = MemTable::new();
            for j in 0..50 {
                let e = Entry::new(
                    Bytes::from((j as u32).to_be_bytes().to_vec()),
                    Bytes::from((j as u32).to_be_bytes().to_vec()),
                    j + i * 50,
                );
                mem.put(e);
            }
            let mut file_meta = FileMetaData::new(i);
            TableBuilder::build_table(
                path.as_path(),
                opt.clone(),
                MemTableIterator::new(&mem),
                &mut file_meta,
            )
            .unwrap();
        }

        // merge
        let mut merge_iter = vec![];
        for i in 0..3 {
            let path = path_of_file(&opt.clone().work_dir, i, Ext::SST);
            let t = Table::new(Box::new(RandomAccessFileImpl::open(path.as_path()))).unwrap();
            merge_iter.push(TableIterator::new(Arc::new(t)).unwrap());
        }
        let iter = MergeIterator::new(merge_iter);
        let (mut i, mut j) = (0, 0);
        for e in iter {
            let key = InternalKey::new(e.key);
            assert_eq!(key.user_key(), (j as u32).to_be_bytes());
            i += 1;
            if i % 3 == 0 {
                j += 1;
            }
        }
    }
}
