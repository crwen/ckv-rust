use std::{
    collections::LinkedList,
    io::Error,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::BufMut;
use parking_lot::RwLock;

use crate::{
    file::{
        log_writer::Writer, path_of_file, writeable::WritableFileImpl, Ext, RandomAccessFileImpl,
        Writable,
    },
    sstable::table::Table,
    utils::{Entry, OP_TYPE_PUT},
    Options,
};

use super::{version_edit::VersionEdit, FileMetaData};

type Result<T> = core::result::Result<T, Error>;

#[derive(Default, Debug)]
pub struct Version {
    files: Vec<Vec<FileMetaData>>,
}

impl Version {
    pub fn new() -> Self {
        let mut files: Vec<Vec<FileMetaData>> = Vec::new();
        files.resize_with(7, std::vec::Vec::new);
        Self { files }
    }

    pub fn build(version: Arc<Version>, edit: &VersionEdit) -> Self {
        let mut files = version.files.clone();

        for f in edit.add_files.iter() {
            let level = f.level as usize;
            files[level].push(f.file_meta.clone());
        }

        for (i, f) in edit.delete_files.iter().enumerate() {
            let level = f.level as usize;
            files[level].remove(i);
        }

        Self { files }
    }

    pub fn files(&self) -> &Vec<Vec<FileMetaData>> {
        &self.files
    }

    pub fn level_files(&self, level: usize) -> &Vec<FileMetaData> {
        &self.files[level]
    }

    // +------------+
    // |  key | tag |
    // +------------+
    fn build_internal_key(user_key: &[u8], seq: u64) -> Vec<u8> {
        let mut internal_key = vec![];
        internal_key.put_slice(user_key);

        // let vkey_sz = varintu32_length(key_sz);

        // internal_key.put_slice(user_key);
        internal_key.put_u64((seq << 8) | OP_TYPE_PUT as u64);

        internal_key
    }

    pub fn get(&self, opt: Options, user_key: &[u8], seq: u64) -> Option<Vec<u8>> {
        // search L0 first
        let mut tmp = Vec::new();
        let internal_key = Version::build_internal_key(user_key, seq);
        for (i, files) in self.files.iter().enumerate() {
            if i == 0 {
                files
                    .iter()
                    .filter(|f| {
                        f.smallest.user_key() <= user_key && f.largest.user_key() >= user_key
                    })
                    .for_each(|f| tmp.push(f));

                if !tmp.is_empty() {
                    // tmp.sort_by(|a, b| a.number.partial_cmp(&b.number).unwrap());
                    tmp.sort_by(|a, b| a.number.cmp(&b.number));
                    for f in tmp.iter() {
                        let path = path_of_file(&opt.work_dir, f.number, Ext::SST);
                        let entry = self.search_sst(&path, &internal_key.clone());
                        if entry.is_none() {
                            continue;
                        }
                        return entry.map(|e| {
                            let value = e.value();
                            // let value_sz = decode_varintu32(value).unwrap();
                            // Bytes::from(value[varintu32_length(value_sz) as usize..].to_vec())
                            value.to_vec()
                        });
                    }
                }
            } else {
                // search other levels
                let f = files.iter().find(|f| {
                    f.smallest.user_key() <= user_key && f.largest.user_key() >= user_key
                });
                if let Some(f) = f {
                    let path = path_of_file(&opt.work_dir, f.number, Ext::SST);
                    let entry = self.search_sst(path.as_path(), &internal_key.clone());
                    if let Some(e) = entry {
                        return Some(e.value);
                    }
                }
            }
        }
        None
    }

    fn search_sst(&self, path: &Path, internal_key: &[u8]) -> Option<Entry> {
        let t = Table::new(Box::new(RandomAccessFileImpl::open(path))).unwrap();
        t.internal_get(internal_key)
    }

    pub fn pick_level_for_mem_table_output(&self, smallest: &[u8], largest: &[u8]) -> u32 {
        let mut level = 0;
        if !self.overlap_in_level(level, smallest, largest) {
            // push t onext level if there is no overlap in next level.
            // and the #bytes overlapping in the level after that are limited
            while level < 3 {
                if self.overlap_in_level(level + 1, smallest, largest) {
                    break;
                }
                // if level + 2 < 3 {
                //     // check that file does not overlap too many grandparent bytes.
                // }
                level += 1;
            }
        }
        level
    }

    fn overlap_in_level(&self, level: u32, smallest: &[u8], largest: &[u8]) -> bool {
        if self.files.len() <= level as usize {
            return false;
        }
        let overlapping: Vec<_> = self.files[level as usize]
            .iter()
            .filter(|f| !(f.smallest.user_key() > largest || f.largest.user_key() < smallest))
            .collect();

        !overlapping.is_empty()
    }
}

pub struct VersionSet {
    #[allow(dead_code)]
    versions: Arc<RwLock<LinkedList<Arc<Version>>>>,
    last_sequence: AtomicU64,
    next_file_number: AtomicU64,
    #[allow(dead_code)]
    log_file: Writer,
}

#[allow(dead_code)]
struct VersionSetInner {
    #[allow(dead_code)]
    versions: LinkedList<Arc<Version>>,
    last_sequence: AtomicU64,
    log_number: AtomicU64,
    next_file_number: AtomicU64,
    #[allow(dead_code)]
    log_file: Box<dyn Writable>,
}

impl VersionSet {
    pub fn new(opt: Options) -> Self {
        let mut versions = LinkedList::new();
        versions.push_back(Arc::new(Version::new()));
        Self {
            versions: Arc::new(RwLock::new(versions)),
            next_file_number: AtomicU64::new(0),
            last_sequence: AtomicU64::new(0),
            log_file: Writer::new(WritableFileImpl::new(&path_of_file(
                &opt.work_dir,
                0,
                Ext::WAL,
            ))),
        }
    }

    pub fn current(&self) -> Arc<Version> {
        let versions = self.versions.read();
        versions.back().unwrap().clone()
    }

    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    pub fn add_last_sequence(&self, n: u64) -> u64 {
        self.last_sequence.fetch_add(n, Ordering::SeqCst)
    }

    pub fn pick_level_for_mem_table_output(&self, smallest: &[u8], largest: &[u8]) -> u32 {
        self.current()
            .pick_level_for_mem_table_output(smallest, largest)
    }

    pub fn log_and_apply(&self, edit: VersionEdit) -> Result<()> {
        // write manifest
        let mut data = vec![];
        edit.encode(&mut data);
        self.log_file.add_recore(&data)?;

        let mut versions = self.versions.write();

        // modify memory metadata
        let base = versions.back().unwrap().clone();
        let current = Version::build(base, &edit);
        versions.push_back(Arc::new(current));
        Ok(())
    }
}
