use std::{
    collections::{HashMap, HashSet, LinkedList},
    io::ErrorKind,
    // io::Error,
    path::Path,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::Ok;
use bytes::{Buf, BufMut};
use parking_lot::RwLock;
use tracing::info;

use crate::{
    cache::Cache,
    // cache::lru::LRUCache,
    compactor::{CompactionState, GCState, SeekTask, Task},
    file::{
        path_of_file, Ext, RandomAccessFileImpl, SequentialFileImpl, Writable, WritableFileImpl,
        Writer, {RandomReader, Reader},
    },
    sstable::{
        Block, MergeIterator, TableBuilder, {Table, TableIterator},
    },
    utils::{Entry, OP_TYPE_PUT},
    Options,
};

use super::{version_edit::VersionEdit, FileMetaData, InternalKey};

// type Result<T> = core::result::Result<T, dyn Error>;
type Result<T> = anyhow::Result<T, anyhow::Error>;

const L0_COMPACTION_TRIGGER: u32 = 4;
const L1_COMPACTION_TRIGGER: f64 = 1048576.0;
const MAX_MEM_COMPACT_LEVEL: u32 = 0x2;
// const L1_COMPACTION_TRIGGER: f64 = 100.0;

pub struct Version {
    files: Vec<Vec<FileMetaData>>,
    refs: AtomicU32,
    smallest_sequence: u64,
    smallest_log_number: u64,
    table_cache: Arc<Cache<u64, Table>>,
    #[allow(unused)]
    index_cache: Arc<Cache<u64, Block>>,
}

impl Version {
    pub fn new(table_cache: Arc<Cache<u64, Table>>, block_cache: Arc<Cache<u64, Block>>) -> Self {
        let mut files: Vec<Vec<FileMetaData>> = Vec::new();
        files.resize_with(7, std::vec::Vec::new);
        Self {
            files,
            refs: AtomicU32::new(1),
            smallest_sequence: 0,
            smallest_log_number: 0,
            table_cache,
            index_cache: block_cache,
        }
    }

    pub fn build(
        table_cache: Arc<Cache<u64, Table>>,
        block_cache: Arc<Cache<u64, Block>>,
        version: Arc<Version>,
        edit: &VersionEdit,
    ) -> Self {
        let mut files = version.files.clone();

        for f in edit.add_files.iter() {
            let level = f.level as usize;
            files[level].push(f.file_meta.clone());
        }

        let mut set = HashSet::new();
        edit.delete_files.iter().for_each(|f| {
            set.insert(f.file_meta.number);
        });
        for f in edit.delete_files.iter() {
            let level = f.level as usize;
            let id = f.file_meta.number;
            if let Some(idx) = files[level].iter().position(|f| f.number == id) {
                files[level].remove(idx);
            }
        }

        Self {
            files,
            refs: AtomicU32::new(1),
            smallest_sequence: edit.last_seq_number,
            smallest_log_number: edit.log_number,
            table_cache,
            index_cache: block_cache,
        }
    }

    pub fn refs(&self) {
        self.refs.fetch_add(1, Ordering::SeqCst);
    }

    pub fn refs_cnt(&self) -> u32 {
        self.refs.load(Ordering::SeqCst)
    }

    pub fn derefs(&self) {
        self.refs.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn smallest_sequence(&self) -> u64 {
        self.smallest_sequence
    }
    pub fn smallest_log_number(&self) -> u64 {
        self.smallest_log_number
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

    pub fn get(&self, opt: Options, user_key: &[u8], seq: u64) -> (Option<Vec<u8>>, Option<Task>) {
        // search L0 first
        let mut tmp = Vec::new();
        let internal_key = Version::build_internal_key(user_key, seq);
        let mut task = (self.files[0].len() > 5).then_some(Task::Major);
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
                        // let path = path_of_file(&opt.work_dir, f.number, Ext::SST);
                        // let entry = self.search_sst(&path, &internal_key.clone());
                        let entry = self.search_sst(&opt, f.number, &internal_key.clone());
                        if entry.is_none() {
                            let seek = f.increase_seek();
                            if seek >= 100 && task.is_none() {
                                f.allow_seek_reset();
                                task = Some(Task::Seek(SeekTask {
                                    level: 0,
                                    fid: f.number,
                                }))
                            }
                            continue;
                        }
                        return (
                            entry.map(|e| {
                                let value = e.value();
                                // let value_sz = decode_varintu32(value).unwrap();
                                // Bytes::from(value[varintu32_length(value_sz) as usize..].to_vec())
                                value.to_vec()
                            }),
                            task,
                        );
                    }
                }
            } else {
                // search other levels
                let f = files.iter().find(|f| {
                    f.smallest.user_key() <= user_key && f.largest.user_key() >= user_key
                });
                if let Some(f) = f {
                    let entry = self.search_sst(&opt, f.number, &internal_key.clone());
                    if let Some(e) = entry {
                        // return (Some(e.value), task);
                        return (Some(e.value.to_vec()), task);
                    } else {
                        let seek = f.increase_seek();
                        if seek >= 100 && task.is_none() {
                            f.allow_seek_reset();
                            task = Some(Task::Seek(SeekTask {
                                level: i as u32,
                                fid: f.number,
                            }))
                        }
                    }
                }
            }
        }
        (None, task)
    }

    fn search_sst(&self, opt: &Options, fid: u64, internal_key: &[u8]) -> Option<Entry> {
        let res = match self.table_cache.get(&fid) {
            Some(t) => t.internal_get(opt, internal_key),
            None => {
                let path = path_of_file(&opt.work_dir, fid, Ext::SST);
                let t = Table::new(Box::new(RandomAccessFileImpl::open(path.as_path()))).unwrap();
                let res = t.internal_get(opt, internal_key);
                let _e = self.table_cache.insert(fid, t, 1);
                res
            }
        };
        let _ = self.table_cache.unpin(&fid);

        res
    }

    pub fn pick_level_for_mem_table_output(&self, smallest: &[u8], largest: &[u8]) -> u32 {
        let mut level = 0;
        if !self.overlap_in_level(level, smallest, largest) {
            // push t onext level if there is no overlap in next level.
            // and the #bytes overlapping in the level after that are limited
            while level < MAX_MEM_COMPACT_LEVEL {
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

    pub fn pick_compact_level(&self) -> Option<usize> {
        let mut best_score = 0_f64;
        let mut best_level = 0_usize;
        for (level, files) in self.files.iter().enumerate() {
            let score = if level == 0 {
                files.len() as f64 / L0_COMPACTION_TRIGGER as f64
            } else {
                self.total_size(level) / Version::max_bytes_for_level(level)
            };
            if score > best_score {
                best_level = level;
                best_score = score;
            }
        }
        best_score.ge(&1.0).then_some(best_level)
        // if best_score > 0.8 {
        //     return Some(best_level);
        // }
        // None
    }

    fn total_size(&self, level: usize) -> f64 {
        if level >= self.files.len() {
            return 0_f64;
        }
        let mut size = 0;
        self.files[level].iter().for_each(|f| size += f.file_size);
        size as f64
    }

    fn max_bytes_for_level(level: usize) -> f64 {
        // let mut result = 1048576.0;
        let mut result = L1_COMPACTION_TRIGGER;
        let mut level = level;
        while level > 1 {
            result *= 10.0;
            level -= 1;
        }
        result
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

    fn overlaping_inputs(&self, level: u32, smallest: &[u8], largest: &[u8]) -> Vec<FileMetaData> {
        if self.files.len() <= level as usize {
            return vec![];
        }
        let mut inputs = vec![];
        self.files[level as usize]
            .iter()
            .filter(|f| !(f.smallest.user_key() > largest || f.largest.user_key() < smallest))
            .for_each(|f| {
                inputs.push(f.clone());
            });

        inputs
    }
}

pub struct VersionSet {
    #[allow(dead_code)]
    versions: Arc<RwLock<LinkedList<Arc<Version>>>>,
    last_sequence: AtomicU64,
    next_file_number: AtomicU64,
    #[allow(dead_code)]
    log_file: Writer,
    table_cache: Arc<Cache<u64, Table>>,
    index_cache: Arc<Cache<u64, Block>>,
    opt: Options,
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
        let table_cache = Arc::new(Cache::with_capacity(1000));
        let index_cache = Arc::new(Cache::with_capacity(opt.cache_size));
        let versions = LinkedList::new();
        // versions.push_back(Arc::new(Version::new(table_cache.clone())));
        Self {
            versions: Arc::new(RwLock::new(versions)),
            next_file_number: AtomicU64::new(0),
            last_sequence: AtomicU64::new(0),
            log_file: Writer::new(WritableFileImpl::new(&path_of_file(
                &opt.work_dir,
                0,
                Ext::MANIFEST,
            ))),
            table_cache,
            index_cache,
            opt,
        }
    }

    pub fn current(&self) -> Arc<Version> {
        let versions = self.versions.read();
        versions.back().unwrap().clone()
    }

    pub fn smallest_sequence(&self) -> u64 {
        let versions = self.versions.read();
        versions.front().unwrap().smallest_sequence()
    }

    pub fn smallest_log_number(&self) -> u64 {
        let versions = self.versions.read();
        versions.front().unwrap().smallest_log_number()
    }

    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn set_file_number(&self, next_file_number: u64) {
        self.next_file_number
            .store(next_file_number, Ordering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    pub fn add_last_sequence(&self, n: u64) -> u64 {
        self.last_sequence.fetch_add(n, Ordering::SeqCst)
    }

    pub fn need_compact(&self) -> bool {
        self.current().pick_compact_level().is_some()
        // if self.current().files[0].len() > 8 {
        //     return true;
        // }
        // false
    }

    pub fn log_and_apply(&self, mut edit: VersionEdit) -> Result<()> {
        // write manifest
        let mut data = vec![];
        edit.last_seq_number(self.last_sequence());
        edit.next_file_number(self.next_file_number.load(Ordering::SeqCst));

        edit.encode(&mut data);
        self.log_file.add_recore(&data)?;

        let mut versions = self.versions.write();

        // modify memory metadata
        let base = versions.back().unwrap().clone();
        let current = Version::build(
            Arc::clone(&self.table_cache),
            Arc::clone(&self.index_cache),
            base.clone(),
            &edit,
        );
        versions.push_back(Arc::new(current));
        base.derefs();

        while let Some(v) = versions.front() {
            // remove useless version
            if v.refs_cnt() == 0 {
                versions.pop_front();
            } else {
                break;
            }
        }
        for table_meta in edit.add_files.iter() {
            let fid = table_meta.file_meta.number;
            let path = path_of_file(&self.opt.work_dir, fid, Ext::SST);
            let t = Table::new(Box::new(RandomAccessFileImpl::open(path.as_path())))?;
            self.table_cache.insert(fid, t, 1)?;
            self.table_cache.unpin(&fid)?;
        }
        Ok(())
    }

    fn pick_compaction(&self) -> Option<CompactionState> {
        let current = self.current();
        let mut base = vec![];
        let target;

        let level = current.pick_compact_level()?;
        let mut files = current.files[level].clone();

        if level == 0 {
            files.sort_by(|f1, f2| match f1.smallest.cmp(&f2.smallest) {
                std::cmp::Ordering::Equal => f1.largest.cmp(&f2.largest),
                other => other,
            });
            let (mut smallest, mut largest) =
                (files[0].smallest.user_key(), files[0].largest.user_key());
            for f in files.iter() {
                if !(f.smallest.user_key() > largest || f.largest.user_key() < smallest) {
                    if f.smallest.user_key() < smallest {
                        smallest = f.smallest.user_key();
                    }
                    if f.largest.user_key() < largest {
                        largest = f.largest.user_key();
                    }
                    base.push(f.clone());
                }
            }
            target = current.overlaping_inputs((level + 1) as u32, smallest, largest);
        } else {
            base.push(current.files[level][0].clone());
            target = current.overlaping_inputs(
                (level + 1) as u32,
                current.files[level][0].smallest.user_key(),
                current.files[level][0].largest.user_key(),
            );
        }

        Some(CompactionState {
            base_level: level,
            target_level: level + 1,
            target,
            base,
        })
    }

    fn pick_seek_compaction(&self, seek_task: &SeekTask) -> Option<CompactionState> {
        let level = seek_task.level as usize;
        let current = self.current();
        let mut base = vec![];
        let target;
        let mut files = current.files[level].clone();

        if level == 0 {
            files.sort_by(|f1, f2| match f1.smallest.cmp(&f2.smallest) {
                std::cmp::Ordering::Equal => f1.largest.cmp(&f2.largest),
                other => other,
            });
            let seek_f = files.iter().find(|f| f.number == seek_task.fid);
            if let Some(seek_f) = seek_f {
                let (mut smallest, mut largest) =
                    (seek_f.smallest.user_key(), seek_f.largest.user_key());
                for f in files.iter() {
                    if !(f.smallest.user_key() > largest || f.largest.user_key() < smallest) {
                        if f.smallest.user_key() < smallest {
                            smallest = f.smallest.user_key();
                        }
                        if f.largest.user_key() < largest {
                            largest = f.largest.user_key();
                        }
                        base.push(f.clone());
                    }
                }
                target = current.overlaping_inputs((level + 1) as u32, smallest, largest);
            } else {
                return None;
            }
        } else {
            let seek_f = current.files[level]
                .iter()
                .find(|f| f.number == seek_task.fid);
            if let Some(seek_f) = seek_f {
                base.push(seek_f.clone());
                target = current.overlaping_inputs(
                    (level + 1) as u32,
                    seek_f.smallest.user_key(),
                    seek_f.largest.user_key(),
                );
            } else {
                return None;
            }
        }

        Some(CompactionState {
            base_level: level,
            target_level: level + 1,
            target,
            base,
        })
    }

    pub fn do_compaction(&self, meta: &mut FileMetaData) -> Result<Option<CompactionState>> {
        if let Some(c) = self.pick_compaction() {
            return self.do_compaction_inner(meta, c);
        }
        Ok(None)
    }

    pub fn do_seek_compaction(
        &self,
        meta: &mut FileMetaData,
        seek_task: &SeekTask,
    ) -> Result<Option<CompactionState>> {
        if let Some(c) = self.pick_seek_compaction(seek_task) {
            if c.base.len() + c.target.len() < 2 {
                return Ok(None);
            }
            return self.do_compaction_inner(meta, c);
        }
        Ok(None)
    }

    pub fn do_compaction_inner(
        &self,
        meta: &mut FileMetaData,
        c: CompactionState,
    ) -> Result<Option<CompactionState>> {
        let skip =
            |internal_key: InternalKey| -> bool { self.smallest_sequence() > internal_key.seq() };

        let mut iters = vec![];
        let mut files_iter = c.base.iter().chain(c.target.iter());
        let mut total_sz = 0;
        files_iter.try_for_each(|f| -> Result<()> {
            let t = match self.table_cache.get(&f.number) {
                Some(t) => t,
                None => {
                    let path = path_of_file(&self.opt.work_dir, f.number, Ext::SST);
                    Arc::new(Table::new(Box::new(RandomAccessFileImpl::open(
                        path.as_path(),
                    )))?)
                }
            };
            total_sz += t.size();
            let iter = TableIterator::new(t)?;
            iters.push(iter);
            self.table_cache.unpin(&f.number)?;
            Ok(())
        })?;

        meta.number = self.new_file_number();
        let merge_iter = MergeIterator::new(iters);
        let path = path_of_file(&self.opt.work_dir, meta.number, Ext::SST);
        let mut vlog_cache = HashMap::<u64, RandomReader>::new();
        let mut vlog = None;

        {
            let mut tb = TableBuilder::new(
                self.opt.clone(),
                Box::new(WritableFileImpl::new(path.as_path())),
                meta.number,
            );
            let mut last_key = InternalKey::from(vec![]);
            for e in merge_iter {
                let key = InternalKey::new(e.key.clone());
                if !(key == last_key && skip(InternalKey::new(e.key.clone()))) {
                    last_key = key;
                    // let mut value = e.value.clone();
                    let mut value = e.value.to_vec();
                    if c.base_level >= 1 && !value.is_empty() && value[0] == 1 {
                        // do vlog merge on

                        // read value in vlog
                        assert!(value.len() >= 17); // tag(1) + fid(8) + offset(8)

                        let fid = (&value[1..9]).get_u64();
                        let offset = (&value[9..17]).get_u64();

                        let log = vlog_cache.entry(fid).or_insert_with(|| {
                            let path =
                                path_of_file(&self.opt.work_dir, fid, crate::file::Ext::VLOG);
                            RandomReader::new(Box::new(RandomAccessFileImpl::open(path.as_path())))
                        });
                        let ivalue = log.read_record(offset).unwrap();

                        let vwriter = vlog.get_or_insert_with(|| {
                            Writer::new(WritableFileImpl::new(&path_of_file(
                                &self.opt.work_dir,
                                meta.number,
                                Ext::VLOG,
                            )))
                        });
                        // construct value in sst
                        let off = vwriter.offset();
                        value.clear();
                        value.put_u8(1);
                        value.put_u64(meta.number);
                        value.put_u64(off);

                        vwriter.add_recore(&ivalue)?;
                    }
                    tb.add(&e.key, &value);
                }
            }
            tb.finish_builder(meta)?;
            if vlog.is_none() {
                // no new vlog produce. merge vlogs that in CompactionState to new group
                c.base.iter().chain(c.target.iter()).for_each(|f| {
                    meta.vlogs.append(&mut f.vlogs.clone());
                });
            } else {
                // only one vlog for sst. vlogs in CompactionState could be removed in the future
                meta.vlogs.push(meta.number);
                let mut drops = vec![];
                c.base.iter().chain(c.target.iter()).for_each(|f| {
                    drops.append(&mut f.vlogs.clone());
                });
                info!(
                    "merge vlogs {:?} to {:?}.vlog -> level {}",
                    drops,
                    format!("{:05}.sst", meta.number),
                    c.target_level
                );
            }
        }
        Ok(Some(c))
    }

    pub fn remove_ssts(&self) -> Result<()> {
        let versions = self.versions.read();
        let mut lives = HashSet::new();
        let mut lives_vlog = HashSet::new();
        let mut deletes = HashSet::new();
        let mut deletes_vlog = HashSet::new();
        versions.iter().for_each(|v| {
            v.files.iter().for_each(|files| {
                files.iter().for_each(|f| {
                    lives.insert(f.number);
                    f.vlogs.iter().for_each(|v| {
                        lives_vlog.insert(v);
                    });
                })
            })
        });
        let dir = std::fs::read_dir(Path::new(&self.opt.work_dir))?;
        for dir_entry in dir {
            if let Some(file_name) = dir_entry?.file_name().to_str() {
                if let Some((name, ext)) = file_name.split_once('.') {
                    let fid = name.parse::<u64>()?;
                    if ext == "sst" && !lives.contains(&fid) {
                        deletes.insert(fid);
                    } else if ext == "vlog" && !lives_vlog.contains(&fid) {
                        deletes_vlog.insert(fid);
                    }
                }
            }
        }

        deletes.iter().try_for_each(|fid| -> Result<()> {
            let path = path_of_file(&self.opt.work_dir, *fid, Ext::SST);
            std::fs::remove_file(path.as_path())?;
            self.table_cache.evict(*fid, 1)?;
            Ok(())
        })?;
        deletes_vlog.iter().try_for_each(|fid| -> Result<()> {
            let path = path_of_file(&self.opt.work_dir, *fid, Ext::VLOG);
            std::fs::remove_file(path.as_path())?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn recover(&self) -> Result<()> {
        let mut f = Reader::new(Box::new(SequentialFileImpl::new(
            path_of_file(&self.opt.work_dir, 0, Ext::MANIFEST).as_path(),
        )));
        let mut edit = VersionEdit::new();
        let mut add_files = vec![];
        let mut delete_files = vec![];
        let mut delete_set = HashSet::new();
        let mut log_number = 0;
        let mut last_seq_number = 0;
        let mut next_file_number = 0;

        let mut end = false;
        while !end {
            let record = f.read_record();
            match record {
                core::result::Result::Ok(record) => {
                    let t_edit = VersionEdit::decode(&record);
                    t_edit
                        .add_files
                        .iter()
                        .for_each(|f| add_files.push(f.clone()));
                    t_edit.delete_files.iter().for_each(|f| {
                        delete_files.push(f.clone());
                        delete_set.insert(f.file_meta.number);
                    });

                    log_number = log_number.max(t_edit.log_number);
                    last_seq_number = last_seq_number.max(t_edit.last_seq_number);
                    next_file_number = next_file_number.max(t_edit.next_file_number);
                    edit.next_file_number(t_edit.next_file_number);
                }
                Err(err) => match err.kind() {
                    ErrorKind::UnexpectedEof => end = true,
                    err => panic!("{:?}", err),
                },
            };
        }
        add_files
            .iter()
            .filter(|f| !delete_set.contains(&f.file_meta.number))
            .for_each(|f| edit.add_files.push(f.clone()));
        edit.log_number(log_number);
        edit.last_seq_number(last_seq_number);
        edit.next_file_number(next_file_number);

        let base = Version::new(Arc::clone(&self.table_cache), Arc::clone(&self.index_cache));
        let ver = Version::build(
            Arc::clone(&self.table_cache),
            Arc::clone(&self.index_cache),
            Arc::new(base),
            &edit,
        );

        let mut versions = self.versions.write();
        versions.push_back(Arc::new(ver));
        self.add_last_sequence(last_seq_number);
        self.next_file_number
            .fetch_add(next_file_number, Ordering::SeqCst);

        Ok(())
    }

    pub fn do_gc(&self, meta: &mut FileMetaData) -> Result<Option<GCState>> {
        let current = self.current();
        let mut target_level = 0;
        let mut target_fid = 0;
        let mut target_sz = 0;
        let mut target_meta = FileMetaData::new(0);

        // pick a sstable to gc
        current
            .files
            .iter()
            .rev()
            .enumerate()
            .for_each(|(level, files)| {
                files.iter().for_each(|f| {
                    if target_sz < f.vlogs.len() {
                        target_sz = f.vlogs.len();
                        target_fid = f.number;
                        target_level = level;
                        target_meta = f.clone();
                    }
                })
            });

        if target_sz < 2 {
            return Ok(None);
        }

        let mut vlog_cache = HashMap::<u64, RandomReader>::new();
        // don't use cache, because we only need to rewrite the sstable
        let path = path_of_file(&self.opt.work_dir, target_fid, Ext::SST);
        let t = Table::new(Box::new(RandomAccessFileImpl::open(path.as_path())))?;

        let new_fid = self.new_file_number();
        let new_path = path_of_file(&self.opt.work_dir, new_fid, Ext::SST);
        let mut tb = TableBuilder::new(
            self.opt.clone(),
            Box::new(WritableFileImpl::new(&new_path)),
            new_fid,
        );
        let vlog_writer = Writer::new(WritableFileImpl::new(&path_of_file(
            &self.opt.work_dir,
            new_fid,
            Ext::VLOG,
        )));

        let mut iter = TableIterator::new(Arc::new(t))?;
        iter.try_for_each(|e| -> Result<()> {
            let value = &e.value;
            let mut value_wrapper = value.to_vec();

            if !value.is_empty() && value[0] == 1 {
                // value_ptr
                let fid = (&value[1..9]).get_u64();
                let offset = (&value[9..17]).get_u64();
                let path = path_of_file(&self.opt.work_dir, fid, crate::file::Ext::VLOG);
                let vlog = vlog_cache.entry(fid).or_insert_with(|| {
                    RandomReader::new(Box::new(RandomAccessFileImpl::open(path.as_path())))
                });

                let ivalue = vlog.read_record(offset).unwrap();
                let off = vlog_writer.offset();
                value_wrapper.clear();
                value_wrapper.put_u8(1);
                value_wrapper.put_u64(new_fid);
                value_wrapper.put_u64(off);

                vlog_writer.add_recore(&ivalue)?;
            }
            tb.add(&e.key, &value_wrapper);
            Ok(())
        })?;
        meta.number = new_fid;
        meta.vlogs.push(new_fid);
        tb.finish_builder(meta)?;

        Ok(Some(GCState {
            level: target_level,
            rewrite_file: target_meta,
            new_file: meta.clone(),
        }))
    }
}
