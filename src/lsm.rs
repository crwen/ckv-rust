use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
};

use anyhow::Ok;
use bytes::{Buf, BufMut};
use parking_lot::RwLock;
use tracing::info;

use crate::{
    compactor::Compactor,
    file::{
        log_reader::Reader, log_writer::Writer, path_of_file, writeable::WritableFileImpl, Ext,
        SequentialFileImpl,
    },
    mem_table::{MemTable, MemTableIterator},
    sstable::table_builder::TableBuilder,
    utils::{
        codec::{decode_varintu32, encode_varintu32, varintu32_length},
        Entry, OP_TYPE_PUT,
    },
    version::{
        version_edit::VersionEdit,
        version_set::{Version, VersionSet},
        FileMetaData,
    },
    Options,
};

type Result<T> = anyhow::Result<T, anyhow::Error>;

struct MemInner {
    mem: Arc<MemTable>,
    imms: VecDeque<Arc<MemTable>>,
    logs: VecDeque<u64>,
    wal: Writer,
}

impl MemInner {
    fn new(opt: Options, next_file_id: u64) -> Self {
        let logs = VecDeque::new();
        // logs.push_back(next_file_id);
        Self {
            // mem_inner: Arc::new(RwLock::new(Arc::new(MemInner::new(opt.clone(), new_fid)))),
            mem: Arc::new(MemTable::new()),
            imms: VecDeque::new(),
            logs,
            wal: Writer::new(WritableFileImpl::new(&path_of_file(
                &opt.work_dir,
                next_file_id,
                Ext::WAL,
            ))),
        }
    }
}

pub struct LsmInner {
    mem_inner: Arc<RwLock<MemInner>>,
    version: Arc<VersionSet>,
    // imms: Vec<Arc<MemTable>>,
    opt: Options,
}
impl LsmInner {
    fn new(opt: Options) -> Self {
        let version = Arc::new(VersionSet::new(opt.clone()));
        let next_file_id = version.new_file_number();
        Self {
            mem_inner: Arc::new(RwLock::new(MemInner::new(opt.clone(), next_file_id))),
            version,
            opt,
        }
    }
    pub fn imms_sz(&self) -> usize {
        let snap = self.mem_inner.read();
        snap.imms.len()
    }
    fn try_make_room(&self) -> Result<()> {
        let mut mem_inner = self.mem_inner.write();
        // let mut snap = mem_inner.as_ref().clone();
        if mem_inner.mem.approximate_memory_usage() > self.opt.mem_size as u64 {
            // switch memtable
            let imm = std::mem::replace(&mut mem_inner.mem, Arc::new(MemTable::new()));

            mem_inner.imms.push_back(imm);

            // switch wal
            mem_inner.wal.flush()?;

            let next_file_id = self.version.new_file_number();
            mem_inner.logs.push_back(next_file_id);
            let wal = Writer::new(WritableFileImpl::new(
                path_of_file(&self.opt.work_dir, next_file_id, Ext::WAL).as_path(),
            ));

            let _ = std::mem::replace(&mut mem_inner.wal, wal);
        }
        Ok(())
    }

    pub fn delete(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.try_make_room()?;

        let inner = self.mem_inner.read();
        // write wal first
        let seq = self.version.add_last_sequence(1);
        self.write_wal(key, value, seq).unwrap();

        // let mem_inner = self.mem_inner.read();
        // write data
        let e = Entry::new(key.to_vec(), value.to_vec(), seq);
        inner.mem.delete(e);

        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.try_make_room()?;

        // write wal first
        let inner = self.mem_inner.read();
        let seq = self.version.add_last_sequence(1);
        self.write_wal(key, value, seq).unwrap();

        // let mem_inner = self.mem_inner.read();
        // write data
        let e = Entry::new(key.to_vec(), value.to_vec(), seq);
        inner.mem.put(e);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.mem_inner.read();

        let seq = self.version.last_sequence();
        // search memtable first
        let result = inner.mem.get(key, seq);
        if result.is_some() {
            return Ok(result.map(|val| val.to_vec()));
        }
        // serach immutable memtable
        for m in inner.imms.iter().rev() {
            if let Some(result) = m.get(key, seq) {
                return Ok(Some(result.to_vec()));
            }
        }
        // search sst
        let current = self.version.current();
        current.refs();
        if let Some(result) = current.get(self.opt.clone(), key, seq) {
            current.derefs();
            return Ok(Some(result.to_vec()));
        }
        current.derefs();
        Ok(None)
    }
    fn write_wal(&self, key: &[u8], value: &[u8], seq: u64) -> Result<()> {
        let mut data = Vec::new();
        data.put_u64(seq);
        encode_varintu32(&mut data, key.len() as u32);
        data.put(key);
        encode_varintu32(&mut data, value.len() as u32);
        data.put(value);
        {
            let inner = self.mem_inner.read_recursive();
            inner.wal.add_recore(&data)
        }
    }
    pub fn compact_mem_table(&self) {
        // write to disk
        // remove files
        let (imm, log_number);
        let base;
        {
            let inner = self.mem_inner.read();
            if inner.imms.is_empty() {
                return;
            }
            base = self.version.current();
            imm = inner.imms[0].clone();
            log_number = inner.logs[0];
            // inner.bitset.set(1, false);
        }
        base.refs();
        self.write_level0_table(base, imm, log_number);
        {
            let mut inner = self.mem_inner.write();
            inner.logs.pop_front();
            inner.imms.pop_front();
        }
    }

    pub fn major_compaction(&self) -> Result<()> {
        let current = self.version.current();
        current.refs();
        let mut file_meta = FileMetaData::new(0);
        if let Some(c) = self.version.do_compaction(&mut file_meta)? {
            let mut edit = VersionEdit::new();
            c.base
                .iter()
                .for_each(|f| edit.delete_file(c.base_level as u32, f.clone()));
            c.target
                .iter()
                .for_each(|f| edit.delete_file(c.target_level as u32, f.clone()));

            edit.add_file(c.target_level as u32, file_meta.clone());

            let inner = self.mem_inner.read();
            edit.log_number(inner.logs[0] - 1);
            current.derefs();
            self.version.log_and_apply(edit).unwrap();

            // delete files
            self.version.remove_ssts()?;
            let mut compacted = vec![];
            c.base
                .iter()
                .chain(c.target.iter())
                .try_for_each(|f| -> Result<()> {
                    compacted.push(format!("{:05}.sst", f.number));
                    Ok(())
                })?;
            info!("Major compact {:?} to level {}", compacted, c.target_level);
        } else {
            current.derefs();
        }

        Ok(())
    }

    // pub fn gc(&self) -> Result<()> {
    //     let current = self.version.current();
    //     current.refs();
    //     let mut file_meta = FileMetaData::new(0);
    //     if let Some(state) = self.version.do_gc(&mut file_meta)? {
    //         let mut edit = VersionEdit::new();
    //         edit.add_file(state.level as u32, file_meta.clone());
    //         edit.delete_file(state.level as u32, state.rewrite_file);
    //         let inner = self.mem_inner.read();
    //         edit.log_number(inner.logs[0] - 1);
    //         current.derefs();
    //
    //         self.version.log_and_apply(edit).unwrap();
    //         // self.version.remove_ssts()?;
    //     } else {
    //         current.derefs();
    //     }
    //     Ok(())
    // }

    fn write_level0_table(&self, version: Arc<Version>, imm: Arc<MemTable>, log_number: u64) {
        {
            // let inner = self.mem_inner.read();
            let mut edit = VersionEdit::new();
            let fid = self.version.new_file_number();
            let mut file_meta = FileMetaData::new(fid);
            // imm  to sst

            TableBuilder::build_table(
                path_of_file(&self.opt.work_dir, fid, Ext::SST).as_path(),
                self.opt.clone(),
                MemTableIterator::new(&imm),
                &mut file_meta,
            )
            .unwrap();

            // pick level to push
            let level = version.pick_level_for_mem_table_output(
                file_meta.smallest().user_key(),
                file_meta.largest().user_key(),
            );

            // let fid = self.version.new_file_number();
            // edit.add_file(level, fid, file_meta.smallest(), file_meta.largest());
            // file_meta.number = fid;
            edit.add_file(level, file_meta);
            edit.log_number(log_number);
            version.derefs();

            self.version.log_and_apply(edit).unwrap();
            // delete wal file
            let wal_path = path_of_file(&self.opt.work_dir, log_number, Ext::WAL);
            std::fs::remove_file(wal_path.as_path()).unwrap();

            info!("Minor compact {:05}.sst to level {:?}", fid, level);
        }
    }
    fn recover(&self) -> Result<()> {
        // recover from manifest
        self.version.recover()?;
        self.recover_mem()?;

        self.version.remove_ssts()?;
        Ok(())
    }

    fn recover_mem(&self) -> Result<()> {
        let mut wal_count = 0;
        let mut seq = 0_u64;
        {
            let mut inner = self.mem_inner.write();

            let log_number = self.version.smallest_log_number();
            let dir = std::fs::read_dir(Path::new(&self.opt.work_dir))?;
            for dir_entry in dir {
                if let Some(file_name) = dir_entry?.file_name().to_str() {
                    if let Some((name, ext)) = file_name.split_once('.') {
                        let fid = name.parse::<u64>()?;
                        if ext == "wal" {
                            if fid > log_number {
                                let mut f = Reader::new(Box::new(SequentialFileImpl::new(
                                    path_of_file(&self.opt.work_dir, fid, Ext::WAL).as_path(),
                                )));

                                let mut end = false;
                                while !end {
                                    let record = f.read_record();
                                    match record {
                                        core::result::Result::Ok(record) => {
                                            seq = seq.max((&record[..8]).get_u64());
                                            let data = &record[8..];
                                            let key_sz = decode_varintu32(data).unwrap();
                                            let var_key_sz = varintu32_length(key_sz) as usize;
                                            let key =
                                                &data[var_key_sz..var_key_sz + key_sz as usize];
                                            let value = &data[var_key_sz + key_sz as usize..];
                                            let val_sz = decode_varintu32(value).unwrap();
                                            let var_val_sz = varintu32_length(val_sz) as usize;
                                            let value = &value[var_val_sz..];
                                            inner.mem.set(
                                                Entry::new(key.to_vec(), value.to_vec(), seq),
                                                OP_TYPE_PUT,
                                            );
                                        }
                                        Err(err) => match err.kind() {
                                            std::io::ErrorKind::UnexpectedEof => end = true,
                                            err => panic!("{:?}", err),
                                        },
                                    };
                                }
                                let imm =
                                    std::mem::replace(&mut inner.mem, Arc::new(MemTable::new()));
                                inner.imms.push_back(imm);
                                inner.logs.push_back(fid);
                                wal_count += 1;
                            } else {
                                let path = path_of_file(&self.opt.work_dir, fid, Ext::WAL);
                                std::fs::remove_file(path.as_path())?;
                            }
                        }
                    }
                }
            }
        }
        for _ in 0..wal_count {
            self.compact_mem_table();
        }

        let mut inner = self.mem_inner.write();
        let next_file_id = self.version.new_file_number();
        inner.logs.push_back(next_file_id);
        let wal = Writer::new(WritableFileImpl::new(
            path_of_file(&self.opt.work_dir, next_file_id, Ext::WAL).as_path(),
        ));
        let _ = std::mem::replace(&mut inner.wal, wal);

        let vseq = self.version.last_sequence();
        self.version.add_last_sequence(seq.abs_diff(vseq));

        Ok(())
    }
}

pub struct Lsm {
    // opt: Options,
    // mem_inner: Arc<RwLock<Arc<MemInner>>>,
    inner: Arc<LsmInner>,
    bg_tx: Option<SyncSender<()>>,
}

impl Lsm {
    pub fn open(opt: Options) -> Self {
        let path = Path::new(&opt.work_dir);
        if !path.exists() {
            std::fs::create_dir(path).expect("create work direction fail!");
        }

        let mut lsm = Self {
            inner: Arc::new(LsmInner::new(opt.clone())),
            bg_tx: None,
            // opt,
        };
        lsm.inner.recover().unwrap();
        lsm.bg_tx = lsm.run_bg_task().into();
        lsm
    }

    pub fn delete(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(tx) = self.bg_tx.as_ref() {
            tx.send(())?;
        }
        self.inner.delete(key, value)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(tx) = self.bg_tx.as_ref() {
            tx.send(())?;
        }
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn run_bg_task(&self) -> SyncSender<()> {
        let (tx, rx) = sync_channel(1000);
        let db = self.inner.clone();
        std::thread::Builder::new()
            .name("bg".to_owned())
            .spawn(move || {
                Compactor::new(rx, db).run_compactor();
            })
            .unwrap();
        tx
    }
}

#[cfg(test)]
mod lsm_test {
    use std::sync::Arc;

    use crate::Options;

    use super::Lsm;

    fn crud(opt: Options) {
        let lsm = Arc::new(Lsm::open(opt));

        let mut handles = vec![];
        for _ in 0..10 {
            let lsm = Arc::clone(&lsm);
            let t = std::thread::spawn(move || {
                for i in 100..200 {
                    let n = i as u32;
                    lsm.put(&n.to_be_bytes(), &n.to_be_bytes()).unwrap();
                    let n = i as u32;
                    let res = lsm.get(&n.to_be_bytes()).unwrap();
                    assert_ne!(res, None);
                    assert_eq!(res.unwrap(), n.to_be_bytes());
                }
            });
            handles.push(t);
        }

        for i in 0..200 {
            let n = i as u32;
            lsm.put(&n.to_be_bytes(), &n.to_be_bytes()).unwrap();
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.unwrap(), n.to_be_bytes());
        }

        while !handles.is_empty() {
            if let Some(h) = handles.pop() {
                h.join().unwrap();
            }
        }
        for i in 0..200 {
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.unwrap(), n.to_be_bytes());
        }
    }

    #[test]
    fn lsm_crud_test() {
        let opt = Options::default_opt()
            .work_dir("work_dir/lsm")
            .kv_separate_threshold(4);
        // .kv_separate_threshold(4);
        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap()
        };
        crud(opt);
    }

    #[test]
    fn lsm_recover_test() {
        {
            let opt = Options::default_opt()
                .work_dir("work_dir/recovery")
                .kv_separate_threshold(4);
            // .kv_separate_threshold(4);
            if std::fs::metadata(&opt.work_dir).is_ok() {
                std::fs::remove_dir_all(&opt.work_dir).unwrap()
            };
            crud(opt);
        }
        // std::thread::sleep(std::time::Duration::from_secs(10));

        // let opt = Options::default_opt().work_dir("work_dir/recovery");
        let opt = Options::default_opt().work_dir("work_dir/recovery");
        let lsm = Arc::new(Lsm::open(opt));
        //
        for i in 0..200 {
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.clone().unwrap(), n.to_be_bytes());
        }
    }
}
