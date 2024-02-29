use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
};

use anyhow::Ok;
use bytes::{Buf, BufMut, Bytes};
use parking_lot::RwLock;
use tracing::info;

use crate::{
    compactor::{Compactor, SeekTask, Task},
    file::{path_of_file, Ext, Reader, SequentialFileImpl, WritableFileImpl, Writer},
    mem_table::{MemTable, MemTableIterator},
    sstable::TableBuilder,
    utils::{
        codec::{decode_varintu32, encode_varintu32, varintu32_length},
        Entry, OP_TYPE_PUT,
    },
    version::{FileMetaData, Version, VersionEdit, VersionSet},
    write_batch::WriteBatch,
    Options,
};

type Result<T> = anyhow::Result<T, anyhow::Error>;

struct MemInner {
    mem: Arc<MemTable>,
    imms: VecDeque<Arc<MemTable>>,
    logs: VecDeque<u64>,
    wal: Writer,
    #[allow(unused)]
    log_buf: Vec<u8>,
    #[allow(unused)]
    miss_count: usize,
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
            log_buf: Vec::new(),
            miss_count: 0,
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
    fn try_make_room(&self) -> Result<bool> {
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
            return Ok(true);
        }
        Ok(mem_inner.imms.len() > 3)
    }

    pub fn delete(&self, key: &[u8]) -> Result<Option<Task>> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.write(&batch)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Task>> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(&batch)
    }

    pub fn write(&self, batch: &WriteBatch) -> Result<Option<Task>> {
        let need_compact = self.try_make_room()?;

        // write wal first
        let mut seq = self.version.add_last_sequence(batch.count as u64);

        self.write_batch_wal(batch, seq).unwrap();

        let inner = self.mem_inner.read();

        // write data
        batch.data.iter().for_each(|e| {
            let mut entry = e.clone();
            entry.seq = seq;
            inner.mem.put(entry);
            seq += 1;
        });

        let task = need_compact
            .then_some(Task::Compact)
            .or(self.version.need_compact().then_some(Task::Major));
        Ok(task)
    }

    pub fn get(&self, key: &[u8]) -> Result<(Option<Vec<u8>>, Option<Task>)> {
        let inner = self.mem_inner.read();

        let seq = self.version.last_sequence();
        // search memtable first
        let result = inner.mem.get(key, seq);

        if let Some(result) = result {
            if result.is_empty() {
                // delete case
                return Ok((None, None));
            }
            return Ok((Some(result.to_vec()), None));
        }
        // serach immutable memtable
        for m in inner.imms.iter().rev() {
            if let Some(result) = m.get(key, seq) {
                return Ok((Some(result.to_vec()), None));
            }
        }
        // search sst
        let current = self.version.current();
        current.refs();
        let (value, task) = current.get(self.opt.clone(), key, seq);

        current.derefs();
        Ok((value, task))
    }

    fn write_wal(&self, key: &[u8], value: &[u8], seq: u64) -> Result<()> {
        let mut data = Vec::new();
        data.put_u64(seq);
        encode_varintu32(&mut data, key.len() as u32);
        data.put(key);
        encode_varintu32(&mut data, value.len() as u32);
        data.put(value);
        {
            let mut inner = self.mem_inner.write();
            // inner.log_buf.append(&mut data);
            inner.miss_count += 1;
            // if inner.miss_count >= self.opt.allow_miss_count
            //     || inner.log_buf.len() >= self.opt.allow_miss_size
            // {
            //     inner.miss_count = 0;
            // inner.wal.add_recore(&inner.log_buf)?;
            inner.wal.add_recore(&data)?;
            //     inner.log_buf.clear();
            // }
            Ok(())
        }
    }

    fn write_batch_wal(&self, batch: &WriteBatch, base_seq: u64) -> Result<()> {
        let mut data = Vec::new();
        let mut seq = base_seq;
        batch.data.iter().for_each(|e| {
            let mut record = vec![];
            record.put_u64(seq);
            encode_varintu32(&mut record, e.key.len() as u32);
            record.put(e.key.clone());
            encode_varintu32(&mut record, e.value.len() as u32);
            record.put(e.value.clone());
            data.push(Bytes::from(record));
            seq += 1;
        });

        let inner = self.mem_inner.write();
        inner.wal.add_recore_batch(&data)
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
        }
        base.refs();
        let iter = MemTableIterator::new(&imm);
        self.write_level0_table(base, iter, log_number);
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
            info!(
                "Major compact {:?} to level {} --> {:?}",
                compacted,
                c.target_level,
                format!("{:05}.sst", file_meta.number)
            );
        } else {
            current.derefs();
        }

        Ok(())
    }

    pub fn seek_compaction(&self, seek_task: &SeekTask) -> Result<()> {
        let current = self.version.current();
        current.refs();
        let mut file_meta = FileMetaData::new(0);
        if let Some(c) = self.version.do_seek_compaction(&mut file_meta, seek_task)? {
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
            // let mut compacted = vec![];
            let mut base = vec![];
            let mut target = vec![];
            for (b, t) in c.base.iter().zip(c.target.iter()) {
                base.push(format!("{:05}.sst", b.number));
                target.push(format!("{:05}.sst", t.number));
            }
            info!(
                "Seek compact\n {:?},\n  {:?}\n to level {} {:?}",
                base,
                target,
                c.target_level,
                format!("{:05}.sst", file_meta.number),
            );
        } else {
            current.derefs();
        }

        Ok(())
    }

    fn write_level0_table<T>(&self, version: Arc<Version>, iter: T, log_number: u64)
    where
        T: Iterator<Item = Entry>,
    {
        {
            // let inner = self.mem_inner.read();
            let mut edit = VersionEdit::new();
            let fid = self.version.new_file_number();
            let mut file_meta = FileMetaData::new(fid);
            // imm  to sst

            TableBuilder::build_table(
                path_of_file(&self.opt.work_dir, fid, Ext::SST).as_path(),
                self.opt.clone(),
                iter,
                &mut file_meta,
            )
            .unwrap();

            // pick level to push
            let level = version.pick_level_for_mem_table_output(
                file_meta.smallest().user_key(),
                file_meta.largest().user_key(),
            );

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
        let mut seq = 0_u64;
        let mut next_file_id = self.version.new_file_number();
        let mut remove_logs = vec![];
        let mut data_count = 0;
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
                                if fid > next_file_id {
                                    next_file_id = fid;
                                }
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
                                                Entry::new(
                                                    Bytes::from(key.to_vec()),
                                                    Bytes::from(value.to_vec()),
                                                    seq,
                                                ),
                                                OP_TYPE_PUT,
                                            );
                                            data_count += 1;
                                        }
                                        Err(err) => match err.kind() {
                                            std::io::ErrorKind::UnexpectedEof => end = true,
                                            err => panic!("{:?}", err),
                                        },
                                    };
                                }
                                remove_logs.push(fid);
                            } else {
                                let path = path_of_file(&self.opt.work_dir, fid, Ext::WAL);
                                std::fs::remove_file(path.as_path())?;
                            }
                        }
                    }
                }
            }
            self.version.set_file_number(next_file_id + 5);
            if data_count != 0 {
                let imm = std::mem::replace(&mut inner.mem, Arc::new(MemTable::new()));
                inner.imms.push_back(imm);
                inner.logs.push_back(remove_logs.pop().unwrap());
            }
        }

        if data_count != 0 {
            self.compact_mem_table();
        }
        // remove wal files
        for fid in remove_logs {
            let path = path_of_file(&self.opt.work_dir, fid, Ext::WAL);
            std::fs::remove_file(path.as_path())?;
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
    bg_tx: Option<SyncSender<Task>>,
}

impl Lsm {
    pub fn open(opt: Options) -> Self {
        let path = Path::new(&opt.work_dir);
        if !path.exists() {
            std::fs::create_dir_all(path).expect("create work direction fail!");
        }

        let mut lsm = Self {
            inner: Arc::new(LsmInner::new(opt.clone())),
            bg_tx: None,
        };
        lsm.inner.recover().unwrap();
        lsm.bg_tx = lsm.run_bg_task().into();
        lsm
    }

    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        let task = self.inner.write(batch)?;
        self.handle_task(task);
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let task = self.inner.delete(key)?;
        self.handle_task(task);
        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let task = self.inner.put(key, value)?;
        self.handle_task(task);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (value, task) = self.inner.get(key)?;
        self.handle_task(task);
        Ok(value)
    }

    fn handle_task(&self, task: Option<Task>) {
        if let Some(tx) = self.bg_tx.as_ref() {
            match task {
                None => {}
                Some(task) => match task {
                    Task::Compact => {
                        let _ = tx.try_send(Task::Compact);
                    }
                    Task::Seek(task) => {
                        let _ = tx.try_send(Task::Seek(task));
                    }
                    Task::Major => {
                        let _ = tx.try_send(Task::Major);
                    }
                },
            }
        }
    }

    fn run_bg_task(&self) -> SyncSender<Task> {
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
                for i in 1000..2000 {
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

        for i in 0..2000 {
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
        for i in 0..2000 {
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
            .mem_size(1 << 12)
            .kv_separate_threshold(4);
        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap()
        };
        crud(opt);
    }

    #[test]
    fn lsm_recover_test() {
        std::thread::spawn(move || {
            let opt = Options::default_opt()
                .work_dir("work_dir/recovery")
                .mem_size(1 << 12)
                .kv_separate_threshold(4);
            if std::fs::metadata(&opt.work_dir).is_ok() {
                std::fs::remove_dir_all(&opt.work_dir).unwrap()
            };
            crud(opt);
        })
        .join()
        .unwrap();

        // wait to release resource
        std::thread::sleep(std::time::Duration::from_secs(1));

        let opt = Options::default_opt().work_dir("work_dir/recovery");
        let lsm = Arc::new(Lsm::open(opt));
        //
        for i in 0..2000 {
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.clone().unwrap(), n.to_be_bytes());
        }
    }
}
