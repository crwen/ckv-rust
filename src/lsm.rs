use std::{
    collections::VecDeque,
    fs,
    io::Error,
    path::Path,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
};

use bytes::BufMut;
use parking_lot::RwLock;
use tracing::info;

use crate::{
    compactor::Compactor,
    file::{log_writer::Writer, path_of_file, writeable::WritableFileImpl, Ext},
    mem_table::{MemTable, MemTableIterator},
    sstable::table_builder::TableBuilder,
    utils::{codec::encode_varintu32, Entry},
    version::{
        version_edit::VersionEdit,
        version_set::{Version, VersionSet},
        FileMetaData,
    },
    Options,
};

type Result<T> = core::result::Result<T, Error>;

struct MemInner {
    mem: Arc<MemTable>,
    imms: VecDeque<Arc<MemTable>>,
    logs: VecDeque<u64>,
    wal: Writer,
}

impl MemInner {
    fn new(opt: Options, next_file_id: u64) -> Self {
        let mut logs = VecDeque::new();
        logs.push_back(next_file_id);
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
            // mem: Arc::new(MemTable::new()),
            // imms: VecDeque::new(),
            // wal: Writer::new(WritableFileImpl::new(&path_of_file(
            //     &opt.work_dir,
            //     next_file_id,
            //     Ext::WAL,
            // ))),
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
        // TODO: search sst
        let current = self.version.current();
        if let Some(result) = current.get(self.opt.clone(), key, seq) {
            return Ok(Some(result.to_vec()));
        }
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
    #[allow(dead_code)]
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
        self.write_level0_table(base, imm, log_number);
        {
            let mut inner = self.mem_inner.write();
            inner.logs.pop_front();
            inner.imms.pop_front();
        }
    }

    #[allow(dead_code)]
    fn write_level0_table(&self, version: Arc<Version>, imm: Arc<MemTable>, log_number: u64) {
        {
            // let inner = self.mem_inner.read();
            let mut edit = VersionEdit::new();
            let fid = self.version.new_file_number();
            // let fid = self.version.new_file_number();
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
            edit.add_file(level, fid, file_meta.smallest(), file_meta.largest());
            edit.log_number(log_number);
            self.version.log_and_apply(edit).unwrap();
            // delete wal file
            let wal_path = path_of_file(&self.opt.work_dir, log_number, Ext::WAL);
            std::fs::remove_file(wal_path.as_path()).unwrap();

            info!("Minor compact {:05}.sst to level {:?}", fid, level);
        }
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
            fs::create_dir(path).expect("create work direction fail!");
        }

        let mut lsm = Self {
            // mem_inner: Arc::new(RwLock::new(Arc::new(MemInner::new(
            //     opt.clone(),
            //     next_file_id,
            // )))),
            inner: Arc::new(LsmInner::new(opt.clone())),
            bg_tx: None,
            // opt,
        };
        lsm.bg_tx = lsm.run_bg_task().into();
        lsm
    }

    pub fn delete(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.delete(key, value)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn run_bg_task(&self) -> SyncSender<()> {
        let (tx, rx) = sync_channel(1);
        // Compactor::new(Arc::clone(&self.inner));
        // Arc::clone(&self.inner);
        // self.bg_tx = tx;
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
    use std::{sync::Arc, time::Duration};

    use crate::Options;

    use super::Lsm;

    #[test]
    fn lsm_crud_test() {
        let opt = Options {
            block_size: 1 << 12,
            work_dir: "work_dir/lsm".to_string(),
            mem_size: 1 << 12,
        };
        if std::fs::metadata(&opt.work_dir).is_ok() {
            std::fs::remove_dir_all(&opt.work_dir).unwrap()
        };
        let lsm = Arc::new(Lsm::open(opt));

        for _ in 0..10 {
            let lsm = Arc::clone(&lsm);
            std::thread::spawn(move || {
                for i in 100..200 {
                    let n = i as u32;
                    lsm.put(&n.to_be_bytes(), &n.to_be_bytes()).unwrap();
                    let n = i as u32;
                    let res = lsm.get(&n.to_be_bytes()).unwrap();
                    assert_ne!(res, None);
                    assert_eq!(res.unwrap(), n.to_be_bytes());
                }
            });
        }

        for i in 0..100 {
            let n = i as u32;
            lsm.put(&n.to_be_bytes(), &n.to_be_bytes()).unwrap();
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.unwrap(), n.to_be_bytes());
        }
        std::thread::sleep(Duration::from_secs(10));
        for i in 0..200 {
            let n = i as u32;
            let res = lsm.get(&n.to_be_bytes()).unwrap();
            assert_ne!(res, None);
            assert_eq!(res.unwrap(), n.to_be_bytes());
        }
    }
}
