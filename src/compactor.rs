use std::{
    sync::{
        mpsc::{Receiver, RecvTimeoutError},
        Arc,
    },
    time::Duration,
};

use crate::{
    file::{path_of_file, Ext},
    lsm::LsmInner,
    mem_table::{MemTable, MemTableIterator},
    sstable::TableBuilder,
    version::{FileMetaData, Version, VersionEdit},
    Options,
};

#[derive(Debug)]
pub struct CompactionState {
    pub base_level: usize,
    pub target_level: usize,
    pub base: Vec<FileMetaData>,
    pub target: Vec<FileMetaData>,
}
pub struct GCState {
    pub level: usize,
    pub rewrite_file: FileMetaData,
    pub new_file: FileMetaData,
}

pub struct Compactor {
    handle: Receiver<Task>,
    lsm_inner: Arc<LsmInner>,
}

pub enum Task {
    Compact,
    Seek(SeekTask),
    Major,
}

pub struct SeekTask {
    pub level: u32,
    pub fid: u64,
}

#[derive(Clone)]
pub struct L0Task {
    pub version: Arc<Version>,
    pub imm: Arc<MemTable>,
    pub log_number: u64,
    pub fid: u64,
}

impl L0Task {
    pub fn execute(&self, opt: Options) -> VersionEdit {
        {
            // let inner = self.mem_inner.read();
            let mut edit = VersionEdit::new();
            // let fid = self.version.new_file_number();
            let mut file_meta = FileMetaData::new(self.fid);
            // imm  to sst

            TableBuilder::build_table(
                path_of_file(&opt.work_dir, self.fid, Ext::SST).as_path(),
                opt.clone(),
                MemTableIterator::new(&self.imm),
                &mut file_meta,
            )
            .unwrap();

            // pick level to push
            let level = self.version.pick_level_for_mem_table_output(
                file_meta.smallest().user_key(),
                file_meta.largest().user_key(),
            );

            edit.add_file(level, file_meta);
            edit.log_number(self.log_number);
            self.version.derefs();
            edit
            // info!("Minor compact {:05}.sst to level {:?}", fid, level);
        }
    }
}
impl Compactor {
    pub fn new(handle: Receiver<Task>, lsm_inner: Arc<LsmInner>) -> Self {
        Self { handle, lsm_inner }
    }

    pub fn run_compactor(&self) {
        loop {
            match self.handle.recv_timeout(Duration::from_secs(2)) {
                Ok(task) => match task {
                    Task::Compact => {
                        let sz = self.lsm_inner.imms_sz();
                        if self.lsm_inner.imms_sz() > 0 {
                            for _ in 0..(sz.max(4) - 3) {
                                self.lsm_inner.compact_mem_table();
                            }
                        } else {
                            // compact sst
                            self.lsm_inner.major_compaction().unwrap();
                        }
                    }
                    Task::Seek(seek_task) => {
                        self.lsm_inner.seek_compaction(&seek_task).unwrap();
                    }
                    Task::Major => {
                        self.lsm_inner.major_compaction().unwrap();
                    }
                },
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    let sz = self.lsm_inner.imms_sz();
                    if self.lsm_inner.imms_sz() > 0 {
                        for _ in 0..(sz.max(4) - 3) {
                            self.lsm_inner.compact_mem_table();
                        }
                    } else {
                        // compact sst
                        self.lsm_inner.major_compaction().unwrap();
                    }
                }
            }
        }
    }
}
