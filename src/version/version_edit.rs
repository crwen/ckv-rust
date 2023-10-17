use bytes::BufMut;

use super::{FileMetaData, InternalKey};

#[derive(Clone)]
pub struct TableMeta {
    pub file_meta: FileMetaData,
    pub level: u32,
}

impl TableMeta {
    pub fn new(file_meta: FileMetaData, level: u32) -> Self {
        Self { file_meta, level }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        // let mut data = Vec::new();
        buf.put_u32(self.level);

        let file_meta_data = self.file_meta.encode();
        buf.put_u32(file_meta_data.len() as u32);
        buf.put(file_meta_data.as_slice());
    }
}

#[derive(Default)]
pub struct VersionEdit {
    // last_sequence: u64,
    // next_file_number: u64,
    pub delete_files: Vec<TableMeta>,
    pub add_files: Vec<TableMeta>,
    pub log_number: u64,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            // last_sequence: 0,
            // next_file_number: 0,
            delete_files: Vec::new(),
            add_files: Vec::new(),
            log_number: 0,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64(self.log_number);
        self.add_files.iter().for_each(|f| f.encode(buf));
        self.delete_files.iter().for_each(|f| f.encode(buf));
    }

    pub fn log_number(&mut self, number: u64) {
        self.log_number = number;
    }

    pub fn add_file(
        &mut self,
        level: u32,
        fid: u64,
        smallest: &InternalKey,
        largest: &InternalKey,
    ) {
        let f = FileMetaData::with_internal_range(fid, smallest.clone(), largest.clone());

        let table_meta = TableMeta::new(f, level);
        self.add_files.push(table_meta);
    }
    pub fn delete_file(&mut self, level: u32, fid: u64, smallest: &[u8], largest: &[u8]) {
        let f = FileMetaData::with_range(fid, smallest, largest);
        let table_meta = TableMeta::new(f, level);
        self.delete_files.push(table_meta);
    }
}
