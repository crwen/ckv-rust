use bytes::BufMut;

use super::FileMetaData;

// enum Tag {
//     LogNumber,
//     NextFileNumber,
//     SeqNumber,
// }

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
    pub next_file_number: u64,
    pub last_seq_number: u64,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            // last_sequence: 0,
            // next_file_number: 0,
            delete_files: Vec::new(),
            add_files: Vec::new(),
            log_number: 0,
            next_file_number: 0,
            last_seq_number: 0,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64(self.log_number);
        buf.put_u64(self.next_file_number);
        buf.put_u64(self.last_seq_number);
        self.add_files.iter().for_each(|f| f.encode(buf));
        self.delete_files.iter().for_each(|f| f.encode(buf));
    }

    pub fn log_number(&mut self, number: u64) {
        self.log_number = number;
    }

    pub fn next_file_number(&mut self, next_file_number: u64) {
        self.next_file_number = next_file_number;
    }

    pub fn last_seq_number(&mut self, last_seq_number: u64) {
        self.log_number = last_seq_number;
    }

    pub fn add_file(&mut self, level: u32, file_meta: FileMetaData) {
        // let f = FileMetaData::with_internal_range(fid, table_meta.smallest.clone(), table_meta.largest.clone());

        let table_meta = TableMeta::new(file_meta, level);
        self.add_files.push(table_meta);
    }
    pub fn delete_file(&mut self, level: u32, file_meta: FileMetaData) {
        let table_meta = TableMeta::new(file_meta, level);
        self.delete_files.push(table_meta);
    }
}
