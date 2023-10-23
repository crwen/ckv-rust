use bytes::{Buf, BufMut};

use super::FileMetaData;

// enum Tag {
//     LogNumber,
//     NextFileNumber,
//     SeqNumber,
// }
//

#[derive(Clone, Debug)]
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

    pub fn decode(data: &[u8]) -> Vec<Self> {
        let mut res = vec![];
        let mut off = 0;

        while off + 8 < data.len() {
            let level = (&data[off..off + 4]).get_u32();
            let sz = (&data[off + 4..off + 8]).get_u32();
            if off + 8 + sz as usize > data.len() {
                break;
            }
            let file_meta = FileMetaData::decode(&data[off + 8..off + 8 + sz as usize]);
            let meta = Self { file_meta, level };
            res.push(meta);
            off += 8 + sz as usize;
        }
        res
    }
}

#[derive(Default, Debug)]
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
    pub fn decode(data: &[u8]) -> Self {
        let log_number = (&data[..8]).get_u64();
        let next_file_number = (&data[8..16]).get_u64();
        let last_seq_number = (&data[16..24]).get_u64();
        let add_file_sz = (&data[24..28]).get_u32();
        let add_files = TableMeta::decode(&data[28..28 + add_file_sz as usize]);
        let _delete_file_sz = (&data[28 + add_file_sz as usize..]).get_u32();
        let delete_files = TableMeta::decode(&data[32 + add_file_sz as usize..]);

        Self {
            delete_files,
            add_files,
            log_number,
            next_file_number,
            last_seq_number,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64(self.log_number);
        buf.put_u64(self.next_file_number);
        buf.put_u64(self.last_seq_number);
        // add files
        let mut add_file_buf = vec![];
        self.add_files
            .iter()
            .for_each(|f| f.encode(&mut add_file_buf));
        buf.put_u32(add_file_buf.len() as u32);
        buf.put_slice(&add_file_buf);
        // delete files
        let mut delete_file_buf = vec![];
        self.delete_files
            .iter()
            .for_each(|f| f.encode(&mut delete_file_buf));
        buf.put_u32(delete_file_buf.len() as u32);
        buf.put_slice(&delete_file_buf);
    }

    pub fn log_number(&mut self, number: u64) {
        self.log_number = number;
    }

    pub fn next_file_number(&mut self, next_file_number: u64) {
        self.next_file_number = next_file_number;
    }

    pub fn last_seq_number(&mut self, last_seq_number: u64) {
        self.last_seq_number = last_seq_number;
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

// #[cfg(test)]
// mod edit_test {
//     use core::panic;
//     use std::io::ErrorKind;
//
//     use crate::file::{log_reader::Reader, path_of_file, SequentialFileImpl};
//
//     use super::VersionEdit;
//
//     #[test]
//     fn edit_decode_test() {
//         let opt = crate::Options::default_opt().work_dir("work_dir/lsm");
//         let mut f = Reader::new(Box::new(SequentialFileImpl::new(
//             path_of_file(&opt.work_dir, 0, crate::file::Ext::MANIFEST).as_path(),
//         )));
//         let mut edit = VersionEdit::new();
//         let mut log_number = 0;
//         let mut last_seq_number = 0;
//
//         let mut end = false;
//         while !end {
//             let record = f.read_record();
//             match record {
//                 Ok(record) => {
//                     let t_edit = VersionEdit::decode(&record);
//                     t_edit
//                         .add_files
//                         .iter()
//                         .for_each(|f| edit.add_files.push(f.clone()));
//                     t_edit
//                         .delete_files
//                         .iter()
//                         .for_each(|f| edit.delete_files.push(f.clone()));
//
//                     log_number = log_number.max(t_edit.log_number);
//                     last_seq_number = last_seq_number.max(t_edit.last_seq_number);
//                     edit.next_file_number(t_edit.next_file_number);
//                 }
//                 Err(err) => match err.kind() {
//                     ErrorKind::UnexpectedEof => end = true,
//                     err => panic!("{:?}", err),
//                 },
//             };
//         }
//         edit.log_number(log_number);
//         edit.last_seq_number(last_seq_number);
//     }
// }
