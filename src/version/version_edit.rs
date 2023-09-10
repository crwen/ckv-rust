use super::FileMetaData;

pub struct TableMeta {
    pub file_meta: FileMetaData,
    pub level: u32,
}

impl TableMeta {
    pub fn new(file_meta: FileMetaData, level: u32) -> Self {
        Self { file_meta, level }
    }
}

pub struct VersionEdit {
    // last_sequence: u64,
    // next_file_number: u64,
    delete_files: Vec<TableMeta>,
    add_files: Vec<TableMeta>,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            // last_sequence: 0,
            // next_file_number: 0,
            delete_files: Vec::new(),
            add_files: Vec::new(),
        }
    }

    pub fn add_file(&mut self, level: u32, fid: u64, smallest: &[u8], largest: &[u8]) {
        let f = FileMetaData::with_range(fid, smallest, largest);
        let table_meta = TableMeta::new(f, level);
        self.add_files.push(table_meta);
    }
    pub fn delete_file(&mut self, level: u32, fid: u64, smallest: &[u8], largest: &[u8]) {
        let f = FileMetaData::with_range(fid, smallest, largest);
        let table_meta = TableMeta::new(f, level);
        self.delete_files.push(table_meta);
    }
}
