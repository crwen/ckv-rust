use std::sync::atomic::{AtomicU64, Ordering};

use super::FileMetaData;

#[derive(Default, Debug)]
pub struct Version {
    files: Vec<Vec<FileMetaData>>,
}

impl Version {
    pub fn new() -> Self {
        Self {
            files: Vec::with_capacity(7),
        }
    }

    pub fn files(&self) -> &Vec<Vec<FileMetaData>> {
        &self.files
    }

    pub fn level_files(&self, level: usize) -> &Vec<FileMetaData> {
        &self.files[level]
    }

    pub fn get(&self, user_key: &[u8]) -> Option<Vec<u8>> {
        // search L0 first
        let mut tmp = Vec::new();
        self.files[0]
            .iter()
            .filter(|f| f.smallest.user_key() <= user_key && f.largest.user_key() >= user_key)
            .for_each(|f| tmp.push(f));

        if !tmp.is_empty() {
            // tmp.sort_by(|a, b| a.number.partial_cmp(&b.number).unwrap());
            tmp.sort_by(|a, b| a.number.cmp(&b.number));
            // TODO: search sst
            // tmp.iter().for_each(|f| {})
        }

        // TODO: search other levels

        unimplemented!();
    }
}

#[derive(Default)]
pub struct VersionSet {
    current: Version,
    last_sequence: AtomicU64,
    next_file_number: AtomicU64,
}

impl VersionSet {
    pub fn new() -> Self {
        Self {
            current: Version::new(),
            next_file_number: AtomicU64::new(0),
            last_sequence: AtomicU64::new(0),
        }
    }

    pub fn new_file_number(&mut self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    pub fn add_last_sequence(&mut self, n: u64) -> u64 {
        self.last_sequence.fetch_add(n, Ordering::SeqCst)
    }

    pub fn current(&self) -> &Version {
        &self.current
    }
}
