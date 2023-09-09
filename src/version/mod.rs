use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Version {
    last_sequence: AtomicU64,
    next_file_number: AtomicU64,
}

impl Version {
    pub fn new() -> Self {
        Self {
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
}
