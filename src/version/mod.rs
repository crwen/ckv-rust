use bytes::{Buf, Bytes};

pub mod verset_edit;
pub mod version_set;

#[derive(Debug, Clone, Default)]
pub struct FileMetaData {
    // refs: i32,
    number: u64,
    // file_size: u64,        // File size in bytes
    smallest: InternalKey, // Smallest internal key served by table
    largest: InternalKey,  // Largest internal key served by table
}

impl FileMetaData {
    pub fn new(number: u64) -> Self {
        Self {
            // refs: 0,
            number,
            // file_size: 0,
            smallest: InternalKey::new(vec![]),
            largest: InternalKey::new(vec![]),
        }
    }

    pub fn with_range(number: u64, smallest: &[u8], largest: &[u8]) -> Self {
        Self {
            // refs: 0,
            number,
            // file_size: 0,
            smallest: InternalKey::new(smallest.to_vec()),
            largest: InternalKey::new(largest.to_vec()),
        }
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn smallest(&self) -> &InternalKey {
        &self.smallest
    }

    pub fn set_smallest(&mut self, smallest: &[u8]) {
        self.smallest = InternalKey::new(smallest.to_vec());
    }

    pub fn largest(&self) -> &InternalKey {
        &self.largest
    }

    pub fn set_largest(&mut self, largest: &[u8]) {
        self.smallest = InternalKey::new(largest.to_vec());
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct InternalKey {
    key: Vec<u8>,
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let k1 = self.user_key();
        let k2 = other.user_key();
        match k1.partial_cmp(k2) {
            Some(ord) => match ord {
                std::cmp::Ordering::Equal => {
                    let seq1 = self.seq();
                    let seq2 = other.seq();
                    seq2.partial_cmp(&seq1)
                }
                other => Some(other),
            },
            None => None,
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let k1 = self.user_key();
        let k2 = other.user_key();
        match k1.cmp(k2) {
            std::cmp::Ordering::Equal => {
                let seq1 = self.seq();
                let seq2 = other.seq();
                seq2.cmp(&seq1)
            }
            other => other,
        }
    }
}

impl InternalKey {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    pub fn user_key(&self) -> &[u8] {
        let len = self.key.len();
        &self.key[..len - 8]
    }

    pub fn seq(&self) -> u64 {
        let key = &self.key;
        let len = key.len();

        let mut bytes = Bytes::copy_from_slice(&key[len - 8..]);
        bytes.get_u64() >> 8
    }

    // pub fn len(&self) -> u64 {
    //     self.key.len() as u64
    // }
}
