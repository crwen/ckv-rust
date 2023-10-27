use bytes::{Buf, BufMut, Bytes};

pub mod version_edit;
pub mod version_set;

#[derive(Debug, Clone, Default)]
pub struct FileMetaData {
    // refs: i32,
    pub number: u64,
    pub file_size: u64,        // File size in bytes
    pub smallest: InternalKey, // Smallest internal key served by table
    pub largest: InternalKey,  // Largest internal key served by table
    pub vlogs: Vec<u64>,
}

impl FileMetaData {
    pub fn new(number: u64) -> Self {
        Self {
            // refs: 0,
            number,
            file_size: 0,
            smallest: InternalKey::new(vec![]),
            largest: InternalKey::new(vec![]),
            vlogs: Vec::new(),
        }
    }

    pub fn with_range(number: u64, smallest: &[u8], largest: &[u8]) -> Self {
        Self {
            // refs: 0,
            number,
            file_size: 0,
            smallest: InternalKey::new(smallest.to_vec()),
            largest: InternalKey::new(largest.to_vec()),
            vlogs: Vec::new(),
        }
    }

    pub fn with_internal_range(number: u64, smallest: InternalKey, largest: InternalKey) -> Self {
        Self {
            // refs: 0,
            number,
            file_size: 0,
            smallest,
            largest,
            vlogs: Vec::new(),
        }
    }

    pub fn set_file_size(&mut self, file_size: u64) {
        self.file_size = file_size;
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn smallest(&self) -> &InternalKey {
        &self.smallest
    }

    pub fn set_smallest(&mut self, smallest: InternalKey) {
        self.smallest = smallest;
    }

    pub fn largest(&self) -> &InternalKey {
        &self.largest
    }

    pub fn set_largest(&mut self, largest: InternalKey) {
        self.largest = largest;
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u64(self.number);
        buf.put_u64(self.file_size);
        buf.put_u32(self.smallest.len());
        buf.put(self.smallest.key().as_slice());
        buf.put_u32(self.largest.len());
        buf.put(self.largest.key().as_slice());
        buf.put_u32(self.vlogs.len() as u32);
        self.vlogs.iter().for_each(|fid| {
            buf.put_u64(*fid);
        });
        buf
    }
    pub fn decode(data: &[u8]) -> Self {
        let number = (&data[..8]).get_u64();
        let file_size = (&data[8..16]).get_u64();
        let smallest_sz = (&data[16..20]).get_u32();
        let smallest = data[20..20 + smallest_sz as usize].to_vec();
        let largest_sz = (&data[20 + smallest_sz as usize..]).get_u32();
        let mut off = 24 + smallest_sz as usize;
        let largest = data[off..off + largest_sz as usize].to_vec();
        off += largest_sz as usize;
        let vlen = (&data[off..]).get_u32();
        off += 4;
        let mut vlogs = vec![];
        for _ in 0..vlen {
            let fid = (&data[off..]).get_u64();
            vlogs.push(fid);
            off += 8;
        }

        Self {
            number,
            file_size,
            smallest: InternalKey::new(smallest),
            largest: InternalKey::new(largest),
            vlogs,
        }
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

    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
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

    pub fn len(&self) -> u32 {
        self.key.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.key.is_empty()
    }
}
