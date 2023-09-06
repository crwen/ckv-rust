use std::sync::Arc;

use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;

use crate::utils::{
    codec::{decode_varintu32, encode_varintu32, varintu32_length},
    Entry,
};

const OP_TYPE_PUT: u8 = 0;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Key {
    key: Vec<u8>,
}

// impl PartialEq for Key {
//     fn eq(&self, other: &Self) -> bool {
//         self.key == other.key
//     }
// }
//
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let k1 = Key::user_key(&self.key);
        let k2 = Key::user_key(&other.key);
        match k1.partial_cmp(k2) {
            Some(ord) => match ord {
                std::cmp::Ordering::Equal => {
                    let seq1 = Key::tag(&self.key);
                    let seq2 = Key::tag(&other.key);
                    seq2.partial_cmp(seq1)
                }
                other => Some(other),
            },
            None => None,
        }
        // self.key.partial_cmp(&other.key)
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let k1 = Key::user_key(&self.key);
        let k2 = Key::user_key(&other.key);
        match k1.cmp(k2) {
            std::cmp::Ordering::Equal => {
                let seq1 = Key::tag(&self.key);
                let seq2 = Key::tag(&other.key);
                seq2.cmp(seq1)
            }
            other => other,
        }
    }
}

impl Key {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    pub fn user_key(key: &[u8]) -> &[u8] {
        let sz = decode_varintu32(key).unwrap();
        let var_sz = varintu32_length(sz) as usize;

        &key[var_sz..var_sz + sz as usize]
    }

    pub fn tag(key: &[u8]) -> &[u8] {
        let sz = decode_varintu32(key).unwrap();
        let var_sz = varintu32_length(sz) as usize;

        &key[var_sz + sz as usize..]
    }
}

type Table = SkipMap<Key, Bytes>;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    table: Arc<Table>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: Arc::new(Table::new()),
        }
    }

    pub fn get(&self, entry: Entry) -> Option<Bytes> {
        let internal_key = MemTable::build_internal_key(&entry, OP_TYPE_PUT);
        let e = Entry::new(entry.key, vec![], 0);
        let right = MemTable::build_internal_key(&e, OP_TYPE_PUT);
        let key = self
            .table
            .range((
                std::ops::Bound::Included(internal_key),
                std::ops::Bound::Included(right),
            ))
            .next();
        key.map(|e| {
            let value = e.value();
            let value_sz = decode_varintu32(value).unwrap();
            Bytes::from(value[varintu32_length(value_sz) as usize..].to_vec())
        })
    }

    // +-----------------------+   +--------------------+
    // |  key_size | key | tag |   | value_size | value |
    // +-----------------------+   +--------------------+
    pub fn set(&self, entry: Entry) {
        let internal_key = MemTable::build_internal_key(&entry, OP_TYPE_PUT);
        self.table
            .insert(internal_key, MemTable::build_value(&entry));
    }

    // +-----------------------+
    // |  key_size | key | tag |
    // +-----------------------+
    fn build_internal_key(entry: &Entry, typ: u8) -> Key {
        let key = entry.key();
        let seq = entry.seq();
        let key_sz = key.len() as u32;
        let mut internal_key = vec![];

        encode_varintu32(&mut internal_key, key_sz);

        internal_key.put_slice(key);
        internal_key.put_u64((seq << 8) | typ as u64);

        Key::new(internal_key)
    }

    // +-----------------+
    // |  val_size | val |
    // +-----------------+
    fn build_value(entry: &Entry) -> Bytes {
        let value = entry.value();
        let value_sz = value.len() as u32;
        let mut value_wrap = vec![];

        encode_varintu32(&mut value_wrap, value_sz);

        value_wrap.put_slice(value);

        Bytes::from(value_wrap)
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_cmp_test() {
        let key1 = Key::new(vec![1, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
        let key2 = Key::new(vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 1]);
        let key3 = Key::new(vec![1, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert!(key1 < key2);
        assert!(key1 > key3);
        assert!(key2 > key3);
    }

    fn build_base_table() -> MemTable {
        let memtable = MemTable::new();
        let e = Entry::new(vec![3], vec![30], 0);
        memtable.set(e);
        let e = Entry::new(vec![1], vec![11], 1);
        memtable.set(e);
        let e = Entry::new(vec![1], vec![12], 2);
        memtable.set(e);
        let e = Entry::new(vec![1], vec![13], 3);
        memtable.set(e);
        let e = Entry::new(vec![3], vec![34], 4);
        memtable.set(e);
        let e = Entry::new(vec![254, 233, 234], vec![254, 233, 234], 9);
        memtable.set(e);

        for ele in memtable.table.iter() {
            println!("{:?}", ele);
        }
        memtable
    }

    #[test]
    fn search_test() {
        let memtable = build_base_table();
        let e = Entry::new(vec![1], vec![], 0);
        // let e3 = Entry::new(vec![3], vec![], 0);
        // let e4 = Entry::new(vec![3], vec![], 4);
        // let e4 = Entry::new(vec![2], vec![], 5);
        let res = memtable.get(e);
        assert_eq!(res, None);
        let e = Entry::new(vec![1], vec![], 2);
        let res = memtable.get(e);
        assert_eq!(res, Some(Bytes::from(vec![12])));
        let e = Entry::new(vec![1], vec![], 5);
        let res = memtable.get(e);
        assert_eq!(res, Some(Bytes::from(vec![13])));

        let e = Entry::new(vec![2], vec![], 5);
        let res = memtable.get(e);
        assert_eq!(res, None);

        let e = Entry::new(vec![3], vec![], 5);
        let res = memtable.get(e);
        assert_eq!(res, Some(Bytes::from(vec![34])));

        let e = Entry::new(vec![22], vec![], 5);
        let res = memtable.get(e);
        assert_eq!(res, None);

        let e = Entry::new(vec![254, 233, 234], vec![], 8);
        let res = memtable.get(e);
        assert_eq!(res, None);

        let e = Entry::new(vec![254, 233, 234], vec![], 9);
        let res = memtable.get(e);
        assert_eq!(res, Some(Bytes::from(vec![254, 233, 234])));
    }
}
