use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;

use crate::utils::{
    codec::{decode_varintu32, encode_varintu32, varintu32_length},
    Entry,
};

const OP_TYPE_PUT: u8 = 0;

type Table = SkipMap<Key, Bytes>;
type TableIterator<'a> = crossbeam_skiplist::map::Iter<'a, Key, Bytes>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Key {
    key: Vec<u8>,
}

impl PartialOrd for Key {
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

impl Ord for Key {
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

impl Key {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    pub fn user_key(&self) -> &[u8] {
        let key = &self.key[..];
        let sz = decode_varintu32(key).unwrap();
        let var_sz = varintu32_length(sz) as usize;

        &key[var_sz..var_sz + sz as usize]
    }

    pub fn internal_key(&self) -> &[u8] {
        let key = &self.key[..];
        let sz = decode_varintu32(key).unwrap();
        let var_sz = varintu32_length(sz) as usize;
        &key[var_sz..]
    }

    pub fn tag(key: &[u8]) -> &[u8] {
        let sz = decode_varintu32(key).unwrap();
        let var_sz = varintu32_length(sz) as usize;

        &key[var_sz + sz as usize..]
    }

    pub fn seq(&self) -> u64 {
        let key = &self.key;
        let len = key.len();

        let mut bytes = Bytes::copy_from_slice(&key[len - 8..]);
        // bytes.get_u64() >> 8
        bytes.get_u64() >> 8
    }
}

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    table: Arc<Table>,
    refs: AtomicU64,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: Arc::new(Table::new()),
            refs: AtomicU64::new(1),
        }
    }

    pub fn incr_refs(&self) {
        self.refs.fetch_add(1, Ordering::SeqCst);
    }

    pub fn decr_refs(&self) {
        self.refs.fetch_sub(1, Ordering::SeqCst);
        if self.refs.load(Ordering::SeqCst) == 0 {
            todo!("Implement me!")
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

    pub fn colse(&self) {
        self.decr_refs()
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

pub struct MemTableIterator<'a> {
    mem: &'a MemTable,
    table_iter: TableIterator<'a>,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let item_op = self.table_iter.next();
        match item_op {
            Some(item) => {
                let value = item.value();
                let value_sz = decode_varintu32(value).unwrap();
                self.key = item.key().internal_key().to_vec();
                self.value = value[varintu32_length(value_sz) as usize..].to_vec();
                Some(Entry::new(
                    self.key.clone(),
                    self.value.clone(),
                    item.key().seq(),
                ))
                // crossbeam_skiplist::map::Entry;
                // item_op.map(|e| e)
                // Some(crossbeam_skiplist::map::Entry::from(j))
            }
            None => {
                self.mem.decr_refs();
                None
            }
        }
    }
}

impl<'a> MemTableIterator<'a> {
    pub fn new(mem: &'a MemTable) -> Self {
        mem.incr_refs();
        Self {
            mem,
            table_iter: mem.table.iter(),
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[cfg(test)]
mod memtable_tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn key_cmp_test() {
        let key1 = Key::new(vec![1, 1, 0, 0, 0, 0, 0, 0, 1, 0]);
        let key2 = Key::new(vec![1, 2, 0, 0, 0, 0, 0, 0, 1, 0]);
        let key3 = Key::new(vec![1, 1, 0, 0, 0, 0, 0, 0, 2, 0]);
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

    #[test]
    fn mem_iter_test() {
        let memtable = MemTable::new();
        assert_eq!(memtable.refs.load(Ordering::SeqCst), 1);
        for i in 0..100 {
            let e = Entry::new(vec![i], vec![i], i as u64);
            memtable.set(e);
        }
        for i in 0..100 {
            let e = Entry::new(vec![i], vec![i, i], (i + 100) as u64);
            memtable.set(e);
        }
        let iter = MemTableIterator::new(&memtable);
        assert_eq!(memtable.refs.load(Ordering::SeqCst), 2);
        for (i, e) in iter.enumerate() {
            // 0, 1, 2, 3, 4, 5, 6, 7
            // 0, 0, 1, 1, 2, 2, 3, 3
            let val = (i / 2) as u8;
            if i % 2 == 0 {
                assert_eq!(e.value, vec![val, val]);
            } else {
                assert_eq!(e.value, vec![val]);
            }
        }
        // iter.for_each(|e| println!("{:?}", e));
        assert_eq!(memtable.refs.load(Ordering::SeqCst), 1);
        // memtable.colse();
    }
}
