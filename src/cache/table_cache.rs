use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::sstable::table::Table;

use super::{CacheError, Result};

#[derive(Clone)]
enum NodeState {
    InUse,
    Lru,
}

#[derive(Clone)]
struct Node<Value> {
    value: Value,
    pinned: u32,
    handle: NodeState,
}

impl<Value> Node<Value> {
    fn new(value: Value, pinned: u32, handle: NodeState) -> Self {
        Self {
            value,
            pinned,
            handle,
        }
    }
}

unsafe impl Send for TableCache {}
unsafe impl Sync for TableCache {}

pub struct TableCache {
    inner: Arc<Mutex<LRUInner>>,
}

impl TableCache {
    // pub(crate) fn new() -> Self {
    //     LRUCache::with_capacity(100)
    // }
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LRUInner::with_capacity(capacity))),
        }
    }
}

impl TableCache {
    pub fn unpin(&self, key: &u64) -> Result<()> {
        self.inner.lock().unpin(key)
    }

    pub fn get(&self, key: &u64) -> Option<Arc<Table>> {
        let mut inner = self.inner.lock();
        inner.get(key)
    }

    pub fn insert(&self, key: u64, value: Arc<Table>) -> Result<()> {
        self.inner.lock().insert(key, value)
    }
}

pub struct LRUInner {
    capacity: usize,
    in_use: VecDeque<u64>,
    lru: VecDeque<u64>,
    table: HashMap<u64, Node<Arc<Table>>>,
    usage: usize,
    lock: Mutex<()>,
}

impl LRUInner {
    // pub fn new() -> Self {
    //     LRUCache::<Key, Value>::with_capacity(100)
    // }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            in_use: VecDeque::with_capacity(capacity),
            lru: VecDeque::with_capacity(capacity),
            table: HashMap::new(),
            lock: Mutex::new(()),
            usage: 0,
        }
    }

    // pub fn insert(&mut self, key: Key, value: Value) {}
    //
    pub fn get(&mut self, key: &u64) -> Option<Arc<Table>> {
        let _lock = self.lock.lock();
        if let Some(mut node) = self.table.get_mut(key) {
            node.pinned += 1;
            // let result = Some(&node.value);
            let handle = node.handle.clone();
            match handle {
                NodeState::InUse => {}
                NodeState::Lru => {
                    node.handle = NodeState::InUse;
                    let idx = self.lru.iter().position(|k| k == key)?;
                    let nd = self.lru.remove(idx)?;
                    self.in_use.push_back(nd);
                }
            }
            self.table.get(key).map(|node| node.value.clone())
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: u64, value: Arc<Table>) -> Result<()> {
        let _lock = self.lock.lock();
        match self.table.get_mut(&key) {
            Some(_) => {
                // node.pinned += 1;
                // node.value = value;
                Err(CacheError::DuplicatedElements)
            }
            None => {
                if self.usage + value.size() as usize > self.capacity && self.lru.is_empty() {
                    return Err(CacheError::AllElementsPinned);
                }
                self.usage += value.size() as usize;
                self.table
                    .insert(key, Node::new(value, 1, NodeState::InUse));
                self.in_use.push_back(key);

                while self.usage > self.capacity && !self.lru.is_empty() {
                    let removed_key = self.lru.pop_front().unwrap();
                    let removed = self.table.remove(&removed_key).unwrap();
                    self.usage -= removed.value.size() as usize;
                }
                Ok(())
            }
        }
    }

    pub fn unpin(&mut self, key: &u64) -> Result<()> {
        let _lock = self.lock.lock();
        if let Some(mut node) = self.table.get_mut(key) {
            if node.pinned == 0 {
                return Err(CacheError::UnpinNonPinned);
            }
            node.pinned -= 1;
            if node.pinned == 0 {
                // move to lru
                node.handle = NodeState::Lru;
                let idx = self.in_use.iter().position(|k| k == key).unwrap();
                let nd = self.in_use.remove(idx).unwrap();
                self.lru.push_back(nd);
            }
        }
        Ok(())
    }
}

// #[cfg(test)]
// mod cache_test {
//     use crate::cache::CacheError;
//
//     use super::LRUInner;
//
//     #[test]
//     fn cache_test() {
//         let mut cache = LRUInner::<i32, i32>::with_capacity(10);
//         for i in 0..10 {
//             cache.insert(i, i).unwrap();
//         }
//
//         assert_eq!(cache.insert(11, 11), Err(CacheError::AllElementsPinned));
//         for i in 0..5 {
//             cache.unpin(&i).unwrap();
//         }
//         assert_eq!(cache.insert(9, 9), Err(CacheError::DuplicatedElements));
//
//         assert!(cache.insert(11, 11).is_ok()); // vicmtim 0
//
//         assert_eq!(cache.get(&11), Some(&11));
//         assert_eq!(cache.get(&0), None);
//         assert_eq!(cache.get(&1), Some(&1)); // 1 pinned
//         assert!(cache.insert(12, 12).is_ok()); // victim 2
//         assert_eq!(cache.get(&12), Some(&12));
//         assert_eq!(cache.get(&2), None);
//
//         assert!(cache.insert(13, 13).is_ok()); // victim 3
//         assert!(cache.insert(14, 14).is_ok()); // victim 4
//         assert_eq!(cache.insert(15, 15), Err(CacheError::AllElementsPinned));
//     }
// }
