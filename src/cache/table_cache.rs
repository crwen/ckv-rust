use std::{
    collections::{hash_map::RandomState, HashMap, VecDeque},
    fmt::Debug,
    hash::{BuildHasher, Hash, Hasher},
    sync::Arc,
};

use parking_lot::Mutex;
use tracing::info;

use super::{CacheError, Result};

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

type CacheValue<V> = Arc<V>;

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

unsafe impl<K: Hash + Eq + Clone, V> Send for Cache<K, V> {}
unsafe impl<K: Hash + Eq + Clone, V> Sync for Cache<K, V> {}
//
pub struct Cache<K: Hash, V> {
    inner: Arc<Vec<Mutex<LRUInner<K, V>>>>,
    hasher: RandomState,
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone + Debug,
{
    pub fn with_capacity(capacity: usize) -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        let per_shard = (capacity + (NUM_SHARDS - 1)) / NUM_SHARDS;
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(LRUInner::with_capacity(per_shard)));
        }
        Self {
            inner: Arc::new(shards),
            hasher: RandomState::default(),
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone + Debug,
{
    pub fn unpin(&self, key: &K) -> Result<()> {
        // self.inner.lock().unpin(key)
        self.inner[self.shards(key)].lock().unpin(key)
    }

    pub fn get(&self, key: &K) -> Option<CacheValue<V>> {
        // let mut inner = self.inner.lock();
        let mut inner = self.inner[self.shards(key)].lock();
        inner.get(key)
    }

    pub fn insert(&self, key: K, value: V, charge: usize) -> Result<()> {
        // self.inner.lock().insert(key, value, charge)
        self.inner[self.shards(&key)]
            .lock()
            .insert(key, value, charge)
    }

    pub fn evict(&self, key: K, charge: usize) -> Result<()> {
        // self.inner.lock().evict(&key, charge)
        self.inner[self.shards(&key)].lock().evict(&key, charge)
    }
    fn shards(&self, key: &K) -> usize {
        let mut hasher = self.hasher.build_hasher();
        // let hasher = &mut RandomState::default().build_hasher();
        key.hash(&mut hasher);
        let h = hasher.finish();
        // let data = key.to_be_bytes();
        // let (seed, m) = (0xbc9f1d34_usize, 0xc6a4a793_usize);
        // let mut h = seed ^ (m.wrapping_mul(data.len()));
        // let mut len = data.len();
        // let mut base = 0;
        // while base + 4 <= len {
        //     let w = (data[base] as usize)
        //         | (data[base + 1] as usize) << 8
        //         | (data[base + 2] as usize) << 16
        //         | (data[base + 3] as usize) << 24;
        //     h = h.wrapping_add(w);
        //     h = h.wrapping_mul(m);
        //     base += 4
        // }
        // len -= base;
        // if len == 3 {
        //     h += (data[base + 2] as usize) << 16;
        // } else if len == 2 {
        //     h += (data[base + 1] as usize) << 8;
        // } else if len == 1 {
        //     h += data[base] as usize;
        //     h *= h.wrapping_mul(m);
        //     h ^= h >> 24;
        // }
        //
        // h % NUM_SHARDS
        h as usize % NUM_SHARDS
    }
}

pub struct LRUInner<K, V> {
    capacity: usize,
    in_use: VecDeque<K>,
    lru: VecDeque<K>,
    table: HashMap<K, Node<CacheValue<V>>>,
    usage: usize,
}

unsafe impl<K: Send, V: Send> Send for LRUInner<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LRUInner<K, V> {}

impl<K, V> LRUInner<K, V>
where
    K: Hash + Eq + Clone + Debug,
{
    // pub fn new() -> Self {
    //     LRUCache::<Key, Value>::with_capacity(100)
    // }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            in_use: VecDeque::with_capacity(capacity),
            lru: VecDeque::with_capacity(capacity),
            table: HashMap::new(),
            usage: 0,
        }
    }

    // pub fn insert(&mut self, key: Key, value: Value) {}
    //
    pub fn get(&mut self, key: &K) -> Option<CacheValue<V>> {
        // let _lock = self.lock.lock();
        if let Some(node) = self.table.get_mut(key) {
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
            // self.table.get(key).map(|node| node.value.clone())
            Some(node.value.clone())
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V, charge: usize) -> Result<()> {
        // let _lock = self.lock.lock();
        match self.table.get_mut(&key) {
            Some(_) => {
                // node.pinned += 1;
                // node.value = value;
                Err(CacheError::DuplicatedElements)
            }
            None => {
                // if self.usage + value.size() as usize > self.capacity && self.lru.is_empty() {
                //     return Err(CacheError::AllElementsPinned);
                // }

                if self.usage + charge > self.capacity && self.lru.is_empty() {
                    return Err(CacheError::AllElementsPinned);
                }

                self.usage += charge;
                self.table
                    .insert(key.clone(), Node::new(Arc::new(value), 1, NodeState::InUse));
                info!(
                    "insert {:?} to cache; usage: {}, capacity: {}",
                    key, self.usage, self.capacity
                );
                self.in_use.push_back(key);

                while self.usage > self.capacity && !self.lru.is_empty() {
                    let removed_key = self.lru.pop_front().unwrap();
                    let _removed = self.table.remove(&removed_key).unwrap();
                    // self.usage -= removed.value.size() as usize;
                    self.usage -= charge;
                }
                Ok(())
            }
        }
    }

    pub fn unpin(&mut self, key: &K) -> Result<()> {
        // let _lock = self.lock.lock();
        if let Some(node) = self.table.get_mut(key) {
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

    pub fn evict(&mut self, key: &K, charge: usize) -> Result<()>
    where
        K: Debug,
    {
        if let Some(node) = self.table.remove(key) {
            match node.handle {
                NodeState::InUse => {
                    let idx = self.in_use.iter().position(|k| k == key).unwrap();
                    self.in_use.remove(idx);
                }
                NodeState::Lru => {
                    let idx = self.lru.iter().position(|k| k == key).unwrap();
                    self.lru.remove(idx);
                }
            }
            self.usage -= charge;
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
