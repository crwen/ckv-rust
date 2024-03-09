use bytes::Bytes;

use crate::utils::{Entry, OP_TYPE_DELETE, OP_TYPE_PUT};

#[derive(Debug, Default)]
pub struct WriteBatch {
    pub(crate) data: Vec<(Entry, u8)>,
    pub(crate) count: usize,
}

impl WriteBatch {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let e = Entry::new(Bytes::from(key.to_vec()), Bytes::from(value.to_vec()), 0);
        self.data.push((e, OP_TYPE_PUT));
        self.count += 1;
    }

    pub fn delete(&mut self, key: &[u8]) {
        let e = Entry::new(Bytes::from(key.to_vec()), Bytes::from(""), 0);
        self.data.push((e, OP_TYPE_DELETE));
        self.count += 1;
    }
}
