use bytes::Bytes;

use crate::utils::Entry;

#[derive(Debug, Default)]
pub struct WriteBatch {
    pub(crate) data: Vec<Entry>,
    pub(crate) count: usize,
}

impl WriteBatch {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let e = Entry::new(Bytes::from(key.to_vec()), Bytes::from(value.to_vec()), 0);
        self.data.push(e);
        self.count += 1;
    }

    pub fn delete(&mut self, key: &[u8]) {
        let e = Entry::new(Bytes::from(key.to_vec()), Bytes::from(""), 0);
        self.data.push(e);
        self.count += 1;
    }
}
