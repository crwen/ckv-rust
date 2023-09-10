pub mod lsm;
pub mod mem_table;
pub mod sstable;
pub mod utils;
pub mod version;

pub struct Options {
    pub block_size: usize,
}
