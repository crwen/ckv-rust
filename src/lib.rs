pub mod compactor;
pub mod file;
pub mod lsm;
pub mod mem_table;
pub mod sstable;
pub mod utils;
pub mod version;

#[derive(Clone)]
pub struct Options {
    pub block_size: usize,
    pub work_dir: String,
    pub mem_size: usize,
}
