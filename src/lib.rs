pub mod cache;
pub mod compactor;
pub mod file;
pub mod lsm;
pub mod mem_table;
pub mod sstable;
pub mod utils;
pub mod version;

#[derive(Clone, Debug)]
pub struct Options {
    pub block_size: usize,
    pub work_dir: String,
    pub mem_size: usize,
    pub cache_size: usize,
}

impl Options {
    pub fn default_opt() -> Options {
        Options {
            block_size: 1 << 12, // 4K
            work_dir: "work_dir".to_string(),
            mem_size: 1 << 12,   // 4K
            cache_size: 1 << 22, // 4M
        }
    }
    pub fn mem_size(&mut self, mem_size: usize) -> Self {
        self.mem_size = mem_size;
        self.clone()
    }

    pub fn block_size(&mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self.clone()
    }

    pub fn cache_size(&mut self, cache_size: usize) -> Self {
        self.cache_size = cache_size;
        self.clone()
    }

    pub fn work_dir(&mut self, work_dir: &str) -> Self {
        self.work_dir = String::from(work_dir);
        self.clone()
    }
}
