pub mod table_cache;

type Result<T> = anyhow::Result<T, CacheError>;

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum CacheError {
    #[error("All elements in cache are pinned")]
    AllElementsPinned,
    #[error("Not support insert duplicated elements")]
    DuplicatedElements,
    #[error("Unpin element that not pinned")]
    UnpinNonPinned,
}

// pub trait Cache {
//     // fn pin(&self, key: &Key);
//     fn unpin(&self, key: &u64) -> Result<()>;
//     fn get(&self, key: &u64) -> Option<&Table>;
//     fn insert(&mut self, key: u64, value: Table) -> Result<()>;
// }
