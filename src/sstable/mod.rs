mod block;
mod block_builder;
mod merge;
mod table;
mod table_builder;

pub use block::Block;
pub use merge::*;
pub use table::*;
pub use table_builder::*;

pub type Result<T> = anyhow::Result<T, TableError>;

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum TableError {
    #[error("fail to decode block handler")]
    DecodeBlockHandlerError,
    #[error("fail to decode block")]
    DecodeBlockError,
    #[error("fail to decode table")]
    DecodeTableError,
}
