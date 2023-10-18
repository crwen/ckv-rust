mod block;
mod block_builder;
pub mod merge;
pub mod table;
pub mod table_builder;

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
