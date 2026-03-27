// txlog/error.rs - Transaction log error types

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TxLogError {
    #[error("Version conflict: attempted version {attempted}, current is {current}")]
    VersionConflict { attempted: i64, current: i64 },

    #[error("Max retries exceeded ({retries}) writing version {last_conflict}")]
    MaxRetriesExceeded { retries: u32, last_conflict: i64 },

    #[error("Transaction log not initialized at {path}")]
    NotInitialized { path: String },

    #[error("Incompatible protocol: requires reader v{required}, have v4")]
    IncompatibleProtocol { required: u32 },

    #[error("Checkpoint corrupted at version {version}: {detail}")]
    CorruptedCheckpoint { version: i64, detail: String },

    #[error("Storage error: {0}")]
    Storage(anyhow::Error),

    #[error("Avro error: {0}")]
    Avro(String),

    #[error("Serialization error: {0}")]
    Serde(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Conditional write failed: version file already exists at {path}")]
    ConditionalWriteFailed { path: String },
}

impl From<serde_json::Error> for TxLogError {
    fn from(e: serde_json::Error) -> Self {
        TxLogError::Serde(e.to_string())
    }
}

impl From<std::io::Error> for TxLogError {
    fn from(e: std::io::Error) -> Self {
        TxLogError::Io(e.to_string())
    }
}

impl From<apache_avro::Error> for TxLogError {
    fn from(e: apache_avro::Error) -> Self {
        TxLogError::Avro(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TxLogError>;
