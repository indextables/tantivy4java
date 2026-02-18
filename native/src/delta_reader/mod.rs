// delta_reader/mod.rs - Delta Lake table file listing via delta-kernel-rs
//
// Provides a standalone utility to read Delta transaction logs and return
// the list of active parquet files with metadata, using the TANT byte buffer
// protocol for efficient Rust→Java data transfer.

pub mod engine;
pub mod scan;
pub mod serialization;
pub mod jni;
pub mod distributed;

pub use engine::DeltaStorageConfig;
pub use scan::{DeltaFileEntry, DeltaSchemaField, list_delta_files, read_delta_schema};
pub use distributed::{DeltaSnapshotInfo, DeltaLogChanges};
