// iceberg_reader/mod.rs - Apache Iceberg table reading via iceberg-rust
//
// Provides a standalone utility to read Iceberg table metadata through
// catalog integration (REST, Glue, HMS) and return file listings, schema,
// and snapshot history using the TANT byte buffer protocol for efficient
// Rust→Java data transfer.

pub mod catalog;
pub mod scan;
pub mod serialization;
pub mod jni;
pub mod distributed;

pub use scan::{
    IcebergFileEntry, IcebergSchemaField, IcebergSnapshot,
    list_iceberg_files, read_iceberg_schema, list_iceberg_snapshots,
};
pub use distributed::{IcebergSnapshotInfo, ManifestFileInfo};
