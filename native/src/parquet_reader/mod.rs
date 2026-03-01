// parquet_reader/mod.rs - Hive-style partitioned parquet directory reading
//
// Provides distributed primitives for listing files in Hive-style partitioned
// parquet directories, complementing Delta and Iceberg readers.

pub mod distributed;
pub mod serialization;
pub mod jni;

pub use distributed::{ParquetTableInfo, ParquetFileEntry};
