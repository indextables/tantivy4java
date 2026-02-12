// parquet_companion/mod.rs - Parquet Companion Mode for minimal Quickwit splits
//
// Enables creation of minimal Quickwit splits that reference external parquet files
// for document storage and optionally fast fields, reducing split size by 80-90%.

pub mod manifest;
pub mod manifest_io;
pub mod parquet_storage;
pub mod cached_reader;
pub mod docid_mapping;
pub mod doc_retrieval;
pub mod validation;
pub mod store_stub;
pub mod schema_derivation;
pub mod indexing;
pub mod augmented_directory;
pub mod transcode;
pub mod statistics;
pub mod name_mapping;
pub mod merge;
pub mod coalescing;
pub mod field_extraction;
pub mod arrow_to_tant;

pub use manifest::{
    ParquetManifest, FastFieldMode, SegmentRowRange, ParquetFileEntry,
    RowGroupEntry, ColumnChunkInfo, ColumnMapping, ParquetStorageConfigMeta,
    SUPPORTED_MANIFEST_VERSION,
};
pub use manifest_io::{serialize_manifest, deserialize_manifest, MANIFEST_FILENAME};
pub use docid_mapping::{translate_to_global_row, locate_row_in_file, group_doc_addresses_by_file};
pub use validation::{FileValidator, MissingFilePolicy};
