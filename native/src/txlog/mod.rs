// txlog/ - Indextables transaction log (Rust implementation)
//
// Drop-in replacement for the Scala TransactionLog in search_test.
// Implements protocol v4 (Avro state format), backward-compatible with existing txlogs.
//
// Rust crate module: indextables_native::txlog (future) / tantivy4java::txlog (current)
// Java package: io.indextables.jni.txlog

pub mod actions;
pub mod arrow_ffi;
pub mod arrow_ffi_import;
pub mod avro;
pub mod cache;
pub mod compression;
pub mod distributed;
pub mod error;
pub mod garbage_collection;
pub mod jni;
pub mod list_files;
pub mod log_replay;
pub mod metrics;
pub mod partition_pruning;
pub mod purge;
pub mod schema_dedup;
pub mod serialization;
pub mod storage;
pub mod streaming;
pub mod tombstone_distributor;
pub mod version_file;

#[cfg(test)]
mod arrow_ffi_tests;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod protocol_regression_tests;
#[cfg(test)]
mod purge_tests;
#[cfg(test)]
mod parallel_bench_tests;
