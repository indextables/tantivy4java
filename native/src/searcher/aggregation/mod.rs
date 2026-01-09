// aggregation/mod.rs - Aggregation result deserialization and Java object creation
// Refactored into submodules for maintainability

// Some helper functions are currently unused but kept for future use
#![allow(dead_code)]

mod bucket_results;
mod byte_parsing;
mod deserialize;
mod json_helpers;
mod metric_results;
mod sub_aggregations;

// Re-export public API - only functions used by searcher/mod.rs
pub(crate) use deserialize::{deserialize_aggregation_results, find_specific_aggregation_result};
