/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Index Component Prewarming Module
//!
//! This module provides async implementations for prewarming various index components
//! to eliminate cache misses during search operations. Follows Quickwit patterns
//! from their leaf.rs warmup implementations.
//!
//! ## Available Components
//!
//! - **TERM**: Term dictionaries (FST) for all indexed text fields
//! - **POSTINGS**: Posting lists for term queries
//! - **FIELDNORM**: Field norm data for scoring
//! - **FASTFIELD**: Fast field data for sorting/filtering
//! - **STORE**: Document storage for retrieval operations
//!
//! ## Usage
//!
//! ```java
//! searcher.preloadComponents(
//!     SplitSearcher.IndexComponent.TERM,
//!     SplitSearcher.IndexComponent.POSTINGS,
//!     SplitSearcher.IndexComponent.FASTFIELD
//! ).join();
//! ```
//!
//! ## Module Structure
//!
//! - `all_fields` - Prewarm entire components (all fields)
//! - `field_specific` - Prewarm specific fields only
//! - `component_sizes` - Calculate per-field component sizes
//! - `cache_extension` - Core file extension caching logic
//! - `helpers` - Utility functions for cache key generation

mod all_fields;
mod cache_extension;
mod component_sizes;
pub(crate) mod field_specific;
mod helpers;

// Re-export public API - all-fields prewarm functions
pub use all_fields::{
    prewarm_fastfields_impl, prewarm_fieldnorms_impl, prewarm_postings_impl, prewarm_store_impl,
    prewarm_term_dictionaries_impl,
};

// Re-export public API - field-specific prewarm functions
pub use field_specific::{
    prewarm_fastfields_for_fields, prewarm_fieldnorms_for_fields, prewarm_positions_for_fields,
    prewarm_postings_for_fields, prewarm_term_dictionaries_for_fields,
};

// Re-export public API - component sizes
pub use component_sizes::get_per_field_component_sizes;

// Re-export public API - helpers (used for manual L1 cache clearing after prewarm)
// Unused internally but kept as public API for optional JNI use
#[allow(unused_imports)]
pub use helpers::clear_l1_cache_after_prewarm;

// Re-export helpers module for crate-internal use (e.g. parquet_companion L2 caching)
pub(crate) use helpers::parse_split_uri;
