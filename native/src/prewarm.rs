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

use std::collections::HashSet;
use std::sync::Arc;

use jni::sys::jlong;
use tantivy::schema::FieldType;

use crate::debug_println;
use crate::split_searcher_replacement::CachedSearcherContext;
use crate::utils::with_arc_safe;

/// Async implementation of term dictionary prewarming
///
/// This follows the Quickwit pattern from leaf.rs:warm_up_term_dict_fields():
/// 1. Get all indexed text fields from the schema
/// 2. For each segment reader, get the inverted index for each field
/// 3. Call warm_up_dictionary() which fetches the entire FST via get_slice()
/// 4. The disk cache stores the full range, and subsequent sub-range requests
///    are served from the cached data via get_coalesced()
pub async fn prewarm_term_dictionaries_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_IMPL: Starting term dictionary warmup");

    // Get the searcher context
    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find all indexed text fields (fields with term dictionaries)
    let indexed_fields: HashSet<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            match field_entry.field_type() {
                // Text fields are indexed if they have indexing options
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        debug_println!("🔥 PREWARM_IMPL: Found indexed text field: {}", field_entry.name());
                        Some(field)
                    } else {
                        None
                    }
                },
                // JSON fields can also have term dictionaries
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        debug_println!("🔥 PREWARM_IMPL: Found indexed JSON field: {}", field_entry.name());
                        Some(field)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("🔥 PREWARM_IMPL: No indexed text fields found, nothing to warm up");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_IMPL: Found {} indexed fields, warming up term dictionaries for {} segments",
                   indexed_fields.len(), searcher.segment_readers().len());

    // Collect warmup futures for parallel execution
    let mut warm_up_futures = Vec::new();

    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            // Clone the inverted index for async move
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move {
                        let dict = inverted_index.terms();
                        dict.warm_up_dictionary().await
                    });
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_IMPL: Failed to get inverted index for field: {}", e);
                    // Continue with other fields/segments
                }
            }
        }
    }

    debug_println!("🔥 PREWARM_IMPL: Executing {} warmup operations in parallel", warm_up_futures.len());

    // Execute all warmup operations in parallel
    let results = futures::future::join_all(warm_up_futures).await;

    // Count successes and failures
    let mut success_count = 0;
    let mut failure_count = 0;
    for result in results {
        match result {
            Ok(_) => success_count += 1,
            Err(e) => {
                debug_println!("⚠️ PREWARM_IMPL: Warmup operation failed: {}", e);
                failure_count += 1;
            }
        }
    }

    debug_println!("✅ PREWARM_IMPL: Term dictionary warmup complete - {} success, {} failures",
                   success_count, failure_count);

    // Consider it success if at least some warmups succeeded
    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All warmup operations failed"))
    }
}

/// Async implementation of postings prewarming
///
/// This follows the Quickwit pattern from leaf.rs:warm_up_postings():
/// For each indexed field, for each segment, call warm_postings_full()
pub async fn prewarm_postings_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_POSTINGS: Starting postings warmup");

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find all indexed text fields
    let indexed_fields: HashSet<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                },
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("🔥 PREWARM_POSTINGS: No indexed fields found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_POSTINGS: Warming up postings for {} fields", indexed_fields.len());

    let mut warm_up_futures = Vec::new();
    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move {
                        inverted_index.warm_postings_full(false).await
                    });
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_POSTINGS: Failed to get inverted index: {}", e);
                }
            }
        }
    }

    debug_println!("🔥 PREWARM_POSTINGS: Executing {} warmup operations", warm_up_futures.len());

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!("✅ PREWARM_POSTINGS: Completed - {} success, {} failures", success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All postings warmup operations failed"))
    }
}

/// Async implementation of field norms prewarming
///
/// This follows the Quickwit pattern from leaf.rs:warm_up_fieldnorms():
/// For each field with norms, load the fieldnorm data by accessing the fieldnorm reader
pub async fn prewarm_fieldnorms_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup");

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();
    let mut warmed_count = 0;

    // For fieldnorms, we simply access the fieldnorms reader for each indexed text field
    // The access itself triggers loading the data into cache
    for (field, field_entry) in schema.fields() {
        // Only text fields have field norms
        if matches!(field_entry.field_type(), FieldType::Str(_)) {
            for segment_reader in searcher.segment_readers() {
                // Accessing the fieldnorm reader loads the data
                if let Ok(Some(_fieldnorm_reader)) = segment_reader.fieldnorms_readers().get_field(field) {
                    debug_println!("🔥 PREWARM_FIELDNORMS: Warmed fieldnorms for field '{}'", field_entry.name());
                    warmed_count += 1;
                }
            }
        }
    }

    debug_println!("✅ PREWARM_FIELDNORMS: Completed - warmed {} fieldnorm readers", warmed_count);
    Ok(())
}

/// Async implementation of fast fields prewarming
///
/// This warms up fast field data for sorting, filtering, and aggregations.
/// For each segment, we warm up the fast field readers.
pub async fn prewarm_fastfields_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup");

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find all fast fields with their names
    let fast_fields: Vec<(tantivy::schema::Field, String)> = schema.fields()
        .filter_map(|(field, field_entry)| {
            if field_entry.is_fast() {
                debug_println!("🔥 PREWARM_FASTFIELDS: Found fast field: {}", field_entry.name());
                Some((field, field_entry.name().to_string()))
            } else {
                None
            }
        })
        .collect();

    if fast_fields.is_empty() {
        debug_println!("🔥 PREWARM_FASTFIELDS: No fast fields found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FASTFIELDS: Warming up {} fast fields", fast_fields.len());

    let mut warm_up_futures = Vec::new();

    for segment_reader in searcher.segment_readers() {
        let fast_field_reader = segment_reader.fast_fields().clone();
        for (field, field_name) in &fast_fields {
            let field_entry = schema.get_field_entry(*field);
            let fast_field_reader = fast_field_reader.clone();
            let field_name = field_name.clone();

            warm_up_futures.push(async move {
                // Warm up based on field type - methods expect field name as &str
                match field_entry.field_type() {
                    FieldType::U64(_) => {
                        let _ = fast_field_reader.u64(&field_name);
                    },
                    FieldType::I64(_) => {
                        let _ = fast_field_reader.i64(&field_name);
                    },
                    FieldType::F64(_) => {
                        let _ = fast_field_reader.f64(&field_name);
                    },
                    FieldType::Bool(_) => {
                        let _ = fast_field_reader.bool(&field_name);
                    },
                    FieldType::Date(_) => {
                        let _ = fast_field_reader.date(&field_name);
                    },
                    FieldType::Bytes(_) => {
                        let _ = fast_field_reader.bytes(&field_name);
                    },
                    _ => {
                        // Text fast fields require column access
                        // This is more complex, skip for now
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
        }
    }

    debug_println!("🔥 PREWARM_FASTFIELDS: Executing {} warmup operations", warm_up_futures.len());

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!("✅ PREWARM_FASTFIELDS: Completed - {} success, {} failures", success_count, failure_count);

    Ok(())
}

// =============================================================================
// FIELD-SPECIFIC PREWARM FUNCTIONS
// =============================================================================

/// Field-specific term dictionary prewarming
///
/// Only preloads term dictionaries for the specified fields, reducing cache usage
/// and prewarm time for schemas with many fields.
pub async fn prewarm_term_dictionaries_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDS: Starting term dictionary warmup for fields: {:?}", field_names);

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find indexed text fields that match the filter
    let indexed_fields: HashSet<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            // Check if this field is in our filter
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        debug_println!("🔥 PREWARM_FIELDS: Found matching indexed text field: {}", field_entry.name());
                        Some(field)
                    } else {
                        debug_println!("⚠️ PREWARM_FIELDS: Field '{}' is not indexed", field_entry.name());
                        None
                    }
                },
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        debug_println!("🔥 PREWARM_FIELDS: Found matching indexed JSON field: {}", field_entry.name());
                        Some(field)
                    } else {
                        debug_println!("⚠️ PREWARM_FIELDS: JSON field '{}' has no text indexing", field_entry.name());
                        None
                    }
                },
                _ => {
                    debug_println!("⚠️ PREWARM_FIELDS: Field '{}' is not a text/JSON field", field_entry.name());
                    None
                }
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching indexed fields found for: {:?}", field_names);
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FIELDS: Warming term dictionaries for {} fields across {} segments",
                   indexed_fields.len(), searcher.segment_readers().len());

    let mut warm_up_futures = Vec::new();

    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move {
                        let dict = inverted_index.terms();
                        dict.warm_up_dictionary().await
                    });
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_FIELDS: Failed to get inverted index: {}", e);
                }
            }
        }
    }

    debug_println!("🔥 PREWARM_FIELDS: Executing {} warmup operations", warm_up_futures.len());

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!("✅ PREWARM_FIELDS: Term dictionary warmup complete - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All field-specific warmup operations failed"))
    }
}

/// Field-specific postings prewarming
pub async fn prewarm_postings_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDS: Starting postings warmup for fields: {:?}", field_names);

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let indexed_fields: HashSet<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                },
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching indexed fields found for postings");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FIELDS: Warming postings for {} fields", indexed_fields.len());

    let mut warm_up_futures = Vec::new();
    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move {
                        inverted_index.warm_postings_full(false).await
                    });
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_FIELDS: Failed to get inverted index: {}", e);
                }
            }
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!("✅ PREWARM_FIELDS: Postings warmup complete - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All field-specific postings warmup operations failed"))
    }
}

/// Field-specific fieldnorms prewarming
pub async fn prewarm_fieldnorms_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDS: Starting fieldnorms warmup for fields: {:?}", field_names);

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();
    let mut warmed_count = 0;

    for (field, field_entry) in schema.fields() {
        // Only process fields in our filter
        if !field_names.contains(field_entry.name()) {
            continue;
        }

        // Only text fields have field norms
        if matches!(field_entry.field_type(), FieldType::Str(_)) {
            for segment_reader in searcher.segment_readers() {
                if let Ok(Some(_fieldnorm_reader)) = segment_reader.fieldnorms_readers().get_field(field) {
                    debug_println!("🔥 PREWARM_FIELDS: Warmed fieldnorms for field '{}'", field_entry.name());
                    warmed_count += 1;
                }
            }
        } else {
            debug_println!("⚠️ PREWARM_FIELDS: Field '{}' is not a text field (no fieldnorms)", field_entry.name());
        }
    }

    debug_println!("✅ PREWARM_FIELDS: Fieldnorms warmup complete - warmed {} readers", warmed_count);
    Ok(())
}

/// Field-specific fast fields prewarming
pub async fn prewarm_fastfields_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDS: Starting fast fields warmup for fields: {:?}", field_names);

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find fast fields that match the filter
    let fast_fields: Vec<(tantivy::schema::Field, String)> = schema.fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            if field_entry.is_fast() {
                debug_println!("🔥 PREWARM_FIELDS: Found matching fast field: {}", field_entry.name());
                Some((field, field_entry.name().to_string()))
            } else {
                debug_println!("⚠️ PREWARM_FIELDS: Field '{}' is not a fast field", field_entry.name());
                None
            }
        })
        .collect();

    if fast_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching fast fields found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FIELDS: Warming {} fast fields", fast_fields.len());

    let mut warm_up_futures = Vec::new();

    for segment_reader in searcher.segment_readers() {
        let fast_field_reader = segment_reader.fast_fields().clone();
        for (field, field_name) in &fast_fields {
            let field_entry = schema.get_field_entry(*field);
            let fast_field_reader = fast_field_reader.clone();
            let field_name = field_name.clone();

            warm_up_futures.push(async move {
                match field_entry.field_type() {
                    FieldType::U64(_) => { let _ = fast_field_reader.u64(&field_name); },
                    FieldType::I64(_) => { let _ = fast_field_reader.i64(&field_name); },
                    FieldType::F64(_) => { let _ = fast_field_reader.f64(&field_name); },
                    FieldType::Bool(_) => { let _ = fast_field_reader.bool(&field_name); },
                    FieldType::Date(_) => { let _ = fast_field_reader.date(&field_name); },
                    FieldType::Bytes(_) => { let _ = fast_field_reader.bytes(&field_name); },
                    _ => {}
                }
                Ok::<(), anyhow::Error>(())
            });
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();

    debug_println!("✅ PREWARM_FIELDS: Fast fields warmup complete - {} operations", success_count);
    Ok(())
}

// =============================================================================
// ALL-FIELDS PREWARM FUNCTIONS (original implementations)
// =============================================================================

/// Async implementation of store (document storage) prewarming
///
/// This reads the entire .store file for each segment into cache,
/// which eliminates cache misses when retrieving documents.
pub async fn prewarm_store_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_STORE: Starting document store warmup");

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let mut warmed_count = 0;

    // For each segment, read the store file into cache
    for segment_reader in searcher.segment_readers() {
        let segment_id = segment_reader.segment_id().uuid_string();

        debug_println!("🔥 PREWARM_STORE: Warming store for segment '{}'", segment_id);

        // Get the store reader to trigger loading the store data
        // The cache size here is for decompressed blocks, we use a small value
        // since we just want to read the raw bytes
        match segment_reader.get_store_reader(1) {
            Ok(store_reader) => {
                // Access the store to trigger loading
                // We read through all documents to warm the store blocks
                let max_doc = segment_reader.max_doc();

                // Read documents in batches to warm all store blocks
                // This is more efficient than reading all bytes directly
                // because it uses Tantivy's block-based store format
                let batch_size = 100;
                for start_doc in (0..max_doc).step_by(batch_size) {
                    let end_doc = std::cmp::min(start_doc + batch_size as u32, max_doc);
                    // Reading a document warms the store block it's in
                    for doc_id in start_doc..end_doc {
                        // Just access the document to warm the block
                        // We use TantivyDocument as the type parameter
                        let _: Result<tantivy::TantivyDocument, _> = store_reader.get(doc_id);
                    }
                }

                debug_println!("🔥 PREWARM_STORE: Warmed store for segment {} ({} docs)", segment_id, max_doc);
                warmed_count += 1;
            },
            Err(e) => {
                debug_println!("⚠️ PREWARM_STORE: Failed to get store reader for segment {}: {}", segment_id, e);
            }
        }
    }

    debug_println!("✅ PREWARM_STORE: Completed - warmed {} segments", warmed_count);
    Ok(())
}
