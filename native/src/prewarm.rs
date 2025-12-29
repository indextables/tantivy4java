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
use std::path::PathBuf;
use std::collections::HashMap;
use std::ops::Range;

use jni::sys::jlong;
use quickwit_storage::{ByteRangeCache, Storage};
use tantivy::schema::FieldType;

use crate::debug_println;
use crate::disk_cache::L2DiskCache;
use crate::global_cache::get_global_disk_cache;
use crate::split_searcher_replacement::CachedSearcherContext;
use crate::utils::with_arc_safe;

/// Parse a split URI into (storage_loc, split_id) for L2 disk cache operations.
///
/// Examples:
/// - "s3://bucket/path/to/splits/mysplit.split" -> ("s3://bucket/path/to/splits", "mysplit.split")
/// - "/local/path/mysplit.split" -> ("/local/path", "mysplit.split")
/// - "file:///path/mysplit.split" -> ("file:///path", "mysplit.split")
fn parse_split_uri(split_uri: &str) -> (String, String) {
    if let Some(last_slash_pos) = split_uri.rfind('/') {
        let storage_loc = split_uri[..last_slash_pos].to_string();
        let split_id = split_uri[last_slash_pos + 1..].to_string();
        (storage_loc, split_id)
    } else {
        // No slash found - use empty string for storage_loc
        ("".to_string(), split_uri.to_string())
    }
}

/// Helper function to cache all files with a given extension from the bundle.
///
/// This implements DISK-ONLY caching to prevent memory exhaustion during prewarm.
/// Data is written directly to L2DiskCache, bypassing the L1 memory cache.
///
/// # Memory-Safe Prewarm Design
///
/// The L1 ByteRangeCache uses `with_infinite_capacity()` and never evicts entries.
/// When prewarming many segments, writing to L1 would eventually exhaust RAM.
/// By writing directly to L2DiskCache, we:
/// 1. Populate persistent disk cache for fast reads
/// 2. Avoid memory exhaustion from unbounded L1 growth
/// 3. Allow subsequent searches to find data in L2 and populate L1 on-demand
///
/// # Arguments
/// * `extension` - File extension to match (e.g., "term", "store", "fast")
/// * `storage` - Storage backend for reading files
/// * `split_uri` - Full URI of the split file (e.g., "s3://bucket/path/split.split")
/// * `bundle_offsets` - Map of inner paths to bundle byte ranges
/// * `disk_cache` - L2 disk cache to populate (if None, falls back to L1)
/// * `byte_range_cache` - L1 memory cache (fallback if disk_cache is None)
///
/// # Returns
/// Tuple of (success_count, failure_count)
async fn cache_files_by_extension(
    extension: &str,
    storage: Arc<dyn Storage>,
    split_uri: &str,
    bundle_offsets: &HashMap<PathBuf, Range<u64>>,
    disk_cache: Option<Arc<L2DiskCache>>,
    byte_range_cache: Option<ByteRangeCache>,
) -> (usize, usize) {
    // Find all files with the given extension
    let files: Vec<_> = bundle_offsets.iter()
        .filter(|(path, _)| {
            path.extension()
                .map(|ext| ext == extension)
                .unwrap_or(false)
        })
        .collect();

    if files.is_empty() {
        debug_println!("⚠️ PREWARM_CACHE: No .{} files found in bundle", extension);
        return (0, 0);
    }

    // Parse split_uri into storage_loc and split_id for L2 disk cache
    let (storage_loc, split_id) = parse_split_uri(split_uri);

    // Extract just the filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };
    let split_path = PathBuf::from(split_filename);

    let use_disk_cache = disk_cache.is_some();
    debug_println!("🔥 PREWARM_CACHE: Found {} .{} files to cache (disk-only={})",
                   files.len(), extension, use_disk_cache);

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    for (inner_path, bundle_range) in files {
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;
        let inner_range = 0..file_length;
        let component = inner_path.to_string_lossy().to_string();

        // Check if data is already cached - skip download if so
        // Prefer checking L2 disk cache (persistent), then fall back to L1
        let already_cached = if let Some(ref dc) = disk_cache {
            dc.get(&storage_loc, &split_id, &component, Some(0..file_length as u64)).is_some()
        } else if let Some(ref brc) = byte_range_cache {
            brc.get_slice(&inner_path, inner_range.clone()).is_some()
        } else {
            false
        };

        if already_cached {
            debug_println!("⏭️ PREWARM_CACHE: Skipping '{}' ({} bytes) - already cached",
                          inner_path.display(), file_length);
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();
        let disk_cache = disk_cache.clone();
        let byte_range_cache = byte_range_cache.clone();
        let storage_loc = storage_loc.clone();
        let split_id = split_id.clone();
        let component = component.clone();

        debug_println!("🔥 PREWARM_CACHE: Queuing '{}' ({} bytes)", inner_path.display(), file_length);

        warm_up_futures.push(async move {
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(bytes) => {
                    // Write to L2 disk cache (preferred) or L1 memory cache (fallback)
                    if let Some(dc) = disk_cache {
                        // DISK-ONLY MODE: Write directly to L2 disk cache
                        // This prevents memory exhaustion during bulk prewarm operations
                        let byte_range = Some(0..file_length as u64);
                        dc.put(&storage_loc, &split_id, &component, byte_range, bytes.as_slice());
                        debug_println!("✅ PREWARM_DISK: Cached '{}' to L2 disk cache (0..{} bytes)",
                                       component, file_length);
                    } else if let Some(brc) = byte_range_cache {
                        // FALLBACK: Write to L1 memory cache if no disk cache
                        let inner_range = 0..file_length;
                        brc.put_slice(inner_path.clone(), inner_range, bytes.clone());
                        debug_println!("✅ PREWARM_MEM: Cached '{}' to L1 memory cache (0..{} bytes)",
                                       inner_path.display(), file_length);
                    }
                    Ok(())
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_CACHE: Failed '{}': {}", inner_path.display(), e);
                    Err(e)
                }
            }
        });
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let downloaded_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - downloaded_count;

    // Success = already cached + newly downloaded
    let success_count = already_cached_count + downloaded_count;

    debug_println!("🔥 PREWARM_CACHE: .{} files - {} already cached, {} downloaded, {} failed",
                   extension, already_cached_count, downloaded_count, failure_count);

    (success_count, failure_count)
}

/// Async implementation of term dictionary prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .term files from the bundle into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// This approach writes directly to L2 disk cache, bypassing the unbounded
/// L1 memory cache to prevent RAM exhaustion during bulk prewarm operations.
pub async fn prewarm_term_dictionaries_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_TERM: Starting term dictionary warmup (disk-only caching)");

    // Get storage, bundle file offsets, split_uri, and byte_range_cache from the context
    let (storage, bundle_offsets, split_uri, byte_range_cache) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
            ctx.byte_range_cache.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Prefer L2 disk cache (memory-safe), fall back to L1 if not available
    let disk_cache = get_global_disk_cache();

    // Cache all .term files
    let (success_count, failure_count) = cache_files_by_extension(
        "term",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
        byte_range_cache,
    ).await;

    debug_println!("✅ PREWARM_TERM: Term dictionary warmup complete - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All term dictionary warmup operations failed"))
    }
}

/// Async implementation of postings prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .idx files (term-to-posting list index) into L2 disk cache
/// 2. Read entire .pos files (positions data) into L2 disk cache
/// 3. Cache with component path and byte range (0..file_length)
/// 4. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// This approach writes directly to L2 disk cache, bypassing the unbounded
/// L1 memory cache to prevent RAM exhaustion during bulk prewarm operations.
pub async fn prewarm_postings_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_POSTINGS: Starting postings warmup (disk-only caching)");

    // Get storage, bundle file offsets, split_uri, and byte_range_cache from the context
    let (storage, bundle_offsets, split_uri, byte_range_cache) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
            ctx.byte_range_cache.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Prefer L2 disk cache (memory-safe), fall back to L1 if not available
    let disk_cache = get_global_disk_cache();

    // Cache all .idx files (term-to-posting list index)
    let (idx_success, idx_failure) = cache_files_by_extension(
        "idx",
        storage.clone(),
        &split_uri,
        &bundle_offsets,
        disk_cache.clone(),
        byte_range_cache.clone(),
    ).await;

    // Cache all .pos files (positions data)
    let (pos_success, pos_failure) = cache_files_by_extension(
        "pos",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
        byte_range_cache,
    ).await;

    let total_success = idx_success + pos_success;
    let total_failure = idx_failure + pos_failure;

    debug_println!("✅ PREWARM_POSTINGS: Completed - {} success ({} idx, {} pos), {} failures",
                   total_success, idx_success, pos_success, total_failure);

    if total_success > 0 || total_failure == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All postings warmup operations failed"))
    }
}

/// Async implementation of field norms prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .fieldnorm files into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// This approach writes directly to L2 disk cache, bypassing the unbounded
/// L1 memory cache to prevent RAM exhaustion during bulk prewarm operations.
pub async fn prewarm_fieldnorms_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup (disk-only caching)");

    // Get storage, bundle file offsets, split_uri, and byte_range_cache from the context
    let (storage, bundle_offsets, split_uri, byte_range_cache) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
            ctx.byte_range_cache.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Prefer L2 disk cache (memory-safe), fall back to L1 if not available
    let disk_cache = get_global_disk_cache();

    // Cache all .fieldnorm files
    let (success_count, failure_count) = cache_files_by_extension(
        "fieldnorm",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
        byte_range_cache,
    ).await;

    debug_println!("✅ PREWARM_FIELDNORMS: Completed - {} success, {} failures",
                   success_count, failure_count);

    // Fieldnorms are optional, so don't fail if none found
    Ok(())
}

/// Async implementation of fast fields prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .fast files into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// This approach writes directly to L2 disk cache, bypassing the unbounded
/// L1 memory cache to prevent RAM exhaustion during bulk prewarm operations.
pub async fn prewarm_fastfields_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup (disk-only caching)");

    // Get storage, bundle file offsets, split_uri, and byte_range_cache from the context
    let (storage, bundle_offsets, split_uri, byte_range_cache) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
            ctx.byte_range_cache.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Prefer L2 disk cache (memory-safe), fall back to L1 if not available
    let disk_cache = get_global_disk_cache();

    // Cache all .fast files
    let (success_count, failure_count) = cache_files_by_extension(
        "fast",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
        byte_range_cache,
    ).await;

    debug_println!("✅ PREWARM_FASTFIELDS: Completed - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All fast field warmup operations failed"))
    }
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
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .store files from the bundle into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// # Memory-Safe Prewarm Design
///
/// When prewarming many segments, writing to L1 memory cache would exhaust RAM
/// since ByteRangeCache uses `with_infinite_capacity()` and never evicts.
/// By writing directly to L2DiskCache, we:
/// 1. Populate persistent disk cache for fast document retrieval
/// 2. Avoid memory exhaustion from unbounded L1 growth
/// 3. Allow subsequent document fetches to find data in L2 and populate L1 on-demand
pub async fn prewarm_store_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_STORE: Starting document store warmup (disk-only caching)");

    // Get storage, bundle file offsets, split_uri, and byte_range_cache from the context
    let (storage, bundle_offsets, split_uri, byte_range_cache) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
            ctx.byte_range_cache.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Prefer L2 disk cache (memory-safe), fall back to L1 if not available
    let disk_cache = get_global_disk_cache();

    // Parse split_uri into storage_loc and split_id for L2 disk cache
    let (storage_loc, split_id) = parse_split_uri(&split_uri);

    // Extract split filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        &split_uri
    };
    let split_path = std::path::PathBuf::from(split_filename);

    let use_disk_cache = disk_cache.is_some();
    debug_println!("🔥 PREWARM_STORE: Split path: {:?} (disk-only={})", split_path, use_disk_cache);

    // Find all .store files in the bundle
    let store_files: Vec<_> = bundle_offsets.iter()
        .filter(|(path, _)| {
            path.extension()
                .map(|ext| ext == "store")
                .unwrap_or(false)
        })
        .collect();

    if store_files.is_empty() {
        debug_println!("⚠️ PREWARM_STORE: No .store files found in bundle");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_STORE: Found {} .store files to warm", store_files.len());

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    for (inner_path, bundle_range) in store_files {
        // Bundle range is the absolute byte range within the split file
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;
        let component = inner_path.to_string_lossy().to_string();

        // Check if data is already cached - skip download if so
        // Prefer checking L2 disk cache (persistent), then fall back to L1
        let already_cached = if let Some(ref dc) = disk_cache {
            dc.get(&storage_loc, &split_id, &component, Some(0..file_length as u64)).is_some()
        } else if let Some(ref brc) = byte_range_cache {
            let inner_range = 0..file_length;
            brc.get_slice(&inner_path, inner_range).is_some()
        } else {
            false
        };

        if already_cached {
            debug_println!("⏭️ PREWARM_STORE: Skipping '{}' ({} bytes) - already cached",
                          inner_path.display(), file_length);
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();
        let disk_cache = disk_cache.clone();
        let byte_range_cache = byte_range_cache.clone();
        let storage_loc = storage_loc.clone();
        let split_id = split_id.clone();
        let component = component.clone();

        debug_println!("🔥 PREWARM_STORE: Queuing warmup for '{}' ({} bytes from split at {}..{})",
                       inner_path.display(), file_length, bundle_start, bundle_end);

        warm_up_futures.push(async move {
            // Read from the split file at the bundle byte range
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(bytes) => {
                    // Write to L2 disk cache (preferred) or L1 memory cache (fallback)
                    if let Some(dc) = disk_cache {
                        // DISK-ONLY MODE: Write directly to L2 disk cache
                        // This prevents memory exhaustion during bulk prewarm operations
                        let byte_range = Some(0..file_length as u64);
                        dc.put(&storage_loc, &split_id, &component, byte_range, bytes.as_slice());
                        debug_println!("✅ PREWARM_STORE_DISK: Cached '{}' to L2 disk cache (0..{} bytes)",
                                       component, file_length);
                    } else if let Some(brc) = byte_range_cache {
                        // FALLBACK: Write to L1 memory cache if no disk cache
                        let inner_range = 0..file_length;
                        brc.put_slice(inner_path.clone(), inner_range, bytes.clone());
                        debug_println!("✅ PREWARM_STORE_MEM: Cached '{}' to L1 memory cache (0..{} bytes)",
                                       inner_path.display(), file_length);
                    }
                    Ok(())
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_STORE: Failed to read from split for '{}': {}",
                                   inner_path.display(), e);
                    Err(anyhow::anyhow!("Failed to cache store file: {}", e))
                }
            }
        });
    }

    debug_println!("🔥 PREWARM_STORE: Executing {} warmup operations in parallel", warm_up_futures.len());

    let results = futures::future::join_all(warm_up_futures).await;
    let downloaded_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - downloaded_count;

    // Success = already cached + newly downloaded
    let success_count = already_cached_count + downloaded_count;

    debug_println!("✅ PREWARM_STORE: Completed - {} already cached, {} downloaded, {} failures",
                   already_cached_count, downloaded_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All store warmup operations failed"))
    }
}
