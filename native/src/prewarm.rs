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
use quickwit_storage::Storage;
use tantivy::schema::FieldType;
use tantivy::HasLen;

use crate::debug_println;
use crate::disk_cache::L2DiskCache;
use crate::global_cache::{get_global_disk_cache, clear_global_l1_cache};
use crate::split_searcher::CachedSearcherContext;
use crate::utils::with_arc_safe;

/// Clear global L1 ByteRangeCache after prewarm to prevent memory exhaustion.
///
/// This can be called after prewarm operations complete to release memory.
/// The prewarmed data is safely persisted in L2 disk cache, so clearing L1 is safe.
/// Subsequent queries will populate L1 on-demand from L2 (disk cache hits).
///
/// Note: Since L1 is now bounded with auto-eviction (256MB default), this is optional
/// but still useful for reclaiming memory immediately after large prewarm operations.
pub fn clear_l1_cache_after_prewarm(_searcher_ptr: jlong) {
    // Clear the global shared L1 cache (no longer per-searcher)
    clear_global_l1_cache();
    debug_println!("🧹 PREWARM_L1_CLEAR: Global L1 ByteRangeCache cleared after prewarm");
}

/// Parse split URI into (storage_loc, split_id) for L2 disk cache keys.
///
/// **CRITICAL**: These keys MUST match how `StorageWithPersistentCache` constructs its keys,
/// otherwise prewarm data will never be found during queries!
///
/// StorageWithPersistentCache uses:
/// - `storage_loc` = full split URI (e.g., `"s3://bucket/path/abc123.split"`)
/// - `split_id` = filename without `.split` extension (e.g., `"abc123"`)
fn parse_split_uri(split_uri: &str) -> (String, String) {
    // storage_loc = full URI (matches StorageWithPersistentCache constructor)
    let storage_loc = split_uri.to_string();

    // split_id = filename without .split extension (matches split_id_for_cache logic)
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };

    let split_id = if split_filename.ends_with(".split") {
        split_filename[..split_filename.len() - 6].to_string()
    } else {
        split_filename.to_string()
    };

    debug_println!("🔑 PREWARM_CACHE_KEY: storage_loc='{}', split_id='{}'", storage_loc, split_id);
    (storage_loc, split_id)
}

/// Helper function to cache all files with a given extension from the bundle.
///
/// This implements DISK-ONLY caching as the primary prewarm strategy.
/// Data is written ONLY to L2DiskCache - bypassing L1 memory cache.
///
/// # Memory-Safe Prewarm Design
///
/// The L1 ByteRangeCache now uses bounded capacity with automatic eviction when full.
/// However, for large prewarm operations, we still prefer writing directly to L2DiskCache:
/// 1. Populate persistent disk cache for fast reads across JVM restarts
/// 2. Avoid L1 churn during bulk prewarm operations
/// 3. Allow subsequent searches to find data in L2 and populate L1 on-demand
///
/// Note: L1 is now bounded (default 256MB, configurable via TANTIVY4JAVA_L1_CACHE_MB)
/// and will auto-evict when full, so OOM is no longer a risk. However, direct L2
/// writes are still preferred for prewarm efficiency.
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm functionality.
///
/// # Arguments
/// * `extension` - File extension to match (e.g., "term", "store", "fast")
/// * `storage` - Storage backend for reading files
/// * `split_uri` - Full URI of the split file (e.g., "s3://bucket/path/split.split")
/// * `bundle_offsets` - Map of inner paths to bundle byte ranges
/// * `disk_cache` - L2 disk cache to populate (if None, prewarm is skipped)
///
/// # Returns
/// Tuple of (success_count, failure_count)
async fn cache_files_by_extension(
    extension: &str,
    storage: Arc<dyn Storage>,
    split_uri: &str,
    bundle_offsets: &HashMap<PathBuf, Range<u64>>,
    disk_cache: Option<Arc<L2DiskCache>>,
) -> (usize, usize) {
    // MEMORY SAFETY: If no disk cache, skip prewarm entirely to prevent OOM
    let disk_cache = match disk_cache {
        Some(dc) => dc,
        None => {
            debug_println!("⚠️ PREWARM_SKIP: No disk cache configured - skipping .{} prewarm to prevent OOM", extension);
            debug_println!("   Configure TieredCacheConfig.withDiskCachePath() to enable prewarm");
            return (0, 0);
        }
    };
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

    debug_println!("🔥 PREWARM_DISK: Found {} .{} files to cache (L2 disk-only)",
                   files.len(), extension);

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    // Extract component name from split_path for cache key consistency
    // StorageWithPersistentCache uses extract_component(path) which returns the filename
    let storage_component = split_path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| if s.starts_with('.') { s[1..].to_string() } else { s.to_string() })
        .unwrap_or_else(|| "unknown".to_string());

    for (inner_path, bundle_range) in files {
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;

        // CRITICAL FIX: Use the SAME cache key format as StorageWithPersistentCache!
        // StorageWithPersistentCache caches with:
        //   - component = split filename (e.g., "mysplit.split")
        //   - range = bundle_start..bundle_end (absolute offset in split file)
        // NOT with inner path and relative range, which was causing cache key mismatches!
        let cache_range = bundle_start as u64..bundle_end as u64;

        debug_println!("🔑 PREWARM_CACHE_KEY: storage_loc='{}', split_id='{}', component='{}', range={:?}",
                      storage_loc, split_id, storage_component, cache_range);

        // Check if data is already in L2 disk cache - skip download if so
        // Uses the same key format as StorageWithPersistentCache
        if disk_cache.get(&storage_loc, &split_id, &storage_component, Some(cache_range.clone())).is_some() {
            debug_println!("⏭️ PREWARM_DISK: Skipping '{}' ({} bytes) - already in disk cache (range {:?})",
                          inner_path.display(), file_length, cache_range);
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();

        debug_println!("🔥 PREWARM_DISK: Queuing '{}' ({} bytes) for download (range {:?})",
                      inner_path.display(), file_length, cache_range);

        warm_up_futures.push(async move {
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(_bytes) => {
                    // NOTE: Download recording AND caching is done by StorageWithPersistentCache::get_slice()
                    // We don't cache here because StorageWithPersistentCache already handles it with
                    // the correct cache key format (component=split_filename, range=bundle_range)
                    debug_println!("✅ PREWARM_DISK: Downloaded '{}' ({} bytes) - cached by storage layer",
                                   inner_path.display(), _bytes.len());
                    Ok(())
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_DISK: Failed '{}': {}", inner_path.display(), e);
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

    // CRITICAL: Flush disk cache to ensure all async writes complete before returning.
    // This prevents race conditions where subsequent cache lookups miss data that's
    // still being written by the background writer thread.
    if downloaded_count > 0 {
        debug_println!("🔄 PREWARM_FLUSH: Flushing disk cache to ensure {} downloads are persisted", downloaded_count);
        disk_cache.flush_async().await;
        debug_println!("✅ PREWARM_FLUSH: Disk cache flush complete");
    }

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
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_term_dictionaries_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_TERM: Starting term dictionary warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .term files (disk-only, no L1)
    let (success_count, failure_count) = cache_files_by_extension(
        "term",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
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
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_postings_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_POSTINGS: Starting postings warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .idx files (term-to-posting list index) - disk-only
    let (idx_success, idx_failure) = cache_files_by_extension(
        "idx",
        storage.clone(),
        &split_uri,
        &bundle_offsets,
        disk_cache.clone(),
    ).await;

    // Cache all .pos files (positions data) - disk-only
    let (pos_success, pos_failure) = cache_files_by_extension(
        "pos",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
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
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_fieldnorms_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .fieldnorm files - disk-only
    let (success_count, failure_count) = cache_files_by_extension(
        "fieldnorm",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
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
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_fastfields_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .fast files - disk-only
    let (success_count, failure_count) = cache_files_by_extension(
        "fast",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
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

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
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

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
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

/// Field-specific positions prewarming
///
/// This warms up the position data (.pos files) for the specified fields.
/// Positions are needed for phrase queries and are separate from postings.
pub async fn prewarm_positions_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDS: Starting positions warmup for fields: {:?}", field_names);

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find all indexed fields that have positions (text fields with positions enabled)
    let indexed_fields: HashSet<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if let Some(indexing_options) = text_options.get_indexing_options() {
                        // Only fields with positions enabled
                        if indexing_options.index_option().has_positions() {
                            Some(field)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                },
                FieldType::JsonObject(json_options) => {
                    if let Some(text_indexing) = json_options.get_text_indexing_options() {
                        if text_indexing.index_option().has_positions() {
                            Some(field)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                },
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching fields with positions found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FIELDS: Warming positions for {} fields", indexed_fields.len());

    let mut warm_up_futures = Vec::new();
    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    // true = include positions data
                    warm_up_futures.push(async move {
                        inverted_index.warm_postings_full(true).await
                    });
                },
                Err(e) => {
                    debug_println!("⚠️ PREWARM_FIELDS: Failed to get inverted index: {}", e);
                }
            }
        }
    }

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!("✅ PREWARM_FIELDS: Positions warmup complete - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All field-specific positions warmup operations failed"))
    }
}

/// Field-specific fieldnorms prewarming
///
/// Uses the same pattern as Quickwit's `warm_up_fieldnorms` function:
/// 1. Get inner file handle using `fieldnorm_readers.get_inner_file().open_read(field)`
/// 2. Read the actual bytes using `file_handle.read_bytes_async().await`
///
/// This ensures the fieldnorm data is actually read into cache.
pub async fn prewarm_fieldnorms_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup for fields: {:?}", field_names);

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();

    // Find text fields that match the filter (only text fields have fieldnorms)
    let matching_fields: Vec<tantivy::schema::Field> = schema.fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            if matches!(field_entry.field_type(), FieldType::Str(_)) {
                debug_println!("🔥 PREWARM_FIELDNORMS: Found matching text field: {}", field_entry.name());
                Some(field)
            } else {
                debug_println!("⚠️ PREWARM_FIELDNORMS: Field '{}' is not a text field (no fieldnorms)", field_entry.name());
                None
            }
        })
        .collect();

    if matching_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDNORMS: No matching text fields found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FIELDNORMS: Warming fieldnorms for {} fields across {} segments",
                   matching_fields.len(), searcher.segment_readers().len());

    let mut warm_up_futures = Vec::new();

    for field in &matching_fields {
        for segment_reader in searcher.segment_readers() {
            let fieldnorm_readers = segment_reader.fieldnorms_readers();
            // Use Quickwit's pattern: get_inner_file().open_read(field) + read_bytes_async()
            if let Some(file_handle) = fieldnorm_readers.get_inner_file().open_read(*field) {
                warm_up_futures.push(async move {
                    let bytes = file_handle.read_bytes_async().await
                        .map_err(|e| anyhow::anyhow!("Failed to read fieldnorm bytes: {}", e))?;
                    debug_println!("🔥 PREWARM_FIELDNORMS: Read {} bytes for fieldnorm", bytes.len());
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    // Log any failures
    for result in &results {
        if let Err(e) = result {
            debug_println!("⚠️ PREWARM_FIELDNORMS: Warmup error: {}", e);
        }
    }

    debug_println!("✅ PREWARM_FIELDNORMS: Fieldnorms warmup complete - {} success, {} failures",
                   success_count, failure_count);

    // Fieldnorms are optional, so don't fail if none found
    Ok(())
}

/// Field-specific fast fields prewarming
///
/// Uses the same pattern as Quickwit's `warm_up_fastfield` function:
/// 1. Get column handles using `list_dynamic_column_handles(field_name)`
/// 2. Read the actual bytes using `col.file_slice().read_bytes_async().await`
///
/// This ensures the column data is actually read into cache, not just creating
/// lazy column readers.
pub async fn prewarm_fastfields_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup for fields: {:?}", field_names);

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();

    // Find fast fields that match the filter
    let matching_fields: Vec<String> = schema.fields()
        .filter_map(|(_field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            if field_entry.is_fast() {
                debug_println!("🔥 PREWARM_FASTFIELDS: Found matching fast field: {}", field_entry.name());
                Some(field_entry.name().to_string())
            } else {
                debug_println!("⚠️ PREWARM_FASTFIELDS: Field '{}' is not a fast field", field_entry.name());
                None
            }
        })
        .collect();

    if matching_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FASTFIELDS: No matching fast fields found");
        return Ok(());
    }

    debug_println!("🔥 PREWARM_FASTFIELDS: Warming {} fast fields across {} segments",
                   matching_fields.len(), searcher.segment_readers().len());

    let mut warm_up_futures = Vec::new();

    for (seg_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
        let fast_field_reader = segment_reader.fast_fields();

        for field_name in &matching_fields {
            let fast_field_reader = fast_field_reader.clone();
            let field_name = field_name.clone();
            let seg_idx = seg_idx;

            // Use Quickwit's pattern: list_dynamic_column_handles + read_bytes_async
            warm_up_futures.push(async move {
                // Get column handles for this field
                let columns = fast_field_reader
                    .list_dynamic_column_handles(&field_name)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to list column handles for '{}': {}", field_name, e))?;

                debug_println!("🔥 PREWARM_FASTFIELDS: Field '{}' segment {} has {} column(s)",
                               field_name, seg_idx, columns.len());

                // Read the actual bytes from each column (triggers I/O)
                for (col_idx, col) in columns.iter().enumerate() {
                    let file_slice = col.file_slice();
                    let slice_len = file_slice.len();
                    debug_println!("🔥 PREWARM_FASTFIELDS: Field '{}' seg {} col {} file_slice.len()={} bytes",
                                   field_name, seg_idx, col_idx, slice_len);
                    let bytes = file_slice.read_bytes_async().await
                        .map_err(|e| anyhow::anyhow!("Failed to read bytes for '{}': {}", field_name, e))?;
                    debug_println!("🔥 PREWARM_FASTFIELDS: Field '{}' seg {} col {} read {} bytes",
                                   field_name, seg_idx, col_idx, bytes.len());
                }

                Ok::<(), anyhow::Error>(())
            });
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    // Log any failures
    for result in &results {
        if let Err(e) = result {
            debug_println!("⚠️ PREWARM_FASTFIELDS: Warmup error: {}", e);
        }
    }

    debug_println!("✅ PREWARM_FASTFIELDS: Fast fields warmup complete - {} success, {} failures",
                   success_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All fast field warmup operations failed"))
    }
}

// =============================================================================
// PER-FIELD COMPONENT SIZE CALCULATION
// =============================================================================

/// Calculate per-field component sizes for all fields in the index.
///
/// Returns a map of "field_name.component" -> size in bytes, where component is one of:
/// - "fastfield" - fast field column size (from list_dynamic_column_handles)
/// - "fieldnorm" - field norm size (from fieldnorm_readers)
///
/// Note: Term dictionary, postings, and positions sizes require reading the data
/// and are calculated during prewarm operations. Store is document-level, not field-level.
pub async fn get_per_field_component_sizes(
    searcher_ptr: jlong,
) -> anyhow::Result<std::collections::HashMap<String, u64>> {
    debug_println!("📊 COMPONENT_SIZES: Calculating per-field component sizes");

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();
    let mut field_sizes: HashMap<String, u64> = HashMap::new();

    // Iterate through all fields in the schema
    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();

        // Fast fields - get column sizes using list_dynamic_column_handles
        if field_entry.is_fast() {
            let mut total_fast_size = 0u64;
            for (seg_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let fast_field_reader = segment_reader.fast_fields();
                if let Ok(columns) = fast_field_reader.list_dynamic_column_handles(&field_name).await {
                    debug_println!("📊 COMPONENT_SIZES: {}.fastfield segment {} has {} column(s)",
                                   field_name, seg_idx, columns.len());
                    for (col_idx, col) in columns.iter().enumerate() {
                        // file_slice() returns a FileSlice, HasLen trait provides len()
                        let col_size = col.file_slice().len() as u64;
                        debug_println!("📊 COMPONENT_SIZES: {}.fastfield segment {} column {} size: {} bytes",
                                       field_name, seg_idx, col_idx, col_size);
                        total_fast_size += col_size;
                    }
                }
            }
            if total_fast_size > 0 {
                field_sizes.insert(format!("{}.fastfield", field_name), total_fast_size);
                debug_println!("📊 COMPONENT_SIZES: {}.fastfield = {} bytes (total)", field_name, total_fast_size);
            }
        }

        // Text fields - get fieldnorm, term, postings, and positions sizes
        // Use file_slice.len() to get size WITHOUT reading data (avoids cache pollution)
        match field_entry.field_type() {
            FieldType::Str(text_options) => {
                // Fieldnorm sizes
                let mut fieldnorm_size = 0u64;
                for segment_reader in searcher.segment_readers() {
                    let fieldnorm_readers = segment_reader.fieldnorms_readers();
                    // open_read returns FileSlice, use .len() to get size without reading
                    if let Some(file_slice) = fieldnorm_readers.get_inner_file().open_read(field) {
                        fieldnorm_size += file_slice.len() as u64;
                    }
                }
                if fieldnorm_size > 0 {
                    field_sizes.insert(format!("{}.fieldnorm", field_name), fieldnorm_size);
                    debug_println!("📊 COMPONENT_SIZES: {}.fieldnorm = {} bytes", field_name, fieldnorm_size);
                }

                // Note: TERM, POSTINGS, POSITIONS sizes require modifications to Tantivy fork
                // to expose file slice lengths. For now, these are validated via download metrics
                // during prewarm (first call > 0, second call == 0).
            },
            FieldType::JsonObject(json_options) => {
                if json_options.get_text_indexing_options().is_some() {
                    let mut fieldnorm_size = 0u64;
                    for segment_reader in searcher.segment_readers() {
                        let fieldnorm_readers = segment_reader.fieldnorms_readers();
                        // open_read returns FileSlice, use .len() to get size without reading
                        if let Some(file_slice) = fieldnorm_readers.get_inner_file().open_read(field) {
                            fieldnorm_size += file_slice.len() as u64;
                        }
                    }
                    if fieldnorm_size > 0 {
                        field_sizes.insert(format!("{}.fieldnorm", field_name), fieldnorm_size);
                    }

                    // Note: TERM, POSTINGS, POSITIONS per-field sizes are not exposed by Tantivy API.
                    // These components exist at the inverted index level but the file slice lengths
                    // are private fields in InvertedIndexReader. Prewarm validation for these components
                    // must use storage download metrics (first call > 0, second call == 0) rather than
                    // exact size matching.
                }
            },
            _ => {
                // Other field types (numeric, etc.) - only fast fields matter
            }
        }
    }

    debug_println!("📊 COMPONENT_SIZES: Calculated {} per-field component sizes", field_sizes.len());
    Ok(field_sizes)
}

// =============================================================================
// ALL-FIELDS PREWARM FUNCTIONS (original implementations)
// =============================================================================

/// Async implementation of store (document storage) prewarming
///
/// This uses DISK-ONLY caching as the primary prewarm strategy:
/// 1. Read entire .store files from the bundle into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// # Memory-Safe Prewarm Design
///
/// The L1 ByteRangeCache is now bounded (256MB default, configurable) with auto-eviction.
/// However, for large prewarm operations, we still prefer writing directly to L2DiskCache:
/// 1. Populate persistent disk cache for fast document retrieval across JVM restarts
/// 2. Avoid L1 churn during bulk prewarm operations
/// 3. Allow subsequent document fetches to find data in L2 and populate L1 on-demand
pub async fn prewarm_store_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_STORE: Starting document store warmup (disk-only caching)");

    // MEMORY SAFETY: Only use L2 disk cache for prewarm to prevent OOM during bulk operations
    // If no disk cache is configured, prewarm is a NO-OP
    let disk_cache = match get_global_disk_cache() {
        Some(dc) => dc,
        None => {
            debug_println!("⚠️ PREWARM_STORE_SKIP: No disk cache configured - skipping .store prewarm to prevent OOM");
            debug_println!("   Configure TieredCacheConfig.withDiskCachePath() to enable prewarm");
            return Ok(());
        }
    };

    // Get storage, bundle file offsets, and split_uri from the context (no L1 byte_range_cache needed)
    let (storage, bundle_offsets, split_uri) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (
            ctx.cached_storage.clone(),
            ctx.bundle_file_offsets.clone(),
            ctx.split_uri.clone(),
        )
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Parse split_uri into storage_loc and split_id for L2 disk cache
    let (storage_loc, split_id) = parse_split_uri(&split_uri);

    // Extract split filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        &split_uri
    };
    let split_path = std::path::PathBuf::from(split_filename);

    debug_println!("🔥 PREWARM_STORE: Split path: {:?} (disk-only=true)", split_path);

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

        // CRITICAL: Use bundle filename as component (not inner path) to match how
        // StorageWithPersistentCache stores data during queries. Both prewarm and
        // queries must use the same cache key format: (storage_loc, split_id, bundle_filename, bundle_range)
        let component = split_filename.to_string();
        let cache_range = bundle_range.start..bundle_range.end;

        // Check if data is already cached in L2 disk cache - skip download if so
        let already_cached = disk_cache.get(&storage_loc, &split_id, &component, Some(cache_range.clone())).is_some();

        if already_cached {
            debug_println!("⏭️ PREWARM_STORE: Skipping '{}' ({} bytes) - already cached",
                          inner_path.display(), file_length);
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();

        debug_println!("🔥 PREWARM_STORE: Queuing warmup for '{}' ({} bytes from split at {}..{})",
                       inner_path.display(), file_length, bundle_start, bundle_end);

        warm_up_futures.push(async move {
            // Read from the split file at the bundle byte range
            // StorageWithPersistentCache.get_slice() handles:
            // 1. Checking disk cache (cache key = bundle filename + bundle range)
            // 2. Fetching from S3 on miss
            // 3. Writing to disk cache on miss
            // No direct disk_cache.put() needed - avoid double writes
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(bytes) => {
                    debug_println!("✅ PREWARM_STORE: Cached '{}' via StorageWithPersistentCache ({} bytes)",
                                   inner_path.display(), bytes.len());
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

    // CRITICAL: Flush disk cache to ensure all async writes complete before returning.
    // This prevents race conditions where subsequent cache lookups miss data that's
    // still being written by the background writer thread.
    if downloaded_count > 0 {
        debug_println!("🔄 PREWARM_STORE_FLUSH: Flushing disk cache to ensure {} downloads are persisted", downloaded_count);
        disk_cache.flush_async().await;
        debug_println!("✅ PREWARM_STORE_FLUSH: Disk cache flush complete");
    }

    debug_println!("✅ PREWARM_STORE: Completed - {} already cached, {} downloaded, {} failures",
                   already_cached_count, downloaded_count, failure_count);

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All store warmup operations failed"))
    }
}
