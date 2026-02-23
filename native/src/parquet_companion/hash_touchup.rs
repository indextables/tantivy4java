// hash_touchup.rs - Phase 3: Resolve hash bucket keys back to string values
//
// After a terms aggregation runs on a `_phash_<field>` U64 field, the result
// buckets have numeric hash keys (e.g. 14891908177209451419). This module
// resolves those hashes back to their original string values by:
//
//   1. Collecting unique hash values from the result buckets
//   2. Scanning the native Column<u64> for each segment to find one representative
//      doc_id per hash value (early exit once all hashes are found)
//   3. Looking up the parquet row for each representative doc_id
//   4. Batch-reading the actual string values from parquet
//   5. Building a HashMap<u64 hash, String value> for use in Java object creation

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use quickwit_storage::Storage;

use tantivy::aggregation::agg_result::{AggregationResult, BucketResult};
use tantivy::aggregation::Key;
use tantivy::aggregation::AggregationLimitsGuard;

use crate::debug_println;
use crate::parquet_companion::hash_field_rewriter::HashFieldTouchupInfo;

/// Build a hash → string resolution map for all touchup infos.
///
/// This is called AFTER the aggregation search (in the async search path) and
/// before storing the `EnhancedSearchResult`. It produces a flat map of all
/// unique hash values → their string equivalents, which is stored in
/// `EnhancedSearchResult.hash_resolution_map`.
///
/// The map is keyed by u64 hash value. At Java result construction time,
/// `create_terms_result_object` looks up each U64 bucket key in this map to
/// produce the human-readable string bucket label.
pub async fn build_hash_resolution_map(
    touchup_infos: &[HashFieldTouchupInfo],
    leaf_response: &quickwit_proto::search::LeafSearchResponse,
    rewritten_agg_json: &str,
    // Context fields needed for Column<u64> scan and parquet reads
    cached_index: &tantivy::Index,
    cached_searcher: &Arc<tantivy::Searcher>,
    split_uri: &str,
    cached_storage: &Arc<dyn Storage>,
    parquet_manifest: Option<&Arc<crate::parquet_companion::manifest::ParquetManifest>>,
    parquet_storage: Option<&Arc<dyn Storage>>,
    parquet_metadata_cache: &crate::parquet_companion::transcode::MetadataCache,
    parquet_byte_range_cache: &crate::parquet_companion::cached_reader::ByteRangeCache,
    parquet_file_hash_index: &HashMap<u64, usize>,
    pq_doc_locations: &std::sync::Arc<std::sync::RwLock<Vec<Option<Vec<(u64, u64)>>>>>,
) -> Result<HashMap<u64, String>> {
    if touchup_infos.is_empty() {
        return Ok(HashMap::new());
    }

    let intermediate_bytes = match &leaf_response.intermediate_aggregation_result {
        Some(b) => b,
        None => {
            return Ok(HashMap::new());
        }
    };

    if parquet_manifest.is_none() || parquet_storage.is_none() {
        debug_println!("⚠️ HASH_TOUCHUP: No parquet manifest or storage — skipping touchup");
        return Ok(HashMap::new());
    }

    // Step 1: Finalize the intermediate aggregation result to get bucket keys
    let aggregations: tantivy::aggregation::agg_req::Aggregations =
        serde_json::from_str(rewritten_agg_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse rewritten agg JSON for touchup: {}", e))?;

    let intermediate_results: tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults =
        postcard::from_bytes(intermediate_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize intermediate agg result: {}", e))?;

    let limits = AggregationLimitsGuard::new(Some(50_000_000), Some(65_000));
    let final_results = intermediate_results
        .into_final_result(aggregations, limits)
        .map_err(|e| anyhow::anyhow!("Failed to finalize agg results for touchup: {}", e))?;

    // Step 2: Collect unique hash values per hash field name
    // Multiple touchup infos may share the same hash field, so we deduplicate
    let mut hashes_per_field: HashMap<String, HashSet<u64>> = HashMap::new();
    for touchup in touchup_infos {
        if let Some(agg_result) = final_results.0.get(&touchup.agg_name) {
            let hashes = collect_u64_bucket_keys(agg_result);
            if !hashes.is_empty() {
                hashes_per_field
                    .entry(touchup.hash_field_name.clone())
                    .or_default()
                    .extend(hashes);
            }
        }
    }

    if hashes_per_field.is_empty() {
        return Ok(HashMap::new());
    }

    debug_println!(
        "🔍 HASH_TOUCHUP: Need to resolve {} hash field(s)",
        hashes_per_field.len()
    );

    // Step 3: For each hash field, find representative doc_ids by scanning Column<u64>
    // Uses selective column reads via FastFieldReaders: downloads only the SSTable index
    // + specific column byte ranges instead of the entire .fast file (~100MB+).
    let seg_metas = cached_index
        .searchable_segment_metas()
        .map_err(|e| anyhow::anyhow!("Failed to get segment metas: {}", e))?;

    let mut hash_to_doc: HashMap<u64, (u32, u32)> = HashMap::new();
    let mut all_needed: HashSet<u64> = hashes_per_field.values().flatten().cloned().collect();

    'seg_loop: for (seg_ord, _seg_meta) in seg_metas.iter().enumerate() {
        if all_needed.is_empty() {
            break;
        }

        let segment_reader = cached_searcher.segment_reader(seg_ord as u32);
        let fast_fields = segment_reader.fast_fields();

        for (hash_field_name, needed_hashes) in &hashes_per_field {
            if needed_hashes.is_empty() {
                continue;
            }

            // Selective read: only download this hash column's bytes via async path
            let handles = match fast_fields.list_dynamic_column_handles(hash_field_name).await {
                Ok(h) => h,
                Err(_) => continue,
            };
            let handle = match handles.into_iter().next() {
                Some(h) => h,
                None => continue,
            };
            // read_bytes_async populates CachingDirectory's ByteRangeCache
            let _bytes = match handle.file_slice().read_bytes_async().await {
                Ok(b) => b,
                Err(_) => continue,
            };
            // open_u64_lenient (sync) hits the CachingDirectory cache — no I/O
            let col: tantivy::columnar::Column<u64> = match handle.open_u64_lenient() {
                Ok(Some(c)) => c,
                _ => continue,
            };

            let num_docs = segment_reader.num_docs() + segment_reader.num_deleted_docs();
            for doc_id in 0..num_docs {
                let hash_val = col.values_for_doc(doc_id).next().unwrap_or(0);
                if hash_val != 0 && all_needed.contains(&hash_val) {
                    hash_to_doc.insert(hash_val, (seg_ord as u32, doc_id as u32));
                    all_needed.remove(&hash_val);
                    if all_needed.is_empty() {
                        break 'seg_loop;
                    }
                }
            }
        }
    }

    if hash_to_doc.is_empty() {
        debug_println!("⚠️ HASH_TOUCHUP: No representative docs found for hash values");
        return Ok(HashMap::new());
    }

    debug_println!(
        "🔍 HASH_TOUCHUP: Found representative docs for {}/{} hash values",
        hash_to_doc.len(),
        hashes_per_field.values().map(|s| s.len()).sum::<usize>()
    );

    // Step 4: Ensure pq_doc_locations are loaded for the segments we'll use.
    // Uses selective column reads via FastFieldReaders — same pattern as
    // CachedSearcherContext::ensure_pq_segment_loaded.
    let seg_ords_needed: HashSet<u32> = hash_to_doc.values().map(|(s, _)| *s).collect();
    for &seg_ord in &seg_ords_needed {
        // Already loaded?
        {
            let cache = pq_doc_locations.read().unwrap();
            if cache.get(seg_ord as usize).and_then(|o| o.as_ref()).is_some() {
                continue;
            }
        }

        let segment_reader = cached_searcher.segment_reader(seg_ord);
        let fast_fields = segment_reader.fast_fields();

        let file_hash_field = crate::parquet_companion::indexing::PARQUET_FILE_HASH_FIELD;
        let row_field = crate::parquet_companion::indexing::PARQUET_ROW_IN_FILE_FIELD;

        // Selective reads: SSTable index + 2 column byte ranges only
        let fh_handles = fast_fields.list_dynamic_column_handles(file_hash_field).await
            .map_err(|e| anyhow::anyhow!("Failed to list {} column handles: {}", file_hash_field, e))?;
        let row_handles = fast_fields.list_dynamic_column_handles(row_field).await
            .map_err(|e| anyhow::anyhow!("Failed to list {} column handles: {}", row_field, e))?;

        let fh_handle = match fh_handles.into_iter().next() {
            Some(h) => h,
            None => continue,
        };
        let row_handle = match row_handles.into_iter().next() {
            Some(h) => h,
            None => continue,
        };

        // Async reads populate CachingDirectory cache
        let (_fh_bytes, _row_bytes) = tokio::try_join!(
            fh_handle.file_slice().read_bytes_async(),
            row_handle.file_slice().read_bytes_async(),
        ).map_err(|e| anyhow::anyhow!("Failed to read __pq column bytes: {}", e))?;

        // Sync opens hit CachingDirectory cache — no I/O
        let fh_col: tantivy::columnar::Column<u64> = match fh_handle.open_u64_lenient() {
            Ok(Some(c)) => c,
            _ => continue,
        };
        let row_col: tantivy::columnar::Column<u64> = match row_handle.open_u64_lenient() {
            Ok(Some(c)) => c,
            _ => continue,
        };

        let num_docs = segment_reader.num_docs() + segment_reader.num_deleted_docs();
        let mut seg_locations = Vec::with_capacity(num_docs as usize);
        for doc_id in 0..num_docs {
            let fh = fh_col.values_for_doc(doc_id).next().unwrap_or(0);
            let ri = row_col.values_for_doc(doc_id).next().unwrap_or(0);
            seg_locations.push((fh, ri));
        }

        let mut cache = pq_doc_locations.write().unwrap();
        while cache.len() <= seg_ord as usize {
            cache.push(None);
        }
        if cache[seg_ord as usize].is_none() {
            cache[seg_ord as usize] = Some(seg_locations);
        }
    }

    // Step 5: Look up parquet rows and read string values for each touchup field
    let manifest = parquet_manifest.unwrap();
    let parquet_storage = parquet_storage.unwrap();

    // Group by original field name to build per-field resolution maps
    let mut field_to_hashes: HashMap<String, Vec<u64>> = HashMap::new();
    for touchup in touchup_infos {
        let hashes = hashes_per_field
            .get(&touchup.hash_field_name)
            .cloned()
            .unwrap_or_default();
        field_to_hashes
            .entry(touchup.original_field_name.clone())
            .or_default()
            .extend(hashes.into_iter().filter(|h| hash_to_doc.contains_key(h)));
    }

    let mut all_resolutions: HashMap<u64, String> = HashMap::new();

    for (original_field_name, hashes) in &field_to_hashes {
        for hash_val in hashes {
            if all_resolutions.contains_key(hash_val) {
                continue; // already resolved
            }

            let (seg_ord, doc_id) = match hash_to_doc.get(hash_val) {
                Some(loc) => *loc,
                None => continue,
            };

            // Get parquet location for this doc
            let (file_hash, row_in_file) = {
                let cache = pq_doc_locations.read().unwrap();
                match cache
                    .get(seg_ord as usize)
                    .and_then(|opt| opt.as_ref())
                    .and_then(|seg| seg.get(doc_id as usize))
                    .copied()
                {
                    Some(loc) => loc,
                    None => {
                        // Fall back to manifest-based resolution
                        match crate::parquet_companion::docid_mapping::translate_to_global_row(
                            seg_ord, doc_id, manifest,
                        ) {
                            Ok(global_row) => {
                                match crate::parquet_companion::docid_mapping::locate_row_in_file(
                                    global_row, manifest,
                                ) {
                                    Ok(loc) => {
                                        let fh = crate::parquet_companion::indexing::hash_parquet_path(
                                            &manifest.parquet_files[loc.file_idx].relative_path,
                                        );
                                        (fh, loc.row_in_file as u64)
                                    }
                                    Err(e) => {
                                        debug_println!("⚠️ HASH_TOUCHUP: Failed to locate row: {}", e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                debug_println!("⚠️ HASH_TOUCHUP: Failed to translate row: {}", e);
                                continue;
                            }
                        }
                    }
                }
            };

            let file_idx = match parquet_file_hash_index.get(&file_hash) {
                Some(idx) => *idx,
                None => continue,
            };

            let fields_to_read = vec![original_field_name.clone()];
            match crate::parquet_companion::doc_retrieval::retrieve_document_by_location(
                manifest,
                file_idx,
                row_in_file,
                Some(&fields_to_read),
                parquet_storage,
                Some(parquet_metadata_cache),
                Some(parquet_byte_range_cache),
                None,
            )
            .await
            {
                Ok(doc_map) => {
                    if let Some(val) = doc_map.get(original_field_name) {
                        if let Some(s) = val.as_str() {
                            all_resolutions.insert(*hash_val, s.to_string());
                        }
                    }
                }
                Err(e) => {
                    debug_println!("⚠️ HASH_TOUCHUP: Failed to read parquet cell: {}", e);
                }
            }
        }
    }

    debug_println!(
        "✅ HASH_TOUCHUP: Resolved {}/{} hash values to strings",
        all_resolutions.len(),
        hash_to_doc.len()
    );

    Ok(all_resolutions)
}

/// Collect all U64 bucket keys from a top-level aggregation result.
fn collect_u64_bucket_keys(result: &AggregationResult) -> HashSet<u64> {
    let mut hashes = HashSet::new();
    if let AggregationResult::BucketResult(BucketResult::Terms { buckets, .. }) = result {
        for bucket in buckets {
            if let Key::U64(h) = &bucket.key {
                if *h != 0 {
                    hashes.insert(*h);
                }
            }
        }
    }
    hashes
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::aggregation::agg_result::{AggregationResults, BucketEntry};

    #[test]
    fn test_collect_u64_bucket_keys() {
        // Create a TermsResult with U64 bucket keys using struct variant syntax
        let result = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![
                BucketEntry {
                    key: Key::U64(12345),
                    doc_count: 42,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                },
                BucketEntry {
                    key: Key::U64(67890),
                    doc_count: 17,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                },
                BucketEntry {
                    key: Key::U64(0), // zero = null sentinel, should be skipped
                    doc_count: 5,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                },
            ],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });
        let keys = collect_u64_bucket_keys(&result);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&12345));
        assert!(keys.contains(&67890));
        assert!(!keys.contains(&0), "zero sentinel should be excluded");
    }
}
