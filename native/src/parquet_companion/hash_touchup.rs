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

use tantivy::aggregation::agg_result::{AggregationResult, AggregationResults, BucketEntries, BucketEntry, BucketResult, RangeBucketEntry};
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
    pq_columns: &std::sync::Arc<std::sync::RwLock<Vec<Option<(tantivy::columnar::Column<u64>, tantivy::columnar::Column<u64>)>>>>,
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

    // Step 2: Collect unique hash values per hash field name.
    // Multiple touchup infos may share the same hash field, so we deduplicate.
    // Use recursive collection because nested terms aggs (e.g. Terms inside a
    // DateHistogram) are not in the top-level results map — they're buried
    // inside bucket sub-aggregations. Each parent bucket has its own independent
    // copy of the nested Terms with potentially different hash keys, so we must
    // traverse ALL buckets to collect the full set.
    // Deduplicate by agg_name so we traverse the tree at most once per unique name.
    let mut hashes_by_agg_name: HashMap<&str, HashSet<u64>> = HashMap::new();
    for name in touchup_infos.iter().map(|t| t.agg_name.as_str()) {
        hashes_by_agg_name.entry(name).or_insert_with(|| {
            let mut hashes = HashSet::new();
            collect_all_hashes_recursive(&final_results, name, &mut hashes);
            hashes
        });
    }
    let mut hashes_per_field: HashMap<String, HashSet<u64>> = HashMap::new();
    for touchup in touchup_infos {
        if let Some(hashes) = hashes_by_agg_name.get(touchup.agg_name.as_str()) {
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
    let _t_scan = std::time::Instant::now();
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

    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::PcHashScanFastField, _t_scan.elapsed().as_nanos() as u64);
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

    // Step 4: Ensure __pq Column handles are loaded for the segments we'll use.
    // Uses selective column reads via FastFieldReaders — same pattern as
    // CachedSearcherContext::ensure_pq_segment_loaded.
    // Caches Column<u64> handles for O(1) random access — no iteration needed.
    let _t_load_pq = std::time::Instant::now();
    let seg_ords_needed: HashSet<u32> = hash_to_doc.values().map(|(s, _)| *s).collect();
    for &seg_ord in &seg_ords_needed {
        // Already loaded?
        {
            let cache = pq_columns.read().unwrap();
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

        // Open Column<u64> handles — O(1) random access, no iteration
        let fh_col: tantivy::columnar::Column<u64> = match fh_handle.open_u64_lenient() {
            Ok(Some(c)) => c,
            _ => continue,
        };
        let row_col: tantivy::columnar::Column<u64> = match row_handle.open_u64_lenient() {
            Ok(Some(c)) => c,
            _ => continue,
        };

        let mut cache = pq_columns.write().unwrap();
        while cache.len() <= seg_ord as usize {
            cache.push(None);
        }
        if cache[seg_ord as usize].is_none() {
            cache[seg_ord as usize] = Some((fh_col, row_col));
        }
    }

    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::PcHashLoadPqColumns, _t_load_pq.elapsed().as_nanos() as u64);
    }

    // Step 5: Look up parquet rows and read string values for each touchup field
    let _t_pq_read = std::time::Instant::now();
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

            // Get parquet location for this doc via O(1) column random access
            let (file_hash, row_in_file) = {
                let cache = pq_columns.read().unwrap();
                match cache
                    .get(seg_ord as usize)
                    .and_then(|opt| opt.as_ref())
                    .map(|(fh_col, row_col)| {
                        let fh = fh_col.values_for_doc(doc_id).next().unwrap_or(0);
                        let ri = row_col.values_for_doc(doc_id).next().unwrap_or(0);
                        (fh, ri)
                    })
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

    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::PcHashParquetRead, _t_pq_read.elapsed().as_nanos() as u64);
    }

    debug_println!(
        "✅ HASH_TOUCHUP: Resolved {}/{} hash values to strings",
        all_resolutions.len(),
        hash_to_doc.len()
    );

    Ok(all_resolutions)
}

/// Recursively collect all U64 bucket keys from every instance of the named
/// aggregation across the entire result tree.
///
/// A bucket aggregation (e.g. DateHistogram) with N parent buckets produces N
/// independent copies of each nested sub-aggregation, each potentially containing
/// different hash keys. This function traverses ALL buckets to accumulate the
/// complete set of hashes into `out`.
fn collect_all_hashes_recursive(
    results: &AggregationResults,
    target_name: &str,
    out: &mut HashSet<u64>,
) {
    // Direct lookup at this level
    if let Some(result) = results.0.get(target_name) {
        out.extend(collect_u64_bucket_keys(result));
    }
    // Recurse into every bucket's sub-aggregations
    for (_name, agg_result) in &results.0 {
        if let AggregationResult::BucketResult(bucket) = agg_result {
            match bucket {
                BucketResult::Terms { buckets, .. } => {
                    for b in buckets {
                        collect_all_hashes_recursive(&b.sub_aggregation, target_name, out);
                    }
                }
                BucketResult::Histogram { buckets } => {
                    recurse_bucket_entries(buckets, |b| &b.sub_aggregation, target_name, out);
                }
                BucketResult::Range { buckets } => {
                    recurse_bucket_entries(buckets, |b| &b.sub_aggregation, target_name, out);
                }
                BucketResult::Filter(_) => {}
                BucketResult::Composite { .. } => {}
            }
        }
    }
}

/// Recurse into sub-aggregations of each entry in a `BucketEntries` (Vec or HashMap).
fn recurse_bucket_entries<T>(
    entries: &BucketEntries<T>,
    get_sub: impl Fn(&T) -> &AggregationResults,
    target_name: &str,
    out: &mut HashSet<u64>,
) {
    match entries {
        BucketEntries::Vec(vec) => {
            for b in vec {
                collect_all_hashes_recursive(get_sub(b), target_name, out);
            }
        }
        BucketEntries::HashMap(map) => {
            for b in map.values() {
                collect_all_hashes_recursive(get_sub(b), target_name, out);
            }
        }
    }
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
    use tantivy::aggregation::agg_result::{AggregationResults, BucketEntries, BucketEntry, RangeBucketEntry};

    /// Helper to build an AggregationResults with entries (avoids needing FxHashMap directly).
    fn make_agg_results(entries: Vec<(&str, AggregationResult)>) -> AggregationResults {
        let mut results = AggregationResults::default();
        for (name, result) in entries {
            results.0.insert(name.to_string(), result);
        }
        results
    }

    #[test]
    fn test_collect_all_hashes_recursive_top_level() {
        // Terms at top level — direct lookup collects hashes
        let results = make_agg_results(vec![
            ("my_terms", AggregationResult::BucketResult(BucketResult::Terms {
                buckets: vec![BucketEntry {
                    key: Key::U64(111),
                    doc_count: 1,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                }],
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            })),
        ]);
        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "my_terms", &mut hashes);
        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&111));

        let mut empty = HashSet::new();
        collect_all_hashes_recursive(&results, "nonexistent", &mut empty);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_collect_all_hashes_recursive_nested_in_histogram() {
        // Terms nested inside a Histogram bucket — must recurse to find it
        let nested_terms = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![BucketEntry {
                key: Key::U64(42),
                doc_count: 10,
                key_as_string: None,
                sub_aggregation: AggregationResults(Default::default()),
            }],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });

        let sub_aggs = make_agg_results(vec![("nested_terms", nested_terms)]);

        let histogram = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(vec![BucketEntry {
                key: Key::F64(1000.0),
                doc_count: 10,
                key_as_string: Some("2026-01-01T00:00:00Z".to_string()),
                sub_aggregation: sub_aggs,
            }]),
        });

        let results = make_agg_results(vec![("bucket_agg", histogram)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "nested_terms", &mut hashes);
        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&42));
    }

    #[test]
    fn test_collect_all_hashes_recursive_multi_bucket() {
        // DateHistogram with 2 buckets, each containing nested Terms with
        // different hash keys. Bucket 1 has {hash_A, hash_B}, bucket 2 has
        // {hash_B, hash_C}. All 3 unique hashes must be collected.
        let make_nested_terms = |keys: Vec<u64>| -> AggregationResult {
            AggregationResult::BucketResult(BucketResult::Terms {
                buckets: keys.into_iter().map(|k| BucketEntry {
                    key: Key::U64(k),
                    doc_count: 1,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                }).collect(),
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            })
        };

        let sub_aggs_1 = make_agg_results(vec![("nested_terms", make_nested_terms(vec![100, 200]))]);
        let sub_aggs_2 = make_agg_results(vec![("nested_terms", make_nested_terms(vec![200, 300]))]);

        let histogram = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(vec![
                BucketEntry {
                    key: Key::F64(1000.0),
                    doc_count: 5,
                    key_as_string: Some("2026-01-01T00:00:00Z".to_string()),
                    sub_aggregation: sub_aggs_1,
                },
                BucketEntry {
                    key: Key::F64(2000.0),
                    doc_count: 3,
                    key_as_string: Some("2026-02-01T00:00:00Z".to_string()),
                    sub_aggregation: sub_aggs_2,
                },
            ]),
        });

        let results = make_agg_results(vec![("bucket_agg", histogram)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "nested_terms", &mut hashes);

        // Must collect all 3 unique hashes across both parent buckets
        assert_eq!(hashes.len(), 3, "expected 3 unique hashes, got {:?}", hashes);
        assert!(hashes.contains(&100), "hash_A (100) missing");
        assert!(hashes.contains(&200), "hash_B (200) missing");
        assert!(hashes.contains(&300), "hash_C (300) missing");
    }

    #[test]
    fn test_collect_all_hashes_recursive_nested_in_range() {
        // Terms nested inside Range buckets — exercises the Range arm of recurse_bucket_entries
        let make_nested_terms = |keys: Vec<u64>| -> AggregationResult {
            AggregationResult::BucketResult(BucketResult::Terms {
                buckets: keys.into_iter().map(|k| BucketEntry {
                    key: Key::U64(k),
                    doc_count: 1,
                    key_as_string: None,
                    sub_aggregation: AggregationResults(Default::default()),
                }).collect(),
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            })
        };

        let sub_aggs_1 = make_agg_results(vec![("nested_terms", make_nested_terms(vec![500, 600]))]);
        let sub_aggs_2 = make_agg_results(vec![("nested_terms", make_nested_terms(vec![600, 700]))]);

        let range = AggregationResult::BucketResult(BucketResult::Range {
            buckets: BucketEntries::Vec(vec![
                RangeBucketEntry {
                    key: Key::Str("*-100".to_string()),
                    doc_count: 5,
                    sub_aggregation: sub_aggs_1,
                    from: None,
                    to: Some(100.0),
                    from_as_string: None,
                    to_as_string: None,
                },
                RangeBucketEntry {
                    key: Key::Str("100-*".to_string()),
                    doc_count: 3,
                    sub_aggregation: sub_aggs_2,
                    from: Some(100.0),
                    to: None,
                    from_as_string: None,
                    to_as_string: None,
                },
            ]),
        });

        let results = make_agg_results(vec![("range_agg", range)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "nested_terms", &mut hashes);

        assert_eq!(hashes.len(), 3, "expected 3 unique hashes from range buckets, got {:?}", hashes);
        assert!(hashes.contains(&500));
        assert!(hashes.contains(&600));
        assert!(hashes.contains(&700));
    }

    #[test]
    fn test_collect_all_hashes_recursive_hashmap_variant() {
        // Histogram with BucketEntries::HashMap — exercises the HashMap path in recurse_bucket_entries
        let nested_terms = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![BucketEntry {
                key: Key::U64(999),
                doc_count: 1,
                key_as_string: None,
                sub_aggregation: AggregationResults(Default::default()),
            }],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });

        let sub_aggs = make_agg_results(vec![("nested_terms", nested_terms)]);

        let histogram = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::HashMap(
                vec![("1000".to_string(), BucketEntry {
                    key: Key::F64(1000.0),
                    doc_count: 10,
                    key_as_string: None,
                    sub_aggregation: sub_aggs,
                })].into_iter().collect()
            ),
        });

        let results = make_agg_results(vec![("hist", histogram)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "nested_terms", &mut hashes);

        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&999), "hash from HashMap variant bucket missing");
    }

    #[test]
    fn test_collect_all_hashes_recursive_nested_in_terms() {
        // Inner Terms nested inside outer Terms — exercises direct Vec iteration (not recurse_bucket_entries)
        let inner_terms = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![BucketEntry {
                key: Key::U64(777),
                doc_count: 2,
                key_as_string: None,
                sub_aggregation: AggregationResults(Default::default()),
            }],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });

        let sub_aggs = make_agg_results(vec![("inner_terms", inner_terms)]);

        let outer_terms = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![BucketEntry {
                key: Key::Str("parent_bucket".to_string()),
                doc_count: 10,
                key_as_string: None,
                sub_aggregation: sub_aggs,
            }],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });

        let results = make_agg_results(vec![("outer_terms", outer_terms)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "inner_terms", &mut hashes);

        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&777), "hash from Terms-in-Terms nesting missing");
    }

    #[test]
    fn test_collect_all_hashes_recursive_deep_nesting() {
        // 3-level: Histogram → Range → Terms. Exercises all three bucket type arms.
        let leaf_terms = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: vec![
                BucketEntry { key: Key::U64(11), doc_count: 1, key_as_string: None, sub_aggregation: AggregationResults(Default::default()) },
                BucketEntry { key: Key::U64(22), doc_count: 1, key_as_string: None, sub_aggregation: AggregationResults(Default::default()) },
            ],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });

        let range_sub = make_agg_results(vec![("leaf_terms", leaf_terms)]);

        let range = AggregationResult::BucketResult(BucketResult::Range {
            buckets: BucketEntries::Vec(vec![RangeBucketEntry {
                key: Key::Str("0-50".to_string()),
                doc_count: 10,
                sub_aggregation: range_sub,
                from: Some(0.0),
                to: Some(50.0),
                from_as_string: None,
                to_as_string: None,
            }]),
        });

        let hist_sub = make_agg_results(vec![("range_agg", range)]);

        let histogram = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(vec![BucketEntry {
                key: Key::F64(1000.0),
                doc_count: 10,
                key_as_string: Some("2026-01-01".to_string()),
                sub_aggregation: hist_sub,
            }]),
        });

        let results = make_agg_results(vec![("histogram_agg", histogram)]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "leaf_terms", &mut hashes);

        assert_eq!(hashes.len(), 2, "expected 2 hashes from 3-level nesting, got {:?}", hashes);
        assert!(hashes.contains(&11));
        assert!(hashes.contains(&22));
    }

    #[test]
    fn test_collect_all_hashes_recursive_empty_buckets() {
        // Empty bucket lists should produce no hashes and not panic
        let empty_hist = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(vec![]),
        });
        let empty_range = AggregationResult::BucketResult(BucketResult::Range {
            buckets: BucketEntries::Vec(vec![]),
        });

        let results = make_agg_results(vec![
            ("hist", empty_hist),
            ("range", empty_range),
        ]);

        let mut hashes = HashSet::new();
        collect_all_hashes_recursive(&results, "anything", &mut hashes);
        assert!(hashes.is_empty(), "empty buckets should yield no hashes");
    }

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
