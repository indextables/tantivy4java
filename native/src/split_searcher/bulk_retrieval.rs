// bulk_retrieval.rs - Bulk search and parquet location resolution
//
// Provides the shared foundation for both fused (Phase 0) and streaming (Phase 3)
// companion-mode retrieval:
//
// 1. perform_bulk_search() — No-score search via DocIdCollector, bypassing
//    Quickwit's leaf_search path entirely. Returns Vec<(segment_ord, doc_id)>.
//
// 2. resolve_to_parquet_locations() — Resolves doc addresses to parquet file
//    locations entirely in Rust via __pq_file_hash / __pq_row_in_file fast fields.
//    No JNI round-trip needed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};

use quickwit_query::query_ast::QueryAst;
use quickwit_query::create_default_quickwit_tokenizer_manager;

use crate::perf_println;
use super::types::CachedSearcherContext;
use super::docid_collector::DocIdCollector;

/// Execute a no-score search using DocIdCollector.
///
/// Parses the Quickwit QueryAst JSON and converts it to a tantivy Query,
/// then executes directly against the cached tantivy Searcher with our
/// custom DocIdCollector (no BM25 scoring, batch collection).
///
/// Before searching, warms up term dictionaries and posting lists for all
/// indexed fields so that the synchronous search can access them through
/// the HotDirectory cache (which only supports async reads for cold data).
///
/// This bypasses Quickwit's leaf_search_single_split entirely — no PartialHit
/// protobuf allocation, no split_id strings, no sort values.
///
/// # Arguments
/// * `ctx` - Cached searcher context with index and searcher
/// * `query_ast_json` - Quickwit QueryAst JSON string
///
/// # Returns
/// Vec of (segment_ord, doc_id) pairs for all matching documents.
pub async fn perform_bulk_search(
    ctx: &CachedSearcherContext,
    query_ast_json: &str,
) -> Result<Vec<(u32, u32)>> {
    let t0 = std::time::Instant::now();

    // ========================================================================
    // Query rewriting for companion splits (must happen before query building)
    // ========================================================================
    // The regular search() path rewrites queries for:
    //   1. FieldPresence on hash-optimized fields → _phash_* fields
    //   2. Term queries on exact_only fields → _phash_<field> hash lookups
    // Without these rewrites, IS NOT NULL and exact_only EqualTo queries fail.
    let effective_json = rewrite_companion_query(ctx, query_ast_json)?;
    let effective_json_str = effective_json.as_deref().unwrap_or(query_ast_json);

    // Parse QueryAst from (possibly rewritten) JSON
    let query_ast: QueryAst = serde_json::from_str(effective_json_str)
        .with_context(|| format!("Failed to parse QueryAst JSON: {}", &effective_json_str[..effective_json_str.len().min(200)]))?;

    // Get schema and tokenizer manager for query building
    let schema = ctx.cached_index.schema();
    let tokenizer_manager = create_default_quickwit_tokenizer_manager();

    // Convert QueryAst → tantivy Query
    // with_validation=false to silently handle fields not in schema
    let tantivy_query = query_ast
        .build_tantivy_query(&schema, &tokenizer_manager, &[], false)
        .map_err(|e| anyhow!("Failed to build tantivy query: {}", e))?;

    perf_println!(
        "⏱️ BULK_SEARCH: query parse + build took {}ms",
        t0.elapsed().as_millis()
    );

    // ========================================================================
    // Warm up: term dictionaries, posting lists, fast fields
    // ========================================================================
    // The HotDirectory only caches a subset of the split data; term, postings, and
    // fast files may not be in the hot cache. Tantivy's searcher.search() is synchronous
    // and will fail with "StorageDirectory only supports async reads" if it hits
    // cold data. The warm_up calls load the data into the HotDirectory cache
    // asynchronously, after which the synchronous search succeeds.
    let t_warmup = std::time::Instant::now();

    // Extract fields that need fast field access (range, field_presence/exists queries)
    // from the REWRITTEN query (so _phash_* fields are included)
    let fast_field_names = crate::parquet_companion::field_extraction::extract_range_query_fields(effective_json_str);

    let mut warmup_futures = Vec::new();
    for segment_reader in ctx.cached_searcher.segment_readers() {
        for (field, field_entry) in schema.fields() {
            if !field_entry.is_indexed() {
                continue;
            }
            if let Ok(inverted_index) = segment_reader.inverted_index(field) {
                let inv = inverted_index.clone();
                warmup_futures.push(async move {
                    // Warm term dictionary (FST)
                    let _ = inv.terms().warm_up_dictionary().await;
                    // Warm posting lists
                    let _ = inv.warm_postings_full(false).await;
                });
            }
        }
    }
    if !warmup_futures.is_empty() {
        futures::future::join_all(warmup_futures).await;
        perf_println!(
            "⏱️ BULK_SEARCH: warmup term+postings took {}ms",
            t_warmup.elapsed().as_millis()
        );
    }

    // Warm fast fields for fields that need them (range, exists, _phash_* queries)
    if !fast_field_names.is_empty() {
        let field_names: Vec<String> = fast_field_names.into_iter().collect();
        perf_println!("⏱️ BULK_SEARCH: warming fast fields for {:?}", field_names);
        let _ = ctx.warm_native_fast_fields_l1_for_fields(&field_names).await;
        perf_println!(
            "⏱️ BULK_SEARCH: warmup fast fields took {}ms",
            t_warmup.elapsed().as_millis()
        );
    }

    // For companion splits with an augmented directory, ensure parquet-sourced fast
    // fields are transcoded and available. This mirrors what ensure_fast_fields_for_query()
    // does in the regular search() path.
    if ctx.augmented_directory.is_some() {
        let _ = crate::split_searcher::async_impl::ensure_fast_fields_for_query(
            ctx, effective_json_str, None,
        ).await?;
        perf_println!(
            "⏱️ BULK_SEARCH: ensure_fast_fields took {}ms",
            t_warmup.elapsed().as_millis()
        );
    }

    // Execute search with DocIdCollector (no scoring)
    let t_search = std::time::Instant::now();
    let doc_ids = ctx
        .cached_searcher
        .search(&*tantivy_query, &DocIdCollector)
        .map_err(|e| anyhow!("DocIdCollector search failed: {}", e))?;

    perf_println!(
        "⏱️ BULK_SEARCH: search returned {} docs in {}ms (no scoring)",
        doc_ids.len(),
        t_search.elapsed().as_millis()
    );

    Ok(doc_ids)
}

/// Apply companion-mode query rewrites (hash field presence + exact_only string indexing).
///
/// Returns `Ok(Some(rewritten))` if any rewrite was applied, `Ok(None)` if no changes needed.
fn rewrite_companion_query(
    ctx: &CachedSearcherContext,
    query_json: &str,
) -> Result<Option<String>> {
    let manifest = match ctx.parquet_manifest.as_ref() {
        Some(m) => m,
        None => return Ok(None), // Not a companion split — no rewrites needed
    };

    let mut current = query_json.to_string();
    let mut changed = false;

    // Rewrite FieldPresence (exists/IS NOT NULL) queries on string hash fields
    // to target _phash_* U64 fields instead of the original string field
    if !manifest.string_hash_fields.is_empty() {
        if let Some(rewritten) = crate::parquet_companion::hash_field_rewriter::rewrite_query_for_hash_fields(
            &current,
            &manifest.string_hash_fields,
        ) {
            perf_println!("⏱️ BULK_SEARCH: rewrote FieldPresence → hash field(s)");
            current = rewritten;
            changed = true;
        }
    }

    // Rewrite term queries for exact_only / text_*_exactonly compact string indexing
    // (converts field term queries to _phash_<field> hash lookups)
    if !manifest.string_indexing_modes.is_empty() {
        match crate::parquet_companion::hash_field_rewriter::rewrite_query_for_string_indexing(
            &current,
            &manifest.string_indexing_modes,
        )? {
            Some(rewritten) => {
                perf_println!("⏱️ BULK_SEARCH: rewrote query for compact string indexing mode(s)");
                current = rewritten;
                changed = true;
            }
            None => {}
        }
    }

    Ok(if changed { Some(current) } else { None })
}

/// Resolve doc addresses to parquet file groups entirely in Rust.
///
/// Uses the __pq_file_hash and __pq_row_in_file fast field columns for O(1)
/// per-doc resolution. Groups results by parquet file index, sorted by
/// row_in_file within each group for sequential I/O.
///
/// This is equivalent to `resolve_doc_addresses_to_groups` in doc_retrieval_jni.rs
/// but extracted as a standalone function for reuse by both fused and streaming paths.
///
/// # Arguments
/// * `ctx` - Cached searcher context with __pq column handles
/// * `doc_ids` - Slice of (segment_ord, doc_id) pairs from DocIdCollector
///
/// # Returns
/// HashMap from file_idx → Vec<(original_index, row_in_file)>, sorted by row_in_file.
///
/// # Fallback
/// When `has_merge_safe_tracking` is false (legacy splits without __pq fields),
/// falls back to manifest-based positional resolution via `group_doc_addresses_by_file`.
pub async fn resolve_to_parquet_locations(
    ctx: &Arc<CachedSearcherContext>,
    doc_ids: &[(u32, u32)],
) -> Result<HashMap<usize, Vec<(usize, u64)>>> {
    if doc_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Fallback for legacy splits without __pq fast fields
    if !ctx.has_merge_safe_tracking {
        perf_println!("⏱️ BULK_RESOLVE: using legacy manifest-based resolution");
        let manifest = ctx
            .parquet_manifest
            .as_ref()
            .ok_or_else(|| anyhow!("no parquet manifest for legacy resolution"))?;
        return crate::parquet_companion::docid_mapping::group_doc_addresses_by_file(
            doc_ids, manifest,
        )
        .map_err(|e| anyhow!("{}", e));
    }

    let t0 = std::time::Instant::now();

    // Ensure __pq columns loaded for all referenced segments
    let unique_segments: HashSet<u32> = doc_ids.iter().map(|&(seg, _)| seg).collect();
    for &seg_ord in &unique_segments {
        ctx.ensure_pq_segment_loaded(seg_ord)
            .await
            .with_context(|| format!("Failed to load __pq fields for segment {}", seg_ord))?;
    }

    perf_println!(
        "⏱️ BULK_RESOLVE: loaded __pq columns for {} segments in {}ms",
        unique_segments.len(),
        t0.elapsed().as_millis()
    );

    // Resolve each doc to (file_idx, row_in_file) via fast fields
    let t_resolve = std::time::Instant::now();
    let pq_cols = ctx.pq_columns.read()
        .map_err(|e| anyhow!("Failed to acquire __pq columns read lock: {}", e))?;
    let mut resolved: Vec<(usize, u64, u64)> = Vec::with_capacity(doc_ids.len());

    for (original_idx, &(seg_ord, doc_id)) in doc_ids.iter().enumerate() {
        let (fh_col, row_col) = pq_cols.get(seg_ord as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| anyhow!("__pq columns not loaded for segment {}", seg_ord))?;

        let file_hash = fh_col
            .values_for_doc(doc_id)
            .next()
            .ok_or_else(|| anyhow!("no __pq_file_hash for seg={} doc={}", seg_ord, doc_id))?;
        let row_in_file = row_col
            .values_for_doc(doc_id)
            .next()
            .ok_or_else(|| anyhow!("no __pq_row_in_file for seg={} doc={}", seg_ord, doc_id))?;

        resolved.push((original_idx, file_hash, row_in_file));
    }
    drop(pq_cols); // Release read lock

    perf_println!(
        "⏱️ BULK_RESOLVE: resolved {} docs via fast fields in {}ms",
        resolved.len(),
        t_resolve.elapsed().as_millis()
    );

    // Group by file using the pre-built hash→index lookup
    let t_group = std::time::Instant::now();
    let groups = crate::parquet_companion::docid_mapping::group_resolved_locations_by_file(
        &resolved,
        &ctx.parquet_file_hash_index,
    )
    .map_err(|e| anyhow!("{}", e))?;

    perf_println!(
        "⏱️ BULK_RESOLVE: grouped into {} files in {}ms",
        groups.len(),
        t_group.elapsed().as_millis()
    );

    Ok(groups)
}

/// Get the parquet storage from the context, with helpful error messages.
pub fn get_parquet_storage(
    ctx: &CachedSearcherContext,
) -> Result<Arc<dyn quickwit_storage::Storage>> {
    match ctx.parquet_storage.as_ref() {
        Some(s) => Ok(s.clone()),
        None => {
            let reason = if ctx.parquet_table_root.is_none() {
                "parquet_table_root was not set. Pass the table root path to createSplitSearcher() \
                 or configure it via CacheConfig.withParquetTableRoot()."
            } else {
                "parquet storage creation failed (likely bad credentials or unreachable endpoint). \
                 Enable TANTIVY4JAVA_DEBUG=1 and check stderr for the storage creation error."
            };
            Err(anyhow!(
                "Parquet companion retrieval failed: {}",
                reason
            ))
        }
    }
}

