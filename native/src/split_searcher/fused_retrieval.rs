// fused_retrieval.rs - Fused companion-mode search + parquet retrieval
//
// Single-operation path that combines DocIdCollector search, fast-field resolution,
// and Arrow FFI export into one call with no JNI round-trips.
//
// Used for non-bulk companion queries (< 50K results) where the streaming session
// overhead isn't warranted. For larger results, use the streaming pipeline.
//
// Eliminates three overheads vs the current search() + docBatchArrowFfi() two-call path:
// 1. BM25 scoring (term frequencies never decompressed)
// 2. PartialHit protobuf allocation (no split_id strings, no sort values)
// 3. JNI round-trip of doc addresses (resolved entirely in Rust)

use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::perf_println;
use super::types::CachedSearcherContext;
use super::bulk_retrieval::{perform_bulk_search, resolve_to_parquet_locations, get_parquet_storage};

/// Fused companion-mode search + parquet retrieval in a single operation.
///
/// Executes the full pipeline: DocIdCollector search → fast-field resolution →
/// parquet batch read → Arrow FFI export. No data crosses the JNI boundary
/// until the final FFI struct write.
///
/// # Arguments
/// * `ctx` - Cached searcher context
/// * `query_ast_json` - Quickwit QueryAst JSON string
/// * `projected_fields` - Optional field name projection
/// * `array_addrs` - Pre-allocated FFI_ArrowArray addresses (one per projected column)
/// * `schema_addrs` - Pre-allocated FFI_ArrowSchema addresses (one per projected column)
///
/// # Returns
/// Number of rows written to FFI addresses, or 0 if no matches.
pub async fn search_and_retrieve_ffi(
    ctx: &Arc<CachedSearcherContext>,
    query_ast_json: &str,
    projected_fields: Option<&[String]>,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    let t_total = std::time::Instant::now();

    // Guard: must be a companion split
    let manifest = ctx
        .parquet_manifest
        .as_ref()
        .ok_or_else(|| anyhow!("not a companion split — no parquet manifest"))?;

    // Phase 1: No-score search
    let doc_ids = perform_bulk_search(ctx, query_ast_json).await?;

    if doc_ids.is_empty() {
        perf_println!("⏱️ FUSED: no matches, returning 0 rows");
        return Ok(0);
    }

    let num_docs = doc_ids.len();
    perf_println!("⏱️ FUSED: search returned {} docs", num_docs);

    // Phase 2: Resolve to parquet locations (stays in Rust)
    let groups = resolve_to_parquet_locations(ctx, &doc_ids).await?;

    perf_println!(
        "⏱️ FUSED: resolved to {} file groups",
        groups.len()
    );

    // Phase 3: Read parquet + FFI export
    let storage = get_parquet_storage(ctx)?;

    let row_count = crate::parquet_companion::arrow_ffi_export::batch_parquet_to_arrow_ffi(
        groups,
        num_docs,
        projected_fields,
        manifest,
        &storage,
        Some(&ctx.parquet_metadata_cache),
        Some(&ctx.parquet_byte_range_cache),
        ctx.parquet_coalesce_config,
        array_addrs,
        schema_addrs,
    )
    .await?;

    perf_println!(
        "⏱️ FUSED: total pipeline: {} rows, {}ms",
        row_count,
        t_total.elapsed().as_millis()
    );

    Ok(row_count)
}
