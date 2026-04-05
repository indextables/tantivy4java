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
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};

use quickwit_doc_mapper::{Automaton, FastFieldWarmupInfo, TermRange, WarmupInfo};
use quickwit_query::query_ast::{
    FieldPresenceQuery, FullTextQuery, PhrasePrefixQuery, QueryAst, QueryAstVisitor,
    RangeQuery, RegexQuery, TermSetQuery, WildcardQuery,
};
use quickwit_query::{create_default_quickwit_tokenizer_manager, find_field_or_hit_dynamic};
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};
use tantivy::Term;

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

    // Rewrite IP CIDR/wildcard term queries to range queries using the execution-time schema.
    // Must run here (not just in perform_search_leaf_response) because the streaming path
    // (startStreamingRetrieval) calls perform_bulk_search directly with pre-serialized JSON.
    let ip_rewritten;
    let effective_json_str = {
        let schema = ctx.cached_index.schema();
        match crate::split_query::rewrite_ip_term_queries(effective_json_str, &schema)? {
            Some(rewritten) => {
                ip_rewritten = rewritten;
                &ip_rewritten as &str
            }
            None => effective_json_str,
        }
    };

    // Parse QueryAst from (possibly rewritten) JSON
    let query_ast: QueryAst = serde_json::from_str(effective_json_str)
        .with_context(|| format!("Failed to parse QueryAst JSON: {}", &effective_json_str[..effective_json_str.len().min(1000)]))?;

    // Get schema and tokenizer manager for query building
    let schema = ctx.cached_index.schema();
    let tokenizer_manager = create_default_quickwit_tokenizer_manager();

    // Convert QueryAst → tantivy Query
    // with_validation=false to silently handle fields not in schema
    let build_context = quickwit_query::query_ast::BuildTantivyAstContext {
        schema: &schema,
        tokenizer_manager: &tokenizer_manager,
        search_fields: &[],
        with_validation: false,
    };
    let tantivy_query = query_ast
        .build_tantivy_query(&build_context)
        .map_err(|e| anyhow!("Failed to build tantivy query: {}", e))?;

    perf_println!(
        "⏱️ BULK_SEARCH: query parse + build took {}ms",
        t0.elapsed().as_millis()
    );

    // ========================================================================
    // Warm up using Quickwit's warmup() — same mechanism as leaf_search_single_split
    // ========================================================================
    // The HotDirectory only caches a subset of the split data. Tantivy's
    // searcher.search() is synchronous and will fail with "StorageDirectory
    // only supports async reads" if it hits cold data. Quickwit's warmup()
    // pre-loads exactly the right data (term dicts, postings, positions, fast
    // fields, automatons) based on what the query actually needs.
    let t_warmup = std::time::Instant::now();

    // Build WarmupInfo from the query, same way Quickwit's build_query() does:
    // query.query_terms() tells us exactly which terms need warming and whether
    // each needs position data (.pos files).
    let warmup_info = build_warmup_info(&*tantivy_query, &schema, effective_json_str);

    quickwit_search::warmup(&ctx.cached_searcher, &warmup_info).await
        .map_err(|e| anyhow!("Warmup failed: {}", e))?;

    perf_println!(
        "⏱️ BULK_SEARCH: quickwit warmup took {}ms",
        t_warmup.elapsed().as_millis()
    );

    // Warm native fast fields via L1 cache for range/exists queries
    let fast_field_names = crate::parquet_companion::field_extraction::extract_range_query_fields(effective_json_str);
    if !fast_field_names.is_empty() {
        let field_names: Vec<String> = fast_field_names.into_iter().collect();
        let _ = ctx.warm_native_fast_fields_l1_for_fields(&field_names).await;
        perf_println!(
            "⏱️ BULK_SEARCH: warmup native fast fields took {}ms",
            t_warmup.elapsed().as_millis()
        );
    }

    // For companion splits, ensure parquet-sourced fast fields are transcoded
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

/// Build a `WarmupInfo` from a built tantivy query and the original QueryAst.
///
/// This mirrors Quickwit's internal `build_query()` in `query_builder.rs` which is
/// `pub(crate)` and not accessible to us. We reconstruct the same warmup information:
///
/// 1. `query.query_terms()` — extracts per-term position needs (critical for .pos files)
/// 2. Range query visitor — extracts fast fields needed for range queries
/// 3. Exists query visitor — extracts fast fields needed for field presence queries
/// 4. TermSet visitor — extracts fields needing full term dictionary warmup
/// 5. Prefix/wildcard/regex visitor — extracts term ranges and automatons
fn build_warmup_info(
    query: &dyn Query,
    schema: &Schema,
    query_ast_json: &str,
) -> WarmupInfo {
    // 1. Extract per-term warmup info from the built tantivy query.
    // query_terms() tells us exactly which terms need warming and whether
    // each needs position data (.pos files).
    let mut terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>> = HashMap::new();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        *terms_grouped_by_field
            .entry(field)
            .or_default()
            .entry(term.clone())
            .or_default() |= need_position;
    });

    // Parse the QueryAst for visitor-based extraction
    let query_ast: std::result::Result<QueryAst, _> = serde_json::from_str(query_ast_json);
    let query_ast = match query_ast {
        Ok(ast) => ast,
        Err(_) => {
            // If we can't parse, return what we have from query_terms()
            return WarmupInfo {
                terms_grouped_by_field,
                ..WarmupInfo::default()
            };
        }
    };

    // 2. Extract range query fields → fast field warmup
    let mut range_visitor = RangeQueryFieldVisitor::default();
    let _: std::result::Result<(), Infallible> = range_visitor.visit(&query_ast);
    let mut fast_fields: HashSet<FastFieldWarmupInfo> = range_visitor
        .field_names
        .into_iter()
        .map(|name| FastFieldWarmupInfo {
            name,
            with_subfields: false,
        })
        .collect();

    // 3. Extract exists/FieldPresence query fields → fast field warmup
    let mut exists_visitor = ExistsQueryFieldVisitor {
        fields: HashSet::new(),
        schema: schema.clone(),
    };
    let _: std::result::Result<(), Infallible> = exists_visitor.visit(&query_ast);
    fast_fields.extend(exists_visitor.fields);

    // 4. Extract term set query fields → full term dictionary warmup
    let mut term_set_visitor = TermSetFieldVisitor {
        fields: HashSet::new(),
        schema,
    };
    let term_dict_fields = match term_set_visitor.visit(&query_ast) {
        Ok(()) => term_set_visitor.fields,
        Err(_) => HashSet::new(),
    };

    // 5. Extract prefix term ranges and automatons (wildcard/regex queries)
    let tokenizer_manager = create_default_quickwit_tokenizer_manager();
    let mut prefix_visitor = PrefixAndAutomatonVisitor::new(schema, &tokenizer_manager);
    let _ = prefix_visitor.visit(&query_ast);

    WarmupInfo {
        term_dict_fields,
        fast_fields,
        field_norms: false, // DocIdCollector doesn't score
        terms_grouped_by_field,
        term_ranges_grouped_by_field: prefix_visitor.term_ranges,
        automatons_grouped_by_field: prefix_visitor.automatons,
    }
}

// ---------------------------------------------------------------------------
// QueryAst visitors — mirrors quickwit-doc-mapper/src/query_builder.rs
// ---------------------------------------------------------------------------

/// Visitor that extracts field names from Range queries for fast field warmup.
#[derive(Default)]
struct RangeQueryFieldVisitor {
    field_names: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for RangeQueryFieldVisitor {
    type Err = Infallible;

    fn visit_range(&mut self, range_query: &'a RangeQuery) -> std::result::Result<(), Infallible> {
        self.field_names.insert(range_query.field.to_string());
        Ok(())
    }
}

/// Visitor that extracts fast fields from FieldPresence (exists/IS NOT NULL) queries.
struct ExistsQueryFieldVisitor {
    fields: HashSet<FastFieldWarmupInfo>,
    schema: Schema,
}

impl<'a> QueryAstVisitor<'a> for ExistsQueryFieldVisitor {
    type Err = Infallible;

    fn visit_exists(&mut self, exists_query: &'a FieldPresenceQuery) -> std::result::Result<(), Infallible> {
        let fields = exists_query.find_field_and_subfields(&self.schema);
        for (_, field_entry, path) in fields {
            if field_entry.is_fast() {
                if field_entry.field_type().is_json() {
                    let full_path = format!("{}.{}", field_entry.name(), path);
                    self.fields.insert(FastFieldWarmupInfo {
                        name: full_path,
                        with_subfields: true,
                    });
                } else if path.is_empty() {
                    self.fields.insert(FastFieldWarmupInfo {
                        name: field_entry.name().to_string(),
                        with_subfields: false,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Visitor that extracts fields from TermSet queries for full dictionary warmup.
struct TermSetFieldVisitor<'s> {
    fields: HashSet<Field>,
    schema: &'s Schema,
}

impl<'a, 's> QueryAstVisitor<'a> for TermSetFieldVisitor<'s> {
    type Err = anyhow::Error;

    fn visit_term_set(&mut self, term_set_query: &'a TermSetQuery) -> anyhow::Result<()> {
        for field_name in term_set_query.terms_per_field.keys() {
            if let Some((field, _field_entry, _path)) =
                find_field_or_hit_dynamic(field_name, self.schema)
            {
                self.fields.insert(field);
            }
        }
        Ok(())
    }
}

/// Visitor that extracts prefix term ranges (from FullText/PhrasePrefix) and
/// automatons (from Wildcard/Regex) for warmup.
struct PrefixAndAutomatonVisitor<'a> {
    schema: &'a Schema,
    tokenizer_manager: &'a quickwit_query::tokenizers::TokenizerManager,
    term_ranges: HashMap<Field, HashMap<TermRange, bool>>,
    automatons: HashMap<Field, HashSet<Automaton>>,
}

impl<'a> PrefixAndAutomatonVisitor<'a> {
    fn new(schema: &'a Schema, tokenizer_manager: &'a quickwit_query::tokenizers::TokenizerManager) -> Self {
        Self {
            schema,
            tokenizer_manager,
            term_ranges: HashMap::new(),
            automatons: HashMap::new(),
        }
    }

    fn add_prefix_term(&mut self, term: Term, max_expansions: u32, position_needed: bool) {
        let field = term.field();
        let (start, end) = prefix_term_to_range(term);
        let term_range = TermRange {
            start,
            end,
            limit: Some(max_expansions as u64),
        };
        *self
            .term_ranges
            .entry(field)
            .or_default()
            .entry(term_range)
            .or_default() |= position_needed;
    }
}

fn prefix_term_to_range(prefix: Term) -> (Bound<Term>, Bound<Term>) {
    let mut end_bound = prefix.serialized_term().to_vec();
    while !end_bound.is_empty() {
        let last_byte = end_bound.last_mut().unwrap();
        if *last_byte != u8::MAX {
            *last_byte += 1;
            return (
                Bound::Included(prefix),
                Bound::Excluded(Term::wrap(&end_bound)),
            );
        }
        end_bound.pop();
    }
    (Bound::Included(prefix), Bound::Unbounded)
}

impl<'a, 'b: 'a> QueryAstVisitor<'a> for PrefixAndAutomatonVisitor<'b> {
    type Err = quickwit_query::InvalidQuery;

    fn visit_full_text(&mut self, full_text_query: &'a FullTextQuery) -> std::result::Result<(), Self::Err> {
        if let Some(prefix_term) =
            full_text_query.get_prefix_term(self.schema, self.tokenizer_manager)
        {
            self.add_prefix_term(prefix_term, u32::MAX, false);
        }
        Ok(())
    }

    fn visit_phrase_prefix(
        &mut self,
        phrase_prefix: &'a PhrasePrefixQuery,
    ) -> std::result::Result<(), Self::Err> {
        let terms = match phrase_prefix.get_terms(self.schema, self.tokenizer_manager) {
            Ok((_, terms)) => terms,
            Err(quickwit_query::InvalidQuery::SchemaError(_))
            | Err(quickwit_query::InvalidQuery::FieldDoesNotExist { .. }) => return Ok(()),
            Err(e) => return Err(e),
        };
        if let Some((_, term)) = terms.last() {
            self.add_prefix_term(term.clone(), phrase_prefix.max_expansions, terms.len() > 1);
        }
        Ok(())
    }

    fn visit_wildcard(&mut self, wildcard_query: &'a WildcardQuery) -> std::result::Result<(), Self::Err> {
        let (field, path, regex) =
            match wildcard_query.to_regex(self.schema, self.tokenizer_manager) {
                Ok(res) => res,
                Err(quickwit_query::InvalidQuery::FieldDoesNotExist { .. }) => return Ok(()),
                Err(e) => return Err(e),
            };
        self.automatons
            .entry(field)
            .or_default()
            .insert(Automaton::Regex(path, regex));
        Ok(())
    }

    fn visit_regex(&mut self, regex_query: &'a RegexQuery) -> std::result::Result<(), Self::Err> {
        let (field, path, regex) = match regex_query.to_field_and_regex(self.schema) {
            Ok(res) => res,
            Err(quickwit_query::InvalidQuery::FieldDoesNotExist { .. }) => return Ok(()),
            Err(e) => return Err(e),
        };
        self.automatons
            .entry(field)
            .or_default()
            .insert(Automaton::Regex(path, regex));
        Ok(())
    }
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

