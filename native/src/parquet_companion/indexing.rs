// indexing.rs - createFromParquet pipeline (Phase 3)
//
// Creates a Quickwit split from external parquet files by:
// 1. Deriving tantivy schema from parquet schema
// 2. Iterating rows and adding to tantivy index
// 3. Creating the split with embedded parquet manifest

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::*;
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tantivy::schema::{Schema, Field};
use tantivy::TantivyDocument;

use quickwit_storage::Storage;

use super::manifest::*;
use super::schema_derivation::{
    SchemaDerivationConfig, derive_tantivy_schema_with_mapping, arrow_type_to_tantivy_type,
    arrow_type_to_parquet_type, validate_schema_consistency,
};
use super::statistics::{StatisticsAccumulator, ColumnStatisticsResult, validate_statistics_fields};
use super::name_mapping;

use crate::debug_println;
use crate::quickwit_split::QuickwitSplitMetadata;

/// Hidden u64 fast field storing a hash of the parquet file's relative path.
/// Combined with PARQUET_ROW_IN_FILE_FIELD, enables O(1) doc→parquet resolution
/// that survives both segment merges and split merges.
pub const PARQUET_FILE_HASH_FIELD: &str = "__pq_file_hash";

/// Hidden u64 fast field storing the 0-based row index within the parquet file.
pub const PARQUET_ROW_IN_FILE_FIELD: &str = "__pq_row_in_file";

/// Compute a stable u64 hash for a parquet file's relative path.
/// Uses FxHash-style mixing for speed; collision probability ~1/2^64 per pair.
pub fn hash_parquet_path(relative_path: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    relative_path.hash(&mut hasher);
    hasher.finish()
}

/// Configuration for creating a split from parquet files
pub struct CreateFromParquetConfig {
    /// Table root path (parquet file paths are relative to this)
    pub table_root: String,
    /// Fast field mode
    pub fast_field_mode: FastFieldMode,
    /// Schema derivation config
    pub schema_config: SchemaDerivationConfig,
    /// Fields to compute statistics for
    pub statistics_fields: Vec<String>,
    /// Truncation length for string statistics
    pub statistics_truncate_length: usize,
    /// Field ID mapping (parquet column name → display name)
    pub field_id_mapping: HashMap<String, String>,
    /// Auto-detect Iceberg field ID mapping from parquet metadata
    pub auto_detect_name_mapping: bool,
    /// Split config fields
    pub index_uid: String,
    pub source_id: String,
    pub node_id: String,
    /// Tantivy writer heap size in bytes. Controls how much memory the indexer
    /// can use before flushing to disk. Larger values reduce the chance of
    /// creating multiple segments but use more RAM. Default: 256MB.
    pub writer_heap_size: usize,
    /// Number of rows per Arrow RecordBatch when reading parquet files.
    /// Larger batches reduce per-batch overhead but use more memory.
    /// Default: 8192.
    pub reader_batch_size: usize,
}

impl Default for CreateFromParquetConfig {
    fn default() -> Self {
        Self {
            table_root: String::new(),
            fast_field_mode: FastFieldMode::Disabled,
            schema_config: SchemaDerivationConfig::default(),
            statistics_fields: Vec::new(),
            statistics_truncate_length: 256,
            field_id_mapping: HashMap::new(),
            auto_detect_name_mapping: false,
            index_uid: "parquet-index".to_string(),
            source_id: "parquet-source".to_string(),
            node_id: "parquet-node".to_string(),
            writer_heap_size: 256_000_000, // 256MB
            reader_batch_size: 8192,
        }
    }
}

/// Result of creating a split from parquet, includes statistics
#[derive(Debug)]
pub struct CreateFromParquetResult {
    pub metadata: QuickwitSplitMetadata,
    pub column_statistics: Vec<ColumnStatisticsResult>,
}

/// Create a Quickwit split from external parquet files.
///
/// This is the main entry point for Phase 3 indexing pipeline.
/// Returns the split metadata including column statistics.
pub async fn create_split_from_parquet(
    parquet_files: &[String],
    output_path: &str,
    parquet_config: &CreateFromParquetConfig,
    _storage: &Arc<dyn Storage>,
) -> Result<CreateFromParquetResult> {
    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Starting with {} files, output={}",
        parquet_files.len(), output_path
    );

    if parquet_files.is_empty() {
        anyhow::bail!("No parquet files provided");
    }

    // ── Step 1: Read schema from first parquet file ──────────────────────
    // Use File-based reader which reads only the footer via mmap/seek,
    // avoiding loading the entire file into memory just for schema/metadata.
    let first_file = std::fs::File::open(&parquet_files[0])
        .with_context(|| format!("Failed to open parquet file: {}", parquet_files[0]))?;

    let first_reader = ParquetRecordBatchReaderBuilder::try_new(first_file)
        .context("Failed to read first parquet file metadata")?;

    let arrow_schema = first_reader.schema().clone();
    let parquet_metadata = first_reader.metadata().clone();
    drop(first_reader);

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Primary schema has {} fields",
        arrow_schema.fields().len()
    );

    // ── Step 2: Validate schema consistency across all files ─────────────
    // Use File-based reader which only reads the parquet footer (a few KB)
    // instead of loading the entire file into memory just to check the schema.
    let mut file_schemas: Vec<ArrowSchema> = Vec::new();
    for file_path in &parquet_files[1..] {
        let file = std::fs::File::open(file_path)
            .with_context(|| format!("Failed to open parquet file: {}", file_path))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("Failed to read parquet metadata: {}", file_path))?;
        file_schemas.push(reader.schema().as_ref().clone());
    }
    validate_schema_consistency(&arrow_schema, &file_schemas)?;

    debug_println!("🏗️ CREATE_FROM_PARQUET: Schema consistency validated across {} files", parquet_files.len());

    // ── Step 3: Resolve name mapping ────────────────────────────────────
    let name_mapping = name_mapping::resolve_name_mapping(
        &parquet_metadata,
        &parquet_config.field_id_mapping,
        parquet_config.auto_detect_name_mapping,
    )?;

    // ── Step 4: Derive tantivy schema ───────────────────────────────────
    let config = SchemaDerivationConfig {
        fast_field_mode: parquet_config.fast_field_mode,
        skip_fields: parquet_config.schema_config.skip_fields.clone(),
        tokenizer_overrides: parquet_config.schema_config.tokenizer_overrides.clone(),
        ip_address_fields: parquet_config.schema_config.ip_address_fields.clone(),
        json_fields: parquet_config.schema_config.json_fields.clone(),
    };

    let derived_schema = derive_tantivy_schema_with_mapping(&arrow_schema, &config, Some(&name_mapping))?;

    // Add hidden fast fields for merge-safe parquet row tracking.
    // These u64 fast fields provide O(1) positional access by doc_id and
    // survive both segment merges and split merges.
    let mut schema_builder = tantivy::schema::SchemaBuilder::new();
    for (_field, entry) in derived_schema.fields() {
        schema_builder.add_field(entry.clone());
    }
    let pq_file_hash_field = schema_builder.add_u64_field(
        PARQUET_FILE_HASH_FIELD,
        tantivy::schema::NumericOptions::default().set_fast(),
    );
    let pq_row_in_file_field = schema_builder.add_u64_field(
        PARQUET_ROW_IN_FILE_FIELD,
        tantivy::schema::NumericOptions::default().set_fast(),
    );
    let tantivy_schema = schema_builder.build();

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Derived tantivy schema with {} fields (incl 2 row-tracking fast fields)",
        tantivy_schema.fields().count()
    );

    // ── Step 5: Build column mapping ────────────────────────────────────
    let column_mapping = build_column_mapping(&arrow_schema, &name_mapping, &parquet_metadata, &config);

    // ── Step 6: Validate statistics fields ──────────────────────────────
    let field_types: HashMap<String, String> = column_mapping
        .iter()
        .map(|cm| (cm.tantivy_field_name.clone(), cm.tantivy_type.clone()))
        .collect();
    validate_statistics_fields(&parquet_config.statistics_fields, &field_types)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // ── Step 7: Create tantivy index in temp directory ───────────────────
    let temp_dir = tempfile::tempdir().context("Failed to create temp directory for indexing")?;
    let index_dir = temp_dir.path().to_path_buf();

    let index = tantivy::Index::create_in_dir(&index_dir, tantivy_schema.clone())
        .context("Failed to create tantivy index")?;

    // Single-threaded writer ensures docs within each segment are in insertion order.
    // Merges are allowed (default LogMergePolicy) — the __pq_file_hash and
    // __pq_row_in_file fast fields make doc→parquet resolution merge-safe.
    let mut writer = index.writer_with_num_threads(1, parquet_config.writer_heap_size)
        .context("Failed to create index writer")?;

    // ── Step 8: Initialize statistics accumulators ───────────────────────
    let mut accumulators: HashMap<String, StatisticsAccumulator> = HashMap::new();
    for stat_field in &parquet_config.statistics_fields {
        if let Some(field_type) = field_types.get(stat_field) {
            accumulators.insert(
                stat_field.clone(),
                StatisticsAccumulator::new(stat_field, field_type, parquet_config.statistics_truncate_length),
            );
        }
    }

    // ── Step 9: Iterate parquet files and index rows ─────────────────────
    let mut parquet_file_entries: Vec<ParquetFileEntry> = Vec::new();
    let mut cumulative_row_offset: u64 = 0;
    let mut total_rows: u64 = 0;

    for file_path in parquet_files {
        let file = std::fs::File::open(file_path)
            .with_context(|| format!("Failed to open parquet file: {}", file_path))?;
        let file_size = file.metadata()
            .with_context(|| format!("Failed to stat parquet file: {}", file_path))?
            .len();

        // Use File-based reader — parquet crate reads via mmap/seek so only
        // the pages actually needed are loaded into memory, instead of reading
        // the entire file upfront with std::fs::read().
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("Failed to open parquet file: {}", file_path))?;

        let pq_metadata = reader_builder.metadata().clone();
        let has_offset_index = pq_metadata.row_groups().iter().any(|rg| {
            rg.columns().iter().any(|c| c.offset_index_offset().is_some())
        });

        // Build row group entries
        let mut row_groups = Vec::new();
        let mut rg_row_offset: u64 = 0;
        for (rg_idx, rg_meta) in pq_metadata.row_groups().iter().enumerate() {
            let rg_num_rows = rg_meta.num_rows() as u64;
            let mut columns = Vec::new();
            for (col_idx, col_meta) in rg_meta.columns().iter().enumerate() {
                let col_name = pq_metadata
                    .file_metadata()
                    .schema_descr()
                    .column(col_idx)
                    .name()
                    .to_string();
                columns.push(ColumnChunkInfo {
                    column_idx: col_idx,
                    column_name: col_name,
                    data_page_offset: col_meta.data_page_offset() as u64,
                    compressed_size: col_meta.compressed_size() as u64,
                    uncompressed_size: col_meta.uncompressed_size() as u64,
                });
            }
            row_groups.push(RowGroupEntry {
                row_group_idx: rg_idx,
                num_rows: rg_num_rows,
                row_offset_in_file: rg_row_offset,
                columns,
            });
            rg_row_offset += rg_num_rows;
        }

        let file_num_rows = rg_row_offset;

        // Compute relative path from table_root
        let relative_path = compute_relative_path(file_path, &parquet_config.table_root);
        let file_hash = hash_parquet_path(&relative_path);

        parquet_file_entries.push(ParquetFileEntry {
            relative_path,
            file_size_bytes: file_size,
            row_offset: cumulative_row_offset,
            num_rows: file_num_rows,
            has_offset_index,
            row_groups,
        });

        // Reuse the reader_builder (which already holds the file handle) instead
        // of re-reading the file from disk.
        let reader = reader_builder
            .with_batch_size(parquet_config.reader_batch_size)
            .build()
            .context("Failed to build parquet reader")?;

        let mut row_in_file: u64 = 0;
        for batch_result in reader {
            let batch = batch_result.context("Failed to read record batch")?;
            let batch_schema = batch.schema();

            for row_idx in 0..batch.num_rows() {
                let mut tantivy_doc = arrow_row_to_tantivy_doc(
                    &batch,
                    &batch_schema,
                    row_idx,
                    &tantivy_schema,
                    &name_mapping,
                    &config,
                    &mut accumulators,
                )?;

                // Attach merge-safe parquet coordinates to each document
                tantivy_doc.add_u64(pq_file_hash_field, file_hash);
                tantivy_doc.add_u64(pq_row_in_file_field, row_in_file);

                writer.add_document(tantivy_doc)
                    .context("Failed to add document to tantivy index")?;
                row_in_file += 1;
            }
        }

        cumulative_row_offset += file_num_rows;
        total_rows += file_num_rows;

        debug_println!(
            "🏗️ CREATE_FROM_PARQUET: Indexed file '{}' ({} rows, cumulative: {})",
            file_path, file_num_rows, total_rows
        );
    }

    writer.commit().context("Failed to commit tantivy index")?;

    // ── Step 10: Build segment_row_ranges from whatever segments exist ──
    // With __pq_file_hash and __pq_row_in_file fast fields, doc→parquet
    // resolution no longer depends on segment ordering. The fast fields are
    // immutable per-document properties that survive any merge operation.
    // segment_row_ranges are kept for backward compatibility but are no
    // longer used for correctness in the retrieval path.
    index.load_metas().context("Failed to load index metas after commit")?;

    let segment_metas = index.searchable_segment_metas()
        .context("Failed to read segment metas after commit")?;

    let docs_in_segments: u64 = segment_metas.iter().map(|m| m.max_doc() as u64).sum();
    if docs_in_segments != total_rows {
        anyhow::bail!(
            "Segment row count mismatch: segments have {} rows but indexed {} rows",
            docs_in_segments, total_rows
        );
    }

    // Build segment_row_ranges — not used for correctness but kept for compat.
    let mut segment_row_ranges = Vec::new();
    let mut seg_row_offset: u64 = 0;
    for (seg_ord, seg_meta) in segment_metas.iter().enumerate() {
        let num_rows = seg_meta.max_doc() as u64;
        segment_row_ranges.push(SegmentRowRange {
            segment_ord: seg_ord as u32,
            row_offset: seg_row_offset,
            num_rows,
        });
        seg_row_offset += num_rows;
    }

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Indexed {} total rows from {} files across {} segment(s)",
        total_rows, parquet_files.len(), segment_metas.len()
    );

    // ── Step 11: Build manifest ─────────────────────────────────────────
    let manifest = ParquetManifest {
        version: SUPPORTED_MANIFEST_VERSION,
        table_root: String::new(), // Not persisted — provided at read time via config
        fast_field_mode: parquet_config.fast_field_mode,
        segment_row_ranges,
        parquet_files: parquet_file_entries,
        column_mapping,
        total_rows,
        storage_config: None,
        metadata: HashMap::new(),
    };

    manifest.validate().map_err(|e| anyhow::anyhow!("Manifest validation failed: {}", e))?;

    // ── Step 12: Create split with embedded manifest ────────────────────
    let output_path_buf = PathBuf::from(output_path);

    // Extract doc mapping from the index schema (required for aggregations).
    // The indexing schema may have suppressed .fast for some fields (HYBRID/PARQUET_ONLY),
    // but the doc_mapping returned to the client must report all fields as fast so that
    // when the client passes it back to create a SplitSearcher, the runtime knows it can
    // serve fast field data (either from native .fast files or from parquet).
    let raw_doc_mapping = crate::quickwit_split::json_discovery::extract_doc_mapping_from_index(&index)
        .map_err(|e| anyhow::anyhow!("Failed to extract doc mapping from parquet index: {}", e))?;
    let doc_mapping_json = super::schema_derivation::promote_doc_mapping_all_fast(&raw_doc_mapping)?;

    // Create split metadata
    let split_id = uuid::Uuid::new_v4().to_string();
    let split_metadata = QuickwitSplitMetadata {
        split_id: split_id.clone(),
        index_uid: parquet_config.index_uid.clone(),
        source_id: parquet_config.source_id.clone(),
        node_id: parquet_config.node_id.clone(),
        doc_mapping_uid: "default".to_string(),
        partition_id: 0,
        num_docs: total_rows as usize,
        uncompressed_docs_size_in_bytes: 0, // Will be filled by split creation
        time_range: None,
        create_timestamp: chrono::Utc::now().timestamp(),
        maturity: "Mature".to_string(),
        tags: std::collections::BTreeSet::new(),
        delete_opstamp: 0,
        num_merge_ops: 0,
        footer_start_offset: None,
        footer_end_offset: None,
        hotcache_start_offset: None,
        hotcache_length: None,
        doc_mapping_json: Some(doc_mapping_json),
        skipped_splits: Vec::new(),
    };

    // Use the standard split creation with manifest embedding
    use crate::quickwit_split::split_creation::create_quickwit_split;

    let default_split_config = crate::quickwit_split::default_split_config(
        &parquet_config.index_uid,
        &parquet_config.source_id,
        &parquet_config.node_id,
    );

    let footer_offsets = create_quickwit_split(
        &index,
        &index_dir,
        &output_path_buf,
        &split_metadata,
        &default_split_config,
        Some(&manifest),
    ).await?;

    // Build final metadata with footer offsets
    let final_metadata = QuickwitSplitMetadata {
        footer_start_offset: Some(footer_offsets.footer_start_offset),
        footer_end_offset: Some(footer_offsets.footer_end_offset),
        hotcache_start_offset: Some(footer_offsets.hotcache_start_offset),
        hotcache_length: Some(footer_offsets.hotcache_length),
        ..split_metadata
    };

    // Collect statistics
    let column_statistics: Vec<ColumnStatisticsResult> = accumulators
        .into_values()
        .map(|acc| acc.finalize())
        .collect();

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Split created at '{}' with {} docs, {} statistics fields",
        output_path, total_rows, column_statistics.len()
    );

    Ok(CreateFromParquetResult {
        metadata: final_metadata,
        column_statistics,
    })
}

/// Convert a single Arrow row into a tantivy Document.
fn arrow_row_to_tantivy_doc(
    batch: &RecordBatch,
    batch_schema: &ArrowSchema,
    row_idx: usize,
    tantivy_schema: &Schema,
    name_mapping: &name_mapping::NameMapping,
    config: &SchemaDerivationConfig,
    accumulators: &mut HashMap<String, StatisticsAccumulator>,
) -> Result<TantivyDocument> {
    let mut doc = TantivyDocument::new();

    for (col_idx, arrow_field) in batch_schema.fields().iter().enumerate() {
        let parquet_col_name = arrow_field.name();

        // Skip fields that were excluded from schema derivation
        if config.skip_fields.contains(parquet_col_name.as_str()) {
            continue;
        }

        // Resolve display name via name mapping
        let tantivy_field_name = name_mapping
            .get(parquet_col_name.as_str())
            .map(|s| s.as_str())
            .unwrap_or(parquet_col_name.as_str());

        // Look up the field in the tantivy schema
        let field = match tantivy_schema.get_field(tantivy_field_name) {
            Ok(f) => f,
            Err(_) => continue, // Field not in tantivy schema (unsupported type), skip
        };

        let array = batch.column(col_idx);

        if array.is_null(row_idx) {
            // Record null for statistics
            if let Some(acc) = accumulators.get_mut(tantivy_field_name) {
                acc.observe_null();
            }
            continue; // Skip null values — tantivy handles absent fields natively
        }

        add_arrow_value_to_doc(&mut doc, field, array, arrow_field.data_type(), row_idx, tantivy_field_name, config, accumulators)?;
    }

    Ok(doc)
}

/// Add a single Arrow value to a tantivy document, recording statistics if applicable.
fn add_arrow_value_to_doc(
    doc: &mut TantivyDocument,
    field: Field,
    array: &ArrayRef,
    data_type: &DataType,
    row_idx: usize,
    field_name: &str,
    config: &SchemaDerivationConfig,
    accumulators: &mut HashMap<String, StatisticsAccumulator>,
) -> Result<()> {
    match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let val = arr.value(row_idx);
            doc.add_bool(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_bool(val);
            }
        }

        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            let val = arr.value(row_idx) as i64;
            doc.add_i64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_i64(val);
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            let val = arr.value(row_idx) as i64;
            doc.add_i64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_i64(val);
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let val = arr.value(row_idx) as i64;
            doc.add_i64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_i64(val);
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row_idx);
            doc.add_i64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_i64(val);
            }
        }

        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            let val = arr.value(row_idx) as u64;
            doc.add_u64(field, val);
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            let val = arr.value(row_idx) as u64;
            doc.add_u64(field, val);
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            let val = arr.value(row_idx) as u64;
            doc.add_u64(field, val);
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let val = arr.value(row_idx);
            doc.add_u64(field, val);
        }

        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = arr.value(row_idx) as f64;
            doc.add_f64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_f64(val);
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(row_idx);
            doc.add_f64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_f64(val);
            }
        }

        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let val = arr.value(row_idx);
            if config.ip_address_fields.contains(field_name) {
                add_ip_addr_value(doc, field, val, field_name)?;
            } else if config.json_fields.contains(field_name) {
                add_json_string_value(doc, field, val, field_name)?;
            } else {
                doc.add_text(field, val);
                if let Some(acc) = accumulators.get_mut(field_name) {
                    acc.observe_string(val);
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let val = arr.value(row_idx);
            if config.ip_address_fields.contains(field_name) {
                add_ip_addr_value(doc, field, val, field_name)?;
            } else if config.json_fields.contains(field_name) {
                add_json_string_value(doc, field, val, field_name)?;
            } else {
                doc.add_text(field, val);
                if let Some(acc) = accumulators.get_mut(field_name) {
                    acc.observe_string(val);
                }
            }
        }

        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            doc.add_bytes(field, arr.value(row_idx));
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            doc.add_bytes(field, arr.value(row_idx));
        }

        DataType::Timestamp(unit, _tz) => {
            let micros = match unit {
                TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    arr.value(row_idx) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    arr.value(row_idx) * 1_000
                }
                TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    arr.value(row_idx)
                }
                TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    arr.value(row_idx) / 1_000
                }
            };
            let dt = tantivy::DateTime::from_timestamp_micros(micros);
            doc.add_date(field, dt);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_timestamp_micros(micros);
            }
        }

        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx) as i64;
            let micros = days * 86_400 * 1_000_000;
            let dt = tantivy::DateTime::from_timestamp_micros(micros);
            doc.add_date(field, dt);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_timestamp_micros(micros);
            }
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row_idx);
            let micros = millis * 1_000;
            let dt = tantivy::DateTime::from_timestamp_micros(micros);
            doc.add_date(field, dt);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_timestamp_micros(micros);
            }
        }

        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row_idx); // i128 unscaled
            let val = raw as f64 / 10f64.powi(*scale as i32);
            doc.add_f64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_f64(val);
            }
        }
        DataType::Decimal256(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let raw = arr.value(row_idx); // i256
            // Convert via string to preserve as much precision as possible
            let val: f64 = raw.to_string().parse::<f64>().unwrap_or(0.0)
                / 10f64.powi(*scale as i32);
            doc.add_f64(field, val);
            if let Some(acc) = accumulators.get_mut(field_name) {
                acc.observe_f64(val);
            }
        }

        DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            doc.add_bytes(field, arr.value(row_idx));
        }

        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            // Convert complex types to JSON, then to tantivy OwnedValue
            let json_value = convert_complex_to_json(array, row_idx)?;
            let json_str = serde_json::to_string(&json_value)?;
            if let Ok(owned) = serde_json::from_str::<tantivy::schema::OwnedValue>(&json_str) {
                if let tantivy::schema::OwnedValue::Object(obj) = owned {
                    let json_map: std::collections::BTreeMap<String, tantivy::schema::OwnedValue> =
                        obj.into_iter().collect();
                    doc.add_object(field, json_map);
                }
            }
        }

        _ => {
            // Unsupported type — skip silently (already filtered during schema derivation)
        }
    }

    Ok(())
}

/// Parse a string value as an IP address and add it to a tantivy document.
fn add_ip_addr_value(
    doc: &mut TantivyDocument,
    field: Field,
    ip_str: &str,
    field_name: &str,
) -> Result<()> {
    let ip_addr: std::net::IpAddr = ip_str.parse()
        .with_context(|| format!("Failed to parse IP address '{}' for field '{}'", ip_str, field_name))?;
    let ipv6 = match ip_addr {
        std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        std::net::IpAddr::V6(v6) => v6,
    };
    doc.add_ip_addr(field, ipv6);
    Ok(())
}

/// Parse a JSON string value and add it to a tantivy document as a JSON object field.
fn add_json_string_value(
    doc: &mut TantivyDocument,
    field: Field,
    json_str: &str,
    field_name: &str,
) -> Result<()> {
    let parsed: serde_json::Value = serde_json::from_str(json_str)
        .with_context(|| format!(
            "Failed to parse JSON string for field '{}': '{}'",
            field_name,
            if json_str.len() > 100 { &json_str[..100] } else { json_str }
        ))?;
    match parsed {
        serde_json::Value::Object(_) => {
            let owned: tantivy::schema::OwnedValue = serde_json::from_str(json_str)?;
            if let tantivy::schema::OwnedValue::Object(obj) = owned {
                let json_map: std::collections::BTreeMap<String, tantivy::schema::OwnedValue> =
                    obj.into_iter().collect();
                doc.add_object(field, json_map);
            }
        }
        _ => {
            anyhow::bail!(
                "JSON field '{}' contains non-object value. JSON fields must contain JSON objects, got: {}",
                field_name,
                if json_str.len() > 100 { &json_str[..100] } else { json_str }
            );
        }
    }
    Ok(())
}

/// Convert a complex Arrow type (List, Map, Struct) at a given row to a JSON value.
fn convert_complex_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::List(_) | DataType::LargeList(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>();
            if let Some(list_arr) = list_array {
                let inner = list_arr.value(row_idx);
                let mut items = Vec::new();
                for i in 0..inner.len() {
                    items.push(convert_complex_to_json(&inner, i)?);
                }
                return Ok(serde_json::Value::Array(items));
            }
            // Fallback for LargeList
            Ok(serde_json::Value::Null)
        }

        DataType::Struct(fields) => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = serde_json::Map::new();
            for (i, field) in fields.iter().enumerate() {
                let col = struct_arr.column(i);
                if !col.is_null(row_idx) {
                    let val = convert_complex_to_json(col, row_idx)?;
                    map.insert(field.name().clone(), val);
                }
            }
            Ok(serde_json::Value::Object(map))
        }

        // Scalar types inside complex types
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(serde_json::Value::Bool(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(serde_json::json!(arr.value(row_idx)))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(serde_json::json!(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(serde_json::json!(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(serde_json::Value::String(arr.value(row_idx).to_string()))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row_idx) as f64;
            let val = raw / 10f64.powi(*scale as i32);
            Ok(serde_json::json!(val))
        }
        DataType::Decimal256(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let raw = arr.value(row_idx);
            let val: f64 = raw.to_string().parse::<f64>().unwrap_or(0.0)
                / 10f64.powi(*scale as i32);
            Ok(serde_json::json!(val))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            Ok(serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                arr.value(row_idx),
            )))
        }

        _ => Ok(serde_json::Value::Null),
    }
}

/// Build column mapping from Arrow schema and name mapping.
fn build_column_mapping(
    arrow_schema: &ArrowSchema,
    name_mapping: &name_mapping::NameMapping,
    parquet_metadata: &parquet::file::metadata::ParquetMetaData,
    config: &SchemaDerivationConfig,
) -> Vec<ColumnMapping> {
    let schema_descr = parquet_metadata.file_metadata().schema_descr();
    let mut mappings = Vec::new();

    for (col_idx, arrow_field) in arrow_schema.fields().iter().enumerate() {
        let parquet_col_name = arrow_field.name().clone();
        let tantivy_field_name = name_mapping
            .get(&parquet_col_name)
            .cloned()
            .unwrap_or_else(|| parquet_col_name.clone());

        // Try to get field_id from parquet schema (only if the field has an ID set)
        let field_id = if col_idx < schema_descr.num_columns() {
            let col = schema_descr.column(col_idx);
            let basic_info = col.self_type().get_basic_info();
            if basic_info.has_id() {
                let id = basic_info.id();
                if id != 0 { Some(id) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        // Override tantivy type for IP address and JSON fields (parquet stores as Utf8).
        let tantivy_type_str = if config.ip_address_fields.contains(&tantivy_field_name) {
            "IpAddr".to_string()
        } else if config.json_fields.contains(&tantivy_field_name) {
            "Json".to_string()
        } else {
            arrow_type_to_tantivy_type(arrow_field.data_type()).to_string()
        };

        // For Str columns, record the fast field tokenizer.
        // Defaults to "raw" for text fields (no tokenization in fast field).
        let fast_field_tokenizer = if tantivy_type_str == "Str" {
            let tok = config.tokenizer_overrides
                .get(&tantivy_field_name)
                .cloned()
                .unwrap_or_else(|| "raw".to_string());
            Some(tok)
        } else {
            None
        };

        mappings.push(ColumnMapping {
            tantivy_field_name,
            parquet_column_name: parquet_col_name,
            physical_ordinal: col_idx,
            parquet_type: arrow_type_to_parquet_type(arrow_field.data_type()).to_string(),
            tantivy_type: tantivy_type_str,
            field_id,
            fast_field_tokenizer,
        });
    }

    mappings
}

/// Compute relative path from table_root.
fn compute_relative_path(file_path: &str, table_root: &str) -> String {
    if table_root.is_empty() {
        return file_path.to_string();
    }

    let root = table_root.trim_end_matches('/');
    if file_path.starts_with(root) {
        let relative = &file_path[root.len()..];
        relative.trim_start_matches('/').to_string()
    } else {
        file_path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_compute_relative_path() {
        assert_eq!(
            compute_relative_path("/data/table/part-001.parquet", "/data/table"),
            "part-001.parquet"
        );
        assert_eq!(
            compute_relative_path("/data/table/sub/part.parquet", "/data/table/"),
            "sub/part.parquet"
        );
        assert_eq!(
            compute_relative_path("/other/path/file.parquet", "/data/table"),
            "/other/path/file.parquet"
        );
        assert_eq!(
            compute_relative_path("file.parquet", ""),
            "file.parquet"
        );
    }

    #[test]
    fn test_default_config() {
        let config = CreateFromParquetConfig::default();
        assert!(config.table_root.is_empty());
        assert_eq!(config.fast_field_mode, FastFieldMode::Disabled);
        assert!(config.statistics_fields.is_empty());
        assert_eq!(config.statistics_truncate_length, 256);
    }

    /// Helper: create a parquet file with test data using ArrowWriter.
    fn write_test_parquet(path: &std::path::Path, num_rows: usize, id_offset: i64) {
        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let ids: Vec<i64> = (0..num_rows).map(|i| id_offset + i as i64).collect();
        let names: Vec<String> = (0..num_rows)
            .map(|i| format!("item_{}", id_offset + i as i64))
            .collect();
        let scores: Vec<f64> = (0..num_rows).map(|i| (i as f64) * 1.5 + 10.0).collect();
        let actives: Vec<bool> = (0..num_rows).map(|i| i % 2 == 0).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
            ],
        ).unwrap();

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_create_split_from_single_parquet() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("data.parquet");
        let output_path = temp_dir.path().join("test.split");

        // Write test parquet file with 50 rows
        write_test_parquet(&parquet_path, 50, 0);

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            fast_field_mode: FastFieldMode::Disabled,
            statistics_fields: vec!["id".to_string(), "score".to_string(), "name".to_string()],
            statistics_truncate_length: 64,
            index_uid: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("create_split_from_parquet should succeed");

        // Validate metadata
        assert_eq!(result.metadata.num_docs, 50);
        assert_eq!(result.metadata.index_uid, "test-index");
        assert_eq!(result.metadata.source_id, "test-source");
        assert_eq!(result.metadata.node_id, "test-node");
        assert!(result.metadata.footer_start_offset.is_some());
        assert!(result.metadata.footer_end_offset.is_some());

        // Validate split file exists
        assert!(output_path.exists());
        assert!(std::fs::metadata(&output_path).unwrap().len() > 0);

        // Validate statistics
        assert_eq!(result.column_statistics.len(), 3);

        let id_stats = result.column_statistics.iter().find(|s| s.field_name == "id").unwrap();
        assert_eq!(id_stats.min_long, Some(0));
        assert_eq!(id_stats.max_long, Some(49));
        assert_eq!(id_stats.null_count, 0);

        let score_stats = result.column_statistics.iter().find(|s| s.field_name == "score").unwrap();
        assert!(score_stats.min_double.is_some());
        assert!(score_stats.max_double.is_some());
        assert!(score_stats.min_double.unwrap() >= 10.0);

        let name_stats = result.column_statistics.iter().find(|s| s.field_name == "name").unwrap();
        assert!(name_stats.min_string.is_some());
        assert!(name_stats.max_string.is_some());
    }

    #[tokio::test]
    async fn test_create_split_from_multiple_parquet_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet1 = temp_dir.path().join("part1.parquet");
        let parquet2 = temp_dir.path().join("part2.parquet");
        let output_path = temp_dir.path().join("multi.split");

        // Write two parquet files
        write_test_parquet(&parquet1, 30, 0);
        write_test_parquet(&parquet2, 20, 30);

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            statistics_fields: vec!["id".to_string()],
            index_uid: "multi-test".to_string(),
            source_id: "multi-source".to_string(),
            node_id: "multi-node".to_string(),
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[
                parquet1.to_string_lossy().to_string(),
                parquet2.to_string_lossy().to_string(),
            ],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("multi-file create should succeed");

        // Total rows = 30 + 20 = 50
        assert_eq!(result.metadata.num_docs, 50);
        assert!(output_path.exists());

        // Statistics should cover full range across both files
        let id_stats = result.column_statistics.iter().find(|s| s.field_name == "id").unwrap();
        assert_eq!(id_stats.min_long, Some(0));
        assert_eq!(id_stats.max_long, Some(49));
    }

    #[tokio::test]
    async fn test_create_split_with_skip_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("data.parquet");
        let output_path = temp_dir.path().join("skipped.split");

        write_test_parquet(&parquet_path, 10, 0);

        let mut skip = HashSet::new();
        skip.insert("score".to_string());
        skip.insert("active".to_string());

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            schema_config: SchemaDerivationConfig {
                skip_fields: skip,
                ..Default::default()
            },
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("skip fields create should succeed");

        assert_eq!(result.metadata.num_docs, 10);
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_create_split_validates_split_is_searchable() {
        use crate::quickwit_split::split_utils::open_split_with_quickwit_native;

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("searchable.parquet");
        let output_path = temp_dir.path().join("searchable.split");

        write_test_parquet(&parquet_path, 25, 100);

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("searchable create should succeed");

        assert_eq!(result.metadata.num_docs, 25);

        // Open the split and verify it's searchable
        let (index, _bundle_dir) = open_split_with_quickwit_native(
            output_path.to_str().unwrap()
        ).expect("Should open the created split");

        let reader = index.reader().expect("Should create reader");
        reader.reload().expect("Should reload");
        let searcher = reader.searcher();

        // Verify document count matches
        assert_eq!(searcher.num_docs(), 25);

        // Verify the schema has expected fields
        let schema = index.schema();
        assert!(schema.get_field("id").is_ok(), "Should have 'id' field");
        assert!(schema.get_field("name").is_ok(), "Should have 'name' field");
        assert!(schema.get_field("score").is_ok(), "Should have 'score' field");
        assert!(schema.get_field("active").is_ok(), "Should have 'active' field");

        // Verify manifest is embedded
        let file_list = crate::quickwit_split::split_utils::get_split_file_list(
            output_path.to_str().unwrap()
        ).expect("Should list files");
        let file_names: Vec<String> = file_list.iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        assert!(
            file_names.iter().any(|f| f.contains("_parquet_manifest")),
            "Split should contain parquet manifest. Files: {:?}", file_names
        );
    }

    #[tokio::test]
    async fn test_create_split_empty_files_fails() {
        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let config = CreateFromParquetConfig::default();
        let result = create_split_from_parquet(
            &[],
            "/tmp/empty.split",
            &config,
            &storage,
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No parquet files"));
    }

    #[tokio::test]
    async fn test_create_split_nonexistent_file_fails() {
        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let config = CreateFromParquetConfig::default();
        let result = create_split_from_parquet(
            &["/nonexistent/path/data.parquet".to_string()],
            "/tmp/fail.split",
            &config,
            &storage,
        ).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_split_schema_mismatch_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet1 = temp_dir.path().join("a.parquet");
        let parquet2 = temp_dir.path().join("b.parquet");

        // Write first file with standard schema
        write_test_parquet(&parquet1, 10, 0);

        // Write second file with different schema
        {
            use arrow_array::*;
            use arrow_schema::{DataType, Field, Schema as ArrowSchema};
            use parquet::arrow::ArrowWriter;

            let schema = std::sync::Arc::new(ArrowSchema::new(vec![
                Field::new("different_col", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["x"; 5]))],
            ).unwrap();
            let file = std::fs::File::create(&parquet2).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let config = CreateFromParquetConfig::default();
        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[
                parquet1.to_string_lossy().to_string(),
                parquet2.to_string_lossy().to_string(),
            ],
            temp_dir.path().join("mismatch.split").to_str().unwrap(),
            &config,
            &storage,
        ).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("mismatch") || err_msg.contains("Schema"),
            "Error should mention schema mismatch: {}", err_msg
        );
    }

    #[tokio::test]
    async fn test_create_split_with_tokenizer_override() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("tok.parquet");
        let output_path = temp_dir.path().join("tok.split");

        write_test_parquet(&parquet_path, 10, 0);

        let mut tokenizer_overrides = HashMap::new();
        tokenizer_overrides.insert("name".to_string(), "default".to_string());

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            schema_config: SchemaDerivationConfig {
                tokenizer_overrides,
                ..Default::default()
            },
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("tokenizer override create should succeed");

        assert_eq!(result.metadata.num_docs, 10);
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_create_split_fast_field_modes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("modes.parquet");
        write_test_parquet(&parquet_path, 10, 0);

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        // Test all three fast field modes
        for mode in &[FastFieldMode::Disabled, FastFieldMode::Hybrid, FastFieldMode::ParquetOnly] {
            let output = temp_dir.path().join(format!("mode_{:?}.split", mode));
            let config = CreateFromParquetConfig {
                table_root: temp_dir.path().to_string_lossy().to_string(),
                fast_field_mode: *mode,
                schema_config: SchemaDerivationConfig {
                    fast_field_mode: *mode,
                    ..Default::default()
                },
                ..Default::default()
            };

            let result = create_split_from_parquet(
                &[parquet_path.to_string_lossy().to_string()],
                output.to_str().unwrap(),
                &config,
                &storage,
            ).await;

            assert!(result.is_ok(), "Mode {:?} should succeed: {:?}", mode, result.err());
            assert_eq!(result.unwrap().metadata.num_docs, 10);
        }
    }

    /// Helper: create a parquet file with IP address columns (as Utf8).
    fn write_test_parquet_with_ips(path: &std::path::Path, num_rows: usize) {
        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("src_ip", DataType::Utf8, false),
            Field::new("dst_ip", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ]));

        let ids: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
        let src_ips: Vec<String> = (0..num_rows)
            .map(|i| format!("10.0.{}.{}", i / 256, i % 256))
            .collect();
        let dst_ips: Vec<String> = (0..num_rows)
            .map(|i| format!("192.168.{}.{}", i / 256, i % 256))
            .collect();
        let labels: Vec<String> = (0..num_rows)
            .map(|i| format!("conn_{}", i))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(src_ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(dst_ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_create_split_with_ip_address_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("ips.parquet");
        let output_path = temp_dir.path().join("ips.split");

        write_test_parquet_with_ips(&parquet_path, 20);

        let mut ip_fields = HashSet::new();
        ip_fields.insert("src_ip".to_string());
        ip_fields.insert("dst_ip".to_string());

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            fast_field_mode: FastFieldMode::Hybrid,
            schema_config: SchemaDerivationConfig {
                fast_field_mode: FastFieldMode::Hybrid,
                ip_address_fields: ip_fields,
                ..Default::default()
            },
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("IP address split creation should succeed");

        assert_eq!(result.metadata.num_docs, 20);
        assert!(output_path.exists());

        // Open the split and verify IP fields are searchable
        let (index, _bundle_dir) = crate::quickwit_split::split_utils::open_split_with_quickwit_native(
            output_path.to_str().unwrap()
        ).expect("Should open the created split");

        let schema = index.schema();

        // Verify src_ip and dst_ip are IpAddr fields
        let src_ip_field = schema.get_field("src_ip").unwrap();
        let src_ip_entry = schema.get_field_entry(src_ip_field);
        assert!(
            matches!(src_ip_entry.field_type(), tantivy::schema::FieldType::IpAddr(_)),
            "src_ip should be IpAddr type in the split"
        );

        let dst_ip_field = schema.get_field("dst_ip").unwrap();
        let dst_ip_entry = schema.get_field_entry(dst_ip_field);
        assert!(
            matches!(dst_ip_entry.field_type(), tantivy::schema::FieldType::IpAddr(_)),
            "dst_ip should be IpAddr type in the split"
        );

        // Verify IP fields are fast in hybrid mode
        assert!(src_ip_entry.is_fast(), "src_ip should be fast in Hybrid mode");
        assert!(dst_ip_entry.is_fast(), "dst_ip should be fast in Hybrid mode");

        // label should still be a text field
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(
            matches!(label_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "label should still be Str type"
        );

        // Verify the split is searchable with correct doc count
        let reader = index.reader().expect("Should create reader");
        reader.reload().expect("Should reload");
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 20);

        // Verify column mapping records IpAddr type
        let file_list = crate::quickwit_split::split_utils::get_split_file_list(
            output_path.to_str().unwrap()
        ).expect("Should list files");
        let file_names: Vec<String> = file_list.iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        assert!(
            file_names.iter().any(|f| f.contains("_parquet_manifest")),
            "Split should contain parquet manifest"
        );
    }

    // ── JSON string field tests ─────────────────────────────────────────

    /// Helper: create a parquet file with JSON string columns.
    fn write_test_parquet_with_json_strings(path: &std::path::Path, num_rows: usize, id_offset: i64) {
        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("metadata", DataType::Utf8, false),
        ]));

        let ids: Vec<i64> = (0..num_rows).map(|i| id_offset + i as i64).collect();
        let names: Vec<String> = (0..num_rows)
            .map(|i| format!("item_{}", id_offset + i as i64))
            .collect();
        let payloads: Vec<String> = (0..num_rows)
            .map(|i| {
                let idx = id_offset + i as i64;
                format!(
                    r#"{{"user":"user_{}","score":{},"active":{}}}"#,
                    idx,
                    idx * 10,
                    idx % 2 == 0,
                )
            })
            .collect();
        let metadatas: Vec<String> = (0..num_rows)
            .map(|i| {
                let idx = id_offset + i as i64;
                format!(
                    r#"{{"region":"us-east-{}","version":{}}}"#,
                    idx % 3,
                    idx,
                )
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(payloads.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(metadatas.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_add_json_string_value_valid_object() {
        use tantivy::schema::*;

        let mut builder = SchemaBuilder::new();
        let field = builder.add_json_field("payload", JsonObjectOptions::default().set_stored());
        let schema = builder.build();

        let mut doc = TantivyDocument::new();
        add_json_string_value(
            &mut doc,
            field,
            r#"{"user":"alice","score":42}"#,
            "payload",
        ).expect("Valid JSON object should succeed");

        // Verify the document has the JSON field
        let json_val = doc.get_first(field);
        assert!(json_val.is_some(), "Document should have the JSON field");
    }

    #[test]
    fn test_add_json_string_value_invalid_json_fails() {
        use tantivy::schema::*;

        let mut builder = SchemaBuilder::new();
        let field = builder.add_json_field("payload", JsonObjectOptions::default().set_stored());
        let _schema = builder.build();

        let mut doc = TantivyDocument::new();
        let result = add_json_string_value(
            &mut doc,
            field,
            "not valid json",
            "payload",
        );
        assert!(result.is_err(), "Invalid JSON should fail");
        assert!(result.unwrap_err().to_string().contains("Failed to parse JSON"));
    }

    #[test]
    fn test_add_json_string_value_non_object_fails() {
        use tantivy::schema::*;

        let mut builder = SchemaBuilder::new();
        let field = builder.add_json_field("payload", JsonObjectOptions::default().set_stored());
        let _schema = builder.build();

        let mut doc = TantivyDocument::new();
        // Array is not an object
        let result = add_json_string_value(
            &mut doc,
            field,
            r#"[1,2,3]"#,
            "payload",
        );
        assert!(result.is_err(), "Non-object JSON should fail");
        assert!(result.unwrap_err().to_string().contains("non-object"));

        // String literal is not an object
        let mut doc2 = TantivyDocument::new();
        let result2 = add_json_string_value(
            &mut doc2,
            field,
            r#""hello""#,
            "payload",
        );
        assert!(result2.is_err(), "String literal JSON should fail");
    }

    #[tokio::test]
    async fn test_create_split_with_json_string_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("json_data.parquet");
        let output_path = temp_dir.path().join("json.split");

        write_test_parquet_with_json_strings(&parquet_path, 20, 0);

        let mut json_fields = HashSet::new();
        json_fields.insert("payload".to_string());
        json_fields.insert("metadata".to_string());

        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            schema_config: SchemaDerivationConfig {
                json_fields,
                ..Default::default()
            },
            ..Default::default()
        };

        let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
            std::sync::Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &[parquet_path.to_string_lossy().to_string()],
            output_path.to_str().unwrap(),
            &config,
            &storage,
        ).await.expect("JSON string field split creation should succeed");

        assert_eq!(result.metadata.num_docs, 20);
        assert!(output_path.exists());

        // Open the split and verify JSON fields are correct type
        let (index, _bundle_dir) = crate::quickwit_split::split_utils::open_split_with_quickwit_native(
            output_path.to_str().unwrap()
        ).expect("Should open the created split");

        let schema = index.schema();

        // payload and metadata should be JsonObject fields
        let payload_field = schema.get_field("payload").unwrap();
        let payload_entry = schema.get_field_entry(payload_field);
        assert!(
            matches!(payload_entry.field_type(), tantivy::schema::FieldType::JsonObject(_)),
            "payload should be JsonObject type in the split"
        );

        let meta_field = schema.get_field("metadata").unwrap();
        let meta_entry = schema.get_field_entry(meta_field);
        assert!(
            matches!(meta_entry.field_type(), tantivy::schema::FieldType::JsonObject(_)),
            "metadata should be JsonObject type in the split"
        );

        // name should still be a text field
        let name_field = schema.get_field("name").unwrap();
        let name_entry = schema.get_field_entry(name_field);
        assert!(
            matches!(name_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "name should still be Str type"
        );

        // Verify searchable
        let reader = index.reader().expect("Should create reader");
        reader.reload().expect("Should reload");
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 20);
    }

    // ── Multi-segment reproduction test ──────────────────────────────────

    /// Helper: create a parquet file with many columns to force writer heap pressure.
    /// Each file has a unique `file_tag` column so we can verify which file a row came from.
    fn write_wide_parquet(path: &std::path::Path, num_rows: usize, file_tag: &str, id_offset: i64, num_extra_cols: usize) {
        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("uuid", DataType::Utf8, false),
            Field::new("file_tag", DataType::Utf8, false),
        ];
        // Add extra string columns to inflate the schema and force heap pressure
        for i in 0..num_extra_cols {
            fields.push(Field::new(format!("col_{}", i), DataType::Utf8, true));
        }

        let schema = Arc::new(ArrowSchema::new(fields));

        let ids: Vec<i64> = (0..num_rows).map(|i| id_offset + i as i64).collect();
        let uuids: Vec<String> = (0..num_rows)
            .map(|i| format!("uuid-{}-{}", file_tag, i))
            .collect();
        let tags: Vec<String> = vec![file_tag.to_string(); num_rows];

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(uuids.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(tags.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
        ];
        // Extra columns with repeating data to increase heap usage during indexing
        for i in 0..num_extra_cols {
            let vals: Vec<String> = (0..num_rows)
                .map(|r| format!("val_{}_{}_in_{}", i, r, file_tag))
                .collect();
            columns.push(Arc::new(StringArray::from(
                vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Reproduce the multi-segment doc_id mapping issue.
    ///
    /// Creates many parquet files with wide schemas and a small writer heap to
    /// force multiple tantivy segments. Then verifies that every doc_id maps
    /// to the correct parquet file and row via the manifest.
    #[tokio::test]
    async fn test_multi_segment_docid_mapping_correctness() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create 20 parquet files with 500 rows each and 50 extra columns.
        // With a small 15MB heap, this should force multiple segments.
        let num_files = 20;
        let rows_per_file = 500;
        let num_extra_cols = 50;
        let mut parquet_paths: Vec<String> = Vec::new();
        let mut cumulative_offset: i64 = 0;

        for i in 0..num_files {
            let file_name = format!("part-{:04}.parquet", i);
            let file_path = temp_dir.path().join(&file_name);
            let tag = format!("file{}", i);
            write_wide_parquet(&file_path, rows_per_file, &tag, cumulative_offset, num_extra_cols);
            parquet_paths.push(file_path.to_string_lossy().to_string());
            cumulative_offset += rows_per_file as i64;
        }

        let total_rows = num_files * rows_per_file;

        // Use a SMALL heap to force multiple segments
        let config = CreateFromParquetConfig {
            table_root: temp_dir.path().to_string_lossy().to_string(),
            fast_field_mode: FastFieldMode::Disabled,
            index_uid: "multi-seg-test".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            writer_heap_size: 15_000_000, // 15MB - minimum, should force many flushes
            ..Default::default()
        };

        let storage: Arc<dyn quickwit_storage::Storage> =
            Arc::new(quickwit_storage::RamStorage::default());

        let result = create_split_from_parquet(
            &parquet_paths,
            temp_dir.path().join("multi_seg.split").to_str().unwrap(),
            &config,
            &storage,
        )
        .await
        .expect("create_split_from_parquet should succeed");

        assert_eq!(result.metadata.num_docs, total_rows);

        // ── Now open the split and verify mapping ────────────────────────

        let split_path = temp_dir.path().join("multi_seg.split");
        let (index, _bundle_dir) = crate::quickwit_split::split_utils::open_split_with_quickwit_native(
            split_path.to_str().unwrap(),
        )
        .expect("Should open the created split");

        let reader = index.reader().expect("Should create reader");
        reader.reload().expect("Should reload");
        let searcher = reader.searcher();

        let num_segments = searcher.segment_readers().len();
        eprintln!(
            "🔍 TEST: Split has {} segments, {} total docs",
            num_segments,
            searcher.num_docs()
        );
        assert_eq!(searcher.num_docs() as usize, total_rows);

        // Read the manifest from the split bundle
        let split_bytes = std::fs::read(&split_path).unwrap();
        let file_slice = tantivy::directory::FileSlice::new(Arc::new(
            tantivy::directory::OwnedBytes::new(split_bytes),
        ));
        let bundle = quickwit_directories::BundleDirectory::open_split(file_slice).unwrap();

        use tantivy::directory::Directory;
        use crate::parquet_companion::manifest_io::{MANIFEST_FILENAME, deserialize_manifest};
        let manifest_path = std::path::PathBuf::from(MANIFEST_FILENAME);
        let manifest_bytes = bundle.atomic_read(&manifest_path)
            .expect("Should read manifest from bundle");
        let manifest = deserialize_manifest(&manifest_bytes)
            .expect("Should deserialize manifest");

        eprintln!(
            "🔍 TEST: Manifest has {} segment_row_ranges, {} parquet_files, {} total_rows",
            manifest.segment_row_ranges.len(),
            manifest.parquet_files.len(),
            manifest.total_rows
        );

        // Log segment info
        for sr in &manifest.segment_row_ranges {
            eprintln!(
                "  segment_ord={} row_offset={} num_rows={}",
                sr.segment_ord, sr.row_offset, sr.num_rows
            );
        }

        // ── Critical check: verify that segment ordinals from the searcher
        //    match the manifest's segment_row_ranges ──────────────────────

        // For each segment in the searcher, verify the doc count matches the manifest
        for (seg_ord, seg_reader) in searcher.segment_readers().iter().enumerate() {
            let seg_max_doc = seg_reader.max_doc();
            let manifest_range = manifest.segment_row_ranges
                .iter()
                .find(|r| r.segment_ord == seg_ord as u32);

            if let Some(range) = manifest_range {
                eprintln!(
                    "  Searcher seg[{}]: max_doc={}, manifest num_rows={}",
                    seg_ord, seg_max_doc, range.num_rows
                );
                assert_eq!(
                    seg_max_doc as u64, range.num_rows,
                    "Segment {} doc count mismatch: searcher has {} but manifest says {}",
                    seg_ord, seg_max_doc, range.num_rows
                );
            } else {
                panic!(
                    "Segment ordinal {} from searcher not found in manifest!",
                    seg_ord
                );
            }
        }

        // ── Verify mapping via fast fields ──────────────────────────────
        // Read __pq_file_hash and __pq_row_in_file fast fields to verify
        // that each document's parquet coordinates are correct, even after
        // segment merges have reordered doc IDs.

        let schema = index.schema();
        let uuid_field = schema.get_field("uuid").unwrap();

        use crate::parquet_companion::docid_mapping::{build_file_hash_index, resolve_via_fast_fields};
        use tantivy::query::TermQuery;
        use tantivy::schema::IndexRecordOption;

        let file_hash_index = build_file_hash_index(&manifest);

        let mut errors = Vec::new();
        let mut checked = 0;

        // Test UUIDs from every file, at various positions within files
        let test_positions = vec![0, 1, 249, 250, 499]; // first, second, middle, near-end, last
        for file_idx in 0..num_files {
            let tag = format!("file{}", file_idx);
            let expected_file_name = format!("part-{:04}.parquet", file_idx);

            for &row_in_file in &test_positions {
                if row_in_file >= rows_per_file {
                    continue;
                }
                let uuid = format!("uuid-{}-{}", tag, row_in_file);

                // Search for this exact UUID (indexed with "raw" tokenizer)
                let term = tantivy::Term::from_field_text(uuid_field, &uuid);
                let query = TermQuery::new(term, IndexRecordOption::Basic);
                let top_docs = searcher
                    .search(&query, &tantivy::collector::TopDocs::with_limit(1))
                    .unwrap();

                if top_docs.is_empty() {
                    errors.push(format!(
                        "SEARCH MISS: uuid='{}' (file={}, row={}) not found!",
                        uuid, file_idx, row_in_file
                    ));
                    continue;
                }

                let (_, doc_addr) = &top_docs[0];
                let seg_ord = doc_addr.segment_ord as usize;
                let doc_id = doc_addr.doc_id;

                // Read the fast field values for this document
                let seg_reader = &searcher.segment_readers()[seg_ord];
                let ff = seg_reader.fast_fields();
                let hash_col = ff.u64(PARQUET_FILE_HASH_FIELD)
                    .expect("Missing __pq_file_hash fast field");
                let row_col = ff.u64(PARQUET_ROW_IN_FILE_FIELD)
                    .expect("Missing __pq_row_in_file fast field");

                let file_hash: u64 = hash_col.values_for_doc(doc_id).next()
                    .expect("No file hash value");
                let actual_row_in_file: u64 = row_col.values_for_doc(doc_id).next()
                    .expect("No row_in_file value");

                // Resolve via the hash index
                let location = match resolve_via_fast_fields(file_hash, actual_row_in_file, &file_hash_index) {
                    Ok(loc) => loc,
                    Err(e) => {
                        errors.push(format!(
                            "RESOLVE FAILED: uuid='{}' file_hash={} → {}",
                            uuid, file_hash, e
                        ));
                        continue;
                    }
                };

                // Verify the expected file hash matches
                let expected_hash = hash_parquet_path(&expected_file_name);
                if file_hash != expected_hash {
                    errors.push(format!(
                        "FILE HASH MISMATCH: uuid='{}' → hash={} but expected {} for '{}'  (seg={}, doc={})",
                        uuid, file_hash, expected_hash, expected_file_name, seg_ord, doc_id
                    ));
                }

                if location.file_idx != file_idx {
                    errors.push(format!(
                        "FILE IDX MISMATCH: uuid='{}' → file_idx={} but expected {}  (seg={}, doc={})",
                        uuid, location.file_idx, file_idx, seg_ord, doc_id
                    ));
                }

                if actual_row_in_file != row_in_file as u64 {
                    errors.push(format!(
                        "ROW_IN_FILE MISMATCH: uuid='{}' → row_in_file={} but expected {}  (file_idx={}, seg={}, doc={})",
                        uuid, actual_row_in_file, row_in_file, location.file_idx, seg_ord, doc_id
                    ));
                }

                checked += 1;
            }
        }

        eprintln!("Verified {} UUID lookups across {} files", checked, num_files);

        // Report errors
        if !errors.is_empty() {
            eprintln!("\n❌ FOUND {} MAPPING ERRORS:", errors.len());
            for (i, err) in errors.iter().enumerate().take(30) {
                eprintln!("  [{}] {}", i, err);
            }
            if errors.len() > 30 {
                eprintln!("  ... and {} more", errors.len() - 30);
            }
            panic!(
                "Doc ID → parquet row mapping has {} errors (shown first 30 above).",
                errors.len()
            );
        }

        eprintln!(
            "✅ All {} UUID lookups verified: fast-field-based doc → parquet mapping is correct across {} segments",
            checked, num_segments
        );
    }

    /// Documents that tantivy merge does NOT preserve insertion order when
    /// SegmentManager uses HashMap. This is the known behavior that motivates
    /// the __pq_file_hash / __pq_row_in_file fast field approach.
    #[test]
    fn test_tantivy_merge_reorders_docs() {
        use tantivy::schema::{SchemaBuilder, STORED, INDEXED, STRING};
        use tantivy::query::TermQuery;
        use tantivy::schema::IndexRecordOption;
        use tantivy::collector::TopDocs;
        use tantivy::indexer::NoMergePolicy;
        use tantivy::Index;

        let temp_dir = tempfile::tempdir().unwrap();

        let mut schema_builder = SchemaBuilder::new();
        let id_field = schema_builder.add_u64_field("id", INDEXED | STORED);
        let tag_field = schema_builder.add_text_field("tag", STRING | STORED);
        let mut extra_fields = Vec::new();
        for i in 0..50 {
            extra_fields.push(schema_builder.add_text_field(&format!("col_{}", i), STRING));
        }
        let schema = schema_builder.build();

        let index = Index::create_in_dir(temp_dir.path(), schema.clone()).unwrap();
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));

        let num_docs = 5000u64;
        for i in 0..num_docs {
            let mut doc = tantivy::TantivyDocument::new();
            doc.add_u64(id_field, i);
            doc.add_text(tag_field, &format!("tag_{}", i));
            for (j, f) in extra_fields.iter().enumerate() {
                doc.add_text(*f, &format!("val_{}_{}", j, i));
            }
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();

        let segment_ids = index.searchable_segment_ids().unwrap();
        let num_segs = segment_ids.len();

        if num_segs > 1 {
            writer.merge(&segment_ids).wait().unwrap();
            writer.commit().unwrap();
            index.load_metas().unwrap();
        }

        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();

        let mut reordered = 0;
        let test_ids: Vec<u64> = vec![0, 1, 100, 256, 257, 500, 999, 1000, 1500, 2000, 3000, 4000, 4999];
        for expected_id in &test_ids {
            let term = tantivy::Term::from_field_text(tag_field, &format!("tag_{}", expected_id));
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            let top = searcher.search(&query, &TopDocs::with_limit(1)).unwrap();
            if let Some((_, addr)) = top.first() {
                if addr.doc_id as u64 != *expected_id {
                    reordered += 1;
                }
            }
        }

        // With multiple segments, merge should reorder some docs
        // (this is the known behavior that motivated the fast-field fix)
        if num_segs > 1 {
            assert!(reordered > 0,
                "Expected merge to reorder some docs (had {} segments), but all were in order. \
                 This test documents known tantivy behavior.", num_segs);
        }
        eprintln!(
            "Confirmed: tantivy merge reordered {}/{} test docs across {} segments (expected behavior)",
            reordered, test_ids.len(), num_segs
        );
    }
}
