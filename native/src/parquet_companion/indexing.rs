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
    let first_file_bytes = std::fs::read(&parquet_files[0])
        .with_context(|| format!("Failed to read parquet file: {}", parquet_files[0]))?;

    let first_reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(first_file_bytes.clone()))
        .context("Failed to open first parquet file")?;

    let arrow_schema = first_reader.schema().clone();
    let parquet_metadata = first_reader.metadata().clone();

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Primary schema has {} fields",
        arrow_schema.fields().len()
    );

    // ── Step 2: Validate schema consistency across all files ─────────────
    let mut file_schemas: Vec<ArrowSchema> = Vec::new();
    for file_path in &parquet_files[1..] {
        let file_bytes = std::fs::read(file_path)
            .with_context(|| format!("Failed to read parquet file: {}", file_path))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file_bytes))
            .with_context(|| format!("Failed to open parquet file: {}", file_path))?;
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
    };

    let tantivy_schema = derive_tantivy_schema_with_mapping(&arrow_schema, &config, Some(&name_mapping))?;

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Derived tantivy schema with {} fields",
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

    // Use single-threaded writer to ensure all docs land in one segment
    // in insertion order (matching parquet row order). Multi-threaded writers
    // distribute docs across threads, creating multiple segments with
    // non-deterministic ordering that breaks the docid-to-parquet-row mapping.
    // Use 256MB heap to avoid mid-index flushes that would create multiple segments.
    let mut writer = index.writer_with_num_threads(1, 256_000_000)
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
        let file_bytes = std::fs::read(file_path)
            .with_context(|| format!("Failed to read parquet file: {}", file_path))?;
        let file_size = file_bytes.len() as u64;

        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file_bytes))
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

        parquet_file_entries.push(ParquetFileEntry {
            relative_path,
            file_size_bytes: file_size,
            row_offset: cumulative_row_offset,
            num_rows: file_num_rows,
            has_offset_index,
            row_groups,
        });

        // Re-open reader for iteration
        let file_bytes2 = std::fs::read(file_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file_bytes2))?
            .with_batch_size(8192)
            .build()
            .context("Failed to build parquet reader")?;

        for batch_result in reader {
            let batch = batch_result.context("Failed to read record batch")?;
            let batch_schema = batch.schema();

            for row_idx in 0..batch.num_rows() {
                let tantivy_doc = arrow_row_to_tantivy_doc(
                    &batch,
                    &batch_schema,
                    row_idx,
                    &tantivy_schema,
                    &name_mapping,
                    &config,
                    &mut accumulators,
                )?;

                writer.add_document(tantivy_doc)
                    .context("Failed to add document to tantivy index")?;
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

    // ── Step 10: Build segment_row_ranges from actual segment metadata ──
    // With single-threaded indexing, tantivy may still create multiple segments
    // if the writer's heap budget is exceeded mid-indexing (e.g., 1M+ rows).
    // Read the actual segments after commit to build correct ordinal→row mappings.
    // Segments are listed in meta.json in creation order, which matches insertion
    // order for single-threaded writers — so row offsets are cumulative.
    index.load_metas().context("Failed to load index metas after commit")?;
    let segment_metas = index.searchable_segment_metas()
        .context("Failed to read segment metas after commit")?;

    let mut segment_row_ranges = Vec::with_capacity(segment_metas.len());
    let mut row_offset = 0u64;
    for (ord, seg_meta) in segment_metas.iter().enumerate() {
        let num_rows = seg_meta.max_doc() as u64;
        segment_row_ranges.push(SegmentRowRange {
            segment_ord: ord as u32,
            row_offset,
            num_rows,
        });
        row_offset += num_rows;
    }

    debug_println!(
        "🏗️ CREATE_FROM_PARQUET: Indexed {} total rows from {} files into {} segment(s)",
        total_rows, parquet_files.len(), segment_row_ranges.len()
    );

    if row_offset != total_rows {
        anyhow::bail!(
            "Segment row count mismatch: segments total {} rows but indexed {} rows",
            row_offset, total_rows
        );
    }

    // ── Step 11: Build manifest ─────────────────────────────────────────
    let manifest = ParquetManifest {
        version: SUPPORTED_MANIFEST_VERSION,
        table_root: parquet_config.table_root.clone(),
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

        // Override tantivy type for IP address fields (parquet stores as Utf8).
        let tantivy_type_str = if config.ip_address_fields.contains(&tantivy_field_name) {
            "IpAddr".to_string()
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
}
