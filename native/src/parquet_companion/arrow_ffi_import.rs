// arrow_ffi_import.rs - Arrow FFI import for standard Quickwit split creation
//
// Receives Arrow columnar batches via the Arrow C Data Interface (FFI) from the JVM
// and creates standard Quickwit splits with stored documents. This enables Spark to
// stream ColumnarBatch data directly into split creation, bypassing row-at-a-time JNI
// overhead and parquet round-trips.
//
// Supports multi-partition writes: full batches cross the FFI boundary with partition
// columns included. Rust extracts partition values per-row, groups them, and routes
// to per-partition IndexWriters internally.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{RecordBatch, StructArray, Array, StringArray, BooleanArray, Int32Array, Int64Array, Float64Array};
use arrow_array::types::TimestampMicrosecondType;
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy::schema::{Schema as TantivySchema, Field};
use uuid::Uuid;

use crate::debug_println;
use super::indexing::{arrow_row_to_tantivy_doc, add_arrow_value_to_doc, add_string_value_to_doc, convert_complex_to_json};
use super::manifest::FastFieldMode;
use super::name_mapping::NameMapping;
use super::schema_derivation::{SchemaDerivationConfig, derive_tantivy_schema_with_mapping};
use super::statistics::{StatisticsAccumulator, ColumnStatisticsResult};
use super::string_indexing::StringIndexingMode;
use crate::quickwit_split::{
    SplitConfig, QuickwitSplitMetadata, FooterOffsets,
    default_split_config, create_quickwit_split,
};
use crate::quickwit_split::json_discovery::extract_doc_mapping_from_index;

/// Context for streaming Arrow FFI split creation.
/// Wrapped in Arc<Mutex<>> at the JNI layer (IndexWriter is !Sync).
///
/// Supports multi-partition writes: full batches cross FFI with partition columns
/// included. Rust extracts partition values, groups rows, and routes to
/// per-partition IndexWriters internally.
pub(crate) struct ArrowFfiSplitContext {
    tantivy_schema: TantivySchema,
    arrow_schema: SchemaRef,
    /// Maps non-partition Arrow column indices to tantivy fields.
    field_mapping: Vec<FieldMapping>,
    /// Partition column indices in the Arrow schema (empty = non-partitioned)
    partition_col_indices: Vec<usize>,
    /// Partition column names (parallel to partition_col_indices)
    partition_col_names: Vec<String>,
    /// Per-partition writers: partition_key (e.g. "event_date=2023-01-15") → PartitionWriter
    partition_writers: HashMap<String, PartitionWriter>,
    /// For non-partitioned tables: single writer (partition_writers is empty)
    default_writer: Option<PartitionWriter>,
    heap_size: usize,
    total_doc_count: u64,
    total_batch_count: u64,
    /// Schema derivation config (field type overrides, tokenizers, etc.)
    /// Stored here so add_arrow_batch can use the same config as schema derivation.
    schema_config: SchemaDerivationConfig,
    /// Max docs per split (0 = unlimited). When a partition writer reaches this threshold
    /// during add_arrow_batch, it is automatically rolled (finalized and replaced).
    /// Rolled splits are accumulated in `rolled_splits` and returned by finishAllSplits.
    max_docs_per_split: u64,
    /// Output directory for auto-rolled splits (set by first call to addArrowBatch when
    /// max_docs_per_split > 0, or by finishAllSplits).
    output_dir: Option<String>,
    /// Splits that were auto-rolled during addArrowBatch due to max_docs_per_split threshold.
    pub(crate) rolled_splits: Vec<PartitionSplitResult>,
    /// Column names for which to compute min/max statistics (empty = none).
    /// Populated from the "stats" flag in fieldConfigJson.
    stats_columns: std::collections::HashSet<String>,
}

/// Per-partition state: each partition gets its own Index + IndexWriter + temp dir.
struct PartitionWriter {
    index: Index,
    writer: IndexWriter,
    index_dir: tempfile::TempDir,
    doc_count: u64,
    /// Partition column values for this partition (col_name → string value)
    partition_values: HashMap<String, String>,
    /// Per-column statistics accumulators (populated when stats_columns is non-empty)
    accumulators: HashMap<String, StatisticsAccumulator>,
}

struct FieldMapping {
    arrow_col_idx: usize,
    tantivy_field: Field,
    field_name: String,
    #[allow(dead_code)]
    data_type: DataType,
}

/// Result for one partition's finalized split
pub(crate) struct PartitionSplitResult {
    /// Partition key (e.g. "event_date=2023-01-15/region=us"), empty for non-partitioned
    pub partition_key: String,
    /// Partition column values
    pub partition_values: HashMap<String, String>,
    /// Path where the split was written
    pub split_path: String,
    /// Split metadata
    pub metadata: QuickwitSplitMetadata,
    /// Per-column min values (column name → string representation). Empty if stats not enabled.
    pub min_values: HashMap<String, String>,
    /// Per-column max values (column name → string representation). Empty if stats not enabled.
    pub max_values: HashMap<String, String>,
}

/// Import an Arrow schema from an FFI pointer, taking ownership.
/// Uses the Comet `std::ptr::replace` pattern for safe ownership transfer.
pub(crate) fn import_arrow_schema(schema_ptr: *mut FFI_ArrowSchema) -> Result<ArrowSchema> {
    if schema_ptr.is_null() {
        bail!("Null FFI_ArrowSchema pointer");
    }
    let ffi_schema = unsafe { std::ptr::replace(schema_ptr, FFI_ArrowSchema::empty()) };
    let arrow_schema = ArrowSchema::try_from(&ffi_schema)
        .context("Failed to import Arrow schema from FFI")?;
    Ok(arrow_schema)
}

/// Import an Arrow RecordBatch from FFI pointers, taking ownership.
/// Uses the Comet `std::ptr::replace` pattern for safe ownership transfer.
pub(crate) fn import_arrow_batch(
    array_ptr: *mut FFI_ArrowArray,
    schema_ptr: *mut FFI_ArrowSchema,
) -> Result<RecordBatch> {
    if array_ptr.is_null() || schema_ptr.is_null() {
        bail!("Null FFI pointer: array={} schema={}", array_ptr.is_null(), schema_ptr.is_null());
    }
    let ffi_array = unsafe { std::ptr::replace(array_ptr, FFI_ArrowArray::empty()) };
    let ffi_schema = unsafe { std::ptr::replace(schema_ptr, FFI_ArrowSchema::empty()) };
    let array_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }
        .context("Failed to import Arrow array from FFI")?;
    let mut array_data = array_data;
    // Safety: Java-allocated Arrow buffers may not be 64-byte aligned
    array_data.align_buffers();
    let struct_array = StructArray::from(array_data);
    Ok(RecordBatch::from(struct_array))
}

/// Parse a field configuration JSON array into a SchemaDerivationConfig.
///
/// Expected format:
/// ```json
/// [
///   {"name": "id", "type": "i64", "fast": true, "indexed": true, "stored": true},
///   {"name": "content", "type": "text", "tokenizer": "default", "record": "position"},
///   {"name": "metadata", "type": "json"},
///   {"name": "ip_addr", "type": "ip", "fast": true}
/// ]
/// ```
///
/// Type overrides applied to SchemaDerivationConfig:
/// - "json" → json_fields
/// - "ip" → ip_address_fields
/// - "text" with tokenizer → tokenizer_overrides (defaults to "default" if no tokenizer specified)
/// - "text" with tokenizer "raw" → no override (raw is already the default)
/// Result of parsing field config JSON: schema derivation config + stats column set.
pub(crate) struct FieldConfigParseResult {
    pub config: SchemaDerivationConfig,
    /// Column names that have "stats": true. Empty if no stats requested.
    pub stats_columns: std::collections::HashSet<String>,
}

pub(crate) fn parse_field_config_json(json_str: &str) -> Result<FieldConfigParseResult> {
    let fields: Vec<serde_json::Value> = serde_json::from_str(json_str)
        .context("Failed to parse field config JSON array")?;

    let mut config = SchemaDerivationConfig {
        fast_field_mode: FastFieldMode::Disabled, // All fields get native fast access
        store_fields: true,                       // Standard splits: docs stored in tantivy
        ..Default::default()
    };
    let mut stats_columns = std::collections::HashSet::new();

    for field_obj in &fields {
        let name = field_obj.get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Field config entry missing 'name': {}", field_obj))?;

        let field_type = field_obj.get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Check "stats" flag (defaults to false)
        if field_obj.get("stats").and_then(|v| v.as_bool()).unwrap_or(false) {
            stats_columns.insert(name.to_string());
        }

        match field_type {
            "json" => {
                config.json_fields.insert(name.to_string());
            }
            "ip" => {
                config.ip_address_fields.insert(name.to_string());
            }
            "text" => {
                let tokenizer = field_obj.get("tokenizer")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                if tokenizer != "raw" {
                    config.tokenizer_overrides.insert(name.to_string(), tokenizer.to_string());
                }
            }
            _ => {}
        }
    }

    debug_println!(
        "ARROW_FFI_IMPORT: Parsed field config: {} json fields, {} ip fields, {} tokenizer overrides, {} stats columns",
        config.json_fields.len(), config.ip_address_fields.len(), config.tokenizer_overrides.len(), stats_columns.len()
    );

    Ok(FieldConfigParseResult { config, stats_columns })
}

/// Begin creating splits from Arrow columnar data.
///
/// Accepts the full Arrow schema (including partition columns) + list of partition column names.
/// Identifies partition columns in the schema; builds field_mapping for non-partition columns only.
/// Derives tantivy schema with STORED flag set on all fields (standard split behavior).
///
/// If `field_config_json` is provided, it overrides the default Arrow→Tantivy type mapping
/// for specific fields (e.g. Utf8 → JSON, Utf8 → IP, tokenizer selection for text fields).
pub(crate) fn begin_split_from_arrow(
    arrow_schema: ArrowSchema,
    partition_col_names: &[String],
    heap_size: usize,
    field_config_json: Option<&str>,
    max_docs_per_split: u64,
    output_dir: Option<String>,
) -> Result<ArrowFfiSplitContext> {
    // Parse field config if provided, otherwise use defaults
    let (config, stats_columns) = match field_config_json {
        Some(json) if !json.is_empty() => {
            let parsed = parse_field_config_json(json)?;
            (parsed.config, parsed.stats_columns)
        }
        _ => (SchemaDerivationConfig {
            fast_field_mode: FastFieldMode::Disabled,
            store_fields: true,
            ..Default::default()
        }, std::collections::HashSet::new()),
    };

    // Identify partition column indices
    let mut partition_col_indices = Vec::new();
    for name in partition_col_names {
        let idx = arrow_schema.fields().iter().position(|f| f.name() == name)
            .ok_or_else(|| anyhow::anyhow!(
                "Partition column '{}' not found in Arrow schema. Available: {:?}",
                name,
                arrow_schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>()
            ))?;
        partition_col_indices.push(idx);
    }
    let partition_set: std::collections::HashSet<usize> = partition_col_indices.iter().copied().collect();

    // Build Arrow schema for non-partition columns only (for tantivy schema derivation)
    let non_partition_fields: Vec<_> = arrow_schema.fields().iter()
        .enumerate()
        .filter(|(i, _)| !partition_set.contains(i))
        .map(|(_, f)| f.clone())
        .collect();
    let non_partition_schema = ArrowSchema::new(non_partition_fields);

    // Derive tantivy schema using the (possibly overridden) config
    let tantivy_schema = derive_tantivy_schema_with_mapping(&non_partition_schema, &config, None)?;

    // Build field mapping: Arrow column index → tantivy field
    let mut field_mapping = Vec::new();
    for (arrow_col_idx, arrow_field) in arrow_schema.fields().iter().enumerate() {
        if partition_set.contains(&arrow_col_idx) {
            continue; // Skip partition columns
        }
        let field_name = arrow_field.name().as_str();
        if let Ok(tantivy_field) = tantivy_schema.get_field(field_name) {
            field_mapping.push(FieldMapping {
                arrow_col_idx,
                tantivy_field,
                field_name: field_name.to_string(),
                data_type: arrow_field.data_type().clone(),
            });
        }
    }

    let arrow_schema_ref = std::sync::Arc::new(arrow_schema);

    // For non-partitioned tables, create single writer immediately
    let default_writer = if partition_col_names.is_empty() {
        Some(create_partition_writer(&tantivy_schema, heap_size, HashMap::new(), &stats_columns, &field_mapping)?)
    } else {
        None
    };

    Ok(ArrowFfiSplitContext {
        tantivy_schema,
        arrow_schema: arrow_schema_ref,
        field_mapping,
        partition_col_indices,
        partition_col_names: partition_col_names.to_vec(),
        partition_writers: HashMap::new(),
        default_writer,
        heap_size,
        total_doc_count: 0,
        total_batch_count: 0,
        schema_config: config,
        max_docs_per_split,
        output_dir,
        rolled_splits: Vec::new(),
        stats_columns,
    })
}

/// Create a new PartitionWriter with its own temp dir, Index, and single-threaded IndexWriter.
/// If `stats_columns` is non-empty, initializes StatisticsAccumulators for each eligible column.
fn create_partition_writer(
    tantivy_schema: &TantivySchema,
    heap_size: usize,
    partition_values: HashMap<String, String>,
    stats_columns: &std::collections::HashSet<String>,
    field_mapping: &[FieldMapping],
) -> Result<PartitionWriter> {
    let index_dir = tempfile::tempdir()
        .context("Failed to create temp directory for partition writer")?;

    let index = Index::create_in_dir(index_dir.path(), tantivy_schema.clone())
        .context("Failed to create tantivy index for partition")?;

    // Use tantivy's default tokenizer manager which includes "raw", "default", etc.
    // Do NOT set Quickwit's fast field normalizer manager here — it doesn't include
    // standard tokenizers needed for text field indexing.

    // Single-threaded writer ensures deterministic doc ordering matching batch insertion order
    let writer = index.writer_with_num_threads(1, heap_size)
        .context("Failed to create index writer for partition")?;

    // Initialize statistics accumulators for requested columns
    let mut accumulators = HashMap::new();
    if !stats_columns.is_empty() {
        for mapping in field_mapping {
            if stats_columns.contains(&mapping.field_name) {
                let field_type = match &mapping.data_type {
                    DataType::Int32 | DataType::Int64 => "i64",
                    DataType::Float32 | DataType::Float64 => "f64",
                    DataType::Boolean => "bool",
                    DataType::Utf8 | DataType::LargeUtf8 => "text",
                    DataType::Date32 => "datetime",
                    DataType::Timestamp(_, _) => "datetime",
                    _ => continue, // Skip non-eligible types
                };
                // truncate_length=0 means no truncation at the native layer.
                // The JVM side applies StatisticsTruncation before writing to
                // the transaction log (default: 32 chars, configurable).
                accumulators.insert(
                    mapping.field_name.clone(),
                    StatisticsAccumulator::new(&mapping.field_name, field_type, 0),
                );
            }
        }
    }

    Ok(PartitionWriter {
        index,
        writer,
        index_dir,
        doc_count: 0,
        partition_values,
        accumulators,
    })
}

/// Extract a string representation of a partition column value at a given row.
fn extract_partition_value(batch: &RecordBatch, col_idx: usize, row_idx: usize) -> Result<String> {
    let array = batch.column(col_idx);
    if array.is_null(row_idx) {
        return Ok("__HIVE_DEFAULT_PARTITION__".to_string());
    }
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected StringArray for partition column"))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Int32Array for partition column"))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Int64Array for partition column"))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray for partition column"))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for partition column"))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Date32 => {
            // Date32 stores days since Unix epoch (1970-01-01) as i32 → convert to YYYY-MM-DD
            let arr = array.as_any().downcast_ref::<arrow_array::Date32Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Date32Array for partition column"))?;
            let days = arr.value(row_idx);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(days as i64);
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            // Timestamp(μs) stores microseconds since epoch as i64 → convert to YYYY-MM-DD HH:MM:SS
            let arr = array.as_any().downcast_ref::<arrow_array::PrimitiveArray<TimestampMicrosecondType>>()
                .ok_or_else(|| anyhow::anyhow!("Expected TimestampMicrosecondArray for partition column"))?;
            let micros = arr.value(row_idx);
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp microseconds: {}", micros))?;
            Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array.as_any().downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::TimestampMillisecondType>>()
                .ok_or_else(|| anyhow::anyhow!("Expected TimestampMillisecondArray for partition column"))?;
            let millis = arr.value(row_idx);
            let secs = millis / 1_000;
            let nanos = ((millis % 1_000) * 1_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp milliseconds: {}", millis))?;
            Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array.as_any().downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::TimestampSecondType>>()
                .ok_or_else(|| anyhow::anyhow!("Expected TimestampSecondArray for partition column"))?;
            let secs = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp seconds: {}", secs))?;
            Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array.as_any().downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::TimestampNanosecondType>>()
                .ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray for partition column"))?;
            let nanos = arr.value(row_idx);
            let secs = nanos / 1_000_000_000;
            let sub_nanos = (nanos % 1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, sub_nanos)
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp nanoseconds: {}", nanos))?;
            Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        dt => bail!("Unsupported partition column type: {:?}", dt),
    }
}

/// Build Hive-style partition key from column values.
/// e.g., "event_date=2023-01-15/region=us"
fn build_partition_key(col_names: &[String], values: &[String]) -> String {
    col_names.iter().zip(values.iter())
        .map(|(name, value)| format!("{}={}", name, value))
        .collect::<Vec<_>>()
        .join("/")
}

/// Add a batch of Arrow columnar data. Rows are automatically routed to the correct
/// partition writer based on partition column values in the batch.
///
/// If `max_docs_per_split > 0` and a partition writer reaches the threshold after inserting
/// a row, it is automatically rolled: finalized into a split file and replaced with a fresh
/// writer. Rolled splits accumulate in `ctx.rolled_splits` and are included in
/// `finish_all_splits` results.
///
/// Returns cumulative total doc count across all partitions.
pub(crate) async fn add_arrow_batch(ctx: &mut ArrowFfiSplitContext, batch: &RecordBatch) -> Result<u64> {
    // Validate schema matches
    let batch_schema = batch.schema();
    if batch_schema.fields().len() != ctx.arrow_schema.fields().len() {
        bail!(
            "Schema mismatch: expected {} columns, got {}",
            ctx.arrow_schema.fields().len(),
            batch_schema.fields().len()
        );
    }

    // Use the config from context (populated from field_config_json if provided)
    let name_mapping: NameMapping = HashMap::new();
    let schema_derivation_config = &ctx.schema_config;
    let string_hash_fields: HashMap<String, String> = HashMap::new();
    let string_indexing_modes: HashMap<String, StringIndexingMode> = HashMap::new();
    let compiled_regexes: HashMap<String, regex::Regex> = HashMap::new();

    let num_rows = batch.num_rows();

    let max_docs = ctx.max_docs_per_split;
    let split_config = default_split_config("arrow-ffi", "arrow-ffi-source", "arrow-ffi-node");

    if ctx.partition_col_indices.is_empty() {
        // Non-partitioned path: add all rows to default writer
        for row_idx in 0..num_rows {
            let pw = ctx.default_writer.as_mut()
                .ok_or_else(|| anyhow::anyhow!("No default writer for non-partitioned context"))?;

            let doc = build_doc_from_arrow_row(
                batch,
                row_idx,
                &ctx.field_mapping,
                &ctx.tantivy_schema,
                &name_mapping,
                schema_derivation_config,
                &string_hash_fields,
                &mut pw.accumulators,
                &string_indexing_modes,
                &compiled_regexes,
            )?;
            pw.writer.add_document(doc)?;
            pw.doc_count += 1;

            // Auto-roll if threshold reached
            if max_docs > 0 && pw.doc_count >= max_docs {
                let output_dir = ctx.output_dir.as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "max_docs_per_split is set but output_dir was not provided to beginSplitFromArrow"
                    ))?.clone();
                let rolled_pw = ctx.default_writer.take().unwrap();
                let result = finalize_partition_writer_into_split(
                    rolled_pw, "", HashMap::new(), &output_dir, &split_config,
                ).await?;
                debug_println!(
                    "ARROW_FFI_IMPORT: Auto-rolled non-partitioned split with {} docs",
                    result.metadata.num_docs
                );
                ctx.rolled_splits.push(result);
                ctx.default_writer = Some(create_partition_writer(
                    &ctx.tantivy_schema, ctx.heap_size, HashMap::new(),
                    &ctx.stats_columns, &ctx.field_mapping)?);
            }
        }
    } else {
        // Partitioned path: route each row to correct partition writer
        for row_idx in 0..num_rows {
            // Extract partition column values
            let mut partition_values_vec = Vec::with_capacity(ctx.partition_col_indices.len());
            for &col_idx in &ctx.partition_col_indices {
                partition_values_vec.push(extract_partition_value(batch, col_idx, row_idx)?);
            }
            let partition_key = build_partition_key(&ctx.partition_col_names, &partition_values_vec);

            // Get or lazily create partition writer
            if !ctx.partition_writers.contains_key(&partition_key) {
                let mut partition_values_map = HashMap::new();
                for (name, value) in ctx.partition_col_names.iter().zip(partition_values_vec.iter()) {
                    partition_values_map.insert(name.clone(), value.clone());
                }
                let pw = create_partition_writer(&ctx.tantivy_schema, ctx.heap_size, partition_values_map,
                    &ctx.stats_columns, &ctx.field_mapping)?;
                ctx.partition_writers.insert(partition_key.clone(), pw);
                debug_println!("ARROW_FFI_IMPORT: Created new partition writer for '{}'", partition_key);
            }

            {
                let pw = ctx.partition_writers.get_mut(&partition_key).unwrap();
                let doc = build_doc_from_arrow_row(
                    batch,
                    row_idx,
                    &ctx.field_mapping,
                    &ctx.tantivy_schema,
                    &name_mapping,
                    schema_derivation_config,
                    &string_hash_fields,
                    &mut pw.accumulators,
                    &string_indexing_modes,
                    &compiled_regexes,
                )?;
                pw.writer.add_document(doc)?;
                pw.doc_count += 1;
            }

            // Auto-roll if threshold reached (borrow of pw is dropped above)
            if max_docs > 0 {
                let should_roll = ctx.partition_writers.get(&partition_key)
                    .map(|pw| pw.doc_count >= max_docs)
                    .unwrap_or(false);
                if should_roll {
                    if let Some(output_dir) = ctx.output_dir.as_ref() {
                        let output_dir = output_dir.clone();
                        let rolled_pw = ctx.partition_writers.remove(&partition_key).unwrap();
                        let partition_values = rolled_pw.partition_values.clone();
                        let result = finalize_partition_writer_into_split(
                            rolled_pw, &partition_key, partition_values.clone(),
                            &output_dir, &split_config,
                        ).await?;
                        debug_println!(
                            "ARROW_FFI_IMPORT: Auto-rolled partition '{}' with {} docs",
                            partition_key, result.metadata.num_docs
                        );
                        ctx.rolled_splits.push(result);
                        let new_pw = create_partition_writer(
                            &ctx.tantivy_schema, ctx.heap_size, partition_values,
                            &ctx.stats_columns, &ctx.field_mapping)?;
                        ctx.partition_writers.insert(partition_key.clone(), new_pw);
                    }
                }
            }
        }
    }

    ctx.total_doc_count += num_rows as u64;
    ctx.total_batch_count += 1;
    Ok(ctx.total_doc_count)
}

/// Build a TantivyDocument from a single Arrow row using the field_mapping.
/// Only non-partition columns are included.
#[allow(clippy::too_many_arguments)]
fn build_doc_from_arrow_row(
    batch: &RecordBatch,
    row_idx: usize,
    field_mapping: &[FieldMapping],
    tantivy_schema: &TantivySchema,
    _name_mapping: &NameMapping,
    config: &SchemaDerivationConfig,
    string_hash_fields: &HashMap<String, String>,
    accumulators: &mut HashMap<String, StatisticsAccumulator>,
    string_indexing_modes: &HashMap<String, StringIndexingMode>,
    compiled_regexes: &HashMap<String, regex::Regex>,
) -> Result<TantivyDocument> {
    let mut doc = TantivyDocument::new();

    for mapping in field_mapping {
        let array = batch.column(mapping.arrow_col_idx);
        if array.is_null(row_idx) {
            continue; // Skip null values — tantivy handles absent fields natively
        }

        add_arrow_value_to_doc(
            &mut doc,
            mapping.tantivy_field,
            array,
            array.data_type(),
            row_idx,
            &mapping.field_name,
            config,
            string_hash_fields,
            tantivy_schema,
            accumulators,
            string_indexing_modes,
            compiled_regexes,
        )?;
    }

    Ok(doc)
}

/// Finalize a single PartitionWriter into a split file. Shared helper used by
/// both auto-rolling in add_arrow_batch and finish_all_splits.
async fn finalize_partition_writer_into_split(
    mut pw: PartitionWriter,
    partition_key: &str,
    partition_values: HashMap<String, String>,
    output_dir: &str,
    split_config: &SplitConfig,
) -> Result<PartitionSplitResult> {
    pw.writer.commit()
        .context("Failed to commit index writer")?;
    pw.writer.wait_merging_threads()
        .map_err(|e| anyhow::anyhow!("Failed waiting for merge threads: {}", e))?;

    let split_filename = format!("part-{}.split", Uuid::new_v4());
    let output_base = PathBuf::from(output_dir);
    let output_path = if partition_key.is_empty() {
        output_base.join(&split_filename)
    } else {
        let partition_dir = output_base.join(partition_key);
        std::fs::create_dir_all(&partition_dir)
            .context("Failed to create partition output directory")?;
        partition_dir.join(&split_filename)
    };

    let reader = pw.index.reader()
        .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
    reader.reload()
        .map_err(|e| anyhow::anyhow!("Failed to reload index reader: {}", e))?;
    let num_docs = reader.searcher().num_docs() as usize;

    let index_dir_path = pw.index_dir.path().to_path_buf();
    let mut total_size = 0u64;
    if let Ok(entries) = std::fs::read_dir(&index_dir_path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total_size += meta.len();
                }
            }
        }
    }

    let mut metadata = QuickwitSplitMetadata {
        split_id: Uuid::new_v4().to_string(),
        index_uid: split_config.index_uid.clone(),
        source_id: split_config.source_id.clone(),
        node_id: split_config.node_id.clone(),
        doc_mapping_uid: split_config.doc_mapping_uid.clone(),
        partition_id: split_config.partition_id,
        num_docs,
        uncompressed_docs_size_in_bytes: total_size,
        time_range: None,
        create_timestamp: chrono::Utc::now().timestamp(),
        maturity: "Mature".to_string(),
        tags: split_config.tags.clone(),
        delete_opstamp: 0,
        num_merge_ops: 0,
        footer_start_offset: None,
        footer_end_offset: None,
        hotcache_start_offset: None,
        hotcache_length: None,
        doc_mapping_json: None,
        skipped_splits: Vec::new(),
    };

    if let Ok(doc_mapping_json) = extract_doc_mapping_from_index(&pw.index) {
        metadata.doc_mapping_json = Some(doc_mapping_json);
    }

    let footer = create_quickwit_split(
        &pw.index, &index_dir_path, &output_path, &metadata, split_config, None,
    ).await?;

    metadata.footer_start_offset = Some(footer.footer_start_offset);
    metadata.footer_end_offset = Some(footer.footer_end_offset);
    metadata.hotcache_start_offset = Some(footer.hotcache_start_offset);
    metadata.hotcache_length = Some(footer.hotcache_length);

    // Finalize statistics accumulators into string min/max maps
    let mut min_values = HashMap::new();
    let mut max_values = HashMap::new();
    for (col_name, acc) in pw.accumulators.drain() {
        let stats = acc.finalize();
        // Convert typed min/max to string representation
        if let Some(v) = stats.min_long {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.min_double {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = &stats.min_string {
            min_values.insert(col_name.clone(), v.clone());
        } else if let Some(v) = stats.min_timestamp_micros {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.min_bool {
            min_values.insert(col_name.clone(), v.to_string());
        }

        if let Some(v) = stats.max_long {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.max_double {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = &stats.max_string {
            max_values.insert(col_name.clone(), v.clone());
        } else if let Some(v) = stats.max_timestamp_micros {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.max_bool {
            max_values.insert(col_name.clone(), v.to_string());
        }
    }

    debug_println!(
        "ARROW_FFI_IMPORT: Finalized partition '{}' with {} docs → {} (stats: {} min, {} max)",
        partition_key, num_docs, output_path.display(), min_values.len(), max_values.len()
    );

    Ok(PartitionSplitResult {
        partition_key: partition_key.to_string(),
        partition_values,
        split_path: output_path.to_string_lossy().to_string(),
        metadata,
        min_values,
        max_values,
    })
}

/// Finalize ALL partition splits, writing each to output_dir.
/// For partitioned tables, creates output_dir/partition_key/part-uuid.split for each partition.
/// For non-partitioned, creates output_dir/part-uuid.split.
///
/// Returns a vector of results — one per partition (or one for non-partitioned).
pub(crate) async fn finish_all_splits(
    mut ctx: ArrowFfiSplitContext,
    output_dir: &str,
    split_config: &SplitConfig,
) -> Result<Vec<PartitionSplitResult>> {
    // Start with any splits that were auto-rolled during addArrowBatch
    let mut results = std::mem::take(&mut ctx.rolled_splits);

    // Collect all remaining writers to finalize
    let writers: Vec<(String, HashMap<String, String>, PartitionWriter)> = if let Some(pw) = ctx.default_writer.take() {
        vec![("".to_string(), HashMap::new(), pw)]
    } else {
        ctx.partition_writers.drain()
            .map(|(key, pw)| {
                let values = pw.partition_values.clone();
                (key, values, pw)
            })
            .collect()
    };

    for (partition_key, partition_values, pw) in writers {
        // Skip empty writers (can happen after auto-roll if no more rows were added)
        if pw.doc_count == 0 {
            continue;
        }
        let result = finalize_partition_writer_into_split(
            pw, &partition_key, partition_values, output_dir, split_config,
        ).await?;
        results.push(result);
    }

    Ok(results)
}

/// Roll (finalize) one partition's current split and start a new writer for it.
///
/// For non-partitioned tables, pass an empty string as partition_key.
/// This enables `maxRowsPerSplit` support: when a partition reaches the row limit,
/// roll it to produce a split file and continue writing into a fresh index.
///
/// Returns the finalized split's result. The context continues to accept batches.
pub(crate) async fn roll_partition_split(
    ctx: &mut ArrowFfiSplitContext,
    partition_key: &str,
    output_dir: &str,
    split_config: &SplitConfig,
) -> Result<PartitionSplitResult> {
    let output_base = PathBuf::from(output_dir);

    // Take the writer out of the context
    let (pw, partition_values) = if partition_key.is_empty() {
        // Non-partitioned: take default_writer
        let pw = ctx.default_writer.take()
            .ok_or_else(|| anyhow::anyhow!("No default writer to roll (non-partitioned)"))?;
        (pw, HashMap::new())
    } else {
        // Partitioned: remove from partition_writers
        let pw = ctx.partition_writers.remove(partition_key)
            .ok_or_else(|| anyhow::anyhow!(
                "No writer for partition key '{}'. Available: {:?}",
                partition_key, ctx.partition_writers.keys().collect::<Vec<_>>()
            ))?;
        let values = pw.partition_values.clone();
        (pw, values)
    };

    // Finalize the writer
    let mut pw = pw;
    pw.writer.commit()
        .context("Failed to commit index writer during roll")?;
    pw.writer.wait_merging_threads()
        .map_err(|e| anyhow::anyhow!("Failed waiting for merge threads during roll: {}", e))?;

    // Construct output path
    let split_filename = format!("part-{}.split", Uuid::new_v4());
    let output_path = if partition_key.is_empty() {
        output_base.join(&split_filename)
    } else {
        let partition_dir = output_base.join(partition_key);
        std::fs::create_dir_all(&partition_dir)
            .context("Failed to create partition output directory during roll")?;
        partition_dir.join(&split_filename)
    };

    // Get doc count
    let reader = pw.index.reader()
        .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
    reader.reload()
        .map_err(|e| anyhow::anyhow!("Failed to reload index reader: {}", e))?;
    let num_docs = reader.searcher().num_docs() as usize;

    // Calculate uncompressed size
    let index_dir_path = pw.index_dir.path().to_path_buf();
    let mut total_size = 0u64;
    if let Ok(entries) = std::fs::read_dir(&index_dir_path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total_size += meta.len();
                }
            }
        }
    }

    // Create split metadata
    let mut metadata = QuickwitSplitMetadata {
        split_id: Uuid::new_v4().to_string(),
        index_uid: split_config.index_uid.clone(),
        source_id: split_config.source_id.clone(),
        node_id: split_config.node_id.clone(),
        doc_mapping_uid: split_config.doc_mapping_uid.clone(),
        partition_id: split_config.partition_id,
        num_docs,
        uncompressed_docs_size_in_bytes: total_size,
        time_range: None,
        create_timestamp: chrono::Utc::now().timestamp(),
        maturity: "Mature".to_string(),
        tags: split_config.tags.clone(),
        delete_opstamp: 0,
        num_merge_ops: 0,
        footer_start_offset: None,
        footer_end_offset: None,
        hotcache_start_offset: None,
        hotcache_length: None,
        doc_mapping_json: None,
        skipped_splits: Vec::new(),
    };

    if let Ok(doc_mapping_json) = extract_doc_mapping_from_index(&pw.index) {
        metadata.doc_mapping_json = Some(doc_mapping_json);
    }

    let footer = create_quickwit_split(
        &pw.index,
        &index_dir_path,
        &output_path,
        &metadata,
        split_config,
        None,
    ).await?;

    metadata.footer_start_offset = Some(footer.footer_start_offset);
    metadata.footer_end_offset = Some(footer.footer_end_offset);
    metadata.hotcache_start_offset = Some(footer.hotcache_start_offset);
    metadata.hotcache_length = Some(footer.hotcache_length);

    // Finalize statistics accumulators from the rolled writer
    let mut min_values = HashMap::new();
    let mut max_values = HashMap::new();
    for (col_name, acc) in pw.accumulators.drain() {
        let stats = acc.finalize();
        if let Some(v) = stats.min_long {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.min_double {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = &stats.min_string {
            min_values.insert(col_name.clone(), v.clone());
        } else if let Some(v) = stats.min_timestamp_micros {
            min_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.min_bool {
            min_values.insert(col_name.clone(), v.to_string());
        }
        if let Some(v) = stats.max_long {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.max_double {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = &stats.max_string {
            max_values.insert(col_name.clone(), v.clone());
        } else if let Some(v) = stats.max_timestamp_micros {
            max_values.insert(col_name.clone(), v.to_string());
        } else if let Some(v) = stats.max_bool {
            max_values.insert(col_name.clone(), v.to_string());
        }
    }

    debug_println!(
        "ARROW_FFI_IMPORT: Rolled partition '{}' with {} docs → {}",
        partition_key, num_docs, output_path.display()
    );

    // Create a fresh writer for this partition so writing can continue
    if partition_key.is_empty() {
        ctx.default_writer = Some(create_partition_writer(
            &ctx.tantivy_schema, ctx.heap_size, HashMap::new(),
            &ctx.stats_columns, &ctx.field_mapping)?);
    } else {
        let new_pw = create_partition_writer(
            &ctx.tantivy_schema, ctx.heap_size, partition_values.clone(),
            &ctx.stats_columns, &ctx.field_mapping)?;
        ctx.partition_writers.insert(partition_key.to_string(), new_pw);
    }

    Ok(PartitionSplitResult {
        partition_key: partition_key.to_string(),
        partition_values,
        split_path: output_path.to_string_lossy().to_string(),
        metadata,
        min_values,
        max_values,
    })
}

/// Cancel an in-progress split creation, releasing ALL resources across all partitions.
/// Drops the context — cleans up ALL partition writers' temp dirs and indices.
pub(crate) fn cancel_split(ctx: ArrowFfiSplitContext) {
    // Simply dropping the context cleans up:
    // - All PartitionWriter temp dirs (via TempDir drop)
    // - All IndexWriter resources
    // - All Index resources
    drop(ctx);
    debug_println!("ARROW_FFI_IMPORT: Cancelled split creation, all resources released");
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, Float64Array, StringArray, BooleanArray, RecordBatch};
    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, DataType};
    use std::sync::Arc;

    /// Helper: create a simple Arrow schema with id, name, score, active fields.
    fn test_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("score", DataType::Float64, true),
            ArrowField::new("active", DataType::Boolean, true),
        ])
    }

    /// Helper: create a RecordBatch with test data.
    fn test_batch(num_rows: usize, id_offset: i64) -> RecordBatch {
        let ids: Vec<i64> = (id_offset..id_offset + num_rows as i64).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i + id_offset as usize)).collect();
        let scores: Vec<f64> = (0..num_rows).map(|i| (i as f64) * 1.5 + id_offset as f64).collect();
        let actives: Vec<bool> = (0..num_rows).map(|i| i % 2 == 0).collect();

        RecordBatch::try_new(
            Arc::new(test_schema()),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
            ],
        ).unwrap()
    }

    /// Helper: create a partitioned Arrow schema with event_date as partition column.
    fn partitioned_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("event_date", DataType::Utf8, false), // partition column
        ])
    }

    /// Helper: create a partitioned batch with event_date values.
    fn partitioned_batch(ids: Vec<i64>, names: Vec<&str>, dates: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(partitioned_schema()),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(dates)),
            ],
        ).unwrap()
    }

    // --- Non-partitioned tests ---

    #[test]
    fn test_begin_creates_schema() {
        let schema = test_schema();
        let ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

        // Verify tantivy schema has all 4 fields
        assert!(ctx.tantivy_schema.get_field("id").is_ok());
        assert!(ctx.tantivy_schema.get_field("name").is_ok());
        assert!(ctx.tantivy_schema.get_field("score").is_ok());
        assert!(ctx.tantivy_schema.get_field("active").is_ok());

        // Verify field_mapping has 4 entries (no partition columns)
        assert_eq!(ctx.field_mapping.len(), 4);

        // Verify STORED flag is set (check via schema introspection)
        let id_field = ctx.tantivy_schema.get_field("id").unwrap();
        let id_entry = ctx.tantivy_schema.get_field_entry(id_field);
        assert!(id_entry.is_stored(), "id field should be stored for standard splits");

        // Verify default writer exists (non-partitioned)
        assert!(ctx.default_writer.is_some());
        assert!(ctx.partition_writers.is_empty());

        cancel_split(ctx);
    }

    #[test]
    fn test_add_single_batch() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            let batch = test_batch(10, 0);
            let count = add_arrow_batch(&mut ctx, &batch).await.unwrap();
            assert_eq!(count, 10);
            assert_eq!(ctx.total_batch_count, 1);

            cancel_split(ctx);
        });
    }

    #[test]
    fn test_add_multiple_batches() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            let batch1 = test_batch(100, 0);
            let batch2 = test_batch(100, 100);
            let batch3 = test_batch(100, 200);

            let count1 = add_arrow_batch(&mut ctx, &batch1).await.unwrap();
            assert_eq!(count1, 100);

            let count2 = add_arrow_batch(&mut ctx, &batch2).await.unwrap();
            assert_eq!(count2, 200);

            let count3 = add_arrow_batch(&mut ctx, &batch3).await.unwrap();
            assert_eq!(count3, 300);

            assert_eq!(ctx.total_batch_count, 3);

            cancel_split(ctx);
        });
    }

    #[test]
    fn test_finish_creates_valid_split() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            let batch = test_batch(50, 0);
            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test-index", "test-source", "test-node");

            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 1);
            let result = &results[0];
            assert!(result.partition_key.is_empty());
            assert_eq!(result.metadata.num_docs, 50);
            assert!(std::path::Path::new(&result.split_path).exists());
        });
    }

    #[test]
    fn test_cancel_cleans_up() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            // Record the temp dir path before cancel
            let temp_path = ctx.default_writer.as_ref().unwrap().index_dir.path().to_path_buf();
            assert!(temp_path.exists());

            let batch = test_batch(10, 0);
            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            cancel_split(ctx);

            // Temp dir should be cleaned up
            assert!(!temp_path.exists());
        });
    }

    #[test]
    fn test_schema_mismatch_rejected() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            // Create a batch with different number of columns
            let wrong_schema = ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int64, false),
                ArrowField::new("name", DataType::Utf8, false),
            ]);
            let wrong_batch = RecordBatch::try_new(
                Arc::new(wrong_schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["test"])),
                ],
            ).unwrap();

            let result = add_arrow_batch(&mut ctx, &wrong_batch).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("Schema mismatch"));

            cancel_split(ctx);
        });
    }

    #[test]
    fn test_all_field_types() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = test_schema();
            let mut ctx = begin_split_from_arrow(schema, &[], 50_000_000, None, 0, None).unwrap();

            // Create batch with all supported types
            let batch = RecordBatch::try_new(
                Arc::new(test_schema()),
                vec![
                    Arc::new(Int64Array::from(vec![42i64])),
                    Arc::new(StringArray::from(vec!["hello world"])),
                    Arc::new(Float64Array::from(vec![3.14])),
                    Arc::new(BooleanArray::from(vec![true])),
                ],
            ).unwrap();

            let count = add_arrow_batch(&mut ctx, &batch).await.unwrap();
            assert_eq!(count, 1);

            cancel_split(ctx);
        });
    }

    // --- Partitioned tests ---

    #[test]
    fn test_partitioned_single_partition() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = partitioned_schema();
            let partition_cols = vec!["event_date".to_string()];
            let mut ctx = begin_split_from_arrow(schema, &partition_cols, 50_000_000, None, 0, None).unwrap();

            // All rows have same partition value
            let batch = partitioned_batch(
                vec![1, 2, 3],
                vec!["a", "b", "c"],
                vec!["2023-01-15", "2023-01-15", "2023-01-15"],
            );

            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test-index", "test-source", "test-node");

            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].partition_key, "event_date=2023-01-15");
            assert_eq!(results[0].metadata.num_docs, 3);
            assert_eq!(results[0].partition_values.get("event_date").unwrap(), "2023-01-15");
        });
    }

    #[test]
    fn test_partitioned_multiple_partitions() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = partitioned_schema();
            let partition_cols = vec!["event_date".to_string()];
            let mut ctx = begin_split_from_arrow(schema, &partition_cols, 50_000_000, None, 0, None).unwrap();

            // 3 distinct partition values
            let batch = partitioned_batch(
                vec![1, 2, 3, 4, 5, 6],
                vec!["a", "b", "c", "d", "e", "f"],
                vec!["2023-01-15", "2023-01-16", "2023-01-17", "2023-01-15", "2023-01-16", "2023-01-17"],
            );

            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test-index", "test-source", "test-node");

            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 3);

            // Each partition should have 2 docs
            for result in &results {
                assert_eq!(result.metadata.num_docs, 2);
            }
        });
    }

    #[test]
    fn test_partitioned_across_batches() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = partitioned_schema();
            let partition_cols = vec!["event_date".to_string()];
            let mut ctx = begin_split_from_arrow(schema, &partition_cols, 50_000_000, None, 0, None).unwrap();

            // Batch 1: partition A rows
            let batch1 = partitioned_batch(
                vec![1, 2],
                vec!["a", "b"],
                vec!["2023-01-15", "2023-01-15"],
            );
            add_arrow_batch(&mut ctx, &batch1).await.unwrap();

            // Batch 2: partition B rows
            let batch2 = partitioned_batch(
                vec![3, 4],
                vec!["c", "d"],
                vec!["2023-01-16", "2023-01-16"],
            );
            add_arrow_batch(&mut ctx, &batch2).await.unwrap();

            // Batch 3: more partition A rows
            let batch3 = partitioned_batch(
                vec![5],
                vec!["e"],
                vec!["2023-01-15"],
            );
            add_arrow_batch(&mut ctx, &batch3).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test-index", "test-source", "test-node");

            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 2);

            let mut doc_counts: HashMap<String, usize> = HashMap::new();
            for result in &results {
                doc_counts.insert(result.partition_key.clone(), result.metadata.num_docs);
            }

            // Partition A (2023-01-15) should have 3 docs (from batches 1 and 3)
            assert_eq!(doc_counts.get("event_date=2023-01-15"), Some(&3));
            // Partition B (2023-01-16) should have 2 docs (from batch 2)
            assert_eq!(doc_counts.get("event_date=2023-01-16"), Some(&2));
        });
    }

    #[test]
    fn test_partition_columns_excluded_from_index() {
        let schema = partitioned_schema();
        let partition_cols = vec!["event_date".to_string()];
        let ctx = begin_split_from_arrow(schema, &partition_cols, 50_000_000, None, 0, None).unwrap();

        // event_date should NOT be in the tantivy schema (it's a partition column)
        assert!(ctx.tantivy_schema.get_field("event_date").is_err());

        // id and name should be in the tantivy schema
        assert!(ctx.tantivy_schema.get_field("id").is_ok());
        assert!(ctx.tantivy_schema.get_field("name").is_ok());

        // field_mapping should have 2 entries (excluding event_date)
        assert_eq!(ctx.field_mapping.len(), 2);

        // partition_col_indices should have 1 entry
        assert_eq!(ctx.partition_col_indices.len(), 1);
        assert_eq!(ctx.partition_col_indices[0], 2); // event_date is at index 2

        cancel_split(ctx);
    }

    #[test]
    fn test_multi_level_partitioning() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int64, false),
                ArrowField::new("event_date", DataType::Utf8, false),
                ArrowField::new("region", DataType::Utf8, false),
            ]);
            let partition_cols = vec!["event_date".to_string(), "region".to_string()];
            let mut ctx = begin_split_from_arrow(schema, &partition_cols, 50_000_000, None, 0, None).unwrap();

            let batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    ArrowField::new("id", DataType::Int64, false),
                    ArrowField::new("event_date", DataType::Utf8, false),
                    ArrowField::new("region", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["2023-01-15", "2023-01-15", "2023-01-16"])),
                    Arc::new(StringArray::from(vec!["us", "eu", "us"])),
                ],
            ).unwrap();

            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test-index", "test-source", "test-node");

            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            // 3 distinct partition combinations
            assert_eq!(results.len(), 3);

            let keys: Vec<&str> = results.iter().map(|r| r.partition_key.as_str()).collect();
            assert!(keys.contains(&"event_date=2023-01-15/region=us"));
            assert!(keys.contains(&"event_date=2023-01-15/region=eu"));
            assert!(keys.contains(&"event_date=2023-01-16/region=us"));
        });
    }

    #[test]
    fn test_statistics_basic() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let schema = test_schema();
            let field_config = r#"[
                {"name": "id", "type": "i64", "stats": true},
                {"name": "name", "type": "text", "tokenizer": "raw", "stats": true},
                {"name": "score", "type": "f64", "stats": true},
                {"name": "active", "type": "bool", "stats": true}
            ]"#;

            let mut ctx = begin_split_from_arrow(
                schema, &[], 50_000_000, Some(field_config), 0, None,
            ).unwrap();

            // Add batch: ids 0..5, names name_0..name_4, scores 0.0..6.0, active alternating
            let batch = test_batch(5, 0);
            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            // Add batch: ids 10..15
            let batch2 = test_batch(5, 10);
            add_arrow_batch(&mut ctx, &batch2).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test", "test-source", "test-node");
            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 1);
            let result = &results[0];

            // Verify min/max statistics
            assert_eq!(result.min_values.get("id"), Some(&"0".to_string()));
            assert_eq!(result.max_values.get("id"), Some(&"14".to_string()));

            assert_eq!(result.min_values.get("score"), Some(&"0".to_string()));
            // max score: batch1 has 0,1.5,3,4.5,6; batch2 has 10,11.5,13,14.5,16
            assert_eq!(result.max_values.get("score"), Some(&"16".to_string()));

            // name_0 < name_10 < name_14 etc (lexicographic)
            assert!(result.min_values.contains_key("name"));
            assert!(result.max_values.contains_key("name"));

            // bool: false and true both present
            assert_eq!(result.min_values.get("active"), Some(&"false".to_string()));
            assert_eq!(result.max_values.get("active"), Some(&"true".to_string()));
        });
    }

    #[test]
    fn test_statistics_not_enabled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let schema = test_schema();
            // No stats flags
            let mut ctx = begin_split_from_arrow(
                schema, &[], 50_000_000, None, 0, None,
            ).unwrap();

            let batch = test_batch(5, 0);
            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test", "test-source", "test-node");
            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 1);
            // No statistics when not enabled
            assert!(results[0].min_values.is_empty());
            assert!(results[0].max_values.is_empty());
        });
    }

    #[test]
    fn test_statistics_partitioned() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let schema = partitioned_schema();
            let field_config = r#"[
                {"name": "id", "type": "i64", "stats": true},
                {"name": "name", "type": "text", "tokenizer": "raw", "stats": true}
            ]"#;

            let mut ctx = begin_split_from_arrow(
                schema,
                &["event_date".to_string()],
                50_000_000,
                Some(field_config),
                0, None,
            ).unwrap();

            let batch = partitioned_batch(
                vec![1, 2, 3, 4],
                vec!["alice", "bob", "charlie", "dave"],
                vec!["2023-01-15", "2023-01-15", "2023-01-16", "2023-01-16"],
            );
            add_arrow_batch(&mut ctx, &batch).await.unwrap();

            let output_dir = tempfile::tempdir().unwrap();
            let split_config = default_split_config("test", "test-source", "test-node");
            let results = finish_all_splits(ctx, output_dir.path().to_str().unwrap(), &split_config).await.unwrap();

            assert_eq!(results.len(), 2);

            // Each partition should have its own statistics
            for result in &results {
                assert!(!result.min_values.is_empty());
                assert!(!result.max_values.is_empty());
                assert!(result.min_values.contains_key("id"));
                assert!(result.max_values.contains_key("id"));
                assert!(result.min_values.contains_key("name"));
                assert!(result.max_values.contains_key("name"));
            }

            // Find partition for 2023-01-15 (ids 1, 2)
            let p1 = results.iter().find(|r| r.partition_key == "event_date=2023-01-15").unwrap();
            assert_eq!(p1.min_values.get("id"), Some(&"1".to_string()));
            assert_eq!(p1.max_values.get("id"), Some(&"2".to_string()));

            // Find partition for 2023-01-16 (ids 3, 4)
            let p2 = results.iter().find(|r| r.partition_key == "event_date=2023-01-16").unwrap();
            assert_eq!(p2.min_values.get("id"), Some(&"3".to_string()));
            assert_eq!(p2.max_values.get("id"), Some(&"4".to_string()));
        });
    }
}
