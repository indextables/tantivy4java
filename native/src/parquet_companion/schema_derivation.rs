// schema_derivation.rs - Derive tantivy schema from parquet schema
//
// Phase 3: Maps Arrow/Parquet types to tantivy field types for automatic
// schema generation during createFromParquet.

use std::collections::{HashMap, HashSet};
use anyhow::{Context, Result};
use arrow_schema::{DataType, Schema as ArrowSchema};
use tantivy::schema::{Schema, SchemaBuilder};

use super::manifest::FastFieldMode;
use super::string_indexing::{self, StringIndexingMode};

use crate::debug_println;

/// Configuration for schema derivation
pub struct SchemaDerivationConfig {
    /// Fast field mode determines which fields get fast field access
    pub fast_field_mode: FastFieldMode,
    /// Fields to skip during schema derivation
    pub skip_fields: HashSet<String>,
    /// Custom tokenizer overrides per field (field_name → tokenizer_name)
    pub tokenizer_overrides: std::collections::HashMap<String, String>,
    /// Fields that should be treated as IP address type (parquet stores as UTF8 strings)
    pub ip_address_fields: HashSet<String>,
    /// Fields that should be treated as JSON object type (parquet stores as UTF8 strings)
    pub json_fields: HashSet<String>,
    /// Whether to enable fieldnorms for text fields (default: false)
    pub fieldnorms_enabled: bool,
}

impl Default for SchemaDerivationConfig {
    fn default() -> Self {
        Self {
            fast_field_mode: FastFieldMode::Disabled,
            skip_fields: HashSet::new(),
            tokenizer_overrides: std::collections::HashMap::new(),
            ip_address_fields: HashSet::new(),
            json_fields: HashSet::new(),
            fieldnorms_enabled: false,
        }
    }
}

/// Derive a tantivy Schema from a parquet/Arrow schema.
///
/// Type mapping:
/// - BOOLEAN → Bool
/// - INT8/16/32/64 → I64
/// - UINT8/16/32/64 → U64
/// - FLOAT32/64 → F64
/// - UTF8/LargeUtf8/BYTE_ARRAY → Text (with tokenizer)
/// - Timestamp → DateTime
/// - Date32/Date64 → DateTime
/// - List/Map/Struct → Json
pub fn derive_tantivy_schema(
    arrow_schema: &ArrowSchema,
    config: &SchemaDerivationConfig,
) -> Result<Schema> {
    derive_tantivy_schema_with_mapping(arrow_schema, config, None)
}

/// Derive tantivy schema with optional name mapping (parquet_col → tantivy_field).
/// When a name mapping is provided, the tantivy field will use the mapped display name.
pub fn derive_tantivy_schema_with_mapping(
    arrow_schema: &ArrowSchema,
    config: &SchemaDerivationConfig,
    name_mapping: Option<&HashMap<String, String>>,
) -> Result<Schema> {
    let mut builder = SchemaBuilder::new();

    // Collect all tantivy field names for collision detection with companion fields.
    let all_field_names: HashSet<String> = arrow_schema.fields().iter()
        .filter(|f| !config.skip_fields.contains(f.name().as_str()))
        .map(|f| {
            name_mapping
                .and_then(|m| m.get(f.name().as_str()))
                .cloned()
                .unwrap_or_else(|| f.name().clone())
        })
        .collect();

    for field in arrow_schema.fields() {
        let parquet_name = field.name();

        if config.skip_fields.contains(parquet_name.as_str()) {
            debug_println!("📋 SCHEMA_DERIVE: Skipping field '{}'", parquet_name);
            continue;
        }

        // Use mapped name if available, otherwise use parquet column name
        let tantivy_name = name_mapping
            .and_then(|m| m.get(parquet_name.as_str()))
            .map(|s| s.as_str())
            .unwrap_or(parquet_name.as_str());

        add_field_for_arrow_type(&mut builder, tantivy_name, field.data_type(), config, &all_field_names)?;
    }

    Ok(builder.build())
}

fn add_field_for_arrow_type(
    builder: &mut SchemaBuilder,
    name: &str,
    data_type: &DataType,
    config: &SchemaDerivationConfig,
    all_field_names: &HashSet<String>,
) -> Result<()> {
    use tantivy::schema::*;

    // Check if this field should be treated as an IP address type.
    // Parquet has no native IP type, so IPs are stored as UTF8 strings.
    // The user declares which fields are IPs via config.
    if config.ip_address_fields.contains(name) {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let mut opts = IpAddrOptions::default();
                opts = opts.set_indexed();
                if should_add_fast(config, name, data_type) {
                    opts = opts.set_fast();
                }
                builder.add_ip_addr_field(name, opts);
                return Ok(());
            }
            _ => {
                debug_println!(
                    "⚠️ SCHEMA_DERIVE: Field '{}' declared as IP but has type {:?} (not Utf8), ignoring IP override",
                    name, data_type
                );
            }
        }
    }

    // Check if this field should be treated as a JSON object type.
    // Parquet has no native JSON type, so JSON is stored as UTF8 strings.
    // The user declares which fields are JSON via config.
    if config.json_fields.contains(name) {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // STRING column forced to JSON: stored + indexed with "default" tokenizer
                let opts = JsonObjectOptions::default()
                    .set_stored()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(IndexRecordOption::Basic)
                            .set_fieldnorms(config.fieldnorms_enabled),
                    );
                builder.add_json_field(name, opts);
                return Ok(());
            }
            _ => {
                // Non-string field declared as json → warn, fall through to normal handling
                debug_println!(
                    "⚠️ SCHEMA_DERIVE: Field '{}' declared as JSON but has type {:?} (not Utf8), ignoring JSON override",
                    name, data_type
                );
            }
        }
    }

    match data_type {
        DataType::Boolean => {
            let mut opts = NumericOptions::default();
            opts = opts.set_indexed();
            // No set_stored() — parquet is the store in companion mode
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_bool_field(name, opts);
        }

        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut opts = NumericOptions::default();
            opts = opts.set_indexed();
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_i64_field(name, opts);
        }

        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let mut opts = NumericOptions::default();
            opts = opts.set_indexed();
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_u64_field(name, opts);
        }

        DataType::Float32 | DataType::Float64 => {
            let mut opts = NumericOptions::default();
            opts = opts.set_indexed();
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_f64_field(name, opts);
        }

        DataType::Utf8 | DataType::LargeUtf8 => {
            let tokenizer = config.tokenizer_overrides.get(name).cloned();

            // Check for compact string indexing modes before standard tokenizer logic
            if let Some(mode) = tokenizer.as_deref().and_then(string_indexing::parse_tokenizer_override) {
                match &mode {
                    StringIndexingMode::ExactOnly => {
                        // Replace Str field entirely with U64 hash field (indexed + fast)
                        let opts = NumericOptions::default().set_indexed().set_fast();
                        builder.add_u64_field(name, opts);
                    }
                    StringIndexingMode::TextUuidExactonly
                    | StringIndexingMode::TextCustomExactonly { .. } => {
                        // Main field: text with "default" tokenizer, no fast
                        let text_opts = TextOptions::default()
                            .set_indexing_options(
                                TextFieldIndexing::default()
                                    .set_tokenizer("default")
                                    .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                                    .set_fieldnorms(config.fieldnorms_enabled),
                            );
                        builder.add_text_field(name, text_opts);
                        // Companion field: U64 hash field for extracted patterns
                        let companion_name = string_indexing::companion_field_name(name);
                        if all_field_names.contains(&companion_name) {
                            anyhow::bail!(
                                "Companion field name '{}' for field '{}' collides with an existing \
                                 column. Rename the column or use a different indexing mode.",
                                companion_name, name
                            );
                        }
                        let companion_opts = NumericOptions::default().set_indexed().set_fast();
                        builder.add_u64_field(&companion_name, companion_opts);
                    }
                    StringIndexingMode::TextUuidStrip
                    | StringIndexingMode::TextCustomStrip { .. } => {
                        // Text with "default" tokenizer, no fast, no companion field
                        let text_opts = TextOptions::default()
                            .set_indexing_options(
                                TextFieldIndexing::default()
                                    .set_tokenizer("default")
                                    .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                                    .set_fieldnorms(config.fieldnorms_enabled),
                            );
                        builder.add_text_field(name, text_opts);
                    }
                }
            } else {
                // Standard tokenizer path
                let tok = tokenizer.as_deref().unwrap_or("raw");
                // Text fields: always indexed, never stored (parquet is the store)
                let mut opts = TextOptions::default()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(tok)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                            .set_fieldnorms(config.fieldnorms_enabled),
                    );
                if should_add_fast(config, name, data_type) {
                    opts = opts.set_fast(Some(tok));
                }
                builder.add_text_field(name, opts);
            }
        }

        DataType::Decimal128(_, _) => {
            // Decimal128: map to f64 for tantivy (lossy for values > 2^53)
            let mut opts = NumericOptions::default();
            opts = opts.set_indexed();
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_f64_field(name, opts);
        }
        DataType::Decimal256(_, _) => {
            // Decimal256: map to text to avoid precision loss (256-bit values exceed f64 range)
            let tok = config.tokenizer_overrides.get(name).cloned();
            let tok = tok.as_deref().unwrap_or("raw");
            let mut opts = TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(tok)
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                        .set_fieldnorms(config.fieldnorms_enabled),
                );
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast(Some(tok));
            }
            builder.add_text_field(name, opts);
        }

        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            // Bytes: indexed only, no stored (parquet is the store)
            builder.add_bytes_field(name, INDEXED);
        }

        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
            let mut opts = DateOptions::default();
            opts = opts.set_indexed();
            if should_add_fast(config, name, data_type) {
                opts = opts.set_fast();
            }
            builder.add_date_field(name, opts);
        }

        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            let opts = JsonObjectOptions::default()
                .set_stored()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::Basic)
                        .set_fieldnorms(config.fieldnorms_enabled),
                );
            builder.add_json_field(name, opts);
        }

        _ => {
            debug_println!(
                "⚠️ SCHEMA_DERIVE: Unsupported type {:?} for field '{}', skipping",
                data_type, name
            );
        }
    }

    Ok(())
}

/// Determine if a field should have fast field access during indexing.
///
/// This controls whether tantivy writes native .fast data for a field:
/// - Disabled: write fast for ALL fields (all native, no parquet augmentation)
/// - Hybrid: write fast for numeric/bool/date/ip (native), NOT for text (from parquet)
/// - ParquetOnly: write fast for NOTHING (all served from parquet at read time)
///
/// After indexing, `promote_all_fields_to_fast()` rewrites the schema in meta.json
/// so the reader sees all fields as fast. The ParquetAugmentedDirectory then intercepts
/// .fast file reads for fields that have no native data and serves from parquet.
fn should_add_fast(config: &SchemaDerivationConfig, field_name: &str, data_type: &DataType) -> bool {
    match config.fast_field_mode {
        FastFieldMode::Disabled => true, // All native fast fields
        FastFieldMode::Hybrid => {
            // IP address fields are treated as native fast fields in hybrid mode,
            // even though they come from Utf8 Arrow columns.
            if config.ip_address_fields.contains(field_name) {
                return true;
            }
            // Only numeric/bool/date get native fast fields in hybrid mode
            // Text fields are served from parquet at read time
            matches!(data_type,
                DataType::Boolean
                | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                | DataType::Float32 | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
            )
        }
        FastFieldMode::ParquetOnly => false, // No native fast fields
    }
}

/// Post-process a Quickwit doc_mapping JSON string to mark all fields as fast.
///
/// The indexing schema may have suppressed .fast for some fields (HYBRID/PARQUET_ONLY),
/// but the doc_mapping returned to the client must report all fields as fast so that
/// when the client passes it back to create a SplitSearcher, the runtime knows it can
/// serve fast field data (either from native .fast files or from parquet via the
/// ParquetAugmentedDirectory).
///
/// Doc mapping format is a JSON array of field descriptors:
/// `[{"name":"id","type":"i64","fast":true,...}, {"name":"text","type":"text","fast":false,...}]`
pub fn promote_doc_mapping_all_fast(doc_mapping_json: &str) -> Result<String> {
    let mut value: serde_json::Value = serde_json::from_str(doc_mapping_json)
        .context("Failed to parse doc_mapping_json for fast promotion")?;

    if let Some(arr) = value.as_array_mut() {
        for field in arr.iter_mut() {
            if let Some(obj) = field.as_object_mut() {
                obj.insert("fast".to_string(), serde_json::Value::Bool(true));
            }
        }
    }

    serde_json::to_string(&value)
        .context("Failed to serialize promoted doc_mapping_json")
}

/// Modify tantivy meta.json bytes to set all fields as fast.
///
/// This is used when re-opening a split Index with a UnionDirectory that shadows
/// the original meta.json. The tantivy meta.json format is:
/// ```json
/// { "schema": [field1, field2, ...], "segments": [...], ... }
/// ```
/// Each field has `"type"` and `"options"`. Fast flags differ by type:
/// - Numeric (i64, u64, f64, bool, date, ip_addr): `options.fast` is a boolean
/// - Text: `options.fast` is `false` or `{"with_tokenizer": "tokenizer_name"}`
pub fn promote_meta_json_all_fast(meta_bytes: &[u8]) -> Result<Vec<u8>> {
    let meta_str = std::str::from_utf8(meta_bytes)
        .context("meta.json is not valid UTF-8")?;
    let mut meta: serde_json::Value = serde_json::from_str(meta_str)
        .context("Failed to parse meta.json")?;

    if let Some(schema_arr) = meta.get_mut("schema").and_then(|s| s.as_array_mut()) {
        for field in schema_arr.iter_mut() {
            if let Some(obj) = field.as_object_mut() {
                let field_type = obj.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();
                if let Some(options) = obj.get_mut("options") {
                    match field_type.as_str() {
                        "text" => {
                            // Promote all text fields to fast, regardless of tokenizer.
                            // Tantivy fast fields store raw bytes independent of the
                            // tokenizer used for the inverted index — so even non-raw
                            // tokenized fields can participate in fast field aggregations.
                            let tok = options.get("indexing")
                                .and_then(|i| i.get("tokenizer"))
                                .and_then(|t| t.as_str())
                                .unwrap_or("raw")
                                .to_string();
                            options["fast"] = serde_json::json!({"with_tokenizer": tok});
                        }
                        "i64" | "u64" | "f64" | "bool" | "date" | "ip_addr" | "bytes" => {
                            options["fast"] = serde_json::Value::Bool(true);
                        }
                        _ => {} // json_object, etc. — leave as-is
                    }
                }
            }
        }
    }

    let result = serde_json::to_string(&meta)
        .context("Failed to serialize modified meta.json")?;
    Ok(result.into_bytes())
}

/// Map an Arrow DataType to a tantivy type string for use in ColumnMapping.
pub fn arrow_type_to_tantivy_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "Bool",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "I64",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "U64",
        DataType::Float32 | DataType::Float64 => "F64",
        DataType::Decimal128(_, _) => "F64",
        DataType::Decimal256(_, _) => "Str",
        DataType::Utf8 | DataType::LargeUtf8 => "Str",
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "Bytes",
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => "Date",
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => "Json",
        _ => "Unknown",
    }
}

/// Map an Arrow DataType to a parquet type string for use in ColumnMapping.
pub fn arrow_type_to_parquet_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "INT32",
        DataType::UInt64 => "INT64",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "BYTE_ARRAY",
        DataType::Binary | DataType::LargeBinary => "BYTE_ARRAY",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "FIXED_LEN_BYTE_ARRAY",
        DataType::FixedSizeBinary(_) => "FIXED_LEN_BYTE_ARRAY",
        DataType::Timestamp(_, _) => "INT64",
        DataType::Date32 => "INT32",
        DataType::Date64 => "INT64",
        _ => "BYTE_ARRAY",
    }
}

/// Validate that multiple parquet files have consistent schemas
pub fn validate_schema_consistency(
    primary: &ArrowSchema,
    file_schemas: &[ArrowSchema],
) -> Result<()> {
    for (i, schema) in file_schemas.iter().enumerate() {
        if primary.fields().len() != schema.fields().len() {
            anyhow::bail!(
                "Schema mismatch in file[{}]: primary has {} fields, file has {}",
                i,
                primary.fields().len(),
                schema.fields().len()
            );
        }

        for (j, (primary_field, file_field)) in
            primary.fields().iter().zip(schema.fields().iter()).enumerate()
        {
            if primary_field.name() != file_field.name() {
                anyhow::bail!(
                    "Field name mismatch in file[{}] column[{}]: '{}' vs '{}'",
                    i, j, primary_field.name(), file_field.name()
                );
            }
            if primary_field.data_type() != file_field.data_type() {
                anyhow::bail!(
                    "Field type mismatch in file[{}] field '{}': {:?} vs {:?}",
                    i, primary_field.name(), primary_field.data_type(), file_field.data_type()
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    fn make_test_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("data", DataType::Binary, true),
        ])
    }

    #[test]
    fn test_derive_schema_basic() {
        let arrow = make_test_arrow_schema();
        let config = SchemaDerivationConfig::default();
        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // Should have 6 fields
        assert_eq!(schema.fields().count(), 6);

        // Check field types via names
        assert!(schema.get_field("id").is_ok());
        assert!(schema.get_field("name").is_ok());
        assert!(schema.get_field("score").is_ok());
        assert!(schema.get_field("active").is_ok());
        assert!(schema.get_field("ts").is_ok());
        assert!(schema.get_field("data").is_ok());
    }

    #[test]
    fn test_derive_schema_skip_fields() {
        let arrow = make_test_arrow_schema();
        let mut config = SchemaDerivationConfig::default();
        config.skip_fields.insert("data".to_string());
        config.skip_fields.insert("ts".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();
        assert_eq!(schema.fields().count(), 4);
        assert!(schema.get_field("data").is_err());
        assert!(schema.get_field("ts").is_err());
    }

    #[test]
    fn test_derive_schema_parquet_only_no_fast() {
        let arrow = ArrowSchema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::ParquetOnly;

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // ParquetOnly: NO fields are fast in the indexing schema (all served from parquet)
        let count_field = schema.get_field("count").unwrap();
        let count_entry = schema.get_field_entry(count_field);
        assert!(!count_entry.is_fast(), "ParquetOnly should NOT mark numeric as fast in indexing schema");

        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(!label_entry.is_fast(), "ParquetOnly should NOT mark text as fast in indexing schema");
    }

    #[test]
    fn test_derive_schema_hybrid_mode() {
        let arrow = ArrowSchema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::Hybrid;

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // Hybrid: numeric/bool/date fields are fast, text fields are NOT
        let count_field = schema.get_field("count").unwrap();
        let count_entry = schema.get_field_entry(count_field);
        assert!(count_entry.is_fast(), "Hybrid should mark numeric as fast");

        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(!label_entry.is_fast(), "Hybrid should NOT mark text as fast in indexing schema");
    }

    #[test]
    fn test_derive_schema_disabled_mode_all_fast() {
        let arrow = ArrowSchema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::Disabled;

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // Disabled: ALL fields should be fast in the indexing schema
        for (field, entry) in schema.fields() {
            assert!(entry.is_fast(),
                "Disabled mode should mark all fields as fast, but '{}' is not", entry.name());
        }
    }

    #[test]
    fn test_arrow_type_to_tantivy_type_mapping() {
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Boolean), "Bool");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Int32), "I64");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Int64), "I64");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::UInt64), "U64");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Float64), "F64");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Utf8), "Str");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Binary), "Bytes");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Date32), "Date");
        assert_eq!(
            arrow_type_to_tantivy_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "Date"
        );
    }

    #[test]
    fn test_arrow_type_to_parquet_type_mapping() {
        assert_eq!(arrow_type_to_parquet_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Int32), "INT32");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Int64), "INT64");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Float32), "FLOAT");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Float64), "DOUBLE");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Utf8), "BYTE_ARRAY");
    }

    #[test]
    fn test_validate_schema_consistency_ok() {
        let schema = make_test_arrow_schema();
        let other = make_test_arrow_schema();
        assert!(validate_schema_consistency(&schema, &[other]).is_ok());
    }

    #[test]
    fn test_validate_schema_consistency_field_count_mismatch() {
        let schema = make_test_arrow_schema();
        let other = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let result = validate_schema_consistency(&schema, &[other]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Schema mismatch"));
    }

    #[test]
    fn test_validate_schema_consistency_type_mismatch() {
        let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let other = ArrowSchema::new(vec![Field::new("id", DataType::Utf8, false)]);
        let result = validate_schema_consistency(&schema, &[other]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Field type mismatch"));
    }

    #[test]
    fn test_derive_schema_tokenizer_override() {
        let arrow = ArrowSchema::new(vec![
            Field::new("content", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert("content".to_string(), "en_stem".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();
        assert!(schema.get_field("content").is_ok());
    }

    #[test]
    fn test_derive_schema_unsupported_type_skipped() {
        let arrow = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("weird", DataType::Null, true),
        ]);
        let config = SchemaDerivationConfig::default();
        let schema = derive_tantivy_schema(&arrow, &config).unwrap();
        // Null type is unsupported, should be skipped
        assert_eq!(schema.fields().count(), 1);
        assert!(schema.get_field("weird").is_err());
    }

    #[test]
    fn test_derive_schema_list_type_as_json() {
        let arrow = ArrowSchema::new(vec![
            Field::new(
                "tags",
                DataType::List(std::sync::Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);
        let config = SchemaDerivationConfig::default();
        let schema = derive_tantivy_schema(&arrow, &config).unwrap();
        assert!(schema.get_field("tags").is_ok());
    }

    #[test]
    fn test_derive_schema_ip_address_field() {
        let arrow = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("src_ip", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.ip_address_fields.insert("src_ip".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // src_ip should be an IpAddr field, not a text field
        let src_ip_field = schema.get_field("src_ip").unwrap();
        let src_ip_entry = schema.get_field_entry(src_ip_field);
        assert!(
            matches!(src_ip_entry.field_type(), tantivy::schema::FieldType::IpAddr(_)),
            "src_ip should be IpAddr type, got {:?}", src_ip_entry.field_type()
        );

        // label should still be a text field
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(
            matches!(label_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "label should still be Str type"
        );
    }

    #[test]
    fn test_ip_address_field_hybrid_mode_is_fast() {
        let arrow = ArrowSchema::new(vec![
            Field::new("src_ip", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::Hybrid;
        config.ip_address_fields.insert("src_ip".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // In hybrid mode, IP fields should be fast (like numerics)
        let src_ip_field = schema.get_field("src_ip").unwrap();
        let src_ip_entry = schema.get_field_entry(src_ip_field);
        assert!(src_ip_entry.is_fast(), "IP fields should be fast in Hybrid mode");

        // Text fields should NOT be fast in hybrid mode
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(!label_entry.is_fast(), "Text fields should NOT be fast in Hybrid mode");
    }

    #[test]
    fn test_ip_address_field_parquet_only_not_fast() {
        let arrow = ArrowSchema::new(vec![
            Field::new("src_ip", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::ParquetOnly;
        config.ip_address_fields.insert("src_ip".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        let src_ip_field = schema.get_field("src_ip").unwrap();
        let src_ip_entry = schema.get_field_entry(src_ip_field);
        assert!(!src_ip_entry.is_fast(), "IP fields should NOT be fast in ParquetOnly mode");
    }

    #[test]
    fn test_ip_address_non_utf8_ignored() {
        // IP address override on a non-Utf8 field should be ignored
        let arrow = ArrowSchema::new(vec![
            Field::new("count", DataType::Int64, false),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.ip_address_fields.insert("count".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // count should still be an I64 field
        let count_field = schema.get_field("count").unwrap();
        let count_entry = schema.get_field_entry(count_field);
        assert!(
            matches!(count_entry.field_type(), tantivy::schema::FieldType::I64(_)),
            "Non-Utf8 IP override should be ignored, got {:?}", count_entry.field_type()
        );
    }

    #[test]
    fn test_promote_doc_mapping_all_fast() {
        let input = r#"[{"name":"id","type":"i64","fast":false},{"name":"text","type":"text","fast":false}]"#;
        let result = promote_doc_mapping_all_fast(input).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["fast"], serde_json::Value::Bool(true));
        assert_eq!(arr[1]["fast"], serde_json::Value::Bool(true));
    }

    #[test]
    fn test_promote_doc_mapping_preserves_other_fields() {
        let input = r#"[{"name":"score","type":"f64","fast":false,"indexed":true}]"#;
        let result = promote_doc_mapping_all_fast(input).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0]["name"], "score");
        assert_eq!(arr[0]["type"], "f64");
        assert_eq!(arr[0]["indexed"], true);
        assert_eq!(arr[0]["fast"], true);
    }

    #[test]
    fn test_promote_doc_mapping_empty_array() {
        let input = "[]";
        let result = promote_doc_mapping_all_fast(input).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_promote_doc_mapping_invalid_json() {
        let result = promote_doc_mapping_all_fast("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_promote_meta_json_numeric_types() {
        let meta = serde_json::json!({
            "schema": [
                {"type": "i64", "options": {"fast": false}},
                {"type": "u64", "options": {"fast": false}},
                {"type": "f64", "options": {"fast": false}},
                {"type": "bool", "options": {"fast": false}},
                {"type": "date", "options": {"fast": false}},
                {"type": "ip_addr", "options": {"fast": false}},
                {"type": "bytes", "options": {"fast": false}},
            ],
            "segments": []
        });
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        let result = promote_meta_json_all_fast(&meta_bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(std::str::from_utf8(&result).unwrap()).unwrap();
        let schema = parsed["schema"].as_array().unwrap();
        for field in schema {
            assert_eq!(field["options"]["fast"], true, "Field type {} should have fast=true", field["type"]);
        }
    }

    #[test]
    fn test_promote_meta_json_text_raw_tokenizer() {
        let meta = serde_json::json!({
            "schema": [
                {"type": "text", "options": {"indexing": {"tokenizer": "raw"}, "fast": false}},
            ],
            "segments": []
        });
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        let result = promote_meta_json_all_fast(&meta_bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(std::str::from_utf8(&result).unwrap()).unwrap();
        let fast = &parsed["schema"][0]["options"]["fast"];
        assert_eq!(fast["with_tokenizer"], "raw");
    }

    #[test]
    fn test_promote_meta_json_text_default_tokenizer_promoted() {
        let meta = serde_json::json!({
            "schema": [
                {"type": "text", "options": {"indexing": {"tokenizer": "default"}, "fast": false}},
            ],
            "segments": []
        });
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        let result = promote_meta_json_all_fast(&meta_bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(std::str::from_utf8(&result).unwrap()).unwrap();
        // Non-raw tokenizer text fields should now be promoted
        assert_eq!(parsed["schema"][0]["options"]["fast"]["with_tokenizer"], "default");
    }

    #[test]
    fn test_derive_schema_decimal_types() {
        let arrow = ArrowSchema::new(vec![
            Field::new("price", DataType::Decimal128(18, 2), true),
            Field::new("big_val", DataType::Decimal256(38, 10), true),
            Field::new("hash", DataType::FixedSizeBinary(16), true),
        ]);
        let config = SchemaDerivationConfig::default();
        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // Decimal128/Decimal256 → F64
        let price_field = schema.get_field("price").unwrap();
        let price_entry = schema.get_field_entry(price_field);
        assert!(
            matches!(price_entry.field_type(), tantivy::schema::FieldType::F64(_)),
            "Decimal128 should map to F64, got {:?}", price_entry.field_type()
        );

        let big_field = schema.get_field("big_val").unwrap();
        let big_entry = schema.get_field_entry(big_field);
        assert!(
            matches!(big_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "Decimal256 should map to Str (text), got {:?}", big_entry.field_type()
        );

        // FixedSizeBinary → Bytes
        let hash_field = schema.get_field("hash").unwrap();
        let hash_entry = schema.get_field_entry(hash_field);
        assert!(
            matches!(hash_entry.field_type(), tantivy::schema::FieldType::Bytes(_)),
            "FixedSizeBinary should map to Bytes, got {:?}", hash_entry.field_type()
        );

        // Verify tantivy type mappings
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Decimal128(18, 2)), "F64");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::Decimal256(38, 10)), "Str");
        assert_eq!(arrow_type_to_tantivy_type(&DataType::FixedSizeBinary(16)), "Bytes");

        // Verify parquet type mappings
        assert_eq!(arrow_type_to_parquet_type(&DataType::Decimal128(18, 2)), "FIXED_LEN_BYTE_ARRAY");
        assert_eq!(arrow_type_to_parquet_type(&DataType::Decimal256(38, 10)), "FIXED_LEN_BYTE_ARRAY");
        assert_eq!(arrow_type_to_parquet_type(&DataType::FixedSizeBinary(16)), "FIXED_LEN_BYTE_ARRAY");
    }

    #[test]
    fn test_promote_meta_json_json_type_unchanged() {
        let meta = serde_json::json!({
            "schema": [
                {"type": "json_object", "options": {"fast": false}},
            ],
            "segments": []
        });
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        let result = promote_meta_json_all_fast(&meta_bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(std::str::from_utf8(&result).unwrap()).unwrap();
        // json_object type should remain unchanged
        assert_eq!(parsed["schema"][0]["options"]["fast"], false);
    }

    // ── JSON fields tests ───────────────────────────────────────────────

    #[test]
    fn test_json_fields_utf8_creates_json_field() {
        let arrow = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.json_fields.insert("payload".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // payload should be a JsonObject field
        let payload_field = schema.get_field("payload").unwrap();
        let payload_entry = schema.get_field_entry(payload_field);
        assert!(
            matches!(payload_entry.field_type(), tantivy::schema::FieldType::JsonObject(_)),
            "payload should be JsonObject type, got {:?}", payload_entry.field_type()
        );

        // label should still be a text field
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(
            matches!(label_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "label should still be Str type"
        );
    }

    #[test]
    fn test_json_fields_large_utf8_creates_json_field() {
        let arrow = ArrowSchema::new(vec![
            Field::new("metadata", DataType::LargeUtf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.json_fields.insert("metadata".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        let meta_field = schema.get_field("metadata").unwrap();
        let meta_entry = schema.get_field_entry(meta_field);
        assert!(
            matches!(meta_entry.field_type(), tantivy::schema::FieldType::JsonObject(_)),
            "LargeUtf8 JSON field should be JsonObject type, got {:?}", meta_entry.field_type()
        );
    }

    #[test]
    fn test_json_fields_non_utf8_ignored() {
        // JSON override on a non-Utf8 field should be ignored (falls through to normal handling)
        let arrow = ArrowSchema::new(vec![
            Field::new("count", DataType::Int64, false),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.json_fields.insert("count".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        let count_field = schema.get_field("count").unwrap();
        let count_entry = schema.get_field_entry(count_field);
        assert!(
            matches!(count_entry.field_type(), tantivy::schema::FieldType::I64(_)),
            "Non-Utf8 JSON override should be ignored, got {:?}", count_entry.field_type()
        );
    }

    #[test]
    fn test_json_fields_coexists_with_ip_fields() {
        let arrow = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("src_ip", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.ip_address_fields.insert("src_ip".to_string());
        config.json_fields.insert("payload".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // src_ip → IpAddr
        let src_ip_field = schema.get_field("src_ip").unwrap();
        let src_ip_entry = schema.get_field_entry(src_ip_field);
        assert!(
            matches!(src_ip_entry.field_type(), tantivy::schema::FieldType::IpAddr(_)),
            "src_ip should be IpAddr type"
        );

        // payload → JsonObject
        let payload_field = schema.get_field("payload").unwrap();
        let payload_entry = schema.get_field_entry(payload_field);
        assert!(
            matches!(payload_entry.field_type(), tantivy::schema::FieldType::JsonObject(_)),
            "payload should be JsonObject type"
        );

        // label → Str (default text)
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(
            matches!(label_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "label should still be Str type"
        );
    }

    #[test]
    fn test_json_fields_not_fast() {
        // JSON fields should never be fast (same as complex types like List/Map/Struct)
        let arrow = ArrowSchema::new(vec![
            Field::new("payload", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.fast_field_mode = FastFieldMode::Disabled; // All native fast
        config.json_fields.insert("payload".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        let payload_field = schema.get_field("payload").unwrap();
        let payload_entry = schema.get_field_entry(payload_field);
        assert!(!payload_entry.is_fast(), "JSON fields should not be fast");
    }

    #[test]
    fn test_json_fields_stored_and_indexed() {
        let arrow = ArrowSchema::new(vec![
            Field::new("payload", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.json_fields.insert("payload".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        let payload_field = schema.get_field("payload").unwrap();
        let payload_entry = schema.get_field_entry(payload_field);

        // Should be stored
        assert!(payload_entry.is_stored(), "JSON field should be stored");

        // Should be indexed (JsonObject with indexing options)
        assert!(payload_entry.is_indexed(), "JSON field should be indexed");
    }

    // ── String indexing mode tests ─────────────────────────────────────

    #[test]
    fn test_exact_only_creates_u64_field() {
        let arrow = ArrowSchema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert("trace_id".to_string(), "exact_only".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // trace_id should be U64 (hash field)
        let trace_field = schema.get_field("trace_id").unwrap();
        let trace_entry = schema.get_field_entry(trace_field);
        assert!(
            matches!(trace_entry.field_type(), tantivy::schema::FieldType::U64(_)),
            "exact_only should create U64 field, got {:?}", trace_entry.field_type()
        );
        assert!(trace_entry.is_fast(), "exact_only U64 should be fast");
        assert!(trace_entry.is_indexed(), "exact_only U64 should be indexed");

        // label should still be standard text
        let label_field = schema.get_field("label").unwrap();
        let label_entry = schema.get_field_entry(label_field);
        assert!(
            matches!(label_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "label should still be Str type"
        );
    }

    #[test]
    fn test_text_uuid_exactonly_creates_text_plus_companion() {
        let arrow = ArrowSchema::new(vec![
            Field::new("message", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert("message".to_string(), "text_uuid_exactonly".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // message should be a text field with "default" tokenizer
        let msg_field = schema.get_field("message").unwrap();
        let msg_entry = schema.get_field_entry(msg_field);
        assert!(
            matches!(msg_entry.field_type(), tantivy::schema::FieldType::Str(_)),
            "text_uuid_exactonly main field should be Str"
        );

        // message__uuids should be a U64 companion field
        let companion_field = schema.get_field("message__uuids").unwrap();
        let companion_entry = schema.get_field_entry(companion_field);
        assert!(
            matches!(companion_entry.field_type(), tantivy::schema::FieldType::U64(_)),
            "companion field should be U64, got {:?}", companion_entry.field_type()
        );
        assert!(companion_entry.is_fast(), "companion U64 should be fast");
        assert!(companion_entry.is_indexed(), "companion U64 should be indexed");
    }

    #[test]
    fn test_text_uuid_strip_creates_text_only() {
        let arrow = ArrowSchema::new(vec![
            Field::new("message", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert("message".to_string(), "text_uuid_strip".to_string());

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        // message should be a text field
        assert!(schema.get_field("message").is_ok());

        // No companion field
        assert!(schema.get_field("message__uuids").is_err());
    }

    #[test]
    fn test_text_custom_exactonly_creates_text_plus_companion() {
        let arrow = ArrowSchema::new(vec![
            Field::new("log_line", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert(
            "log_line".to_string(),
            "text_custom_exactonly:\\d{3}-\\d{4}".to_string(),
        );

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        assert!(schema.get_field("log_line").is_ok());
        assert!(schema.get_field("log_line__uuids").is_ok());
    }

    #[test]
    fn test_text_custom_strip_creates_text_only() {
        let arrow = ArrowSchema::new(vec![
            Field::new("log_line", DataType::Utf8, true),
        ]);
        let mut config = SchemaDerivationConfig::default();
        config.tokenizer_overrides.insert(
            "log_line".to_string(),
            "text_custom_strip:\\d{3}-\\d{4}".to_string(),
        );

        let schema = derive_tantivy_schema(&arrow, &config).unwrap();

        assert!(schema.get_field("log_line").is_ok());
        assert!(schema.get_field("log_line__uuids").is_err());
    }
}

