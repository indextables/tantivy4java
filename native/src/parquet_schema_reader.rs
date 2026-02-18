// parquet_schema_reader.rs - Read schema from a single parquet file
//
// Provides a standalone utility to read the Arrow schema from a parquet file's
// footer and return it in the same TANT byte buffer format as delta_reader,
// reusing DeltaSchemaField and serialize_delta_schema for consistency.

use std::sync::Arc;
use anyhow::Result;
use url::Url;
use object_store::path::Path as ObjectPath;

use parquet::arrow::async_reader::ParquetObjectReader;
use arrow_schema::DataType;

use crate::debug_println;
use crate::delta_reader::engine::{DeltaStorageConfig, create_object_store};
use crate::delta_reader::scan::DeltaSchemaField;
use crate::delta_reader::serialization::serialize_delta_schema;

/// Read column name mapping from a parquet file using Iceberg field IDs.
///
/// Databricks (and other engines) may use physical column names in parquet files
/// (e.g. "col_1", "col_2") that differ from the logical Iceberg column names
/// (e.g. "id", "name"). This function reads the parquet file's metadata, extracts
/// field IDs, and matches them against the provided Iceberg field-ID-to-name map
/// to produce a physical-name → logical-name mapping.
///
/// Resolution order:
/// 1. Arrow field metadata `PARQUET:field_id` (set by Arrow parquet writers)
/// 2. Parquet schema descriptor field IDs (set by all Iceberg-aware writers)
/// 3. `iceberg.schema` KV metadata in the parquet footer (embedded by Iceberg writers)
/// 4. Identity mapping (physical = logical) as fallback
///
/// Returns a `HashMap<String, String>` where keys are physical parquet column names
/// and values are logical Iceberg column names.
pub fn read_column_mapping(
    parquet_url: &str,
    field_id_to_name: &std::collections::HashMap<i32, String>,
    config: &DeltaStorageConfig,
) -> Result<std::collections::HashMap<String, String>> {
    debug_println!("🔧 PARQUET_COLUMN_MAPPING: Reading mapping for url={}", parquet_url);

    let (url, object_path) = parse_file_url(parquet_url)?;
    let store = create_object_store(&url, config)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;

    rt.block_on(async {
        // HEAD to get file size (required for Azure compatibility)
        let meta = store.head(&object_path).await
            .map_err(|e| anyhow::anyhow!("Failed to get metadata for '{}': {}", parquet_url, e))?;

        let reader = ParquetObjectReader::new(Arc::clone(&store), object_path.clone())
            .with_file_size(meta.size as u64);

        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader).await
            .map_err(|e| anyhow::anyhow!("Failed to read parquet metadata from '{}': {}", parquet_url, e))?;

        let arrow_schema = builder.schema();
        let parquet_metadata = builder.metadata();
        let mut mapping = std::collections::HashMap::new();
        let mut resolved_count = 0;

        // Strategy 1: Arrow field metadata (PARQUET:field_id)
        for field in arrow_schema.fields() {
            let physical_name = field.name().clone();
            if let Some(field_id_str) = field.metadata().get("PARQUET:field_id") {
                if let Ok(field_id) = field_id_str.parse::<i32>() {
                    if let Some(logical_name) = field_id_to_name.get(&field_id) {
                        debug_println!(
                            "🔧 PARQUET_COLUMN_MAPPING: Arrow metadata: {} (field_id={}) → {}",
                            physical_name, field_id, logical_name
                        );
                        mapping.insert(physical_name, logical_name.clone());
                        resolved_count += 1;
                        continue;
                    }
                }
            }
            // Not resolved via Arrow metadata — will try other strategies below
            mapping.insert(physical_name.clone(), physical_name);
        }

        // Strategy 2: Parquet schema descriptor field IDs (if Arrow metadata didn't resolve all)
        if resolved_count < arrow_schema.fields().len() {
            let schema_descr = parquet_metadata.file_metadata().schema_descr();
            let root = schema_descr.root_schema();
            for field_type in root.get_fields() {
                let info = field_type.get_basic_info();
                let physical_name = info.name().to_string();
                if info.has_id() {
                    let field_id = info.id();
                    if let Some(logical_name) = field_id_to_name.get(&field_id) {
                        // Only update if still identity-mapped (not already resolved)
                        if mapping.get(&physical_name) == Some(&physical_name) {
                            debug_println!(
                                "🔧 PARQUET_COLUMN_MAPPING: Parquet schema: {} (field_id={}) → {}",
                                physical_name, field_id, logical_name
                            );
                            mapping.insert(physical_name, logical_name.clone());
                            resolved_count += 1;
                        }
                    }
                }
            }
        }

        // Strategy 3: iceberg.schema KV metadata in parquet footer
        if resolved_count < arrow_schema.fields().len() {
            if let Some(kv_meta) = parquet_metadata.file_metadata().key_value_metadata() {
                if let Some(iceberg_schema_kv) = kv_meta.iter().find(|kv| kv.key == "iceberg.schema") {
                    if let Some(ref json_str) = iceberg_schema_kv.value {
                        if let Ok(schema_val) = serde_json::from_str::<serde_json::Value>(json_str) {
                            if let Some(fields) = schema_val.get("fields").and_then(|f| f.as_array()) {
                                // Build field_id → name map from embedded Iceberg schema
                                let mut embedded_id_to_name = std::collections::HashMap::new();
                                for field_val in fields {
                                    if let (Some(id), Some(name)) = (
                                        field_val.get("id").and_then(|v| v.as_i64()),
                                        field_val.get("name").and_then(|v| v.as_str()),
                                    ) {
                                        embedded_id_to_name.insert(id as i32, name.to_string());
                                    }
                                }

                                // Match parquet schema field IDs to embedded schema names
                                let schema_descr = parquet_metadata.file_metadata().schema_descr();
                                let root = schema_descr.root_schema();
                                for field_type in root.get_fields() {
                                    let info = field_type.get_basic_info();
                                    let physical_name = info.name().to_string();
                                    if info.has_id() {
                                        let field_id = info.id();
                                        if let Some(logical_name) = embedded_id_to_name.get(&field_id) {
                                            if mapping.get(&physical_name) == Some(&physical_name) {
                                                debug_println!(
                                                    "🔧 PARQUET_COLUMN_MAPPING: Embedded schema: {} (field_id={}) → {}",
                                                    physical_name, field_id, logical_name
                                                );
                                                mapping.insert(physical_name, logical_name.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        debug_println!(
            "🔧 PARQUET_COLUMN_MAPPING: Resolved {} of {} columns",
            mapping.iter().filter(|(k, v)| k != v).count(),
            mapping.len()
        );

        Ok(mapping)
    })
}

/// Read the Arrow schema from a single parquet file.
///
/// Returns `(fields, schema_json)` where:
/// - `fields` is the list of top-level columns with Arrow types
/// - `schema_json` is the full Arrow schema as a JSON string
pub fn read_parquet_schema(
    url_str: &str,
    config: &DeltaStorageConfig,
) -> Result<(Vec<DeltaSchemaField>, String)> {
    debug_println!("🔧 PARQUET_SCHEMA: Reading schema for url={}", url_str);

    let (url, object_path) = parse_file_url(url_str)?;
    let store = create_object_store(&url, config)?;

    // Use a tokio runtime to drive the async parquet reader
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;

    let (fields, schema_json) = rt.block_on(async {
        // HEAD request to get file size — required for Azure which doesn't
        // support suffix range requests. Providing file_size switches the
        // parquet reader from suffix ranges to bounded ranges.
        let meta = store.head(&object_path).await
            .map_err(|e| anyhow::anyhow!("Failed to get metadata for '{}': {}", url_str, e))?;

        let reader = ParquetObjectReader::new(Arc::clone(&store), object_path.clone())
            .with_file_size(meta.size as u64);

        // Read parquet metadata from footer (uses bounded range requests)
        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader).await
            .map_err(|e| anyhow::anyhow!("Failed to read parquet metadata from '{}': {}", url_str, e))?;

        let arrow_schema = builder.schema();

        debug_println!(
            "🔧 PARQUET_SCHEMA: Found {} fields in parquet file",
            arrow_schema.fields().len()
        );

        // Convert Arrow fields to DeltaSchemaField (reusing the same struct)
        let fields: Vec<DeltaSchemaField> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let data_type = arrow_type_to_string(field.data_type());
                let metadata = if field.metadata().is_empty() {
                    "{}".to_string()
                } else {
                    serde_json::to_string(field.metadata())
                        .unwrap_or_else(|_| "{}".to_string())
                };
                DeltaSchemaField {
                    name: field.name().clone(),
                    data_type,
                    nullable: field.is_nullable(),
                    metadata,
                }
            })
            .collect();

        // Serialize Arrow schema as JSON manually (arrow Schema doesn't implement Serialize)
        let schema_json = arrow_schema_to_json(arrow_schema.as_ref());

        Ok::<_, anyhow::Error>((fields, schema_json))
    })?;

    Ok((fields, schema_json))
}

/// Convert an Arrow DataType to a human-readable string.
///
/// Primitives get simple names: "boolean", "int32", "string", etc.
/// Complex types (struct, list, map) are serialized as JSON via serde.
fn arrow_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Interval(_) => "interval".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            format!("decimal({},{})", p, s)
        }
        DataType::FixedSizeBinary(size) => format!("fixed_binary({})", size),
        DataType::Null => "null".to_string(),
        // Complex types: use Debug representation (Arrow types don't implement Serialize)
        _ => format!("{:?}", dt),
    }
}

/// Serialize an Arrow schema to a JSON string.
///
/// Since arrow_schema::Schema doesn't implement serde::Serialize, we build
/// the JSON manually from the schema's fields.
pub(crate) fn arrow_schema_to_json(schema: &arrow_schema::Schema) -> String {
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            let mut obj = serde_json::Map::new();
            obj.insert("name".to_string(), serde_json::Value::String(f.name().clone()));
            obj.insert("data_type".to_string(), serde_json::Value::String(arrow_type_to_string(f.data_type())));
            obj.insert("nullable".to_string(), serde_json::Value::Bool(f.is_nullable()));
            if !f.metadata().is_empty() {
                let meta: serde_json::Map<String, serde_json::Value> = f.metadata()
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect();
                obj.insert("metadata".to_string(), serde_json::Value::Object(meta));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    let mut root = serde_json::Map::new();
    root.insert("fields".to_string(), serde_json::Value::Array(fields));
    if !schema.metadata().is_empty() {
        let meta: serde_json::Map<String, serde_json::Value> = schema.metadata()
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        root.insert("metadata".to_string(), serde_json::Value::Object(meta));
    }
    serde_json::to_string(&serde_json::Value::Object(root))
        .unwrap_or_else(|_| "{}".to_string())
}

/// Parse a file URL string into a base URL (for ObjectStore) and an object path.
///
/// For cloud URLs (s3://, azure://), the base URL is scheme://host/ and
/// the path is the remainder. For local paths, we use a file:// URL.
fn parse_file_url(url_str: &str) -> Result<(Url, ObjectPath)> {
    if url_str.starts_with("s3://") || url_str.starts_with("s3a://") {
        let url = Url::parse(url_str)
            .map_err(|e| anyhow::anyhow!("Invalid S3 URL '{}': {}", url_str, e))?;
        let path = url.path().trim_start_matches('/');
        let object_path = ObjectPath::from(path);
        // Base URL is just scheme://bucket/
        let base = Url::parse(&format!("{}://{}/", url.scheme(), url.host_str().unwrap_or("")))
            .map_err(|e| anyhow::anyhow!("Failed to construct base URL: {}", e))?;
        Ok((base, object_path))
    } else if url_str.starts_with("az://")
        || url_str.starts_with("azure://")
        || url_str.starts_with("abfs://")
        || url_str.starts_with("abfss://")
    {
        let url = Url::parse(url_str)
            .map_err(|e| anyhow::anyhow!("Invalid Azure URL '{}': {}", url_str, e))?;
        let path = url.path().trim_start_matches('/');
        let object_path = ObjectPath::from(path);
        let base = Url::parse(&format!("{}://{}/", url.scheme(), url.host_str().unwrap_or("")))
            .map_err(|e| anyhow::anyhow!("Failed to construct base URL: {}", e))?;
        Ok((base, object_path))
    } else if url_str.starts_with("file://") {
        let url = Url::parse(url_str)
            .map_err(|e| anyhow::anyhow!("Invalid file URL '{}': {}", url_str, e))?;
        let file_path = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("Cannot convert URL to file path: {}", url_str))?;
        let base = Url::parse("file:///").unwrap();
        let path_str = file_path.to_string_lossy();
        let object_path = ObjectPath::from(path_str.trim_start_matches('/'));
        Ok((base, object_path))
    } else {
        // Bare local path
        let abs_path = std::path::Path::new(url_str)
            .canonicalize()
            .map_err(|e| anyhow::anyhow!("Cannot resolve path '{}': {}", url_str, e))?;
        let base = Url::parse("file:///").unwrap();
        let path_str = abs_path.to_string_lossy();
        let object_path = ObjectPath::from(path_str.trim_start_matches('/'));
        Ok((base, object_path))
    }
}

// --- JNI ---

use jni::objects::{JClass, JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::common::to_java_exception;

/// Helper to extract a String value from a Java HashMap<String,String>.
fn extract_string(env: &mut JNIEnv, map: &JObject, key: &str) -> Option<String> {
    let key_jstr = env.new_string(key).ok()?;
    let value = env
        .call_method(
            map,
            "get",
            "(Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&key_jstr).into()],
        )
        .ok()?
        .l()
        .ok()?;
    if value.is_null() {
        return None;
    }
    let value_jstr = JString::from(value);
    let value_str = env.get_string(&value_jstr).ok()?;
    Some(value_str.to_string_lossy().to_string())
}

/// Build a DeltaStorageConfig from a Java HashMap<String,String>.
fn build_config(env: &mut JNIEnv, config_map: &JObject) -> DeltaStorageConfig {
    if config_map.is_null() {
        return DeltaStorageConfig::default();
    }

    DeltaStorageConfig {
        aws_access_key: extract_string(env, config_map, "aws_access_key_id"),
        aws_secret_key: extract_string(env, config_map, "aws_secret_access_key"),
        aws_session_token: extract_string(env, config_map, "aws_session_token"),
        aws_region: extract_string(env, config_map, "aws_region"),
        aws_endpoint: extract_string(env, config_map, "aws_endpoint"),
        aws_force_path_style: extract_string(env, config_map, "aws_force_path_style")
            .map(|s| s == "true")
            .unwrap_or(false),
        azure_account_name: extract_string(env, config_map, "azure_account_name"),
        azure_access_key: extract_string(env, config_map, "azure_access_key"),
        azure_bearer_token: extract_string(env, config_map, "azure_bearer_token"),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetSchemaReader_nativeReadParquetSchema(
    mut env: JNIEnv,
    _class: JClass,
    file_url: JString,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 PARQUET_SCHEMA_JNI: nativeReadParquetSchema called");

    let url_str = match env.get_string(&file_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read file URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    debug_println!(
        "🔧 PARQUET_SCHEMA_JNI: url={}, has_aws={}, has_azure={}",
        url_str,
        config.aws_access_key.is_some(),
        config.azure_account_name.is_some()
    );

    match read_parquet_schema(&url_str, &config) {
        Ok((fields, schema_json)) => {
            debug_println!(
                "🔧 PARQUET_SCHEMA_JNI: Schema has {} fields",
                fields.len()
            );

            // Reuse delta schema serialization with table_version = -1 (not applicable)
            let buffer = serialize_delta_schema(&fields, &schema_json, u64::MAX);

            match env.new_byte_array(buffer.len() as i32) {
                Ok(byte_array) => {
                    let byte_slice: &[i8] = unsafe {
                        std::slice::from_raw_parts(buffer.as_ptr() as *const i8, buffer.len())
                    };
                    if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to copy byte array: {}", e),
                        );
                        return std::ptr::null_mut();
                    }
                    byte_array.into_raw()
                }
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to allocate byte array: {}", e),
                    );
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Write a test parquet file with physical column names and Iceberg field IDs.
///
/// Schema: col_1 (Int64, field_id=1), col_2 (Utf8, field_id=2), col_3 (Float64, field_id=3)
///
/// Simulates a Databricks-style parquet file where physical names differ from logical names.
/// Used by Java integration tests for column mapping verification.
fn write_test_parquet_with_field_ids(path: &str) -> Result<()> {
    use arrow_schema::{Field, Schema};
    use arrow_array::{Int64Array, StringArray, Float64Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int64, false)
            .with_metadata(HashMap::from([
                ("PARQUET:field_id".to_string(), "1".to_string()),
            ])),
        Field::new("col_2", DataType::Utf8, true)
            .with_metadata(HashMap::from([
                ("PARQUET:field_id".to_string(), "2".to_string()),
            ])),
        Field::new("col_3", DataType::Float64, true)
            .with_metadata(HashMap::from([
                ("PARQUET:field_id".to_string(), "3".to_string()),
            ])),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            Arc::new(Float64Array::from(vec![Some(99.9), Some(88.8), Some(77.7)])),
        ],
    )
    .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))?;

    let file = std::fs::File::create(path)
        .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", path, e))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .map_err(|e| anyhow::anyhow!("Failed to create ArrowWriter: {}", e))?;
    writer.write(&batch)
        .map_err(|e| anyhow::anyhow!("Failed to write batch: {}", e))?;
    writer.close()
        .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

    Ok(())
}

/// Write a small test parquet file with a known multi-type schema.
///
/// Schema: id (Int64, not null), name (Utf8, nullable), score (Float64, nullable),
///         active (Boolean, not null), created (Timestamp µs, nullable)
///
/// Used by Java integration tests to create a fixture without a Java parquet dependency.
fn write_test_parquet(path: &str) -> Result<()> {
    use arrow_schema::{Field, Schema, TimeUnit};
    use arrow_array::{
        Int64Array, StringArray, Float64Array, BooleanArray, TimestampMicrosecondArray, RecordBatch,
    };
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("created", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            Arc::new(Float64Array::from(vec![Some(95.5), None, Some(87.0)])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000),
                Some(1_700_000_001_000_000),
                None,
            ])),
        ],
    )
    .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))?;

    let file = std::fs::File::create(path)
        .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", path, e))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .map_err(|e| anyhow::anyhow!("Failed to create ArrowWriter: {}", e))?;
    writer
        .write(&batch)
        .map_err(|e| anyhow::anyhow!("Failed to write batch: {}", e))?;
    writer
        .close()
        .map_err(|e| anyhow::anyhow!("Failed to close writer: {}", e))?;

    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetSchemaReader_nativeWriteTestParquet(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) {
    let path_str = match env.get_string(&path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read path: {}", e));
            return;
        }
    };

    if let Err(e) = write_test_parquet(&path_str) {
        to_java_exception(&mut env, &e);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetSchemaReader_nativeWriteTestParquetWithFieldIds(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) {
    let path_str = match env.get_string(&path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read path: {}", e));
            return;
        }
    };

    if let Err(e) = write_test_parquet_with_field_ids(&path_str) {
        to_java_exception(&mut env, &e);
    }
}

/// JNI: Read column name mapping from a parquet file using Iceberg field IDs.
///
/// Parameters:
///   parquet_url: path to the parquet file (local, file://, s3://, azure://)
///   field_id_to_name_json: JSON string mapping field_id (int) → logical_name (string)
///                          e.g. {"1":"id","2":"name","3":"price"}
///   config_map: storage credentials (same as readParquetSchema)
///
/// Returns: JSON string mapping physical_name → logical_name
///          e.g. {"col_1":"id","col_2":"name","col_3":"price"}
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetSchemaReader_nativeReadColumnMapping(
    mut env: JNIEnv,
    _class: JClass,
    parquet_url: JString,
    field_id_to_name_json: JString,
    config_map: JObject,
) -> jbyteArray {
    use jni::objects::JByteArray;

    debug_println!("🔧 PARQUET_COLUMN_MAPPING_JNI: nativeReadColumnMapping called");

    let url_str = match env.get_string(&parquet_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read parquet URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let mapping_json_str = match env.get_string(&field_id_to_name_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read field mapping JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Parse the field_id → name JSON into a HashMap<i32, String>
    let field_id_to_name: std::collections::HashMap<i32, String> = match serde_json::from_str::<
        std::collections::HashMap<String, String>,
    >(&mapping_json_str)
    {
        Ok(str_map) => {
            let mut int_map = std::collections::HashMap::new();
            for (k, v) in str_map {
                if let Ok(id) = k.parse::<i32>() {
                    int_map.insert(id, v);
                }
            }
            int_map
        }
        Err(e) => {
            to_java_exception(
                &mut env,
                &anyhow::anyhow!("Failed to parse field mapping JSON: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    debug_println!(
        "🔧 PARQUET_COLUMN_MAPPING_JNI: url={}, field_count={}",
        url_str, field_id_to_name.len()
    );

    match read_column_mapping(&url_str, &field_id_to_name, &config) {
        Ok(mapping) => {
            // Serialize mapping as JSON string → return as byte array (UTF-8 bytes)
            let result_json = serde_json::to_string(&mapping)
                .unwrap_or_else(|_| "{}".to_string());

            debug_println!(
                "🔧 PARQUET_COLUMN_MAPPING_JNI: Result: {}",
                result_json
            );

            let result_bytes = result_json.as_bytes();
            match env.new_byte_array(result_bytes.len() as i32) {
                Ok(byte_array) => {
                    let byte_slice: &[i8] = unsafe {
                        std::slice::from_raw_parts(result_bytes.as_ptr() as *const i8, result_bytes.len())
                    };
                    if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to copy byte array: {}", e),
                        );
                        return std::ptr::null_mut();
                    }
                    byte_array.into_raw()
                }
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to allocate byte array: {}", e),
                    );
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_type_to_string_primitives() {
        assert_eq!(arrow_type_to_string(&DataType::Boolean), "boolean");
        assert_eq!(arrow_type_to_string(&DataType::Int8), "int8");
        assert_eq!(arrow_type_to_string(&DataType::Int16), "int16");
        assert_eq!(arrow_type_to_string(&DataType::Int32), "int32");
        assert_eq!(arrow_type_to_string(&DataType::Int64), "int64");
        assert_eq!(arrow_type_to_string(&DataType::UInt8), "uint8");
        assert_eq!(arrow_type_to_string(&DataType::UInt16), "uint16");
        assert_eq!(arrow_type_to_string(&DataType::UInt32), "uint32");
        assert_eq!(arrow_type_to_string(&DataType::UInt64), "uint64");
        assert_eq!(arrow_type_to_string(&DataType::Float16), "float16");
        assert_eq!(arrow_type_to_string(&DataType::Float32), "float");
        assert_eq!(arrow_type_to_string(&DataType::Float64), "double");
        assert_eq!(arrow_type_to_string(&DataType::Utf8), "string");
        assert_eq!(arrow_type_to_string(&DataType::LargeUtf8), "string");
        assert_eq!(arrow_type_to_string(&DataType::Binary), "binary");
        assert_eq!(arrow_type_to_string(&DataType::Date32), "date");
        assert_eq!(arrow_type_to_string(&DataType::Date64), "date");
        assert_eq!(arrow_type_to_string(&DataType::Null), "null");
        assert_eq!(arrow_type_to_string(&DataType::FixedSizeBinary(16)), "fixed_binary(16)");
    }

    #[test]
    fn test_arrow_type_to_string_timestamp() {
        use arrow_schema::TimeUnit;
        assert_eq!(
            arrow_type_to_string(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp"
        );
        assert_eq!(
            arrow_type_to_string(&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            )),
            "timestamp"
        );
    }

    #[test]
    fn test_arrow_type_to_string_decimal() {
        assert_eq!(
            arrow_type_to_string(&DataType::Decimal128(10, 2)),
            "decimal(10,2)"
        );
        assert_eq!(
            arrow_type_to_string(&DataType::Decimal256(38, 18)),
            "decimal(38,18)"
        );
    }

    #[test]
    fn test_arrow_type_to_string_complex() {
        use arrow_schema::Field;

        // List type → should produce JSON
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let result = arrow_type_to_string(&list_type);
        assert!(
            result.contains("List") || result.contains("list") || result.contains("Int32"),
            "Expected JSON for list type, got: {}",
            result
        );

        // Struct type → should produce JSON
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ]
            .into(),
        );
        let result = arrow_type_to_string(&struct_type);
        assert!(
            result.contains("Struct") || result.contains("struct") || result.contains("Float64"),
            "Expected JSON for struct type, got: {}",
            result
        );
    }

    #[test]
    fn test_read_parquet_schema_local() {
        use arrow_array::{RecordBatch, Int64Array, Float64Array, BooleanArray, StringArray};
        use arrow_schema::{Schema, Field};
        use parquet::arrow::ArrowWriter;

        // Create a temporary parquet file with a known schema
        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_schema.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
                Arc::new(Float64Array::from(vec![Some(95.5), None, Some(87.0)])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Now read the schema back
        let config = DeltaStorageConfig::default();
        let (fields, schema_json) =
            read_parquet_schema(parquet_path.to_str().unwrap(), &config).unwrap();

        assert_eq!(fields.len(), 4);

        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "int64");
        assert!(!fields[0].nullable);

        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].data_type, "string");
        assert!(fields[1].nullable);

        assert_eq!(fields[2].name, "score");
        assert_eq!(fields[2].data_type, "double");
        assert!(fields[2].nullable);

        assert_eq!(fields[3].name, "active");
        assert_eq!(fields[3].data_type, "boolean");
        assert!(!fields[3].nullable);

        // Verify schema JSON contains field names
        assert!(schema_json.contains("id"));
        assert!(schema_json.contains("name"));
        assert!(schema_json.contains("score"));
        assert!(schema_json.contains("active"));
    }

    #[test]
    fn test_read_parquet_schema_complex_types() {
        use arrow_array::{RecordBatch, Int32Array, ListArray, StructArray};
        use arrow_schema::{Schema, Field, Fields};
        use arrow_array::builder::ListBuilder;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_complex.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "point",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ])),
                true,
            ),
        ]));

        // Build a simple record batch
        let ids = Int32Array::from(vec![1, 2]);

        let mut list_builder = ListBuilder::new(arrow_array::builder::StringBuilder::new());
        list_builder.values().append_value("a");
        list_builder.values().append_value("b");
        list_builder.append(true);
        list_builder.values().append_value("c");
        list_builder.append(true);
        let tags = list_builder.finish();

        let x_vals = arrow_array::Float64Array::from(vec![1.0, 2.0]);
        let y_vals = arrow_array::Float64Array::from(vec![3.0, 4.0]);
        let point = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(x_vals) as _,
            ),
            (
                Arc::new(Field::new("y", DataType::Float64, false)),
                Arc::new(y_vals) as _,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids), Arc::new(tags), Arc::new(point)],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let config = DeltaStorageConfig::default();
        let (fields, _) = read_parquet_schema(parquet_path.to_str().unwrap(), &config).unwrap();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "int32");

        // Complex types should not be simple primitive strings
        assert_eq!(fields[1].name, "tags");
        assert!(!fields[1].data_type.is_empty());
        // Should contain something about List or Utf8
        assert!(
            fields[1].data_type.contains("List") || fields[1].data_type.contains("list") || fields[1].data_type.contains("Utf8"),
            "Expected complex type for tags, got: {}", fields[1].data_type
        );

        assert_eq!(fields[2].name, "point");
        assert!(
            fields[2].data_type.contains("Struct") || fields[2].data_type.contains("struct") || fields[2].data_type.contains("Float64"),
            "Expected complex type for point, got: {}", fields[2].data_type
        );
    }

    #[test]
    fn test_read_parquet_schema_with_metadata() {
        use arrow_array::{RecordBatch, Int32Array};
        use arrow_schema::{Schema, Field};
        use std::collections::HashMap;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_metadata.parquet");

        let mut field_metadata = HashMap::new();
        field_metadata.insert("comment".to_string(), "primary key".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_metadata),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let config = DeltaStorageConfig::default();
        let (fields, _) = read_parquet_schema(parquet_path.to_str().unwrap(), &config).unwrap();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "id");
        // Metadata should contain our comment
        assert!(
            fields[0].metadata.contains("primary key"),
            "Expected metadata to contain 'primary key', got: {}",
            fields[0].metadata
        );
    }

    #[test]
    fn test_read_parquet_schema_file_url() {
        use arrow_array::{RecordBatch, Int32Array};
        use arrow_schema::{Schema, Field};

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_file_url.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![42]))],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Use file:// URL instead of bare path
        let file_url = format!("file://{}", parquet_path.to_str().unwrap());
        let config = DeltaStorageConfig::default();
        let (fields, _) = read_parquet_schema(&file_url, &config).unwrap();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "val");
        assert_eq!(fields[0].data_type, "int32");
    }

    #[test]
    fn test_read_column_mapping_with_field_ids() {
        use arrow_array::{RecordBatch, Int64Array, StringArray, Float64Array};
        use arrow_schema::{Schema, Field};
        use std::collections::HashMap;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_column_mapping.parquet");

        // Create a parquet file with physical names (col_1, col_2, col_3)
        // but with PARQUET:field_id metadata mapping to Iceberg field IDs
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_1", DataType::Int64, false)
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "1".to_string()),
                ])),
            Field::new("col_2", DataType::Utf8, true)
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "2".to_string()),
                ])),
            Field::new("col_3", DataType::Float64, true)
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "3".to_string()),
                ])),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
                Arc::new(Float64Array::from(vec![Some(99.9), Some(88.8)])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Build the Iceberg field_id → logical_name map
        let mut field_id_to_name = HashMap::new();
        field_id_to_name.insert(1, "id".to_string());
        field_id_to_name.insert(2, "name".to_string());
        field_id_to_name.insert(3, "price".to_string());

        let config = DeltaStorageConfig::default();
        let mapping = read_column_mapping(
            parquet_path.to_str().unwrap(),
            &field_id_to_name,
            &config,
        )
        .unwrap();

        // Verify physical → logical mapping
        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping.get("col_1").unwrap(), "id");
        assert_eq!(mapping.get("col_2").unwrap(), "name");
        assert_eq!(mapping.get("col_3").unwrap(), "price");
    }

    #[test]
    fn test_read_column_mapping_identity_when_no_field_ids() {
        use arrow_array::{RecordBatch, Int64Array, StringArray};
        use arrow_schema::{Schema, Field};
        use std::collections::HashMap;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_identity_mapping.parquet");

        // Create a parquet file WITHOUT field_id metadata
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("alice")])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Even with an Iceberg mapping, should fall back to identity since no field IDs
        let mut field_id_to_name = HashMap::new();
        field_id_to_name.insert(1, "identifier".to_string());
        field_id_to_name.insert(2, "full_name".to_string());

        let config = DeltaStorageConfig::default();
        let mapping = read_column_mapping(
            parquet_path.to_str().unwrap(),
            &field_id_to_name,
            &config,
        )
        .unwrap();

        // Should be identity mapping since parquet has no field IDs
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping.get("id").unwrap(), "id");
        assert_eq!(mapping.get("name").unwrap(), "name");
    }

    #[test]
    fn test_read_column_mapping_partial_match() {
        use arrow_array::{RecordBatch, Int64Array, StringArray, Float64Array};
        use arrow_schema::{Schema, Field};
        use std::collections::HashMap;

        let tmp = tempfile::tempdir().unwrap();
        let parquet_path = tmp.path().join("test_partial_mapping.parquet");

        // Only some fields have PARQUET:field_id
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_1", DataType::Int64, false)
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "1".to_string()),
                ])),
            Field::new("untagged_col", DataType::Utf8, true), // no field_id
            Field::new("col_3", DataType::Float64, true)
                .with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "3".to_string()),
                ])),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("val")])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut field_id_to_name = HashMap::new();
        field_id_to_name.insert(1, "id".to_string());
        field_id_to_name.insert(2, "name".to_string()); // won't match — no field with id=2
        field_id_to_name.insert(3, "price".to_string());

        let config = DeltaStorageConfig::default();
        let mapping = read_column_mapping(
            parquet_path.to_str().unwrap(),
            &field_id_to_name,
            &config,
        )
        .unwrap();

        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping.get("col_1").unwrap(), "id");
        assert_eq!(mapping.get("untagged_col").unwrap(), "untagged_col"); // identity
        assert_eq!(mapping.get("col_3").unwrap(), "price");
    }
}
