// iceberg_reader/scan.rs - Core logic: catalog → table → snapshots/files/schema
//
// Reads an Iceberg table's metadata through a catalog and returns file listings,
// schema information, and snapshot history.

use std::collections::HashMap;
use anyhow::Result;

use iceberg::{NamespaceIdent, TableIdent};
use iceberg::spec::{ManifestStatus, DataContentType, DataFileFormat};

use iceberg::Catalog;
use crate::debug_println;
use super::catalog::create_catalog;

// ── Result types ──────────────────────────────────────────────────────────────

/// Metadata about a single active data file in an Iceberg table.
#[derive(Debug, Clone)]
pub struct IcebergFileEntry {
    /// Data file path (full URI with scheme)
    pub path: String,
    /// File format: "parquet", "orc", "avro", "puffin"
    pub file_format: String,
    /// Number of records in the file
    pub record_count: i64,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Partition column name → value mappings
    pub partition_values: HashMap<String, String>,
    /// Content type: "data", "equality_deletes", "position_deletes"
    pub content_type: String,
    /// Snapshot ID that added this file
    pub snapshot_id: i64,
}

/// A field in the Iceberg table schema.
#[derive(Debug, Clone)]
pub struct IcebergSchemaField {
    /// Column name
    pub name: String,
    /// Data type string (e.g. "long", "string") or JSON for complex types
    pub data_type: String,
    /// Iceberg field ID
    pub field_id: i32,
    /// Whether the field is nullable
    pub nullable: bool,
    /// Field documentation (from schema metadata)
    pub doc: Option<String>,
}

/// A snapshot in the Iceberg table's history.
#[derive(Debug, Clone)]
pub struct IcebergSnapshot {
    /// Unique snapshot identifier
    pub snapshot_id: i64,
    /// Parent snapshot ID (None for the first snapshot)
    pub parent_snapshot_id: Option<i64>,
    /// Monotonically increasing sequence number
    pub sequence_number: i64,
    /// Timestamp in milliseconds since epoch
    pub timestamp_ms: i64,
    /// Path to the manifest list file
    pub manifest_list: String,
    /// Operation that created this snapshot: "append", "overwrite", "replace", "delete"
    pub operation: String,
    /// Snapshot summary properties (includes operation, added/deleted counts, etc.)
    pub summary: HashMap<String, String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse a namespace string into a NamespaceIdent.
/// Supports dotted notation: "db.schema" → ["db", "schema"]
fn parse_namespace(namespace: &str) -> NamespaceIdent {
    if namespace.contains('.') {
        let parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();
        NamespaceIdent::from_vec(parts).unwrap_or_else(|_| NamespaceIdent::new(namespace.to_string()))
    } else {
        NamespaceIdent::new(namespace.to_string())
    }
}

fn format_to_string(fmt: DataFileFormat) -> String {
    match fmt {
        DataFileFormat::Parquet => "parquet".to_string(),
        DataFileFormat::Avro => "avro".to_string(),
        DataFileFormat::Orc => "orc".to_string(),
        DataFileFormat::Puffin => "puffin".to_string(),
    }
}

fn content_type_to_string(ct: DataContentType) -> String {
    match ct {
        DataContentType::Data => "data".to_string(),
        DataContentType::EqualityDeletes => "equality_deletes".to_string(),
        DataContentType::PositionDeletes => "position_deletes".to_string(),
    }
}

/// Convert an Iceberg Literal to a string for partition values.
fn literal_to_string(lit: &iceberg::spec::Literal) -> String {
    match lit {
        iceberg::spec::Literal::Primitive(p) => {
            use iceberg::spec::PrimitiveLiteral;
            match p {
                PrimitiveLiteral::Boolean(b) => b.to_string(),
                PrimitiveLiteral::Int(i) => i.to_string(),
                PrimitiveLiteral::Long(l) => l.to_string(),
                PrimitiveLiteral::Float(f) => f.to_string(),
                PrimitiveLiteral::Double(d) => d.to_string(),
                PrimitiveLiteral::String(s) => s.clone(),
                PrimitiveLiteral::Int128(i) => i.to_string(),
                PrimitiveLiteral::UInt128(u) => u.to_string(),
                _ => format!("{:?}", p),
            }
        }
        other => format!("{:?}", other),
    }
}

/// Convert an Iceberg Type to a human-readable string.
/// Primitive types return simple names; complex types return JSON.
fn type_to_string(ty: &iceberg::spec::Type) -> String {
    match ty {
        iceberg::spec::Type::Primitive(p) => {
            // PrimitiveType Display gives lowercase names
            format!("{}", p)
        }
        other => {
            // Complex types (struct, list, map) → JSON representation
            serde_json::to_string(other).unwrap_or_else(|_| "unknown".to_string())
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// List active data files in an Iceberg table at the current or specific snapshot.
///
/// Loads the table via catalog, reads manifest list → manifests → data files.
/// If `snapshot_id` is None, uses the current snapshot.
///
/// Returns `(file_entries, actual_snapshot_id)`.
pub fn list_iceberg_files(
    catalog_name: &str,
    config: &HashMap<String, String>,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<(Vec<IcebergFileEntry>, i64)> {
    debug_println!(
        "🔧 ICEBERG_SCAN: Listing files for {}.{} snapshot={:?}",
        namespace, table_name, snapshot_id
    );

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        list_files_with_catalog(catalog.as_ref(), namespace, table_name, snapshot_id).await
    })
}

/// Inner async function for listing files — accepts a catalog reference directly,
/// enabling unit tests with MemoryCatalog.
pub(crate) async fn list_files_with_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<(Vec<IcebergFileEntry>, i64)> {
    let ns = parse_namespace(namespace);
    let table_ident = TableIdent::new(ns, table_name.to_string());
    let table = catalog.load_table(&table_ident).await
        .map_err(|e| anyhow::anyhow!("Failed to load table {}.{}: {}", namespace, table_name, e))?;

    let metadata = table.metadata();

    // Resolve snapshot
    let snapshot = match snapshot_id {
        Some(id) => metadata
            .snapshot_by_id(id)
            .ok_or_else(|| anyhow::anyhow!("Snapshot {} not found in table {}.{}", id, namespace, table_name))?,
        None => metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("Table {}.{} has no current snapshot", namespace, table_name))?,
    };
    let actual_snap_id = snapshot.snapshot_id();

    debug_println!(
        "🔧 ICEBERG_SCAN: Using snapshot {} for {}.{}",
        actual_snap_id, namespace, table_name
    );

    // Load manifest list from snapshot
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load manifest list: {}", e))?;

    // Get partition spec for field name resolution
    let partition_spec = metadata.default_partition_spec();

    let mut entries = Vec::new();

    for manifest_file in manifest_list.entries() {
        // Load each manifest file
        let manifest_input = file_io.new_input(&manifest_file.manifest_path)
            .map_err(|e| anyhow::anyhow!("Failed to open manifest {}: {}", manifest_file.manifest_path, e))?;
        let manifest_bytes = manifest_input.read().await
            .map_err(|e| anyhow::anyhow!("Failed to read manifest {}: {}", manifest_file.manifest_path, e))?;

        let manifest = iceberg::spec::Manifest::parse_avro(manifest_bytes.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to parse manifest: {}", e))?;

        for entry in manifest.entries() {
            // Skip deleted entries
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }

            let data_file = entry.data_file();

            // Extract partition values using partition spec field names
            let mut partition_values = HashMap::new();
            let partition_fields = data_file.partition().fields();
            for (idx, spec_field) in partition_spec.fields().iter().enumerate() {
                if let Some(Some(literal)) = partition_fields.get(idx) {
                    partition_values.insert(
                        spec_field.name.clone(),
                        literal_to_string(literal),
                    );
                }
            }

            entries.push(IcebergFileEntry {
                path: data_file.file_path().to_string(),
                file_format: format_to_string(data_file.file_format()),
                record_count: data_file.record_count() as i64,
                file_size_bytes: data_file.file_size_in_bytes() as i64,
                partition_values,
                content_type: content_type_to_string(data_file.content_type()),
                snapshot_id: entry.snapshot_id().unwrap_or(manifest_file.added_snapshot_id),
            });
        }
    }

    debug_println!(
        "🔧 ICEBERG_SCAN: Found {} active files at snapshot {}",
        entries.len(), actual_snap_id
    );

    Ok((entries, actual_snap_id))
}

/// Read the schema of an Iceberg table.
///
/// Returns `(fields, schema_json, actual_snapshot_id)` where:
/// - `fields` is the list of top-level columns with types and IDs
/// - `schema_json` is the full Iceberg schema as a JSON string
/// - `actual_snapshot_id` is the resolved snapshot ID (or -1 if no snapshot)
pub fn read_iceberg_schema(
    catalog_name: &str,
    config: &HashMap<String, String>,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<(Vec<IcebergSchemaField>, String, i64)> {
    debug_println!(
        "🔧 ICEBERG_SCHEMA: Reading schema for {}.{} snapshot={:?}",
        namespace, table_name, snapshot_id
    );

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        read_schema_with_catalog(catalog.as_ref(), namespace, table_name, snapshot_id).await
    })
}

/// Inner async function for reading schema — accepts a catalog reference directly.
pub(crate) async fn read_schema_with_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<(Vec<IcebergSchemaField>, String, i64)> {
    let ns = parse_namespace(namespace);
    let table_ident = TableIdent::new(ns, table_name.to_string());
    let table = catalog.load_table(&table_ident).await
        .map_err(|e| anyhow::anyhow!("Failed to load table {}.{}: {}", namespace, table_name, e))?;

    let metadata = table.metadata();

    // Get the schema — use snapshot-specific schema if available, otherwise current
    let schema = if let Some(snap_id) = snapshot_id {
        if let Some(snapshot) = metadata.snapshot_by_id(snap_id) {
            // Try to get schema for this snapshot's schema_id
            if let Some(schema_id) = snapshot.schema_id() {
                metadata.schema_by_id(schema_id)
                    .unwrap_or_else(|| metadata.current_schema())
            } else {
                metadata.current_schema()
            }
        } else {
            return Err(anyhow::anyhow!("Snapshot {} not found", snap_id));
        }
    } else {
        metadata.current_schema()
    };

    // Determine actual snapshot ID
    let actual_snap_id = match snapshot_id {
        Some(id) => id,
        None => metadata
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(-1),
    };

    // Convert schema fields
    let fields: Vec<IcebergSchemaField> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| IcebergSchemaField {
            name: field.name.clone(),
            data_type: type_to_string(&field.field_type),
            field_id: field.id,
            nullable: !field.required,
            doc: field.doc.clone(),
        })
        .collect();

    // Serialize full schema to JSON
    let schema_json = serde_json::to_string(schema.as_ref())
        .map_err(|e| anyhow::anyhow!("Failed to serialize schema: {}", e))?;

    debug_println!(
        "🔧 ICEBERG_SCHEMA: Schema has {} fields at snapshot {}",
        fields.len(), actual_snap_id
    );

    Ok((fields, schema_json, actual_snap_id))
}

/// List all snapshots (transaction history) of an Iceberg table.
///
/// Returns snapshots in the order they appear in the table metadata,
/// with summary properties including operation type and file counts.
pub fn list_iceberg_snapshots(
    catalog_name: &str,
    config: &HashMap<String, String>,
    namespace: &str,
    table_name: &str,
) -> Result<Vec<IcebergSnapshot>> {
    debug_println!(
        "🔧 ICEBERG_SNAPSHOTS: Listing snapshots for {}.{}",
        namespace, table_name
    );

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        list_snapshots_with_catalog(catalog.as_ref(), namespace, table_name).await
    })
}

/// Inner async function for listing snapshots — accepts a catalog reference directly.
pub(crate) async fn list_snapshots_with_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
) -> Result<Vec<IcebergSnapshot>> {
    let ns = parse_namespace(namespace);
    let table_ident = TableIdent::new(ns, table_name.to_string());
    let table = catalog.load_table(&table_ident).await
        .map_err(|e| anyhow::anyhow!("Failed to load table {}.{}: {}", namespace, table_name, e))?;

    let metadata = table.metadata();
    let mut snapshots = Vec::new();

    for snapshot in metadata.snapshots() {
        let summary_map = snapshot.summary();

        // Extract operation from summary
        let operation = summary_map
            .additional_properties
            .get("operation")
            .cloned()
            .unwrap_or_else(|| format!("{:?}", summary_map.operation).to_lowercase());

        // Build full summary HashMap (operation + additional properties)
        let mut summary = summary_map.additional_properties.clone();
        summary.insert("operation".to_string(), operation.clone());

        snapshots.push(IcebergSnapshot {
            snapshot_id: snapshot.snapshot_id(),
            parent_snapshot_id: snapshot.parent_snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            timestamp_ms: snapshot.timestamp_ms(),
            manifest_list: snapshot.manifest_list().to_string(),
            operation,
            summary,
        });
    }

    debug_println!(
        "🔧 ICEBERG_SNAPSHOTS: Found {} snapshots for {}.{}",
        snapshots.len(), namespace, table_name
    );

    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_namespace_simple() {
        let ns = parse_namespace("default");
        assert_eq!(ns.to_url_string(), "default");
    }

    #[test]
    fn test_parse_namespace_dotted() {
        let ns = parse_namespace("my_db.my_schema");
        // Dotted namespace should have 2 levels
        assert!(ns.to_url_string().contains("my_db"));
    }

    #[test]
    fn test_format_to_string() {
        assert_eq!(format_to_string(DataFileFormat::Parquet), "parquet");
        assert_eq!(format_to_string(DataFileFormat::Avro), "avro");
        assert_eq!(format_to_string(DataFileFormat::Orc), "orc");
    }

    #[test]
    fn test_content_type_to_string() {
        assert_eq!(content_type_to_string(DataContentType::Data), "data");
        assert_eq!(content_type_to_string(DataContentType::EqualityDeletes), "equality_deletes");
        assert_eq!(content_type_to_string(DataContentType::PositionDeletes), "position_deletes");
    }

    #[test]
    fn test_iceberg_file_entry_construction() {
        let entry = IcebergFileEntry {
            path: "s3://bucket/data/part-00000.parquet".to_string(),
            file_format: "parquet".to_string(),
            record_count: 1000,
            file_size_bytes: 50000,
            partition_values: HashMap::new(),
            content_type: "data".to_string(),
            snapshot_id: 12345,
        };
        assert_eq!(entry.path, "s3://bucket/data/part-00000.parquet");
        assert_eq!(entry.record_count, 1000);
    }

    #[test]
    fn test_iceberg_schema_field_construction() {
        let field = IcebergSchemaField {
            name: "id".to_string(),
            data_type: "long".to_string(),
            field_id: 1,
            nullable: false,
            doc: Some("Primary key".to_string()),
        };
        assert_eq!(field.name, "id");
        assert_eq!(field.field_id, 1);
        assert!(!field.nullable);
        assert_eq!(field.doc, Some("Primary key".to_string()));
    }

    #[test]
    fn test_iceberg_snapshot_construction() {
        let mut summary = HashMap::new();
        summary.insert("operation".to_string(), "append".to_string());
        summary.insert("added-files-count".to_string(), "5".to_string());

        let snapshot = IcebergSnapshot {
            snapshot_id: 100,
            parent_snapshot_id: Some(99),
            sequence_number: 10,
            timestamp_ms: 1700000000000,
            manifest_list: "s3://bucket/metadata/snap-100-manifest-list.avro".to_string(),
            operation: "append".to_string(),
            summary,
        };
        assert_eq!(snapshot.snapshot_id, 100);
        assert_eq!(snapshot.parent_snapshot_id, Some(99));
        assert_eq!(snapshot.operation, "append");
    }

    // ── MemoryCatalog integration tests ─────────────────────────────────────

    use std::sync::Arc;
    use iceberg::spec::{NestedField, PrimitiveType, Type, Schema};
    use iceberg::{Catalog, CatalogBuilder, TableCreation};
    use iceberg::memory::MemoryCatalogBuilder;

    /// Helper: create a MemoryCatalog with a warehouse rooted in a temp dir.
    async fn create_test_catalog(warehouse: &std::path::Path) -> Arc<dyn Catalog> {
        let mut props = HashMap::new();
        props.insert("warehouse".to_string(), warehouse.to_str().unwrap().to_string());
        let catalog = MemoryCatalogBuilder::default()
            .load("test_catalog", props)
            .await
            .expect("Failed to create MemoryCatalog");
        Arc::new(catalog)
    }

    /// Helper: build a simple Iceberg schema with id, name, score, active fields.
    fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
                Arc::new(NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))),
                Arc::new(NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Double))),
                Arc::new(NestedField::optional(4, "active", Type::Primitive(PrimitiveType::Boolean))),
            ])
            .build()
            .expect("Failed to build test schema")
    }

    /// Helper: create a namespace and table in the catalog, return the table.
    async fn create_test_table(catalog: &dyn Catalog) -> iceberg::table::Table {
        let ns = NamespaceIdent::new("test_db".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await
            .expect("Failed to create namespace");

        let creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(test_schema())
            .build();

        catalog.create_table(&ns, creation).await
            .expect("Failed to create table")
    }

    #[tokio::test]
    async fn test_read_schema_from_memory_catalog() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let _table = create_test_table(catalog.as_ref()).await;

        let (fields, schema_json, snap_id) =
            read_schema_with_catalog(catalog.as_ref(), "test_db", "test_table", None)
                .await
                .expect("read_schema_with_catalog failed");

        // No snapshots → snap_id = -1
        assert_eq!(snap_id, -1);

        // Should have 4 fields
        assert_eq!(fields.len(), 4);

        // Verify field names and types
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "long");
        assert_eq!(fields[0].field_id, 1);
        assert!(!fields[0].nullable); // required

        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].data_type, "string");
        assert!(fields[1].nullable); // optional

        assert_eq!(fields[2].name, "score");
        assert_eq!(fields[2].data_type, "double");

        assert_eq!(fields[3].name, "active");
        assert_eq!(fields[3].data_type, "boolean");

        // schema_json should be valid JSON containing field info
        assert!(!schema_json.is_empty());
        let parsed: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        assert!(parsed.is_object());
    }

    #[tokio::test]
    async fn test_read_schema_complex_types() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;

        let ns = NamespaceIdent::new("test_db".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        // Create table with complex types: list and map
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
                Arc::new(NestedField::optional(
                    2,
                    "tags",
                    Type::List(iceberg::spec::ListType {
                        element_field: Arc::new(NestedField::list_element(
                            3,
                            Type::Primitive(PrimitiveType::String),
                            false,
                        )),
                    }),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "metadata",
                    Type::Map(iceberg::spec::MapType {
                        key_field: Arc::new(NestedField::map_key_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        value_field: Arc::new(NestedField::map_value_element(
                            6,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )),
                    }),
                )),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .name("complex_table".to_string())
            .schema(schema)
            .build();

        catalog.create_table(&ns, creation).await.unwrap();

        let (fields, _json, _snap) =
            read_schema_with_catalog(catalog.as_ref(), "test_db", "complex_table", None)
                .await
                .unwrap();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].data_type, "long"); // primitive

        // Complex types should be JSON-serialized (starts with "{")
        assert!(fields[1].data_type.starts_with('{'), "List type should be JSON: {}", fields[1].data_type);
        assert!(fields[2].data_type.starts_with('{'), "Map type should be JSON: {}", fields[2].data_type);
    }

    #[tokio::test]
    async fn test_list_snapshots_empty_table() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let _table = create_test_table(catalog.as_ref()).await;

        let snapshots =
            list_snapshots_with_catalog(catalog.as_ref(), "test_db", "test_table")
                .await
                .expect("list_snapshots_with_catalog failed");

        // Freshly created table has no snapshots
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_list_files_no_snapshot_error() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let _table = create_test_table(catalog.as_ref()).await;

        let result =
            list_files_with_catalog(catalog.as_ref(), "test_db", "test_table", None).await;

        // Table has no current snapshot → should error
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no current snapshot"),
            "Expected 'no current snapshot' error, got: {}", err_msg
        );
    }

    #[tokio::test]
    async fn test_list_files_nonexistent_snapshot_error() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let _table = create_test_table(catalog.as_ref()).await;

        let result =
            list_files_with_catalog(catalog.as_ref(), "test_db", "test_table", Some(99999))
                .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("99999") && err_msg.contains("not found"),
            "Expected snapshot-not-found error, got: {}", err_msg
        );
    }

    #[tokio::test]
    async fn test_read_schema_nonexistent_table_error() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;

        let ns = NamespaceIdent::new("test_db".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let result =
            read_schema_with_catalog(catalog.as_ref(), "test_db", "no_such_table", None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_snapshots_with_fast_append() {
        use iceberg::spec::{DataFileBuilder, DataContentType as DC, DataFileFormat as DFF, Struct};
        use iceberg::transaction::Transaction;
        use iceberg::transaction::ApplyTransactionAction;

        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let table = create_test_table(catalog.as_ref()).await;

        let spec_id = table.metadata().default_partition_spec_id();

        // Build a synthetic data file entry
        let data_file = DataFileBuilder::default()
            .content(DC::Data)
            .file_path(format!("{}/data/part-00000.parquet", tmpdir.path().display()))
            .file_format(DFF::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(spec_id)
            .record_count(100)
            .file_size_in_bytes(4096)
            .build()
            .expect("Failed to build DataFile");

        // Fast-append the data file via a transaction
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog.as_ref()).await
            .expect("Failed to commit fast_append transaction");

        // Now list snapshots — should have exactly 1
        let snapshots =
            list_snapshots_with_catalog(catalog.as_ref(), "test_db", "test_table")
                .await
                .expect("list_snapshots_with_catalog failed");

        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].snapshot_id > 0);
        assert!(snapshots[0].timestamp_ms > 0);
        assert!(!snapshots[0].manifest_list.is_empty());
        // First snapshot has no parent
        assert_eq!(snapshots[0].parent_snapshot_id, None);

        // Second append → should create a parent chain
        let data_file2 = DataFileBuilder::default()
            .content(DC::Data)
            .file_path(format!("{}/data/part-00001.parquet", tmpdir.path().display()))
            .file_format(DFF::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(spec_id)
            .record_count(200)
            .file_size_in_bytes(8192)
            .build()
            .unwrap();

        let tx2 = Transaction::new(&table);
        let action2 = tx2.fast_append().add_data_files(vec![data_file2]);
        let tx2 = action2.apply(tx2).unwrap();
        let _table2 = tx2.commit(catalog.as_ref()).await
            .expect("Failed to commit second fast_append");

        let snapshots2 =
            list_snapshots_with_catalog(catalog.as_ref(), "test_db", "test_table")
                .await
                .unwrap();

        assert_eq!(snapshots2.len(), 2);
        // Both snapshots should have distinct IDs
        assert_ne!(snapshots2[0].snapshot_id, snapshots2[1].snapshot_id);
        // Both should have valid timestamps
        assert!(snapshots2[0].timestamp_ms > 0);
        assert!(snapshots2[1].timestamp_ms > 0);
        // If parent chain is set (depends on catalog implementation), verify it
        if let Some(parent_id) = snapshots2[1].parent_snapshot_id {
            assert_eq!(parent_id, snapshots2[0].snapshot_id);
        }
        // Sequence numbers should be non-negative
        assert!(snapshots2[0].sequence_number >= 0);
        assert!(snapshots2[1].sequence_number >= 0);
    }

    #[tokio::test]
    async fn test_list_files_from_memory_catalog() {
        use iceberg::spec::{DataFileBuilder, DataContentType as DC, DataFileFormat as DFF, Struct};
        use iceberg::transaction::Transaction;
        use iceberg::transaction::ApplyTransactionAction;

        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let table = create_test_table(catalog.as_ref()).await;

        let spec_id = table.metadata().default_partition_spec_id();

        let data_file = DataFileBuilder::default()
            .content(DC::Data)
            .file_path(format!("{}/data/part-00000.parquet", tmpdir.path().display()))
            .file_format(DFF::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(spec_id)
            .record_count(500)
            .file_size_in_bytes(32768)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = action.apply(tx).unwrap();
        let _table = tx.commit(catalog.as_ref()).await.unwrap();

        let (entries, snap_id) =
            list_files_with_catalog(catalog.as_ref(), "test_db", "test_table", None)
                .await
                .expect("list_files_with_catalog failed");

        assert!(snap_id > 0);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.contains("part-00000.parquet"));
        assert_eq!(entries[0].file_format, "parquet");
        assert_eq!(entries[0].record_count, 500);
        assert_eq!(entries[0].file_size_bytes, 32768);
        assert_eq!(entries[0].content_type, "data");
    }

    #[tokio::test]
    async fn test_list_files_at_specific_snapshot() {
        use iceberg::spec::{DataFileBuilder, DataContentType as DC, DataFileFormat as DFF, Struct};
        use iceberg::transaction::Transaction;
        use iceberg::transaction::ApplyTransactionAction;

        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let table = create_test_table(catalog.as_ref()).await;

        let spec_id = table.metadata().default_partition_spec_id();

        // First append: 1 file
        let df1 = DataFileBuilder::default()
            .content(DC::Data)
            .file_path(format!("{}/data/batch1.parquet", tmpdir.path().display()))
            .file_format(DFF::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(spec_id)
            .record_count(100)
            .file_size_in_bytes(1024)
            .build()
            .unwrap();

        let tx1 = Transaction::new(&table);
        let action1 = tx1.fast_append().add_data_files(vec![df1]);
        let tx1 = action1.apply(tx1).unwrap();
        let table = tx1.commit(catalog.as_ref()).await.unwrap();

        // Capture first snapshot ID
        let first_snap_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Second append: 1 more file
        let df2 = DataFileBuilder::default()
            .content(DC::Data)
            .file_path(format!("{}/data/batch2.parquet", tmpdir.path().display()))
            .file_format(DFF::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(spec_id)
            .record_count(200)
            .file_size_in_bytes(2048)
            .build()
            .unwrap();

        let tx2 = Transaction::new(&table);
        let action2 = tx2.fast_append().add_data_files(vec![df2]);
        let tx2 = action2.apply(tx2).unwrap();
        let _table2 = tx2.commit(catalog.as_ref()).await.unwrap();

        // List files at first snapshot — should see only 1 file
        let (entries1, snap1) =
            list_files_with_catalog(catalog.as_ref(), "test_db", "test_table", Some(first_snap_id))
                .await
                .unwrap();

        assert_eq!(snap1, first_snap_id);
        assert_eq!(entries1.len(), 1);
        assert!(entries1[0].path.contains("batch1.parquet"));

        // List files at current snapshot — should see 2 files
        let (entries_all, _) =
            list_files_with_catalog(catalog.as_ref(), "test_db", "test_table", None)
                .await
                .unwrap();

        assert_eq!(entries_all.len(), 2);
    }

    #[tokio::test]
    async fn test_schema_json_round_trip() {
        let tmpdir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog(tmpdir.path()).await;
        let _table = create_test_table(catalog.as_ref()).await;

        let (_fields, schema_json, _) =
            read_schema_with_catalog(catalog.as_ref(), "test_db", "test_table", None)
                .await
                .unwrap();

        // Parse the JSON and verify it has expected structure
        let parsed: serde_json::Value = serde_json::from_str(&schema_json).unwrap();

        // Iceberg schema JSON should have schema-id and fields
        assert!(parsed.get("schema-id").is_some() || parsed.get("schemaId").is_some(),
            "Schema JSON should contain schema-id: {}", schema_json);
    }
}
