// name_mapping.rs - Field ID name resolution (Phase 3)
//
// Resolves parquet column names to display names using:
// 1. Explicit field ID mapping provided by the user
// 2. Auto-detection from Iceberg schema metadata in parquet files
// 3. Direct 1:1 name matching (fallback)

use std::collections::HashMap;
use anyhow::Result;

use crate::debug_println;

/// Resolved name mapping: parquet column name → display/tantivy field name
pub type NameMapping = HashMap<String, String>;

/// Resolve the name mapping for parquet columns.
///
/// Priority (highest first):
/// 1. Explicit mapping from user config
/// 2. Auto-detected from iceberg.schema metadata
/// 3. Identity mapping (column name = field name)
pub fn resolve_name_mapping(
    parquet_metadata: &parquet::file::metadata::ParquetMetaData,
    explicit_mapping: &HashMap<String, String>,
    auto_detect: bool,
) -> Result<NameMapping> {
    let mut mapping = NameMapping::new();

    // Start with an identity mapping for every top-level field.
    //
    // We deliberately enumerate the root group's direct children rather than
    // parquet *leaf* columns: the top-level fields are exactly what become
    // tantivy fields (see `derive_tantivy_schema_with_mapping`). For nested
    // types (struct/list/map) the leaf enumeration explodes a single top-level
    // column into multiple leaves — and, critically for Iceberg, hangs the
    // field-id off the *group* node, which never appears in the leaf list.
    for field_name in top_level_field_names(parquet_metadata) {
        mapping.insert(field_name.clone(), field_name);
    }

    // Layer 2: Auto-detect Iceberg mapping if requested
    if auto_detect {
        if let Some(iceberg_mapping) = auto_detect_iceberg_mapping(parquet_metadata) {
            debug_println!(
                "📋 NAME_MAPPING: Auto-detected {} Iceberg field mappings",
                iceberg_mapping.len()
            );
            for (col_name, display_name) in iceberg_mapping {
                mapping.insert(col_name, display_name);
            }
        }
    }

    // Layer 3: Explicit mapping overrides everything
    for (col_name, display_name) in explicit_mapping {
        mapping.insert(col_name.clone(), display_name.clone());
    }

    Ok(mapping)
}

/// Enumerate the top-level (root) schema field names.
///
/// These are the columns that become tantivy fields. We intentionally use the
/// root group's direct children rather than parquet leaf columns — for nested
/// types (struct/list/map) a single top-level column expands into multiple
/// leaves, and Iceberg attaches the field-id to the *group* node, which never
/// appears in the leaf enumeration.
fn top_level_field_names(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Vec<String> {
    metadata
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

/// Auto-detect field ID → name mapping from Iceberg schema metadata.
///
/// Iceberg stores schema as JSON in the parquet file's key-value metadata
/// under the key "iceberg.schema". This contains field IDs that map to the
/// parquet field-ids carried on each top-level schema node (including group
/// nodes for nested types).
fn auto_detect_iceberg_mapping(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Option<NameMapping> {
    let kv_metadata = metadata.file_metadata().key_value_metadata()?;

    let iceberg_schema_json = kv_metadata
        .iter()
        .find(|kv| kv.key == "iceberg.schema")?
        .value
        .as_ref()?;

    // Parse the Iceberg schema JSON
    let iceberg_schema: serde_json::Value = serde_json::from_str(iceberg_schema_json).ok()?;

    let fields = iceberg_schema.get("fields")?.as_array()?;

    // Build a one-pass index of field_id → physical column name over the
    // top-level schema nodes. Iceberg attaches the field-id to the top-level
    // node (group nodes included), which is the granularity that becomes a
    // tantivy field. This avoids the previous O(fields × leaf-columns) scan and
    // — unlike leaf enumeration — resolves field-ids on nested/group columns.
    let mut id_to_column: HashMap<i32, String> = HashMap::new();
    for field in metadata
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields()
    {
        let info = field.get_basic_info();
        if info.has_id() {
            id_to_column.insert(info.id(), field.name().to_string());
        }
    }

    let mut mapping = NameMapping::new();
    for field in fields {
        let id = match field.get("id").and_then(|v| v.as_i64()) {
            Some(id) => id,
            None => continue, // Skip fields with missing/invalid id
        };
        let name = match field.get("name").and_then(|v| v.as_str()) {
            Some(name) => name,
            None => continue, // Skip fields with missing/invalid name
        };

        // Look up the physical column carrying this field-id (if any).
        if let Some(col_name) = id_to_column.get(&(id as i32)) {
            mapping.insert(col_name.clone(), name.to_string());
        }
    }

    if mapping.is_empty() {
        None
    } else {
        Some(mapping)
    }
}

/// Validate the resolved name mapping against the parquet schema.
///
/// Both invariants are checked against the set of *top-level* fields (the
/// columns that become tantivy fields), not parquet leaf columns:
///
/// 1. **Completeness** — every top-level field has a mapping entry.
/// 2. **No dangling keys** — every mapping key names a real top-level field.
///    This makes the check non-vacuous: it catches typo'd explicit-mapping
///    keys (which are otherwise silent no-ops) and any mapping key that
///    resolves to no real column.
///
/// # Limitation
/// Mapping is performed at top-level field granularity. Renaming a sub-field
/// *inside* a nested struct/list/map is not modeled here — only the top-level
/// column (which is what tantivy indexes) is validated.
pub fn validate_name_mapping_completeness(
    mapping: &NameMapping,
    parquet_metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<()> {
    use std::collections::HashSet;

    let field_names: HashSet<String> = top_level_field_names(parquet_metadata)
        .into_iter()
        .collect();

    // (1) Every real top-level field must be mapped.
    let mut unmapped: Vec<String> = field_names
        .iter()
        .filter(|name| !mapping.contains_key(*name))
        .cloned()
        .collect();
    unmapped.sort();

    // (2) Every mapping key must correspond to a real top-level field. This is
    //     where typo'd explicit-mapping keys surface instead of being silently
    //     dropped.
    let mut unknown: Vec<String> = mapping
        .keys()
        .filter(|key| !field_names.contains(*key))
        .cloned()
        .collect();
    unknown.sort();

    if !unmapped.is_empty() || !unknown.is_empty() {
        anyhow::bail!(
            "Invalid name mapping: {} unmapped column(s): {:?}; \
             {} mapping key(s) matching no column: {:?}",
            unmapped.len(),
            unmapped,
            unknown.len(),
            unknown
        );
    }

    Ok(())
}

/// Validate that the same name mapping is used across all files in a split
pub fn validate_consistent_mapping_across_files(
    primary_mapping: &NameMapping,
    file_mappings: &[NameMapping],
) -> Result<()> {
    for (i, mapping) in file_mappings.iter().enumerate() {
        if mapping != primary_mapping {
            let diffs: Vec<_> = primary_mapping
                .iter()
                .filter(|(k, v)| mapping.get(*k) != Some(v))
                .map(|(k, v)| format!("'{}' ({} vs {:?})", k, v, mapping.get(k)))
                .collect();

            anyhow::bail!(
                "Inconsistent name mapping in file[{}]: {} differences: {}",
                i,
                diffs.len(),
                diffs.join(", ")
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
    use std::sync::Arc;

    /// A test schema field: a primitive top-level column with an optional
    /// Iceberg field-id.
    fn primitive_field(name: &str, field_id: Option<i32>) -> TypePtr {
        Arc::new(
            Type::primitive_type_builder(name, parquet::basic::Type::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_id(field_id)
                .build()
                .unwrap(),
        )
    }

    /// A test schema field: a top-level *group* (nested) column carrying an
    /// optional Iceberg field-id on the group node itself. Exercises the M5
    /// case where the field-id lives on a group node, not a leaf.
    fn group_field(name: &str, field_id: Option<i32>, child: &str) -> TypePtr {
        Arc::new(
            Type::group_type_builder(name)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_id(field_id)
                .with_fields(vec![primitive_field(child, None)])
                .build()
                .unwrap(),
        )
    }

    /// Build test parquet metadata from a set of top-level fields and an
    /// optional `iceberg.schema` JSON payload.
    fn make_metadata(fields: Vec<TypePtr>, iceberg_schema: Option<&str>) -> ParquetMetaData {
        let schema = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        ));
        let kv = iceberg_schema.map(|json| {
            vec![KeyValue::new(
                "iceberg.schema".to_string(),
                Some(json.to_string()),
            )]
        });
        let file_metadata = FileMetaData::new(1, 0, None, kv, Arc::new(schema), None);
        ParquetMetaData::new(file_metadata, vec![])
    }

    #[test]
    fn test_resolve_identity_mapping_top_level_fields() {
        let meta = make_metadata(
            vec![primitive_field("col_a", None), primitive_field("col_b", None)],
            None,
        );
        let mapping = resolve_name_mapping(&meta, &HashMap::new(), false).unwrap();
        assert_eq!(mapping.get("col_a"), Some(&"col_a".to_string()));
        assert_eq!(mapping.get("col_b"), Some(&"col_b".to_string()));
        assert_eq!(mapping.len(), 2);
    }

    #[test]
    fn test_resolve_identity_uses_group_node_not_leaf() {
        // A nested group column has a child leaf named "child"; the top-level
        // field is "nested". The mapping must key on the top-level field.
        let meta = make_metadata(vec![group_field("nested", None, "child")], None);
        let mapping = resolve_name_mapping(&meta, &HashMap::new(), false).unwrap();
        assert!(mapping.contains_key("nested"), "top-level group must be mapped");
        assert!(!mapping.contains_key("child"), "leaf child must not appear");
    }

    #[test]
    fn test_explicit_mapping_overrides() {
        let meta = make_metadata(vec![primitive_field("col_a", None)], None);
        let mut explicit = HashMap::new();
        explicit.insert("col_a".to_string(), "renamed_a".to_string());
        let mapping = resolve_name_mapping(&meta, &explicit, false).unwrap();
        assert_eq!(mapping.get("col_a"), Some(&"renamed_a".to_string()));
    }

    #[test]
    fn test_auto_detect_iceberg_primitive() {
        let iceberg = r#"{"fields":[{"id":1,"name":"user_name"},{"id":2,"name":"user_age"}]}"#;
        let meta = make_metadata(
            vec![
                primitive_field("col1", Some(1)),
                primitive_field("col2", Some(2)),
            ],
            Some(iceberg),
        );
        let mapping = resolve_name_mapping(&meta, &HashMap::new(), true).unwrap();
        assert_eq!(mapping.get("col1"), Some(&"user_name".to_string()));
        assert_eq!(mapping.get("col2"), Some(&"user_age".to_string()));
    }

    #[test]
    fn test_auto_detect_iceberg_matches_group_node_field_id() {
        // M5 regression: the field-id lives on the top-level group node, which
        // the old leaf-column scan could never match.
        let iceberg = r#"{"fields":[{"id":7,"name":"address"}]}"#;
        let meta = make_metadata(vec![group_field("addr_struct", Some(7), "city")], Some(iceberg));
        let mapping = resolve_name_mapping(&meta, &HashMap::new(), true).unwrap();
        assert_eq!(
            mapping.get("addr_struct"),
            Some(&"address".to_string()),
            "renaming a nested/group column via field-id must work"
        );
    }

    #[test]
    fn test_validate_completeness_ok() {
        let meta = make_metadata(
            vec![primitive_field("col_a", None), primitive_field("col_b", None)],
            None,
        );
        let mapping = resolve_name_mapping(&meta, &HashMap::new(), false).unwrap();
        assert!(validate_name_mapping_completeness(&mapping, &meta).is_ok());
    }

    #[test]
    fn test_validate_detects_unmapped_column() {
        let meta = make_metadata(
            vec![primitive_field("col_a", None), primitive_field("col_b", None)],
            None,
        );
        // Mapping is missing col_b.
        let mut mapping = NameMapping::new();
        mapping.insert("col_a".to_string(), "col_a".to_string());
        let err = validate_name_mapping_completeness(&mapping, &meta).unwrap_err();
        assert!(err.to_string().contains("unmapped"), "got: {}", err);
        assert!(err.to_string().contains("col_b"), "got: {}", err);
    }

    #[test]
    fn test_validate_detects_typoed_explicit_key() {
        // L13 regression: an explicit-mapping key naming a non-existent column
        // must be reported, not silently dropped.
        let meta = make_metadata(vec![primitive_field("col_a", None)], None);
        let mut explicit = HashMap::new();
        explicit.insert("col_typo".to_string(), "renamed".to_string());
        let mapping = resolve_name_mapping(&meta, &explicit, false).unwrap();
        let err = validate_name_mapping_completeness(&mapping, &meta).unwrap_err();
        assert!(
            err.to_string().contains("matching no column"),
            "got: {}",
            err
        );
        assert!(err.to_string().contains("col_typo"), "got: {}", err);
    }

    #[test]
    fn test_consistent_mapping_ok() {
        let mut primary = NameMapping::new();
        primary.insert("col_a".to_string(), "field_a".to_string());
        primary.insert("col_b".to_string(), "field_b".to_string());

        let same = primary.clone();
        assert!(validate_consistent_mapping_across_files(&primary, &[same]).is_ok());
    }

    #[test]
    fn test_consistent_mapping_empty_ok() {
        let primary = NameMapping::new();
        assert!(validate_consistent_mapping_across_files(&primary, &[]).is_ok());
    }

    #[test]
    fn test_consistent_mapping_mismatch() {
        let mut primary = NameMapping::new();
        primary.insert("col_a".to_string(), "field_a".to_string());

        let mut different = NameMapping::new();
        different.insert("col_a".to_string(), "field_x".to_string());

        let result = validate_consistent_mapping_across_files(&primary, &[different]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Inconsistent"));
    }

    #[test]
    fn test_consistent_mapping_multiple_files() {
        let mut primary = NameMapping::new();
        primary.insert("id".to_string(), "id".to_string());

        let same1 = primary.clone();
        let same2 = primary.clone();
        assert!(validate_consistent_mapping_across_files(&primary, &[same1, same2]).is_ok());
    }

    #[test]
    fn test_consistent_mapping_second_file_differs() {
        let mut primary = NameMapping::new();
        primary.insert("col_a".to_string(), "field_a".to_string());

        let same = primary.clone();
        let mut different = NameMapping::new();
        different.insert("col_a".to_string(), "field_z".to_string());

        let result = validate_consistent_mapping_across_files(&primary, &[same, different]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("file[1]"));
    }
}
