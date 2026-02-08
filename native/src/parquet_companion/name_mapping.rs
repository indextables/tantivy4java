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
    let schema = parquet_metadata.file_metadata().schema_descr();
    let mut mapping = NameMapping::new();

    // Start with identity mapping for all columns
    for i in 0..schema.num_columns() {
        let col = schema.column(i);
        mapping.insert(col.name().to_string(), col.name().to_string());
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

/// Auto-detect field ID → name mapping from Iceberg schema metadata.
///
/// Iceberg stores schema as JSON in the parquet file's key-value metadata
/// under the key "iceberg.schema". This contains field IDs that map to
/// physical column ordinals.
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
    let mut mapping = NameMapping::new();

    for field in fields {
        let id = field.get("id")?.as_i64()?;
        let name = field.get("name")?.as_str()?;

        // In Iceberg, the field ID corresponds to the parquet column's field_id
        // We need to find which parquet column has this field_id
        let schema_descr = metadata.file_metadata().schema_descr();
        for i in 0..schema_descr.num_columns() {
            let col = schema_descr.column(i);
            if col.self_type().get_basic_info().id() == id as i32 {
                mapping.insert(col.name().to_string(), name.to_string());
                break;
            }
        }
    }

    if mapping.is_empty() {
        None
    } else {
        Some(mapping)
    }
}

/// Validate that all parquet columns have a name mapping
pub fn validate_name_mapping_completeness(
    mapping: &NameMapping,
    parquet_metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<()> {
    let schema = parquet_metadata.file_metadata().schema_descr();
    let mut unmapped = Vec::new();

    for i in 0..schema.num_columns() {
        let col = schema.column(i);
        let col_name = col.name();
        if !mapping.contains_key(col_name) {
            unmapped.push(col_name.to_string());
        }
    }

    if !unmapped.is_empty() {
        anyhow::bail!(
            "Incomplete name mapping: {} columns unmapped: {:?}",
            unmapped.len(),
            unmapped
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
