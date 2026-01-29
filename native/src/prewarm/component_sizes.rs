// component_sizes.rs - Per-field component size calculation
// Extracted from mod.rs during refactoring
//
// Uses the bundle file offsets from the split footer to calculate per-field
// component sizes for all segment components. File naming conventions:
// - .term files contain term dictionaries (FST)
// - .idx files contain posting lists
// - .pos files contain term positions
// - .fast files contain fast field data
// - .fieldnorm files contain field normalization data
// - .store files contain document storage

use std::collections::HashMap;
use std::sync::Arc;

use jni::sys::jlong;
use tantivy::schema::FieldType;
use tantivy::HasLen;

use crate::debug_println;
use crate::split_searcher::CachedSearcherContext;
use crate::utils::with_arc_safe;

/// Calculate per-field component sizes for all fields in the index.
///
/// This function uses a hybrid approach:
/// 1. For FASTFIELD and FIELDNORM: Uses async reader APIs (file_slice().len())
/// 2. For TERM, POSTINGS, POSITIONS, STORE: Uses bundle_file_offsets from split footer
///
/// Returns a map of "field_name.component" -> size in bytes, where component is one of:
/// - "term" - term dictionary (FST) size (from bundle .term files)
/// - "postings" - posting lists size (from bundle .idx files)
/// - "positions" - term positions size (from bundle .pos files)
/// - "fastfield" - fast field column size (from async column reader)
/// - "fieldnorm" - field norm size (from async fieldnorm reader)
/// - "_store" - total document store size (from bundle .store files)
///
/// Example output:
/// ```json
/// {
///   "title.term": 1024,
///   "title.postings": 4096,
///   "title.positions": 2048,
///   "title.fieldnorm": 512,
///   "score.fastfield": 256,
///   "_store": 8192
/// }
/// ```
pub async fn get_per_field_component_sizes(
    searcher_ptr: jlong,
) -> anyhow::Result<HashMap<String, u64>> {
    debug_println!("📊 COMPONENT_SIZES: Calculating per-field component sizes");

    // Get searcher context with all needed data
    let (searcher, bundle_offsets) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.bundle_file_offsets.clone())
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();
    let mut field_sizes: HashMap<String, u64> = HashMap::new();

    // ===== PART 1: Extract sizes from bundle file offsets =====
    // Bundle files follow naming convention: <segment_uuid>.<extension>
    // Extensions: .term, .idx (postings), .pos (positions), .store, .fast, .fieldnorm

    // Aggregate sizes by extension type (segment-level, not per-field)
    let mut total_term_size: u64 = 0;
    let mut total_postings_size: u64 = 0;
    let mut total_positions_size: u64 = 0;
    let mut total_store_size: u64 = 0;

    for (path, range) in &bundle_offsets {
        let path_str = path.to_string_lossy();
        let size = range.end - range.start;

        if path_str.ends_with(".term") {
            total_term_size += size;
        } else if path_str.ends_with(".idx") {
            total_postings_size += size;
        } else if path_str.ends_with(".pos") {
            total_positions_size += size;
        } else if path_str.ends_with(".store") {
            total_store_size += size;
        }
        // Note: .fast and .fieldnorm are handled per-field below using async APIs
    }

    // Add segment-level totals for components that aren't per-field in the bundle
    // These are totals across all indexed fields
    if total_term_size > 0 {
        // Distribute to indexed text fields (term dictionaries are per-field in tantivy)
        // For now, report total since bundle doesn't break down by field
        field_sizes.insert("_term_total".to_string(), total_term_size);
    }
    if total_postings_size > 0 {
        field_sizes.insert("_postings_total".to_string(), total_postings_size);
    }
    if total_positions_size > 0 {
        field_sizes.insert("_positions_total".to_string(), total_positions_size);
    }
    if total_store_size > 0 {
        field_sizes.insert("_store".to_string(), total_store_size);
    }

    // ===== PART 2: Get per-field sizes using async reader APIs =====
    // FASTFIELD and FIELDNORM can be calculated per-field using file_slice().len()

    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();

        // Fast fields - get column sizes using list_dynamic_column_handles
        if field_entry.is_fast() {
            let mut total_fast_size = 0u64;
            for segment_reader in searcher.segment_readers() {
                let fast_field_reader = segment_reader.fast_fields();
                if let Ok(columns) = fast_field_reader
                    .list_dynamic_column_handles(&field_name)
                    .await
                {
                    for col in columns.iter() {
                        let col_size = col.file_slice().len() as u64;
                        total_fast_size += col_size;
                    }
                }
            }
            if total_fast_size > 0 {
                field_sizes.insert(format!("{}.fastfield", field_name), total_fast_size);
                debug_println!(
                    "📊 COMPONENT_SIZES: {}.fastfield = {} bytes",
                    field_name,
                    total_fast_size
                );
            }
        }

        // Text fields - get fieldnorm sizes
        match field_entry.field_type() {
            FieldType::Str(_text_options) => {
                let mut fieldnorm_size = 0u64;
                for segment_reader in searcher.segment_readers() {
                    let fieldnorm_readers = segment_reader.fieldnorms_readers();
                    if let Some(file_slice) = fieldnorm_readers.get_inner_file().open_read(field) {
                        fieldnorm_size += file_slice.len() as u64;
                    }
                }
                if fieldnorm_size > 0 {
                    field_sizes.insert(format!("{}.fieldnorm", field_name), fieldnorm_size);
                    debug_println!(
                        "📊 COMPONENT_SIZES: {}.fieldnorm = {} bytes",
                        field_name,
                        fieldnorm_size
                    );
                }
            }
            FieldType::JsonObject(json_options) => {
                if json_options.get_text_indexing_options().is_some() {
                    let mut fieldnorm_size = 0u64;
                    for segment_reader in searcher.segment_readers() {
                        let fieldnorm_readers = segment_reader.fieldnorms_readers();
                        if let Some(file_slice) = fieldnorm_readers.get_inner_file().open_read(field) {
                            fieldnorm_size += file_slice.len() as u64;
                        }
                    }
                    if fieldnorm_size > 0 {
                        field_sizes.insert(format!("{}.fieldnorm", field_name), fieldnorm_size);
                    }
                }
            }
            _ => {
                // Other field types (numeric, etc.) - only fast fields matter
            }
        }
    }

    debug_println!(
        "📊 COMPONENT_SIZES: Calculated {} per-field component sizes",
        field_sizes.len()
    );

    // Log all sizes for debugging
    for (key, size) in &field_sizes {
        debug_println!("📊 COMPONENT_SIZES: {} = {} bytes", key, size);
    }

    Ok(field_sizes)
}
