// component_sizes.rs - Per-field component size calculation
// Extracted from mod.rs during refactoring

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
/// Returns a map of "field_name.component" -> size in bytes, where component is one of:
/// - "fastfield" - fast field column size (from list_dynamic_column_handles)
/// - "fieldnorm" - field norm size (from fieldnorm_readers)
///
/// Note: Term dictionary, postings, and positions sizes require reading the data
/// and are calculated during prewarm operations. Store is document-level, not field-level.
pub async fn get_per_field_component_sizes(
    searcher_ptr: jlong,
) -> anyhow::Result<HashMap<String, u64>> {
    debug_println!("📊 COMPONENT_SIZES: Calculating per-field component sizes");

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();
    let mut field_sizes: HashMap<String, u64> = HashMap::new();

    // Iterate through all fields in the schema
    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();

        // Fast fields - get column sizes using list_dynamic_column_handles
        if field_entry.is_fast() {
            let mut total_fast_size = 0u64;
            for (seg_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let fast_field_reader = segment_reader.fast_fields();
                if let Ok(columns) = fast_field_reader
                    .list_dynamic_column_handles(&field_name)
                    .await
                {
                    debug_println!(
                        "📊 COMPONENT_SIZES: {}.fastfield segment {} has {} column(s)",
                        field_name,
                        seg_idx,
                        columns.len()
                    );
                    for (col_idx, col) in columns.iter().enumerate() {
                        // file_slice() returns a FileSlice, HasLen trait provides len()
                        let col_size = col.file_slice().len() as u64;
                        debug_println!(
                            "📊 COMPONENT_SIZES: {}.fastfield segment {} column {} size: {} bytes",
                            field_name,
                            seg_idx,
                            col_idx,
                            col_size
                        );
                        total_fast_size += col_size;
                    }
                }
            }
            if total_fast_size > 0 {
                field_sizes.insert(format!("{}.fastfield", field_name), total_fast_size);
                debug_println!(
                    "📊 COMPONENT_SIZES: {}.fastfield = {} bytes (total)",
                    field_name,
                    total_fast_size
                );
            }
        }

        // Text fields - get fieldnorm, term, postings, and positions sizes
        // Use file_slice.len() to get size WITHOUT reading data (avoids cache pollution)
        match field_entry.field_type() {
            FieldType::Str(_text_options) => {
                // Fieldnorm sizes
                let mut fieldnorm_size = 0u64;
                for segment_reader in searcher.segment_readers() {
                    let fieldnorm_readers = segment_reader.fieldnorms_readers();
                    // open_read returns FileSlice, use .len() to get size without reading
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

                // Note: TERM, POSTINGS, POSITIONS sizes require modifications to Tantivy fork
                // to expose file slice lengths. For now, these are validated via download metrics
                // during prewarm (first call > 0, second call == 0).
            }
            FieldType::JsonObject(json_options) => {
                if json_options.get_text_indexing_options().is_some() {
                    let mut fieldnorm_size = 0u64;
                    for segment_reader in searcher.segment_readers() {
                        let fieldnorm_readers = segment_reader.fieldnorms_readers();
                        // open_read returns FileSlice, use .len() to get size without reading
                        if let Some(file_slice) = fieldnorm_readers.get_inner_file().open_read(field)
                        {
                            fieldnorm_size += file_slice.len() as u64;
                        }
                    }
                    if fieldnorm_size > 0 {
                        field_sizes.insert(format!("{}.fieldnorm", field_name), fieldnorm_size);
                    }

                    // Note: TERM, POSTINGS, POSITIONS per-field sizes are not exposed by Tantivy API.
                    // These components exist at the inverted index level but the file slice lengths
                    // are private fields in InvertedIndexReader. Prewarm validation for these components
                    // must use storage download metrics (first call > 0, second call == 0) rather than
                    // exact size matching.
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
    Ok(field_sizes)
}
