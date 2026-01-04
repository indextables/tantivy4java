// field_specific.rs - Field-specific prewarm implementations
// Extracted from mod.rs during refactoring

use std::collections::HashSet;
use std::sync::Arc;

use jni::sys::jlong;
use tantivy::schema::FieldType;
use tantivy::HasLen;

use crate::debug_println;
use crate::split_searcher::CachedSearcherContext;
use crate::utils::with_arc_safe;

/// Field-specific term dictionary prewarming
///
/// Only preloads term dictionaries for the specified fields, reducing cache usage
/// and prewarm time for schemas with many fields.
pub async fn prewarm_term_dictionaries_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!(
        "🔥 PREWARM_FIELDS: Starting term dictionary warmup for fields: {:?}",
        field_names
    );

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find indexed text fields that match the filter
    let indexed_fields: HashSet<tantivy::schema::Field> = schema
        .fields()
        .filter_map(|(field, field_entry)| {
            // Check if this field is in our filter
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        debug_println!(
                            "🔥 PREWARM_FIELDS: Found matching indexed text field: {}",
                            field_entry.name()
                        );
                        Some(field)
                    } else {
                        debug_println!(
                            "⚠️ PREWARM_FIELDS: Field '{}' is not indexed",
                            field_entry.name()
                        );
                        None
                    }
                }
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        debug_println!(
                            "🔥 PREWARM_FIELDS: Found matching indexed JSON field: {}",
                            field_entry.name()
                        );
                        Some(field)
                    } else {
                        debug_println!(
                            "⚠️ PREWARM_FIELDS: JSON field '{}' has no text indexing",
                            field_entry.name()
                        );
                        None
                    }
                }
                _ => {
                    debug_println!(
                        "⚠️ PREWARM_FIELDS: Field '{}' is not a text/JSON field",
                        field_entry.name()
                    );
                    None
                }
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!(
            "⚠️ PREWARM_FIELDS: No matching indexed fields found for: {:?}",
            field_names
        );
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_FIELDS: Warming term dictionaries for {} fields across {} segments",
        indexed_fields.len(),
        searcher.segment_readers().len()
    );

    let mut warm_up_futures = Vec::new();

    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move {
                        let dict = inverted_index.terms();
                        dict.warm_up_dictionary().await
                    });
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ PREWARM_FIELDS: Failed to get inverted index: {}",
                        e
                    );
                }
            }
        }
    }

    debug_println!(
        "🔥 PREWARM_FIELDS: Executing {} warmup operations",
        warm_up_futures.len()
    );

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!(
        "✅ PREWARM_FIELDS: Term dictionary warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "All field-specific warmup operations failed"
        ))
    }
}

/// Field-specific postings prewarming
pub async fn prewarm_postings_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!(
        "🔥 PREWARM_FIELDS: Starting postings warmup for fields: {:?}",
        field_names
    );

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let indexed_fields: HashSet<tantivy::schema::Field> = schema
        .fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if text_options.get_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                }
                FieldType::JsonObject(json_options) => {
                    if json_options.get_text_indexing_options().is_some() {
                        Some(field)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching indexed fields found for postings");
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_FIELDS: Warming postings for {} fields",
        indexed_fields.len()
    );

    let mut warm_up_futures = Vec::new();
    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    warm_up_futures.push(async move { inverted_index.warm_postings_full(false).await });
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ PREWARM_FIELDS: Failed to get inverted index: {}",
                        e
                    );
                }
            }
        }
    }

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!(
        "✅ PREWARM_FIELDS: Postings warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "All field-specific postings warmup operations failed"
        ))
    }
}

/// Field-specific positions prewarming
///
/// This warms up the position data (.pos files) for the specified fields.
/// Positions are needed for phrase queries and are separate from postings.
pub async fn prewarm_positions_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!(
        "🔥 PREWARM_FIELDS: Starting positions warmup for fields: {:?}",
        field_names
    );

    let (searcher, schema) = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        (ctx.cached_searcher.clone(), ctx.cached_index.schema())
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Find all indexed fields that have positions (text fields with positions enabled)
    let indexed_fields: HashSet<tantivy::schema::Field> = schema
        .fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            match field_entry.field_type() {
                FieldType::Str(text_options) => {
                    if let Some(indexing_options) = text_options.get_indexing_options() {
                        // Only fields with positions enabled
                        if indexing_options.index_option().has_positions() {
                            Some(field)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                FieldType::JsonObject(json_options) => {
                    if let Some(text_indexing) = json_options.get_text_indexing_options() {
                        if text_indexing.index_option().has_positions() {
                            Some(field)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect();

    if indexed_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDS: No matching fields with positions found");
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_FIELDS: Warming positions for {} fields",
        indexed_fields.len()
    );

    let mut warm_up_futures = Vec::new();
    for field in &indexed_fields {
        for segment_reader in searcher.segment_readers() {
            match segment_reader.inverted_index(*field) {
                Ok(inverted_index) => {
                    let inverted_index = inverted_index.clone();
                    // true = include positions data
                    warm_up_futures.push(async move { inverted_index.warm_postings_full(true).await });
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ PREWARM_FIELDS: Failed to get inverted index: {}",
                        e
                    );
                }
            }
        }
    }

    let results: Vec<std::io::Result<()>> = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!(
        "✅ PREWARM_FIELDS: Positions warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "All field-specific positions warmup operations failed"
        ))
    }
}

/// Field-specific fieldnorms prewarming
///
/// Uses the same pattern as Quickwit's `warm_up_fieldnorms` function:
/// 1. Get inner file handle using `fieldnorm_readers.get_inner_file().open_read(field)`
/// 2. Read the actual bytes using `file_handle.read_bytes_async().await`
///
/// This ensures the fieldnorm data is actually read into cache.
pub async fn prewarm_fieldnorms_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!(
        "🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup for fields: {:?}",
        field_names
    );

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();

    // Find text fields that match the filter (only text fields have fieldnorms)
    let matching_fields: Vec<tantivy::schema::Field> = schema
        .fields()
        .filter_map(|(field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            if matches!(field_entry.field_type(), FieldType::Str(_)) {
                debug_println!(
                    "🔥 PREWARM_FIELDNORMS: Found matching text field: {}",
                    field_entry.name()
                );
                Some(field)
            } else {
                debug_println!(
                    "⚠️ PREWARM_FIELDNORMS: Field '{}' is not a text field (no fieldnorms)",
                    field_entry.name()
                );
                None
            }
        })
        .collect();

    if matching_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FIELDNORMS: No matching text fields found");
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_FIELDNORMS: Warming fieldnorms for {} fields across {} segments",
        matching_fields.len(),
        searcher.segment_readers().len()
    );

    let mut warm_up_futures = Vec::new();

    for field in &matching_fields {
        for segment_reader in searcher.segment_readers() {
            let fieldnorm_readers = segment_reader.fieldnorms_readers();
            // Use Quickwit's pattern: get_inner_file().open_read(field) + read_bytes_async()
            if let Some(file_handle) = fieldnorm_readers.get_inner_file().open_read(*field) {
                warm_up_futures.push(async move {
                    let bytes = file_handle
                        .read_bytes_async()
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to read fieldnorm bytes: {}", e))?;
                    debug_println!(
                        "🔥 PREWARM_FIELDNORMS: Read {} bytes for fieldnorm",
                        bytes.len()
                    );
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    // Log any failures
    for result in &results {
        if let Err(e) = result {
            debug_println!("⚠️ PREWARM_FIELDNORMS: Warmup error: {}", e);
        }
    }

    debug_println!(
        "✅ PREWARM_FIELDNORMS: Fieldnorms warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    // Fieldnorms are optional, so don't fail if none found
    Ok(())
}

/// Field-specific fast fields prewarming
///
/// Uses the same pattern as Quickwit's `warm_up_fastfield` function:
/// 1. Get column handles using `list_dynamic_column_handles(field_name)`
/// 2. Read the actual bytes using `col.file_slice().read_bytes_async().await`
///
/// This ensures the column data is actually read into cache, not just creating
/// lazy column readers.
pub async fn prewarm_fastfields_for_fields(
    searcher_ptr: jlong,
    field_names: &HashSet<String>,
) -> anyhow::Result<()> {
    debug_println!(
        "🔥 PREWARM_FASTFIELDS: Starting fast fields warmup for fields: {:?}",
        field_names
    );

    let searcher = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.cached_searcher.clone()
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    let schema = searcher.schema();

    // Find fast fields that match the filter
    let matching_fields: Vec<String> = schema
        .fields()
        .filter_map(|(_field, field_entry)| {
            if !field_names.contains(field_entry.name()) {
                return None;
            }

            if field_entry.is_fast() {
                debug_println!(
                    "🔥 PREWARM_FASTFIELDS: Found matching fast field: {}",
                    field_entry.name()
                );
                Some(field_entry.name().to_string())
            } else {
                debug_println!(
                    "⚠️ PREWARM_FASTFIELDS: Field '{}' is not a fast field",
                    field_entry.name()
                );
                None
            }
        })
        .collect();

    if matching_fields.is_empty() {
        debug_println!("⚠️ PREWARM_FASTFIELDS: No matching fast fields found");
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_FASTFIELDS: Warming {} fast fields across {} segments",
        matching_fields.len(),
        searcher.segment_readers().len()
    );

    let mut warm_up_futures = Vec::new();

    for (seg_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
        let fast_field_reader = segment_reader.fast_fields();

        for field_name in &matching_fields {
            let fast_field_reader = fast_field_reader.clone();
            let field_name = field_name.clone();
            let seg_idx = seg_idx;

            // Use Quickwit's pattern: list_dynamic_column_handles + read_bytes_async
            warm_up_futures.push(async move {
                // Get column handles for this field
                let columns = fast_field_reader
                    .list_dynamic_column_handles(&field_name)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to list column handles for '{}': {}",
                            field_name,
                            e
                        )
                    })?;

                debug_println!(
                    "🔥 PREWARM_FASTFIELDS: Field '{}' segment {} has {} column(s)",
                    field_name,
                    seg_idx,
                    columns.len()
                );

                // Read the actual bytes from each column (triggers I/O)
                for (col_idx, col) in columns.iter().enumerate() {
                    let file_slice = col.file_slice();
                    let slice_len = file_slice.len();
                    debug_println!(
                        "🔥 PREWARM_FASTFIELDS: Field '{}' seg {} col {} file_slice.len()={} bytes",
                        field_name,
                        seg_idx,
                        col_idx,
                        slice_len
                    );
                    let bytes = file_slice.read_bytes_async().await.map_err(|e| {
                        anyhow::anyhow!("Failed to read bytes for '{}': {}", field_name, e)
                    })?;
                    debug_println!(
                        "🔥 PREWARM_FASTFIELDS: Field '{}' seg {} col {} read {} bytes",
                        field_name,
                        seg_idx,
                        col_idx,
                        bytes.len()
                    );
                }

                Ok::<(), anyhow::Error>(())
            });
        }
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    // Log any failures
    for result in &results {
        if let Err(e) = result {
            debug_println!("⚠️ PREWARM_FASTFIELDS: Warmup error: {}", e);
        }
    }

    debug_println!(
        "✅ PREWARM_FASTFIELDS: Fast fields warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All fast field warmup operations failed"))
    }
}
