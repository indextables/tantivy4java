// doc_retrieval.rs - Document retrieval from external parquet files
//
// Given a manifest and a tantivy DocAddress, retrieves the corresponding row
// from the appropriate parquet file using projection and row selection.
//
// Performance strategy for batch retrieval:
//   1. Group doc addresses by parquet file (minimizes file opens)
//   2. Row-group filtering: skip entire row groups that contain no selected rows
//   3. RowSelection: within selected row groups, surgically skip/select only needed rows
//   4. Column projection: only decode requested columns
//   5. Parallel file processing: process multiple files concurrently via try_join_all
//   6. Byte range coalescing: CachedParquetReader merges nearby get_byte_ranges requests

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{RecordBatch, Array, ArrayRef};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use quickwit_storage::Storage;

use super::cached_reader::{CachedParquetReader, ByteRangeCache, CoalesceConfig};
use super::docid_mapping::{translate_to_global_row, locate_row_in_file, group_doc_addresses_by_file};
use super::manifest::ParquetManifest;
use super::transcode::MetadataCache;

use crate::debug_println;

/// Retrieve a single document from parquet files.
///
/// # Arguments
/// * `manifest` - The parquet manifest describing file locations
/// * `segment_ord` - Tantivy segment ordinal
/// * `doc_id` - Tantivy local document ID within the segment
/// * `projected_fields` - Optional set of field names to retrieve (None = all)
/// * `storage` - Storage instance for accessing parquet files
/// * `metadata_cache` - Optional cache of parquet metadata per file path (avoids re-reading footer from S3)
/// * `byte_cache` - Optional cache of fetched byte ranges (dictionary page reuse)
/// * `coalesce_config` - Optional coalescing parameters (defaults to cloud-optimized settings)
pub async fn retrieve_document_from_parquet(
    manifest: &ParquetManifest,
    segment_ord: u32,
    doc_id: u32,
    projected_fields: Option<&[String]>,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<HashMap<String, serde_json::Value>> {
    let global_row = translate_to_global_row(segment_ord, doc_id, manifest)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let location = locate_row_in_file(global_row, manifest)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let file_entry = &manifest.parquet_files[location.file_idx];
    // Use relative_path directly — the storage is rooted at table_root
    let parquet_path = &file_entry.relative_path;

    debug_println!(
        "📖 PARQUET_DOC: Retrieving seg={} doc={} → global_row={} → file[{}]='{}' row_in_file={}",
        segment_ord, doc_id, global_row, location.file_idx, parquet_path, location.row_in_file
    );

    // Check metadata cache first to avoid re-reading footer from S3/Azure
    let path_buf = std::path::PathBuf::from(parquet_path);
    let cached_meta = metadata_cache.and_then(|cache| {
        cache.lock().ok().and_then(|guard| guard.get(&path_buf).cloned())
    });

    let reader = if let Some(meta) = cached_meta {
        debug_println!("📖 PARQUET_DOC: Using cached metadata for {:?}", parquet_path);
        CachedParquetReader::with_metadata(
            storage.clone(),
            path_buf.clone(),
            file_entry.file_size_bytes,
            meta,
        )
    } else {
        CachedParquetReader::new(
            storage.clone(),
            path_buf.clone(),
            file_entry.file_size_bytes,
        )
    };
    let reader = if let Some(bc) = byte_cache {
        reader.with_byte_cache(bc.clone())
    } else {
        reader
    };
    let reader = if let Some(config) = coalesce_config {
        reader.with_coalesce_config(config)
    } else {
        reader
    };

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("Failed to create parquet stream builder")?;

    // Cache the metadata for subsequent reads
    if let Some(cache) = metadata_cache {
        if let Ok(mut guard) = cache.lock() {
            if !guard.contains_key(&path_buf) {
                guard.insert(path_buf.clone(), builder.metadata().clone());
                debug_println!("📖 PARQUET_DOC: Cached metadata for {:?}", parquet_path);
            }
        }
    }

    let parquet_schema = builder.schema().clone();

    // Build column projection
    let projection = build_column_projection(
        projected_fields,
        &parquet_schema,
        &manifest.column_mapping,
    );

    if let Some(ref proj) = projection {
        if proj.is_empty() {
            // No matching columns found - return empty document
            return Ok(HashMap::new());
        }
    }

    // Build a RowSelection to read only the target row
    let row_in_file = location.row_in_file as usize;

    // Get metadata before consuming builder with projections
    let parquet_metadata = builder.metadata().clone();
    let parquet_file_schema = builder.parquet_schema().clone();

    let builder = if let Some(ref proj) = projection {
        // Use roots() not leaves() because build_column_projection returns top-level
        // arrow field indices, not parquet leaf column indices. For schemas with nested
        // types (List, Map, Struct), leaves diverge from top-level indices.
        builder.with_projection(parquet::arrow::ProjectionMask::roots(
            &parquet_file_schema,
            proj.iter().cloned(),
        ))
    } else {
        builder
    };

    // Step 1: Filter to only the target row group (avoids downloading column pages
    // for ALL row groups — critical for large files on S3/Azure).
    let rg_filter = compute_row_group_filter(&[row_in_file], &parquet_metadata);
    let builder = if let Some(ref filter) = rg_filter {
        let selected_rgs: Vec<usize> = filter
            .iter()
            .enumerate()
            .filter(|(_, selected)| **selected)
            .map(|(idx, _)| idx)
            .collect();
        debug_println!(
            "📖 PARQUET_DOC: single-doc selecting row group(s) {:?} of {}",
            selected_rgs, parquet_metadata.num_row_groups()
        );
        builder.with_row_groups(selected_rgs)
    } else {
        builder
    };

    // Step 2: Build RowSelection within the selected row group to skip to exact row
    let row_selection = build_row_selection_for_rows_in_selected_groups(
        &[row_in_file],
        &parquet_metadata,
        rg_filter.as_deref(),
    );
    let builder = if let Some(selection) = row_selection {
        builder.with_row_selection(selection)
    } else {
        // Fallback: set offset and limit
        builder.with_offset(row_in_file).with_limit(1)
    };

    let mut stream = builder.build()
        .context("Failed to build parquet record batch stream")?;

    // Read the first (and ideally only) batch
    use futures::StreamExt;
    if let Some(batch_result) = stream.next().await {
        let batch = batch_result.context("Failed to read parquet record batch")?;
        if batch.num_rows() > 0 {
            return extract_row_as_map(&batch, 0, projected_fields);
        }
    }

    Ok(HashMap::new())
}

/// Retrieve a single document using pre-resolved file coordinates.
///
/// Unlike `retrieve_document_from_parquet`, this skips the segment→global→file
/// resolution and directly uses the file_idx and row_in_file from fast fields.
pub async fn retrieve_document_by_location(
    manifest: &ParquetManifest,
    file_idx: usize,
    row_in_file: u64,
    projected_fields: Option<&[String]>,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<HashMap<String, serde_json::Value>> {
    let file_entry = manifest.parquet_files.get(file_idx)
        .ok_or_else(|| anyhow::anyhow!("file_idx {} out of range (have {} files)",
            file_idx, manifest.parquet_files.len()))?;
    let parquet_path = &file_entry.relative_path;

    debug_println!(
        "📖 PARQUET_DOC: Retrieving by location → file[{}]='{}' row_in_file={}",
        file_idx, parquet_path, row_in_file
    );

    let path_buf = std::path::PathBuf::from(parquet_path);
    let cached_meta = metadata_cache.and_then(|cache| {
        cache.lock().ok().and_then(|guard| guard.get(&path_buf).cloned())
    });

    let reader = if let Some(meta) = cached_meta {
        CachedParquetReader::with_metadata(
            storage.clone(), path_buf.clone(), file_entry.file_size_bytes, meta,
        )
    } else {
        CachedParquetReader::new(
            storage.clone(), path_buf.clone(), file_entry.file_size_bytes,
        )
    };
    let reader = if let Some(bc) = byte_cache {
        reader.with_byte_cache(bc.clone())
    } else { reader };
    let reader = if let Some(config) = coalesce_config {
        reader.with_coalesce_config(config)
    } else { reader };

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await.context("Failed to create parquet stream builder")?;

    if let Some(cache) = metadata_cache {
        if let Ok(mut guard) = cache.lock() {
            if !guard.contains_key(&path_buf) {
                guard.insert(path_buf.clone(), builder.metadata().clone());
            }
        }
    }

    let parquet_schema = builder.schema().clone();
    let projection = build_column_projection(projected_fields, &parquet_schema, &manifest.column_mapping);
    if let Some(ref proj) = projection {
        if proj.is_empty() { return Ok(HashMap::new()); }
    }

    let row_in_file_usize = row_in_file as usize;
    let parquet_metadata = builder.metadata().clone();
    let parquet_file_schema = builder.parquet_schema().clone();

    let builder = if let Some(ref proj) = projection {
        builder.with_projection(parquet::arrow::ProjectionMask::roots(
            &parquet_file_schema, proj.iter().cloned(),
        ))
    } else { builder };

    let rg_filter = compute_row_group_filter(&[row_in_file_usize], &parquet_metadata);
    let builder = if let Some(ref filter) = rg_filter {
        let selected_rgs: Vec<usize> = filter.iter().enumerate()
            .filter(|(_, s)| **s).map(|(i, _)| i).collect();
        builder.with_row_groups(selected_rgs)
    } else { builder };

    let row_selection = build_row_selection_for_rows_in_selected_groups(
        &[row_in_file_usize], &parquet_metadata, rg_filter.as_deref(),
    );
    let builder = if let Some(selection) = row_selection {
        builder.with_row_selection(selection)
    } else {
        builder.with_offset(row_in_file_usize).with_limit(1)
    };

    let mut stream = builder.build().context("Failed to build parquet record batch stream")?;
    use futures::StreamExt;
    if let Some(batch_result) = stream.next().await {
        let batch = batch_result.context("Failed to read parquet record batch")?;
        if batch.num_rows() > 0 {
            return extract_row_as_map(&batch, 0, projected_fields);
        }
    }
    Ok(HashMap::new())
}

/// Batch retrieve multiple documents from parquet files.
///
/// Optimized for efficient column page access with groups of rows:
///   1. Groups addresses by parquet file (minimizes file opens)
///   2. Processes files in parallel using try_join_all
///   3. Filters row groups: skips entire row groups with no selected rows
///   4. Builds surgical RowSelection within selected row groups
///   5. Uses column projection to only decode requested columns
///   6. Coalesces nearby byte ranges to reduce S3/Azure round-trips
pub async fn batch_retrieve_from_parquet(
    addresses: &[(u32, u32)], // (segment_ord, doc_id)
    projected_fields: Option<&[String]>,
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    let groups = group_doc_addresses_by_file(addresses, manifest)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let result_count = addresses.len();

    // Share projected_fields across parallel tasks via Arc (avoid per-task Vec clone)
    let projected_fields_shared: Option<Arc<[String]>> =
        projected_fields.map(|f| f.into());

    // Process each file's rows in parallel
    let file_futures: Vec<_> = groups
        .into_iter()
        .map(|(file_idx, rows)| {
            let storage = storage.clone();
            let manifest = manifest.clone();
            let projected_fields_owned = projected_fields_shared.clone();
            let metadata_cache = metadata_cache.cloned();
            let byte_cache = byte_cache.cloned();

            async move {
                let file_entry = &manifest.parquet_files[file_idx];
                // Use relative_path directly — the storage is rooted at table_root
                let parquet_path = &file_entry.relative_path;

                debug_println!(
                    "📖 PARQUET_BATCH: file[{}]='{}' retrieving {} rows",
                    file_idx, parquet_path, rows.len()
                );

                // Check metadata cache to avoid re-reading footer from S3/Azure
                let path_buf = std::path::PathBuf::from(parquet_path);
                let cached_meta = metadata_cache.as_ref().and_then(|cache| {
                    cache.lock().ok().and_then(|guard| guard.get(&path_buf).cloned())
                });

                let reader = if let Some(meta) = cached_meta {
                    CachedParquetReader::with_metadata(
                        storage.clone(),
                        path_buf.clone(),
                        file_entry.file_size_bytes,
                        meta,
                    )
                } else {
                    CachedParquetReader::new(
                        storage.clone(),
                        path_buf.clone(),
                        file_entry.file_size_bytes,
                    )
                };
                let reader = if let Some(ref bc) = byte_cache {
                    reader.with_byte_cache(bc.clone())
                } else {
                    reader
                };
                let reader = if let Some(config) = coalesce_config {
                    reader.with_coalesce_config(config)
                } else {
                    reader
                };

                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .context("Failed to create parquet stream builder")?;

                // Cache the metadata for subsequent reads
                if let Some(cache) = &metadata_cache {
                    if let Ok(mut guard) = cache.lock() {
                        if !guard.contains_key(&path_buf) {
                            guard.insert(path_buf.clone(), builder.metadata().clone());
                        }
                    }
                }

                let parquet_schema = builder.schema().clone();
                let parquet_metadata = builder.metadata().clone();
                let parquet_file_schema = builder.parquet_schema().clone();

                // Build column projection
                let proj_fields = projected_fields_owned.as_deref();
                let projection = build_column_projection(
                    proj_fields,
                    &parquet_schema,
                    &manifest.column_mapping,
                );

                let builder = if let Some(ref proj) = projection {
                    // Use roots() not leaves() because build_column_projection returns top-level
                    // arrow field indices, not parquet leaf column indices. For schemas with nested
                    // types (List, Map, Struct), leaves diverge from top-level indices.
                    builder.with_projection(parquet::arrow::ProjectionMask::roots(
                        &parquet_file_schema,
                        proj.iter().cloned(),
                    ))
                } else {
                    builder
                };

                // Row indices within the file (already sorted by group_doc_addresses_by_file)
                let row_indices: Vec<usize> = rows.iter().map(|(_, row)| *row as usize).collect();

                // Step 1: Determine which row groups contain our target rows
                let rg_filter = compute_row_group_filter(&row_indices, &parquet_metadata);

                // Step 2: Apply row group filter to skip entire unneeded row groups
                let builder = if let Some(ref filter) = rg_filter {
                    let selected_rgs: Vec<usize> = filter
                        .iter()
                        .enumerate()
                        .filter(|(_, selected)| **selected)
                        .map(|(idx, _)| idx)
                        .collect();

                    debug_println!(
                        "📖 PARQUET_BATCH: file[{}] selecting {}/{} row groups",
                        file_idx, selected_rgs.len(), parquet_metadata.num_row_groups()
                    );

                    builder.with_row_groups(selected_rgs)
                } else {
                    builder
                };

                // Step 3: Build RowSelection within the selected row groups
                // This is relative to the selected row groups, not all row groups
                let row_selection = build_row_selection_for_rows_in_selected_groups(
                    &row_indices,
                    &parquet_metadata,
                    rg_filter.as_deref(),
                );
                let builder = if let Some(selection) = row_selection {
                    builder.with_row_selection(selection)
                } else {
                    builder
                };

                let mut stream = builder.build()
                    .context("Failed to build parquet record batch stream")?;

                // Collect all rows from the stream
                let mut collected_rows: Vec<HashMap<String, serde_json::Value>> = Vec::new();
                use futures::StreamExt;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.context("Failed to read parquet batch")?;
                    for row_idx in 0..batch.num_rows() {
                        let row_map = extract_row_as_map(&batch, row_idx, proj_fields)?;
                        collected_rows.push(row_map);
                    }
                }

                debug_println!(
                    "📖 PARQUET_BATCH: file[{}] collected {} rows (expected {})",
                    file_idx, collected_rows.len(), rows.len()
                );

                // Return (original_index, row_data) pairs for reassembly.
                // Use drain() to move HashMaps instead of cloning them.
                let mut drain_iter = collected_rows.drain(..);
                let indexed_results: Vec<(usize, HashMap<String, serde_json::Value>)> = rows
                    .iter()
                    .map(|(original_idx, _)| {
                        let data = drain_iter.next().unwrap_or_default();
                        (*original_idx, data)
                    })
                    .collect();

                Ok::<_, anyhow::Error>(indexed_results)
            }
        })
        .collect();

    // Execute all file retrievals in parallel
    let all_file_results = futures::future::try_join_all(file_futures).await?;

    // Reassemble results in original order
    let mut results: Vec<Option<HashMap<String, serde_json::Value>>> = vec![None; result_count];
    for file_results in all_file_results {
        for (original_idx, data) in file_results {
            results[original_idx] = Some(data);
        }
    }

    Ok(results
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
}

/// Build column projection indices from field names.
///
/// Resolves tantivy field names to parquet column indices via the column_mapping.
pub(crate) fn build_column_projection(
    projected_fields: Option<&[String]>,
    parquet_schema: &arrow_schema::SchemaRef,
    column_mapping: &[super::manifest::ColumnMapping],
) -> Option<Vec<usize>> {
    let fields = projected_fields?;
    let total_columns = parquet_schema.fields().len();
    let indices: Vec<usize> = fields
        .iter()
        .filter_map(|f| {
            let parquet_name = column_mapping
                .iter()
                .find(|m| m.tantivy_field_name == *f)
                .map(|m| m.parquet_column_name.as_str())
                .unwrap_or(f.as_str());
            let pos = parquet_schema
                .fields()
                .iter()
                .position(|field| field.name() == parquet_name);
            if pos.is_none() {
                perf_println!(
                    "⏱️ PROJ_DIAG: build_column_projection: field '{}' (parquet='{}') NOT FOUND in schema",
                    f, parquet_name
                );
            }
            pos
        })
        .collect();
    perf_println!(
        "⏱️ PROJ_DIAG: build_column_projection: {} requested fields → {} matched indices out of {} total columns",
        fields.len(), indices.len(), total_columns
    );
    Some(indices)
}

/// Compute a row group filter: for each row group, whether it contains any of the target rows.
///
/// Returns `None` if all row groups are needed (optimization: skip the filter entirely).
/// Returns `Some(Vec<bool>)` where `filter[i]` is true if row group `i` should be read.
///
/// This is the key optimization for batch retrieval: by knowing which row groups contain
/// our target rows, we can skip reading entire row groups (potentially millions of rows
/// and many column pages) that don't contain any documents we need.
pub(crate) fn compute_row_group_filter(
    sorted_row_indices: &[usize],
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Option<Vec<bool>> {
    let num_rgs = metadata.num_row_groups();
    if num_rgs == 0 || sorted_row_indices.is_empty() {
        return None;
    }

    let mut filter = vec![false; num_rgs];
    let mut all_selected = true;

    // Build cumulative row offset table for row groups
    let rg_boundaries = compute_row_group_boundaries(metadata);

    // For each target row, find which row group it belongs to using binary search
    let mut row_iter = sorted_row_indices.iter().peekable();
    for (rg_idx, boundary) in rg_boundaries.iter().enumerate() {
        // Consume all rows that fall within this row group
        while let Some(&&row) = row_iter.peek() {
            if row >= boundary.start && row < boundary.end {
                filter[rg_idx] = true;
                row_iter.next();
            } else if row < boundary.start {
                // Row is before this row group (shouldn't happen with sorted input)
                row_iter.next();
            } else {
                break; // Row is in a later row group
            }
        }
        if !filter[rg_idx] {
            all_selected = false;
        }
    }

    if all_selected {
        None // All row groups needed, no point filtering
    } else {
        Some(filter)
    }
}

/// Row range [start, end) for a row group within the file
pub(crate) struct RowGroupBoundary {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// Compute cumulative row boundaries for each row group.
pub(crate) fn compute_row_group_boundaries(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Vec<RowGroupBoundary> {
    let mut boundaries = Vec::with_capacity(metadata.num_row_groups());
    let mut offset = 0usize;
    for rg in metadata.row_groups() {
        let num_rows = rg.num_rows() as usize;
        boundaries.push(RowGroupBoundary {
            start: offset,
            end: offset + num_rows,
        });
        offset += num_rows;
    }
    boundaries
}

/// Build a RowSelection for specific rows, but only within the selected row groups.
///
/// When `with_row_groups()` filters out some row groups, the RowSelection must be
/// expressed relative to the remaining (selected) row groups only. This function
/// builds skip/select selectors that are consistent with the row group filter.
///
/// The key insight: after `with_row_groups([1, 3])`, the parquet reader only sees
/// the rows from row groups 1 and 3. So our RowSelection must address positions
/// within that reduced row space, not the full file row space.
pub(crate) fn build_row_selection_for_rows_in_selected_groups(
    sorted_file_rows: &[usize],
    metadata: &parquet::file::metadata::ParquetMetaData,
    rg_filter: Option<&[bool]>,
) -> Option<parquet::arrow::arrow_reader::RowSelection> {
    use parquet::arrow::arrow_reader::RowSelector;

    if sorted_file_rows.is_empty() {
        return None;
    }

    let rg_boundaries = compute_row_group_boundaries(metadata);

    // Compute the total row count in selected row groups, and build a mapping
    // from file-level row index to position within the selected-groups row space
    let mut selected_rows_total = 0usize;
    let mut selectors = Vec::new();
    let mut current_pos = 0usize; // position within selected-groups row space

    let mut row_iter = sorted_file_rows.iter().peekable();

    for (rg_idx, boundary) in rg_boundaries.iter().enumerate() {
        let rg_selected = rg_filter.map_or(true, |f| f[rg_idx]);
        if !rg_selected {
            // This row group is filtered out — skip any rows that fall in it
            // (they shouldn't be here if compute_row_group_filter is correct,
            // but be defensive)
            while let Some(&&row) = row_iter.peek() {
                if row >= boundary.start && row < boundary.end {
                    row_iter.next(); // discard
                } else {
                    break;
                }
            }
            continue;
        }

        let rg_rows = boundary.end - boundary.start;
        selected_rows_total += rg_rows;

        // Process target rows within this row group
        while let Some(&&file_row) = row_iter.peek() {
            if file_row >= boundary.end {
                break; // This row is in a later row group
            }
            if file_row < boundary.start {
                row_iter.next(); // shouldn't happen, defensive
                continue;
            }

            // Convert file-level row to position within selected-groups space
            let pos_in_rg = file_row - boundary.start;
            let absolute_pos = (selected_rows_total - rg_rows) + pos_in_rg;

            if absolute_pos > current_pos {
                selectors.push(RowSelector::skip(absolute_pos - current_pos));
            }
            selectors.push(RowSelector::select(1));
            current_pos = absolute_pos + 1;

            row_iter.next();
        }
    }

    // Skip any remaining rows after the last selected row
    if current_pos < selected_rows_total {
        selectors.push(RowSelector::skip(selected_rows_total - current_pos));
    }

    if selectors.is_empty() {
        return None;
    }

    Some(parquet::arrow::arrow_reader::RowSelection::from(selectors))
}

/// Extract a single row from a RecordBatch as a JSON-compatible map.
/// Null values are omitted from the map.
fn extract_row_as_map(
    batch: &RecordBatch,
    row_idx: usize,
    _projected_fields: Option<&[String]>,
) -> Result<HashMap<String, serde_json::Value>> {
    let schema = batch.schema();
    let mut map = HashMap::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.column(col_idx);

        if array.is_null(row_idx) {
            continue; // Omit null values
        }

        let value = arrow_value_to_json(array, row_idx)?;
        map.insert(field.name().clone(), value);
    }

    Ok(map)
}

/// Convert an Arrow array value at a given row index to a serde_json::Value
fn arrow_value_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }

    let value = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| anyhow::anyhow!("Expected BooleanArray array"))?;
            serde_json::Value::Bool(arr.value(row_idx))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| anyhow::anyhow!("Expected Int8Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| anyhow::anyhow!("Expected Int16Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("Expected Int32Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow::anyhow!("Expected Int64Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt8Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt16Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt32Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt64Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| anyhow::anyhow!("Expected Float32Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| anyhow::anyhow!("Expected Float64Array array"))?;
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("Expected StringArray array"))?;
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeStringArray array"))?;
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected BinaryArray array"))?;
            serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                arr.value(row_idx),
            ))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeBinaryArray array"))?;
            serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                arr.value(row_idx),
            ))
        }
        DataType::Timestamp(unit, _tz) => {
            use arrow_schema::TimeUnit;
            let micros = match unit {
                TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampSecondArray array"))?;
                    arr.value(row_idx) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampMillisecondArray array"))?;
                    arr.value(row_idx) * 1_000
                }
                TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampMicrosecondArray array"))?;
                    arr.value(row_idx)
                }
                TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray array"))?;
                    arr.value(row_idx) / 1_000
                }
            };
            serde_json::json!(micros)
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| anyhow::anyhow!("Expected Date32Array array"))?;
            serde_json::json!(arr.value(row_idx) as i64 * 86_400_000_000i64) // days → micros
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().ok_or_else(|| anyhow::anyhow!("Expected Date64Array array"))?;
            serde_json::json!(arr.value(row_idx) * 1_000i64) // millis → micros
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().ok_or_else(|| anyhow::anyhow!("Expected Decimal128Array array"))?;
            let raw = arr.value(row_idx) as f64;
            let val = raw / 10f64.powi(*scale as i32);
            serde_json::json!(val)
        }
        DataType::Decimal256(_, scale) => {
            // Decimal256 can exceed f64 precision — return as string representation
            let arr = array.as_any().downcast_ref::<Decimal256Array>().ok_or_else(|| anyhow::anyhow!("Expected Decimal256Array array"))?;
            let raw = arr.value(row_idx);
            let unscaled = raw.to_string();
            if *scale == 0 {
                serde_json::Value::String(unscaled)
            } else {
                // Represent as "unscaled / 10^scale" in f64 string for JSON
                let val: f64 = unscaled.parse::<f64>().unwrap_or(0.0)
                    / 10f64.powi(*scale as i32);
                serde_json::Value::String(val.to_string())
            }
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected FixedSizeBinaryArray array"))?;
            serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                arr.value(row_idx),
            ))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            // Complex types: convert directly to JSON Value (no serialize/deserialize roundtrip)
            let slice = array.slice(row_idx, 1);
            arrow_json_value(&slice, 0)
        }
        _ => {
            // Fallback: try string representation
            serde_json::Value::String(format!("{:?}", array.as_ref()))
        }
    };

    Ok(value)
}

/// Convert a complex Arrow array value to JSON (for List/Map/Struct types)
pub(crate) fn arrow_json_value(array: &ArrayRef, row_idx: usize) -> serde_json::Value {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(row_idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().expect("Expected ListArray array");
            let values = list.value(row_idx);
            let mut arr = Vec::new();
            for i in 0..values.len() {
                arr.push(arrow_json_value(&values, i));
            }
            serde_json::Value::Array(arr)
        }
        DataType::Struct(_) => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>().expect("Expected StructArray array");
            let mut map = serde_json::Map::new();
            for (i, field) in struct_arr.fields().iter().enumerate() {
                let col = struct_arr.column(i);
                map.insert(field.name().clone(), arrow_json_value(col, row_idx));
            }
            serde_json::Value::Object(map)
        }
        DataType::Map(_, _) => {
            let map_arr = array.as_any().downcast_ref::<MapArray>().expect("Expected MapArray array");
            let entry = map_arr.value(row_idx); // StructArray with keys + values
            let struct_arr = entry.as_any().downcast_ref::<StructArray>().expect("Expected StructArray array");
            let keys = struct_arr.column(0); // key column
            let values = struct_arr.column(1); // value column
            let mut map = serde_json::Map::new();
            for i in 0..struct_arr.len() {
                let key = arrow_json_value(keys, i);
                let val = arrow_json_value(values, i);
                if let serde_json::Value::String(k) = key {
                    map.insert(k, val);
                }
            }
            serde_json::Value::Object(map)
        }
        _ => {
            // Leaf types
            arrow_value_to_json(&array.clone(), row_idx).unwrap_or(serde_json::Value::Null)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};
    use std::sync::Arc;

    /// Create test ParquetMetaData with specified row group sizes.
    /// e.g. make_metadata(&[500, 300, 200]) creates 3 row groups with those row counts.
    fn make_metadata(rg_sizes: &[i64]) -> ParquetMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        use parquet::basic::Encoding;
        use parquet::schema::types::ColumnDescriptor;

        let col_type = Arc::new(
            Type::primitive_type_builder("id", parquet::basic::Type::INT64)
                .build()
                .unwrap(),
        );

        let schema = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(vec![col_type.clone()])
                .build()
                .unwrap(),
        ));
        let schema_arc = Arc::new(schema);

        // Build a ColumnDescriptor for our single column
        let col_desc = Arc::new(ColumnDescriptor::new(
            col_type.clone(),
            0, // max_def_level
            0, // max_rep_level
            parquet::schema::types::ColumnPath::new(vec!["id".to_string()]),
        ));

        let total_rows: i64 = rg_sizes.iter().sum();
        let row_groups: Vec<RowGroupMetaData> = rg_sizes
            .iter()
            .map(|&num_rows| {
                let col_chunk = ColumnChunkMetaData::builder(col_desc.clone())
                    .set_num_values(num_rows)
                    .set_encodings(vec![Encoding::PLAIN])
                    .set_total_compressed_size(num_rows * 8)
                    .set_total_uncompressed_size(num_rows * 8)
                    .build()
                    .unwrap();
                RowGroupMetaData::builder(schema_arc.clone())
                    .set_num_rows(num_rows)
                    .set_total_byte_size(num_rows * 100)
                    .set_column_metadata(vec![col_chunk])
                    .build()
                    .unwrap()
            })
            .collect();

        let file_metadata = FileMetaData::new(
            1,
            total_rows,
            None,
            None,
            schema_arc,
            None,
        );

        ParquetMetaData::new(file_metadata, row_groups)
    }

    // ------- compute_row_group_boundaries tests -------

    #[test]
    fn test_rg_boundaries_single() {
        let meta = make_metadata(&[1000]);
        let b = compute_row_group_boundaries(&meta);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].start, 0);
        assert_eq!(b[0].end, 1000);
    }

    #[test]
    fn test_rg_boundaries_multiple() {
        let meta = make_metadata(&[500, 300, 200]);
        let b = compute_row_group_boundaries(&meta);
        assert_eq!(b.len(), 3);
        assert_eq!((b[0].start, b[0].end), (0, 500));
        assert_eq!((b[1].start, b[1].end), (500, 800));
        assert_eq!((b[2].start, b[2].end), (800, 1000));
    }

    // ------- compute_row_group_filter tests -------

    #[test]
    fn test_rg_filter_all_in_one_group() {
        // 3 row groups: [0..500), [500..800), [800..1000)
        // All target rows in first group → filter = [true, false, false]
        let meta = make_metadata(&[500, 300, 200]);
        let rows = vec![10, 100, 499];
        let filter = compute_row_group_filter(&rows, &meta);
        assert_eq!(filter, Some(vec![true, false, false]));
    }

    #[test]
    fn test_rg_filter_spread_across_groups() {
        let meta = make_metadata(&[500, 300, 200]);
        // Rows in groups 0 and 2, but not group 1
        let rows = vec![10, 499, 800, 999];
        let filter = compute_row_group_filter(&rows, &meta);
        assert_eq!(filter, Some(vec![true, false, true]));
    }

    #[test]
    fn test_rg_filter_all_groups_needed() {
        let meta = make_metadata(&[500, 300, 200]);
        let rows = vec![0, 500, 800];
        let filter = compute_row_group_filter(&rows, &meta);
        // All groups needed → None (optimization: no filter)
        assert!(filter.is_none());
    }

    #[test]
    fn test_rg_filter_empty_rows() {
        let meta = make_metadata(&[500, 300]);
        let rows: Vec<usize> = vec![];
        let filter = compute_row_group_filter(&rows, &meta);
        assert!(filter.is_none());
    }

    #[test]
    fn test_rg_filter_only_last_group() {
        let meta = make_metadata(&[100, 100, 100, 100]);
        // Only rows in last group [300..400)
        let rows = vec![300, 350, 399];
        let filter = compute_row_group_filter(&rows, &meta);
        assert_eq!(filter, Some(vec![false, false, false, true]));
    }

    // ------- build_row_selection_for_rows_in_selected_groups tests -------

    #[test]
    fn test_row_selection_no_filter() {
        // Single row group with 10 rows, select rows 2 and 7
        let meta = make_metadata(&[10]);
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[2, 7],
            &meta,
            None,
        );
        assert!(selection.is_some());
        // Selection should be: skip(2), select(1), skip(4), select(1), skip(2)
        // Total: 2 + 1 + 4 + 1 + 2 = 10 rows
    }

    #[test]
    fn test_row_selection_with_filter_skips_groups() {
        // 3 row groups: [0..500), [500..800), [800..1000)
        // Filter: [true, false, true] — only groups 0 and 2 selected
        // Target rows: 10 (in group 0) and 900 (in group 2)
        let meta = make_metadata(&[500, 300, 200]);
        let filter = vec![true, false, true];
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[10, 900],
            &meta,
            Some(&filter),
        );
        assert!(selection.is_some());
        // After filtering, selected groups have 500+200=700 rows total
        // Row 10 → position 10 in selected space (within group 0)
        // Row 900 → position 500 + (900-800) = 600 in selected space (within group 2)
    }

    #[test]
    fn test_row_selection_single_row_first() {
        let meta = make_metadata(&[100]);
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[0],
            &meta,
            None,
        );
        assert!(selection.is_some());
        // Should be: select(1), skip(99)
    }

    #[test]
    fn test_row_selection_single_row_last() {
        let meta = make_metadata(&[100]);
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[99],
            &meta,
            None,
        );
        assert!(selection.is_some());
        // Should be: skip(99), select(1)
    }

    #[test]
    fn test_row_selection_empty() {
        let meta = make_metadata(&[100]);
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[],
            &meta,
            None,
        );
        assert!(selection.is_none());
    }

    #[test]
    fn test_row_selection_consecutive_rows() {
        // Consecutive rows should produce separate select(1) calls
        let meta = make_metadata(&[100]);
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[50, 51, 52],
            &meta,
            None,
        );
        assert!(selection.is_some());
        // skip(50), select(1), select(1), select(1), skip(47)
    }

    #[test]
    fn test_row_selection_multi_group_with_filter() {
        // 4 row groups: [0..100), [100..200), [200..300), [300..400)
        // Filter: [false, true, false, true] — only groups 1 and 3
        // Selected space has 100+100=200 rows
        // Target rows: 150 (in group 1, pos 50) and 350 (in group 3, pos 150)
        let meta = make_metadata(&[100, 100, 100, 100]);
        let filter = vec![false, true, false, true];
        let selection = build_row_selection_for_rows_in_selected_groups(
            &[150, 350],
            &meta,
            Some(&filter),
        );
        assert!(selection.is_some());
    }

    // ------- build_column_projection tests -------

    #[test]
    fn test_projection_none_returns_none() {
        let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        ]));
        let result = build_column_projection(None, &schema, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_projection_direct_name_match() {
        let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("score", arrow_schema::DataType::Float64, false),
        ]));
        let fields = vec!["name".to_string(), "score".to_string()];
        let result = build_column_projection(Some(&fields), &schema, &[]);
        assert_eq!(result, Some(vec![1, 2]));
    }

    #[test]
    fn test_projection_via_column_mapping() {
        let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("col_a", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("col_b", arrow_schema::DataType::Utf8, false),
        ]));
        let mapping = vec![
            super::super::manifest::ColumnMapping {
                tantivy_field_name: "my_field".to_string(),
                parquet_column_name: "col_b".to_string(),
                physical_ordinal: 1,
                parquet_type: "BYTE_ARRAY".to_string(),
                tantivy_type: "Str".to_string(),
                field_id: None,
                fast_field_tokenizer: None,
            },
        ];
        let fields = vec!["my_field".to_string()];
        let result = build_column_projection(Some(&fields), &schema, &mapping);
        assert_eq!(result, Some(vec![1]));
    }

    #[test]
    fn test_projection_unknown_field_skipped() {
        let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        ]));
        let fields = vec!["nonexistent".to_string()];
        let result = build_column_projection(Some(&fields), &schema, &[]);
        assert_eq!(result, Some(vec![])); // Empty, no match
    }
}
