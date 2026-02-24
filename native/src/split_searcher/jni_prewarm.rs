// jni_prewarm.rs - JNI prewarm/preload functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: preloadComponentsNative, preloadFieldsNative, helper functions

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jboolean};
use jni::JNIEnv;

use crate::debug_println;
use crate::runtime_manager::block_on_operation;

/// Preload index components into cache for improved search performance.
///
/// This implements Quickwit's warm_up_term_dict_fields() pattern for the TERM component,
/// which preloads entire term dictionaries (FSTs) into the disk cache. Once cached,
/// sub-range requests for different terms are served from the cached data via get_coalesced().
///
/// Components supported:
/// - TERM: Preloads term dictionaries for all indexed text fields
/// - (Other components currently no-op, can be extended in future)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_preloadComponentsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    components: jobject,
) -> jboolean {
    debug_println!("🔥 PREWARM: preloadComponentsNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ PREWARM: Invalid searcher pointer (0)");
        return 0;
    }

    // Parse the IndexComponent array from Java to determine what to prewarm
    let components_set = match parse_index_components(&mut env, components) {
        Ok(set) => set,
        Err(e) => {
            debug_println!("❌ PREWARM: Failed to parse components: {}", e);
            return 0;
        }
    };

    debug_println!("🔥 PREWARM: Requested components: {:?}", components_set);

    // Check which components are requested
    let prewarm_term = components_set.contains("TERM");
    let prewarm_postings = components_set.contains("POSTINGS");
    let prewarm_fieldnorm = components_set.contains("FIELDNORM");
    let prewarm_fastfield = components_set.contains("FASTFIELD");
    let prewarm_store = components_set.contains("STORE");

    if !prewarm_term && !prewarm_postings && !prewarm_fieldnorm && !prewarm_fastfield && !prewarm_store {
        debug_println!("🔥 PREWARM: No supported components requested, returning success");
        return 1; // Success - nothing to do
    }

    // Perform the warmup using async runtime
    match block_on_operation(async move {
        let mut errors = Vec::new();

        // Prewarm TERM component (FST/term dictionaries)
        if prewarm_term {
            debug_println!("🔥 PREWARM: Warming up TERM component (FST)...");
            if let Err(e) = crate::prewarm::prewarm_term_dictionaries_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: TERM warmup failed: {}", e);
                errors.push(format!("TERM: {}", e));
            } else {
                debug_println!("✅ PREWARM: TERM warmup completed");
            }
        }

        // Prewarm POSTINGS component
        if prewarm_postings {
            debug_println!("🔥 PREWARM: Warming up POSTINGS component...");
            if let Err(e) = crate::prewarm::prewarm_postings_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: POSTINGS warmup failed: {}", e);
                errors.push(format!("POSTINGS: {}", e));
            } else {
                debug_println!("✅ PREWARM: POSTINGS warmup completed");
            }
        }

        // Prewarm FIELDNORM component
        if prewarm_fieldnorm {
            debug_println!("🔥 PREWARM: Warming up FIELDNORM component...");
            if let Err(e) = crate::prewarm::prewarm_fieldnorms_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: FIELDNORM warmup failed: {}", e);
                errors.push(format!("FIELDNORM: {}", e));
            } else {
                debug_println!("✅ PREWARM: FIELDNORM warmup completed");
            }
        }

        // Prewarm FASTFIELD component
        if prewarm_fastfield {
            debug_println!("🔥 PREWARM: Warming up FASTFIELD component...");
            if let Err(e) = crate::prewarm::prewarm_fastfields_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: FASTFIELD warmup failed: {}", e);
                errors.push(format!("FASTFIELD: {}", e));
            } else {
                debug_println!("✅ PREWARM: FASTFIELD warmup completed");
            }
        }

        // Prewarm STORE component (document storage)
        if prewarm_store {
            debug_println!("🔥 PREWARM: Warming up STORE component (document storage)...");
            if let Err(e) = crate::prewarm::prewarm_store_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: STORE warmup failed: {}", e);
                errors.push(format!("STORE: {}", e));
            } else {
                debug_println!("✅ PREWARM: STORE warmup completed");
            }
        }

        // After component prewarm, eagerly load __pq columns and warm all native
        // fast field columns into L1 for companion splits.
        // The FASTFIELD prewarm writes to L2 disk cache, but the CachingDirectory L1
        // ByteRangeCache needs individual column byte ranges to avoid downloads at query time.
        if let Some(ctx) = crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
            if ctx.has_merge_safe_tracking && ctx.parquet_manifest.is_some() {
                let num_segments = ctx.cached_searcher.segment_readers().len();
                for seg_ord in 0..num_segments {
                    if let Err(e) = ctx.ensure_pq_segment_loaded(seg_ord as u32).await {
                        debug_println!("⚠️ PREWARM: Failed to pre-load __pq columns for seg {}: {}", seg_ord, e);
                    }
                }
                debug_println!("✅ PREWARM: Pre-loaded __pq columns for {} segments", num_segments);
            }
            // Warm all native fast field columns into L1 CachingDirectory cache
            if let Err(e) = ctx.warm_native_fast_fields_l1().await {
                debug_println!("⚠️ PREWARM: Failed to warm native fast fields into L1: {}", e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Some warmups failed: {}", errors.join(", ")))
        }
    }) {
        Ok(_) => {
            debug_println!("✅ PREWARM: All requested components warmed up successfully");
            1 // success
        },
        Err(e) => {
            debug_println!("❌ PREWARM: Component warmup failed: {}", e);
            0 // failure
        }
    }
}

/// Field-specific preloading - preloads a single component type for only the specified fields
///
/// This provides fine-grained control over which fields are preloaded, reducing cache usage
/// and prewarm time compared to preloadComponentsNative which preloads all fields.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_preloadFieldsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    component: jobject,
    field_names: jobject,
) -> jboolean {
    debug_println!("🔥 PREWARM_FIELDS: preloadFieldsNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ PREWARM_FIELDS: Invalid searcher pointer (0)");
        return 0;
    }

    // Parse the single IndexComponent enum
    let component_name = match parse_single_index_component(&mut env, component) {
        Ok(name) => name,
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Failed to parse component: {}", e);
            return 0;
        }
    };

    // Parse the field names array
    let fields = match parse_string_array(&mut env, field_names) {
        Ok(fields) => fields,
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Failed to parse field names: {}", e);
            return 0;
        }
    };

    if fields.is_empty() {
        debug_println!("❌ PREWARM_FIELDS: No field names provided");
        return 0;
    }

    debug_println!("🔥 PREWARM_FIELDS: Component: {}, Fields: {:?}", component_name, fields);

    // Convert to HashSet for efficient lookup
    let field_filter: std::collections::HashSet<String> = fields.into_iter().collect();

    // Perform the warmup using async runtime
    match block_on_operation(async move {
        let result = match component_name.as_str() {
            "TERM" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up TERM for specific fields...");
                crate::prewarm::prewarm_term_dictionaries_for_fields(searcher_ptr, &field_filter).await
            },
            "POSTINGS" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up POSTINGS for specific fields...");
                crate::prewarm::prewarm_postings_for_fields(searcher_ptr, &field_filter).await
            },
            "POSITIONS" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up POSITIONS for specific fields...");
                crate::prewarm::prewarm_positions_for_fields(searcher_ptr, &field_filter).await
            },
            "FIELDNORM" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up FIELDNORM for specific fields...");
                crate::prewarm::prewarm_fieldnorms_for_fields(searcher_ptr, &field_filter).await
            },
            "FASTFIELD" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up FASTFIELD for specific fields...");
                crate::prewarm::prewarm_fastfields_for_fields(searcher_ptr, &field_filter).await
            },
            "STORE" => {
                // STORE is not field-specific, just call the regular implementation
                debug_println!("🔥 PREWARM_FIELDS: STORE is not field-specific, warming all...");
                crate::prewarm::prewarm_store_impl(searcher_ptr).await
            },
            _ => {
                Err(anyhow::anyhow!("Unsupported component for field-specific preloading: {}", component_name))
            }
        };

        // After field-specific prewarm, eagerly load __pq columns and warm native
        // fast field columns into L1 for companion splits — scoped to requested fields
        // plus their companion fields (_phash_*, *__uuids)
        if result.is_ok() {
            if let Some(ctx) = crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
                if ctx.has_merge_safe_tracking && ctx.parquet_manifest.is_some() {
                    let num_segments = ctx.cached_searcher.segment_readers().len();
                    for seg_ord in 0..num_segments {
                        if let Err(e) = ctx.ensure_pq_segment_loaded(seg_ord as u32).await {
                            debug_println!("⚠️ PREWARM_FIELDS: Failed to pre-load __pq columns for seg {}: {}", seg_ord, e);
                        }
                    }
                    debug_println!("✅ PREWARM_FIELDS: Pre-loaded __pq columns for {} segments", num_segments);
                }
                // Warm only the requested fields + their companion native fast field columns into L1
                let schema = ctx.cached_searcher.index().schema();
                let expanded = crate::prewarm::field_specific::expand_companion_fields(&field_filter, &schema);
                let expanded_vec: Vec<String> = expanded.into_iter().collect();
                if let Err(e) = ctx.warm_native_fast_fields_l1_for_fields(&expanded_vec).await {
                    debug_println!("⚠️ PREWARM_FIELDS: Failed to warm native fast fields into L1: {}", e);
                }
            }
        }

        result
    }) {
        Ok(_) => {
            debug_println!("✅ PREWARM_FIELDS: Field-specific warmup completed successfully");
            1 // success
        },
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Field-specific warmup failed: {}", e);
            0 // failure
        }
    }
}

/// Parse a single Java IndexComponent enum into its name
fn parse_single_index_component(env: &mut JNIEnv, component: jobject) -> anyhow::Result<String> {
    if component.is_null() {
        return Err(anyhow::anyhow!("Component is null"));
    }

    let element = unsafe { JObject::from_raw(component) };

    // Get the enum name using Enum.name() method
    let name_obj = env.call_method(&element, "name", "()Ljava/lang/String;", &[])
        .map_err(|e| anyhow::anyhow!("Failed to call name() on enum: {}", e))?
        .l()
        .map_err(|e| anyhow::anyhow!("Failed to get string from name(): {}", e))?;

    let name_jstring = JString::from(name_obj);
    let name: String = env.get_string(&name_jstring)
        .map_err(|e| anyhow::anyhow!("Failed to convert JString: {}", e))?
        .into();

    Ok(name)
}

/// Parse a Java String[] array into a Vec<String>
fn parse_string_array(env: &mut JNIEnv, array_obj: jobject) -> anyhow::Result<Vec<String>> {
    use jni::objects::JObjectArray;

    let mut result = Vec::new();

    if array_obj.is_null() {
        return Ok(result);
    }

    let array = unsafe { JObjectArray::from_raw(array_obj) };
    let length = env.get_array_length(&array)
        .map_err(|e| anyhow::anyhow!("Failed to get array length: {}", e))?;

    for i in 0..length {
        let element = env.get_object_array_element(&array, i)
            .map_err(|e| anyhow::anyhow!("Failed to get array element {}: {}", i, e))?;

        if element.is_null() {
            continue;
        }

        let jstring = JString::from(element);
        let s: String = env.get_string(&jstring)
            .map_err(|e| anyhow::anyhow!("Failed to convert JString at index {}: {}", i, e))?
            .into();

        result.push(s);
    }

    Ok(result)
}

/// Prewarm parquet fast fields by transcoding from parquet data.
///
/// This method transcodes the specified columns (or all applicable columns based on
/// the manifest's FastFieldMode) from parquet into tantivy's columnar format and
/// caches them in the ParquetAugmentedDirectory.
///
/// After this call, the tantivy searcher's fast field reads for these columns will
/// be served from the transcoded cache.
///
/// # Arguments
/// * `searcher_ptr` - Pointer to the CachedSearcherContext
/// * `columns` - Java String[] of column names to transcode, or null for all applicable
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativePrewarmParquetFastFields(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    columns: jobject,
) -> jboolean {
    debug_println!("📊 PARQUET_PREWARM: nativePrewarmParquetFastFields called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ PARQUET_PREWARM: Invalid searcher pointer (0)");
        return 0;
    }

    // Parse optional column names
    let requested_columns: Option<Vec<String>> = if columns.is_null() {
        None
    } else {
        match parse_string_array(&mut env, columns) {
            Ok(cols) if cols.is_empty() => None,
            Ok(cols) => Some(cols),
            Err(e) => {
                debug_println!("❌ PARQUET_PREWARM: Failed to parse column names: {}", e);
                return 0;
            }
        }
    };

    debug_println!("📊 PARQUET_PREWARM: Requested columns: {:?}", requested_columns);

    match block_on_operation(async move {
        // Get the searcher context
        let context = match crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
            Some(ctx) => ctx,
            None => return Err(anyhow::anyhow!("Invalid searcher pointer")),
        };

        // Check if augmented directory is available
        let augmented_dir = match &context.augmented_directory {
            Some(dir) => dir.clone(),
            None => return Err(anyhow::anyhow!(
                "No ParquetAugmentedDirectory configured. The split may not have a parquet manifest \
                 with fast_field_mode != Disabled."
            )),
        };

        // Find the segment .fast file paths from the index
        let index = &context.cached_index;
        let segment_metas = index.searchable_segment_metas()
            .map_err(|e| anyhow::anyhow!("Failed to get segment metas: {}", e))?;

        if segment_metas.is_empty() {
            debug_println!("📊 PARQUET_PREWARM: No segments found");
            return Ok(());
        }

        let cols_ref = requested_columns.as_deref();

        for seg_meta in &segment_metas {
            let fast_path = seg_meta.relative_path(tantivy::index::SegmentComponent::FastFields);
            debug_println!(
                "📊 PARQUET_PREWARM: Transcoding fast fields for segment {} -> {:?}",
                seg_meta.id().uuid_string(), fast_path
            );

            augmented_dir.transcode_and_cache(
                &fast_path,
                cols_ref,
            ).await
            .map_err(|e| anyhow::anyhow!(
                "Failed to transcode fast fields for segment {}: {}",
                seg_meta.id().uuid_string(), e
            ))?;
        }

        // Update transcoded_fast_columns so that the first aggregation after prewarm
        // skips the transcode loop and uses the L1 cache directly. Without this update,
        // ensure_fast_fields_for_query sees an empty set and redundantly re-reads all
        // parquet files — causing unnecessary I/O (and OOM on large datasets).
        let transcoded_names = augmented_dir.effective_column_names(cols_ref);
        if !transcoded_names.is_empty() {
            let mut transcoded = context.transcoded_fast_columns.lock().unwrap();
            for name in transcoded_names {
                transcoded.insert(name);
            }
            debug_println!(
                "📊 PARQUET_PREWARM: Updated transcoded_fast_columns with {} entries",
                transcoded.len()
            );
        }

        debug_println!("📊 PARQUET_PREWARM: All segments transcoded successfully");
        Ok(())
    }) {
        Ok(_) => {
            debug_println!("✅ PARQUET_PREWARM: Parquet fast field warmup completed successfully");
            1
        }
        Err(e) => {
            debug_println!("❌ PARQUET_PREWARM: Parquet fast field warmup failed: {}", e);
            0
        }
    }
}

/// Prewarm parquet columns by pre-fetching their pages into the storage cache (L2).
///
/// This method pre-reads parquet column pages for the specified columns across all
/// parquet files in the manifest, populating the disk cache for faster doc retrieval.
///
/// Unlike `nativePrewarmParquetFastFields` which transcodes for fast field access,
/// this method focuses on warming the storage cache for document retrieval operations.
///
/// # Arguments
/// * `searcher_ptr` - Pointer to the CachedSearcherContext
/// * `columns` - Java String[] of column names to prewarm, or null for all columns
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativePrewarmParquetColumns(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    columns: jobject,
) -> jboolean {
    debug_println!("📊 PARQUET_COL_PREWARM: nativePrewarmParquetColumns called");

    if searcher_ptr == 0 {
        return 0;
    }

    let requested_columns: Option<Vec<String>> = if columns.is_null() {
        None
    } else {
        match parse_string_array(&mut env, columns) {
            Ok(cols) if cols.is_empty() => None,
            Ok(cols) => Some(cols),
            Err(e) => {
                debug_println!("❌ PARQUET_COL_PREWARM: Failed to parse columns: {}", e);
                return 0;
            }
        }
    };

    match block_on_operation(async move {
        let context = match crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
            Some(ctx) => ctx,
            None => return Err(anyhow::anyhow!("Invalid searcher pointer")),
        };

        let manifest = match &context.parquet_manifest {
            Some(m) => m.clone(),
            None => return Err(anyhow::anyhow!("No parquet manifest available")),
        };

        let storage = match &context.parquet_storage {
            Some(s) => s.clone(),
            None => {
                let reason = if context.parquet_table_root.is_none() {
                    "parquet_table_root was not set. Pass the table root path to createSplitSearcher() \
                     or configure it via CacheConfig.withParquetTableRoot()."
                } else {
                    "parquet storage creation failed (likely bad credentials or unreachable endpoint). \
                     Enable TANTIVY4JAVA_DEBUG=1 and check stderr for the storage creation error."
                };
                return Err(anyhow::anyhow!(
                    "Parquet column prewarm failed: {}", reason
                ));
            }
        };

        // Determine which column names to prewarm (tantivy field names → parquet column names)
        let columns_to_warm: Vec<&crate::parquet_companion::ColumnMapping> = match &requested_columns {
            Some(names) => manifest.column_mapping.iter()
                .filter(|cm| names.contains(&cm.tantivy_field_name))
                .collect(),
            None => manifest.column_mapping.iter().collect(),
        };

        let warm_parquet_names: std::collections::HashSet<&str> = columns_to_warm.iter()
            .map(|cm| cm.parquet_column_name.as_str())
            .collect();

        debug_println!(
            "📊 PARQUET_COL_PREWARM: Warming {} columns across {} files via CachedParquetReader (L1+L2)",
            columns_to_warm.len(), manifest.parquet_files.len()
        );

        // For each parquet file, use CachedParquetReader to read columns.
        // This populates both the L1 ByteRangeCache (used by doc retrieval) and
        // the L2 disk cache (used across JVM restarts).
        for file_entry in &manifest.parquet_files {
            let path_buf = std::path::PathBuf::from(&file_entry.relative_path);

            // Check metadata cache for a pre-loaded footer
            let cached_meta = {
                context.parquet_metadata_cache.lock().ok()
                    .and_then(|guard| guard.get(&path_buf).cloned())
            };

            let reader = if let Some(meta) = cached_meta {
                debug_println!("📊 PARQUET_COL_PREWARM: Using cached metadata for '{}'", file_entry.relative_path);
                crate::parquet_companion::cached_reader::CachedParquetReader::with_metadata(
                    storage.clone(),
                    path_buf.clone(),
                    file_entry.file_size_bytes,
                    meta,
                )
            } else {
                crate::parquet_companion::cached_reader::CachedParquetReader::new(
                    storage.clone(),
                    path_buf.clone(),
                    file_entry.file_size_bytes,
                )
            };
            let reader = reader.with_byte_cache(context.parquet_byte_range_cache.clone());
            let reader = if let Some(config) = context.parquet_coalesce_config {
                reader.with_coalesce_config(config)
            } else {
                reader
            };

            // Build the stream — this reads the footer and populates metadata
            let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| anyhow::anyhow!(
                    "Failed to create parquet stream builder for '{}': {}", file_entry.relative_path, e
                ))?;

            // Cache the footer in parquet_metadata_cache for doc retrieval
            {
                if let Ok(mut guard) = context.parquet_metadata_cache.lock() {
                    if !guard.contains_key(&path_buf) {
                        guard.insert(path_buf.clone(), builder.metadata().clone());
                        debug_println!("📊 PARQUET_COL_PREWARM: Cached footer for '{}'", file_entry.relative_path);
                    }
                }
            }

            // Build column projection from warm_parquet_names
            let parquet_schema = builder.schema().clone();
            let parquet_file_schema = builder.parquet_schema().clone();
            let col_indices: Vec<usize> = parquet_schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, field)| {
                    if warm_parquet_names.contains(field.name().as_str()) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();

            if col_indices.is_empty() {
                debug_println!("📊 PARQUET_COL_PREWARM: No matching columns in file '{}', skipping", file_entry.relative_path);
                continue;
            }

            let projection = parquet::arrow::ProjectionMask::roots(
                &parquet_file_schema,
                col_indices.iter().cloned(),
            );

            let stream = builder.with_projection(projection).build()
                .map_err(|e| anyhow::anyhow!(
                    "Failed to build parquet stream for '{}': {}", file_entry.relative_path, e
                ))?;

            // Read all batches — CachedParquetReader populates L1 ByteRangeCache
            // with dictionary pages and data pages. Data itself is discarded.
            use futures::StreamExt;
            let mut stream = std::pin::pin!(stream);
            let mut batch_count = 0u32;
            while let Some(batch_result) = stream.next().await {
                let _batch = batch_result.map_err(|e| anyhow::anyhow!(
                    "Failed to read batch from '{}': {}", file_entry.relative_path, e
                ))?;
                batch_count += 1;
            }

            debug_println!(
                "📊 PARQUET_COL_PREWARM: Warmed file '{}' ({} batches, {} columns) — L1 ByteRangeCache populated",
                file_entry.relative_path, batch_count, col_indices.len()
            );
        }

        debug_println!("📊 PARQUET_COL_PREWARM: All columns warmed successfully (L1+L2)");
        Ok(())
    }) {
        Ok(_) => 1,
        Err(e) => {
            debug_println!("❌ PARQUET_COL_PREWARM: Failed: {}", e);
            0
        }
    }
}

/// Get parquet retrieval statistics from the searcher context.
/// Returns a JSON string with metrics, or null if not available.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeGetParquetRetrievalStats(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    use jni::sys::jobject;
    use crate::utils::string_to_jstring;

    if searcher_ptr == 0 {
        return std::ptr::null_mut();
    }

    let context = match crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
        Some(ctx) => ctx,
        None => return std::ptr::null_mut(),
    };

    let manifest = match &context.parquet_manifest {
        Some(m) => m,
        None => return std::ptr::null_mut(),
    };

    // Build stats JSON from manifest metadata
    let stats = serde_json::json!({
        "hasParquetManifest": true,
        "totalFiles": manifest.parquet_files.len(),
        "totalRows": manifest.total_rows,
        "totalRowGroups": manifest.parquet_files.iter()
            .map(|f| f.row_groups.len())
            .sum::<usize>(),
        "totalColumns": manifest.column_mapping.len(),
        "fastFieldMode": format!("{:?}", manifest.fast_field_mode),
        "tableRoot": context.parquet_table_root.as_deref().unwrap_or(""),
        "fileSizes": manifest.parquet_files.iter()
            .map(|f| serde_json::json!({
                "path": &f.relative_path,
                "sizeBytes": f.file_size_bytes,
                "numRows": f.num_rows,
                "numRowGroups": f.row_groups.len(),
            }))
            .collect::<Vec<_>>(),
    });

    let json_str = serde_json::to_string(&stats).unwrap_or_default();
    match string_to_jstring(&mut env, &json_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get the column mapping from the parquet companion manifest.
/// Returns a JSON string with column mapping details, or null if no manifest.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeGetColumnMapping(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    use crate::utils::{string_to_jstring, convert_throwable};

    convert_throwable(&mut env, |env| {
        if searcher_ptr == 0 {
            return Ok(std::ptr::null_mut());
        }

        let context = match crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
            Some(ctx) => ctx,
            None => return Ok(std::ptr::null_mut()),
        };

        let manifest = match &context.parquet_manifest {
            Some(m) => m,
            None => return Ok(std::ptr::null_mut()),
        };

        let mapping: Vec<serde_json::Value> = manifest.column_mapping.iter()
            .map(|cm| serde_json::json!({
                "tantivy_field_name": cm.tantivy_field_name,
                "parquet_column_name": cm.parquet_column_name,
                "physical_ordinal": cm.physical_ordinal,
                "parquet_type": cm.parquet_type,
                "tantivy_type": cm.tantivy_type,
            }))
            .collect();

        let json_str = serde_json::to_string(&mapping).unwrap_or_default();
        match string_to_jstring(env, &json_str) {
            Ok(jstr) => Ok(jstr.into_raw()),
            Err(_) => Ok(std::ptr::null_mut()),
        }
    }).unwrap_or(std::ptr::null_mut())
}

/// Get the string indexing modes from the parquet companion manifest.
/// Returns a JSON string mapping field name to indexing mode, or null if no manifest.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeGetStringIndexingModes(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    use crate::utils::{string_to_jstring, convert_throwable};

    convert_throwable(&mut env, |env| {
        if searcher_ptr == 0 {
            return Ok(std::ptr::null_mut());
        }

        let context = match crate::utils::jlong_to_arc::<super::types::CachedSearcherContext>(searcher_ptr) {
            Some(ctx) => ctx,
            None => return Ok(std::ptr::null_mut()),
        };

        let manifest = match &context.parquet_manifest {
            Some(m) => m,
            None => return Ok(std::ptr::null_mut()),
        };

        if manifest.string_indexing_modes.is_empty() {
            // Return empty JSON object rather than null to distinguish
            // "no manifest" from "manifest exists but no compact modes"
            let json_str = "{}";
            return match string_to_jstring(env, json_str) {
                Ok(jstr) => Ok(jstr.into_raw()),
                Err(_) => Ok(std::ptr::null_mut()),
            };
        }

        // Serialize directly — StringIndexingMode derives serde::Serialize
        let json_str = serde_json::to_string(&manifest.string_indexing_modes).unwrap_or_default();
        match string_to_jstring(env, &json_str) {
            Ok(jstr) => Ok(jstr.into_raw()),
            Err(_) => Ok(std::ptr::null_mut()),
        }
    }).unwrap_or(std::ptr::null_mut())
}

/// Resolve a relative parquet file path against the effective table root.
/// If the path is already absolute or has a protocol, returns it as-is.
fn resolve_parquet_path(table_root: &str, relative_path: &str) -> String {
    if relative_path.starts_with('/') || relative_path.contains("://") {
        relative_path.to_string()
    } else {
        let root = table_root.trim_end_matches('/');
        format!("{}/{}", root, relative_path)
    }
}

/// Parse the Java IndexComponent[] array into a HashSet of component names
fn parse_index_components(env: &mut JNIEnv, components: jobject) -> anyhow::Result<std::collections::HashSet<String>> {
    use jni::objects::JObjectArray;

    let mut result = std::collections::HashSet::new();

    if components.is_null() {
        return Ok(result);
    }

    let array = unsafe { JObjectArray::from_raw(components) };
    let length = env.get_array_length(&array)
        .map_err(|e| anyhow::anyhow!("Failed to get array length: {}", e))?;

    for i in 0..length {
        let element = env.get_object_array_element(&array, i)
            .map_err(|e| anyhow::anyhow!("Failed to get array element {}: {}", i, e))?;

        if element.is_null() {
            continue;
        }

        // Get the enum name using Enum.name() method
        let name_obj = env.call_method(&element, "name", "()Ljava/lang/String;", &[])
            .map_err(|e| anyhow::anyhow!("Failed to call name() on enum: {}", e))?
            .l()
            .map_err(|e| anyhow::anyhow!("Failed to get string from name(): {}", e))?;

        let name_jstring = JString::from(name_obj);
        let name: String = env.get_string(&name_jstring)
            .map_err(|e| anyhow::anyhow!("Failed to convert JString: {}", e))?
            .into();

        debug_println!("🔥 PREWARM: Parsed component: {}", name);
        result.insert(name);
    }

    Ok(result)
}
