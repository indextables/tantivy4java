// doc_retrieval_jni.rs - JNI entry points for document retrieval
// Extracted from document_retrieval.rs during refactoring

use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jbyteArray, jint, jlong, jobject};
use jni::JNIEnv;
use std::sync::Arc;

use crate::common::to_java_exception;
use crate::debug_println;
use crate::perf_println;
use crate::runtime_manager::block_on_operation;
use crate::split_searcher::async_impl::perform_doc_retrieval_async_impl_thread_safe;
use crate::split_searcher::types::CachedSearcherContext;
use crate::utils::with_arc_safe;

use super::batch_doc_retrieval::retrieve_documents_batch_from_split_optimized;
use super::batch_serialization::{serialize_documents_to_buffer, serialize_documents_to_buffer_with_filter};

/// Batch document retrieval for SplitSearcher using Quickwit's optimized approach
/// This implementation follows Quickwit's patterns from fetch_docs.rs:
/// 1. Sort addresses by segment for cache locality
/// 2. Open index with proper cache settings
/// 3. Use doc_async for optimal performance
/// 4. Reuse index, reader, and searcher across all documents
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docBatchNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
) -> jobject {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Convert JNI arrays to proper JArray types
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };

    // Get the segment and doc ID arrays
    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(
                &mut env,
                &anyhow::anyhow!("Failed to get segments array length: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get segments array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();

    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get doc_ids array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();

    if segments_vec.len() != doc_ids_vec.len() {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Segments and doc_ids arrays must have same length"),
        );
        return std::ptr::null_mut();
    }

    // Create DocAddress objects with original indices for ordering
    let mut indexed_addresses: Vec<(usize, tantivy::DocAddress)> = segments_vec
        .iter()
        .zip(doc_ids_vec.iter())
        .enumerate()
        .map(|(idx, (&seg, &doc))| (idx, tantivy::DocAddress::new(seg, doc)))
        .collect();

    // Sort by document address for cache locality (following Quickwit pattern)
    indexed_addresses.sort_by_key(|(_, addr)| *addr);

    // Extract sorted addresses for batch retrieval
    let sorted_addresses: Vec<tantivy::DocAddress> = indexed_addresses
        .iter()
        .map(|(_, addr)| *addr)
        .collect();

    // Use Quickwit-optimized bulk document retrieval
    let retrieval_result =
        retrieve_documents_batch_from_split_optimized(searcher_ptr, sorted_addresses);

    match retrieval_result {
        Ok(sorted_docs) => {
            // Reorder documents back to original input order
            let mut ordered_doc_ptrs = vec![std::ptr::null_mut(); indexed_addresses.len()];
            for (i, (original_idx, _)) in indexed_addresses.iter().enumerate() {
                if i < sorted_docs.len() {
                    ordered_doc_ptrs[*original_idx] = sorted_docs[i];
                }
            }

            // Create a Java Document array
            let document_class =
                match env.find_class("io/indextables/tantivy4java/core/Document") {
                    Ok(class) => class,
                    Err(e) => {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to find Document class: {}", e),
                        );
                        return std::ptr::null_mut();
                    }
                };

            let doc_array = match env.new_object_array(
                ordered_doc_ptrs.len() as i32,
                &document_class,
                JObject::null(),
            ) {
                Ok(array) => array,
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to create Document array: {}", e),
                    );
                    return std::ptr::null_mut();
                }
            };

            // Create Document objects and add to array
            for (i, doc_ptr) in ordered_doc_ptrs.iter().enumerate() {
                if doc_ptr.is_null() {
                    continue; // Skip null documents
                }

                // Create Document object with the pointer
                let doc_obj = match env.new_object(
                    &document_class,
                    "(J)V",
                    &[JValue::Long(*doc_ptr as jlong)],
                ) {
                    Ok(obj) => obj,
                    Err(e) => {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to create Document object: {}", e),
                        );
                        continue;
                    }
                };

                if let Err(e) = env.set_object_array_element(&doc_array, i as i32, doc_obj) {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to set array element: {}", e),
                    );
                }
            }

            doc_array.into_raw()
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Async-first replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_docNative
/// Implements document retrieval using Quickwit's async approach without deadlocks
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segment_ord: jint,
    doc_id: jint,
) -> jobject {
    debug_println!(
        "🔥🔥🔥 JNI DEBUG: docNative called - ptr:{}, seg:{}, doc:{}",
        searcher_ptr,
        segment_ord,
        doc_id
    );
    debug_println!("🚀 ASYNC_JNI: docNative called with async-first architecture");

    // Add this line to verify the method is actually being called
    // Use simplified async pattern that returns thread-safe types
    // Note: env cannot be moved into async block due to thread safety
    match block_on_operation(async move {
        perform_doc_retrieval_async_impl_thread_safe(
            searcher_ptr,
            segment_ord as u32,
            doc_id as u32,
        )
        .await
    }) {
        Ok(document_ptr) => {
            debug_println!(
                "🔥 JNI DEBUG: Document retrieval successful, creating Java Document object from pointer: {}",
                document_ptr
            );

            // Check if the pointer is valid (non-zero)
            if document_ptr == 0 {
                debug_println!("🔥 JNI DEBUG: ERROR - Document pointer is null/zero!");
                std::ptr::null_mut()
            } else {
                debug_println!(
                    "🔥 JNI DEBUG: Document pointer is valid ({}), proceeding with Java object creation",
                    document_ptr
                );

                // Create Java Document object properly using JNI
                let mut env_mut = env;
                debug_println!("🔥 JNI DEBUG: About to call create_java_document_object...");
                match create_java_document_object(&mut env_mut, document_ptr) {
                    Ok(java_doc_obj) => {
                        debug_println!(
                            "🔥 JNI DEBUG: Successfully created Java Document object, returning: {:?}",
                            java_doc_obj
                        );
                        java_doc_obj
                    }
                    Err(e) => {
                        debug_println!(
                            "🔥 JNI DEBUG: Failed to create Java Document object: {}",
                            e
                        );
                        crate::common::to_java_exception(&mut env_mut, &e);
                        std::ptr::null_mut()
                    }
                }
            }
        }
        Err(e) => {
            debug_println!("🔥 JNI DEBUG: Document retrieval failed: {}", e);
            debug_println!("❌ ASYNC_JNI: Document retrieval operation failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

/// Create a Java Document object from a native document pointer
/// This properly converts the Rust DocumentWrapper pointer to a Java Document object
fn create_java_document_object(env: &mut JNIEnv, document_ptr: jlong) -> anyhow::Result<jobject> {
    debug_println!(
        "🔧 JNI_CONVERT: Creating Java Document object from pointer: {}",
        document_ptr
    );

    // Find the Document class
    let document_class = env
        .find_class("io/indextables/tantivy4java/core/Document")
        .map_err(|e| anyhow::anyhow!("Failed to find Document class: {}", e))?;

    // Create a new Document object with the pointer constructor: Document(long nativePtr)
    let document_obj = env
        .new_object(
            &document_class,
            "(J)V", // Constructor signature: takes a long (J) and returns void (V)
            &[JValue::Long(document_ptr)],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create Document object: {}", e))?;

    debug_println!("🔧 JNI_CONVERT: Successfully created Java Document object");
    Ok(document_obj.into_raw())
}

/// Bulk document retrieval returning a serialized byte buffer instead of JNI Document objects.
/// This reduces JNI overhead for large batch retrievals by returning all documents in a single
/// byte buffer that can be parsed on the Java side.
///
/// The byte buffer uses the same format as BatchDocumentBuilder:
/// - Header magic (4 bytes)
/// - Document data (variable)
/// - Offset table (N * 4 bytes)
/// - Footer (offset table position + doc count + footer magic)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
) -> jbyteArray {
    debug_println!(
        "🚀 BULK_RETRIEVAL: docsBulkNative called with searcher_ptr: {}",
        searcher_ptr
    );

    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Convert JNI arrays to proper JArray types
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };

    // Get the segment and doc ID arrays
    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(
                &mut env,
                &anyhow::anyhow!("Failed to get segments array length: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get segments array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();

    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get doc_ids array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();

    if segments_vec.len() != doc_ids_vec.len() {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Segments and doc_ids arrays must have same length"),
        );
        return std::ptr::null_mut();
    }

    debug_println!(
        "🚀 BULK_RETRIEVAL: Retrieving {} documents as byte buffer",
        array_len
    );

    // Use the optimized bulk document retrieval implementation
    match retrieve_documents_bulk_as_buffer(searcher_ptr, segments_vec, doc_ids_vec) {
        Ok(buffer) => {
            debug_println!(
                "🚀 BULK_RETRIEVAL: Successfully serialized {} bytes",
                buffer.len()
            );

            // Create Java byte array
            match env.new_byte_array(buffer.len() as i32) {
                Ok(byte_array) => {
                    // Copy data to Java array
                    let byte_slice: &[i8] =
                        unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const i8, buffer.len()) };
                    if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to set byte array data: {}", e),
                        );
                        return std::ptr::null_mut();
                    }
                    byte_array.into_raw()
                }
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to create byte array: {}", e),
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

/// Bulk document retrieval with field filtering.
/// Same as docsBulkNative but only serializes the specified fields.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNativeWithFields(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
    field_names: jni::objects::JObjectArray,
) -> jbyteArray {
    debug_println!(
        "🚀 BULK_RETRIEVAL: docsBulkNativeWithFields called with searcher_ptr: {}",
        searcher_ptr
    );

    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Convert JNI arrays to proper JArray types
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };

    // Get the segment and doc ID arrays
    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(
                &mut env,
                &anyhow::anyhow!("Failed to get segments array length: {}", e),
            );
            return std::ptr::null_mut();
        }
    };

    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get segments array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();

    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(
            &mut env,
            &anyhow::anyhow!("Failed to get doc_ids array: {}", e),
        );
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();

    // Extract field names from Java String array
    let field_filter: std::collections::HashSet<String> = {
        let field_names_len = match env.get_array_length(&field_names) {
            Ok(len) => len as usize,
            Err(e) => {
                to_java_exception(
                    &mut env,
                    &anyhow::anyhow!("Failed to get field_names array length: {}", e),
                );
                return std::ptr::null_mut();
            }
        };

        let mut filter = std::collections::HashSet::with_capacity(field_names_len);
        for i in 0..field_names_len {
            let field_name_obj = match env.get_object_array_element(&field_names, i as i32) {
                Ok(obj) => obj,
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to get field name at index {}: {}", i, e),
                    );
                    return std::ptr::null_mut();
                }
            };
            let field_name_jstring = jni::objects::JString::from(field_name_obj);
            let field_name_str: String = match env.get_string(&field_name_jstring) {
                Ok(s) => s.into(),
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to convert field name at index {}: {}", i, e),
                    );
                    return std::ptr::null_mut();
                }
            };
            filter.insert(field_name_str);
        }
        filter
    };

    debug_println!(
        "🚀 BULK_RETRIEVAL: Retrieving {} documents with {} field filter",
        array_len,
        field_filter.len()
    );

    // Use the optimized bulk document retrieval with field filtering
    match retrieve_documents_bulk_as_buffer_with_filter(searcher_ptr, segments_vec, doc_ids_vec, Some(&field_filter)) {
        Ok(buffer) => {
            debug_println!(
                "🚀 BULK_RETRIEVAL: Successfully serialized {} bytes (filtered)",
                buffer.len()
            );

            // Create Java byte array
            match env.new_byte_array(buffer.len() as i32) {
                Ok(byte_array) => {
                    // Copy data to Java array
                    let byte_slice: &[i8] =
                        unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const i8, buffer.len()) };
                    if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                        to_java_exception(
                            &mut env,
                            &anyhow::anyhow!("Failed to set byte array data: {}", e),
                        );
                        return std::ptr::null_mut();
                    }
                    byte_array.into_raw()
                }
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to create byte array: {}", e),
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

/// Retrieve documents and serialize them to a byte buffer.
/// This is the core implementation that:
/// 1. Uses the same S3 optimizations as docBatchNative (range consolidation, prefetching)
/// 2. Retrieves TantivyDocuments
/// 3. Serializes them to the batch protocol format
fn retrieve_documents_bulk_as_buffer(
    searcher_ptr: jlong,
    segments_vec: Vec<u32>,
    doc_ids_vec: Vec<u32>,
) -> Result<Vec<u8>, anyhow::Error> {
    retrieve_documents_bulk_as_buffer_with_filter(searcher_ptr, segments_vec, doc_ids_vec, None)
}

/// Retrieve documents and serialize them to a byte buffer with optional field filtering.
/// When field_filter is Some, only the specified fields are included in the output.
fn retrieve_documents_bulk_as_buffer_with_filter(
    searcher_ptr: jlong,
    segments_vec: Vec<u32>,
    doc_ids_vec: Vec<u32>,
    field_filter: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<u8>, anyhow::Error> {
    // Create DocAddress objects with original indices for ordering
    let mut indexed_addresses: Vec<(usize, tantivy::DocAddress)> = segments_vec
        .iter()
        .zip(doc_ids_vec.iter())
        .enumerate()
        .map(|(idx, (&seg, &doc))| (idx, tantivy::DocAddress::new(seg, doc)))
        .collect();

    // Sort by document address for cache locality (following Quickwit pattern)
    indexed_addresses.sort_by_key(|(_, addr)| *addr);

    // Extract sorted addresses for batch retrieval
    let sorted_addresses: Vec<tantivy::DocAddress> = indexed_addresses
        .iter()
        .map(|(_, addr)| *addr)
        .collect();

    // Get the schema from the searcher context (clone it to avoid lifetime issues)
    let schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        searcher_context.cached_searcher.schema().clone()
    })
    .ok_or_else(|| anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr))?;

    // Use the existing optimized batch retrieval, but retrieve actual documents
    let documents_result = retrieve_documents_as_tantivy_docs(searcher_ptr, sorted_addresses.clone())?;

    // Reorder documents back to original input order
    let mut ordered_docs: Vec<Option<tantivy::schema::TantivyDocument>> =
        vec![None; indexed_addresses.len()];
    for (i, (original_idx, _)) in indexed_addresses.iter().enumerate() {
        if i < documents_result.len() {
            ordered_docs[*original_idx] = Some(documents_result[i].clone());
        }
    }

    // Collect documents with schema for serialization
    let docs_with_schema: Vec<(tantivy::schema::TantivyDocument, tantivy::schema::Schema)> =
        ordered_docs
            .into_iter()
            .filter_map(|doc_opt| doc_opt.map(|doc| (doc, schema.clone())))
            .collect();

    // Serialize to byte buffer with optional field filtering
    serialize_documents_to_buffer_with_filter(&docs_with_schema, field_filter)
        .map_err(|e| anyhow::anyhow!("Failed to serialize documents: {}", e))
}

/// Retrieve documents as TantivyDocuments (for serialization, not JNI objects).
/// Uses the same S3 optimizations as the main batch retrieval.
fn retrieve_documents_as_tantivy_docs(
    searcher_ptr: jlong,
    doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<tantivy::schema::TantivyDocument>, anyhow::Error> {
    use crate::batch_retrieval::simple::{SimpleBatchConfig, SimpleBatchOptimizer};
    use crate::split_searcher::cache_config::BASE_CONCURRENT_REQUESTS;

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let storage_resolver = &context.cached_storage;
        let byte_range_cache = &context.byte_range_cache;
        let bundle_file_offsets = &context.bundle_file_offsets;

        let _guard = runtime.enter();

        tokio::task::block_in_place(|| {
            let timeout_duration = std::time::Duration::from_secs(10);
            runtime.block_on(tokio::time::timeout(timeout_duration, async {
                let cached_searcher = &context.cached_searcher;

                // Apply batch optimization (same as docBatchNative)
                let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());
                let doc_count = doc_addresses.len();

                // Count unique segments for metrics
                let num_segments = {
                    let mut segments: std::collections::HashSet<u32> = std::collections::HashSet::new();
                    for addr in &doc_addresses {
                        segments.insert(addr.segment_ord);
                    }
                    segments.len()
                };

                if optimizer.should_optimize(doc_count) {
                    debug_println!(
                        "🚀 BULK_RETRIEVAL: Starting range consolidation for {} documents",
                        doc_count
                    );

                    if let Ok(ranges) = optimizer.consolidate_ranges(&doc_addresses, &cached_searcher) {
                        debug_println!(
                            "🚀 BULK_RETRIEVAL: Consolidated {} docs → {} ranges",
                            doc_count,
                            ranges.len()
                        );

                        // Prefetch ranges and record metrics
                        if let Some(cache) = byte_range_cache {
                            match crate::batch_retrieval::simple::prefetch_ranges_with_cache(
                                ranges.clone(),
                                storage_resolver.clone(),
                                split_uri,
                                cache,
                                bundle_file_offsets,
                            )
                            .await {
                                Ok(stats) => {
                                    debug_println!(
                                        "🚀 BULK_RETRIEVAL SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                        stats.ranges_fetched,
                                        stats.bytes_fetched,
                                        stats.duration_ms
                                    );

                                    // Record batch optimization metrics
                                    let bytes_wasted = 0; // TODO: Calculate actual gap bytes
                                    crate::split_cache_manager::record_batch_metrics(
                                        None, // cache_name not available in this context
                                        doc_count,
                                        &stats,
                                        num_segments,
                                        bytes_wasted,
                                    );
                                }
                                Err(e) => {
                                    debug_println!(
                                        "⚠️ BULK_RETRIEVAL: Prefetch failed (continuing with normal retrieval): {}",
                                        e
                                    );
                                    // Non-fatal: continue with normal doc_async which will fetch on-demand
                                }
                            }
                        }
                    }
                }

                // Validate segment ordinals before retrieval to prevent index-out-of-bounds panics
                let num_segments = cached_searcher.segment_readers().len();

                // Retrieve documents concurrently
                let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                    let moved_searcher = cached_searcher.clone();
                    async move {
                        if doc_addr.segment_ord as usize >= num_segments {
                            return Err(anyhow::anyhow!(
                                "Invalid segment ordinal {}: index has {} segment(s)",
                                doc_addr.segment_ord, num_segments
                            ));
                        }
                        let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            moved_searcher.doc_async(doc_addr),
                        )
                        .await
                        .map_err(|_| {
                            anyhow::anyhow!("Document retrieval timed out for {:?}", doc_addr)
                        })?
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to retrieve document at address {:?}: {}",
                                doc_addr,
                                e
                            )
                        })?;

                        Ok::<tantivy::schema::TantivyDocument, anyhow::Error>(doc)
                    }
                });

                use futures::stream::{StreamExt, TryStreamExt};
                // IMPORTANT: Use buffered() NOT buffer_unordered() to maintain document order.
                // buffer_unordered() returns results in completion order, which corrupts the
                // document-to-address mapping and causes data corruption (wrong field values).
                let docs: Vec<tantivy::schema::TantivyDocument> = futures::stream::iter(doc_futures)
                    .buffered(BASE_CONCURRENT_REQUESTS)
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok::<Vec<tantivy::schema::TantivyDocument>, anyhow::Error>(docs)
            }))
            .map_err(|_| anyhow::anyhow!("Document retrieval timed out after 10 seconds"))?
        })
    });

    match result {
        Some(Ok(docs)) => Ok(docs),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!(
            "Searcher context not found for pointer {}",
            searcher_ptr
        )),
    }
}

/// Check if the split has a parquet companion manifest
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeHasParquetManifest(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jni::sys::jboolean {
    if searcher_ptr == 0 {
        return 0;
    }

    let has_manifest = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.parquet_manifest.is_some()
    })
    .unwrap_or(false);

    if has_manifest { 1 } else { 0 }
}

/// Extract JNI int arrays into Rust Vec<u32> pairs.
/// Returns (segments, doc_ids) or an error string.
fn extract_jni_int_arrays(
    env: &mut JNIEnv,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
) -> Result<(Vec<u32>, Vec<u32>), String> {
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };

    let array_len = env
        .get_array_length(&segments_array)
        .map_err(|e| format!("Failed to get segments array length: {}", e))?
        as usize;

    let mut segments_vec = vec![0i32; array_len];
    env.get_int_array_region(&segments_array, 0, &mut segments_vec)
        .map_err(|e| format!("Failed to get segments array: {}", e))?;
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();

    let mut doc_ids_vec = vec![0i32; array_len];
    env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec)
        .map_err(|e| format!("Failed to get doc_ids array: {}", e))?;
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();

    Ok((segments_vec, doc_ids_vec))
}

/// Extract field names from a JNI String[] (jobjectArray).
/// Returns None if the array is null, Some(Vec<String>) otherwise.
fn extract_jni_field_names(
    env: &mut JNIEnv,
    field_names: jni::sys::jobjectArray,
) -> Option<Vec<String>> {
    if field_names.is_null() {
        return None;
    }
    let field_array = unsafe { jni::objects::JObjectArray::from_raw(field_names) };
    match env.get_array_length(&field_array) {
        Ok(len) => {
            let mut fields = Vec::with_capacity(len as usize);
            for i in 0..len {
                if let Ok(field_obj) = env.get_object_array_element(&field_array, i) {
                    if !field_obj.is_null() {
                        if let Ok(field_str) = env.get_string((&field_obj).into()) {
                            fields.push(String::from(field_str));
                        }
                    }
                }
            }
            Some(fields)
        }
        Err(_) => None,
    }
}

/// Parse type hint strings from a JNI String[] into a HashMap<String, DataType>.
///
/// Type hints are passed as alternating pairs: [fieldName, arrowType, fieldName, arrowType, ...].
/// Supported type strings: "i8", "i16", "i32", "i64", "u64", "f32", "f64", "bool", "utf8",
/// "date", "binary".
///
/// Returns None if the array is null or empty.
fn parse_type_hints_from_env(
    env: &mut JNIEnv,
    type_hint_strs: jni::sys::jobjectArray,
) -> anyhow::Result<Option<std::collections::HashMap<String, arrow_schema::DataType>>> {
    if type_hint_strs.is_null() {
        return Ok(None);
    }
    let hint_array = unsafe { jni::objects::JObjectArray::from_raw(type_hint_strs) };
    let len = env.get_array_length(&hint_array)
        .map_err(|e| anyhow::anyhow!("Failed to get type hints array length: {}", e))?;
    if len == 0 || len % 2 != 0 {
        return Ok(None);
    }

    let mut hints = std::collections::HashMap::new();
    let mut i = 0;
    while i < len {
        let name_obj = env.get_object_array_element(&hint_array, i)
            .map_err(|e| anyhow::anyhow!("Failed to get type hint name at {}: {}", i, e))?;
        let type_obj = env.get_object_array_element(&hint_array, i + 1)
            .map_err(|e| anyhow::anyhow!("Failed to get type hint type at {}: {}", i + 1, e))?;

        if !name_obj.is_null() && !type_obj.is_null() {
            let name: String = env.get_string((&name_obj).into())
                .map_err(|e| anyhow::anyhow!("Failed to convert type hint name: {}", e))?
                .into();
            let type_str: String = env.get_string((&type_obj).into())
                .map_err(|e| anyhow::anyhow!("Failed to convert type hint type: {}", e))?
                .into();

            if let Some(dt) = parse_arrow_type_string(&type_str) {
                hints.insert(name, dt);
            }
        }
        i += 2;
    }

    if hints.is_empty() {
        Ok(None)
    } else {
        Ok(Some(hints))
    }
}

/// Parse an Arrow type string to a DataType.
///
/// Supports scalar type names (e.g. "i32", "string") and JSON-formatted
/// complex types for Struct, List, and Map:
///
/// ```json
/// {"struct": {"name": "string", "age": "i32"}}
/// {"list": "string"}
/// {"list": {"struct": {"x": "f64", "y": "f64"}}}
/// {"map": ["string", "i64"]}
/// ```
fn parse_arrow_type_string(s: &str) -> Option<arrow_schema::DataType> {
    // Try JSON complex type first (starts with '{')
    if s.starts_with('{') {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
            return parse_complex_type_json(&json);
        }
    }

    // Scalar types
    match s {
        "i8" | "int8" | "byte" => Some(arrow_schema::DataType::Int8),
        "i16" | "int16" | "short" => Some(arrow_schema::DataType::Int16),
        "i32" | "int32" | "int" => Some(arrow_schema::DataType::Int32),
        "i64" | "int64" | "long" => Some(arrow_schema::DataType::Int64),
        "u64" | "uint64" => Some(arrow_schema::DataType::UInt64),
        "f32" | "float32" | "float" => Some(arrow_schema::DataType::Float32),
        "f64" | "float64" | "double" => Some(arrow_schema::DataType::Float64),
        "bool" | "boolean" => Some(arrow_schema::DataType::Boolean),
        "utf8" | "string" => Some(arrow_schema::DataType::Utf8),
        "date" | "timestamp" => Some(arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond, None,
        )),
        "date32" => Some(arrow_schema::DataType::Date32),
        "binary" | "bytes" => Some(arrow_schema::DataType::Binary),
        _ => None,
    }
}

/// Parse a JSON value into an Arrow DataType, supporting recursive complex types.
///
/// String values are parsed as scalar types. Object values describe complex types:
/// - `{"struct": {"field1": <type>, "field2": <type>, ...}}` → Struct
/// - `{"list": <element_type>}` → List
/// - `{"map": [<key_type>, <value_type>]}` → Map
///
/// Types can be nested arbitrarily:
/// `{"struct": {"tags": {"list": "string"}, "coords": {"struct": {"x": "f64", "y": "f64"}}}}`
/// Parse a JSON value into an Arrow DataType, supporting recursive complex types.
///
/// String values are parsed as scalar types. Object values describe complex types:
/// - `{"struct": [["field1", <type>], ["field2", <type>], ...]}` → Struct (order-preserving)
/// - `{"list": <element_type>}` → List
/// - `{"map": [<key_type>, <value_type>]}` → Map
///
/// Struct fields use an array-of-pairs format to guarantee ordering. JSON objects
/// are unordered by spec, and serde_json uses BTreeMap (alphabetical sort). Spark
/// reads struct children by ordinal position, so ordering must be preserved.
///
/// Types can be nested arbitrarily:
/// `{"struct": [["tags", {"list": "string"}], ["name", "string"]]}`
fn parse_complex_type_json(v: &serde_json::Value) -> Option<arrow_schema::DataType> {
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    match v {
        serde_json::Value::String(s) => parse_arrow_type_string(s),
        serde_json::Value::Object(map) => {
            if let Some(fields_val) = map.get("struct") {
                // Struct: {"struct": [["name", "string"], ["age", "i32"]]}
                // Array-of-pairs format guarantees field ordering.
                let fields_arr = fields_val.as_array()?;
                let mut fields = Vec::with_capacity(fields_arr.len());
                for pair in fields_arr {
                    let pair_arr = pair.as_array()?;
                    if pair_arr.len() != 2 {
                        return None;
                    }
                    let name = pair_arr[0].as_str()?;
                    let dt = parse_complex_type_json(&pair_arr[1])?;
                    fields.push(Arc::new(Field::new(name, dt, true)));
                }
                Some(DataType::Struct(fields.into()))
            } else if let Some(elem_val) = map.get("list") {
                // List: {"list": "string"} or {"list": {"struct": [...]}}
                let elem_type = parse_complex_type_json(elem_val)?;
                Some(DataType::List(Arc::new(Field::new("item", elem_type, true))))
            } else if let Some(kv_val) = map.get("map") {
                // Map: {"map": ["string", "i64"]}
                let kv_arr = kv_val.as_array()?;
                if kv_arr.len() != 2 {
                    return None;
                }
                let key_type = parse_complex_type_json(&kv_arr[0])?;
                let val_type = parse_complex_type_json(&kv_arr[1])?;
                let entries_field = Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key_type, false)),
                            Arc::new(Field::new("value", val_type, true)),
                        ]
                        .into(),
                    ),
                    false,
                );
                Some(DataType::Map(Arc::new(entries_field), false))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Resolve doc addresses to parquet file groups via fast fields.
///
/// This is the shared pipeline used by both nativeDocBatchProjected (TANT) and
/// nativeDocBatchArrowFfi (Arrow FFI). It:
/// 1. Lazy-loads __pq fast field columns for all needed segments
/// 2. Resolves (seg, doc) → (file_hash, row_in_file) via O(1) fast field lookups
/// 3. Groups by parquet file, sorted by row_in_file for sequential access
///
/// Returns the groups HashMap and the count of addresses, or an error.
async fn resolve_doc_addresses_to_groups(
    ctx: &Arc<CachedSearcherContext>,
    addresses: &[(u32, u32)],
) -> anyhow::Result<std::collections::HashMap<usize, Vec<(usize, u64)>>> {
    // Lazy-load __pq fast field data for all segments we need.
    let t_pq_load = std::time::Instant::now();
    let unique_segments: std::collections::HashSet<u32> =
        addresses.iter().map(|&(seg, _)| seg).collect();
    for &seg in &unique_segments {
        ctx.ensure_pq_segment_loaded(seg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load __pq fields for seg {}: {}", seg, e))?;
    }
    perf_println!(
        "⏱️ PROJ_DIAG: ensure_pq_segment_loaded({} segments) took {}ms",
        unique_segments.len(),
        t_pq_load.elapsed().as_millis()
    );

    // Resolve all addresses via pre-loaded __pq data
    let t_resolve = std::time::Instant::now();
    let mut resolved_locations: Vec<(usize, u64, u64)> = Vec::with_capacity(addresses.len());
    for (idx, &(seg_ord, doc_id)) in addresses.iter().enumerate() {
        let (file_hash, row_in_file) = ctx.get_pq_location(seg_ord, doc_id)?;
        resolved_locations.push((idx, file_hash, row_in_file));
    }
    perf_println!(
        "⏱️ PROJ_DIAG: get_pq_location({} docs) took {}ms",
        addresses.len(),
        t_resolve.elapsed().as_millis()
    );

    let t_group = std::time::Instant::now();
    let groups = crate::parquet_companion::docid_mapping::group_resolved_locations_by_file(
        &resolved_locations,
        &ctx.parquet_file_hash_index,
    )
    .map_err(|e| anyhow::anyhow!("{}", e))?;
    perf_println!(
        "⏱️ PROJ_DIAG: group_resolved_locations_by_file → {} file groups, took {}ms",
        groups.len(),
        t_group.elapsed().as_millis()
    );

    Ok(groups)
}

/// Validate and get parquet storage from context.
/// Returns an error with helpful diagnostics if storage is unavailable.
fn get_parquet_storage(
    ctx: &CachedSearcherContext,
) -> anyhow::Result<Arc<dyn quickwit_storage::Storage>> {
    match ctx.parquet_storage.as_ref() {
        Some(s) => Ok(s.clone()),
        None => {
            let reason = if ctx.parquet_table_root.is_none() {
                "parquet_table_root was not set. Pass the table root path to createSplitSearcher() \
                 or configure it via CacheConfig.withParquetTableRoot()."
            } else {
                "parquet storage creation failed (likely bad credentials or unreachable endpoint). \
                 Enable TANTIVY4JAVA_DEBUG=1 and check stderr for the storage creation error."
            };
            Err(anyhow::anyhow!(
                "Parquet companion batch retrieval failed: {}",
                reason
            ))
        }
    }
}

/// Batch projected document retrieval from parquet companion, returning a byte buffer.
/// Returns a JSON array of documents as a byte array for efficient JNI boundary crossing.
/// Format: UTF-8 encoded JSON string: [{"field":"value",...}, {"field":"value",...}, ...]
///
/// If the split has no parquet manifest, falls back to standard bulk retrieval.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeDocBatchProjected(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
    field_names: jni::sys::jobjectArray,
) -> jbyteArray {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Check if this split has a parquet manifest
    let has_manifest = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.parquet_manifest.is_some()
    })
    .unwrap_or(false);

    if !has_manifest {
        // No parquet manifest - fall back to standard bulk retrieval
        return Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative(
            env, _class, searcher_ptr, segments, doc_ids,
        );
    }

    // Wrap core logic with convert_throwable to catch panics at the JNI boundary.
    // Panics in async parquet I/O or Arrow compute would otherwise cross JNI → UB.
    use crate::utils::convert_throwable;
    convert_throwable(&mut env, |env| {
        // Extract JNI arrays
        let (segments_vec, doc_ids_vec) = extract_jni_int_arrays(env, segments, doc_ids)
            .map_err(|msg| anyhow::anyhow!("{}", msg))?;

        // Extract field names
        let projected_fields = extract_jni_field_names(env, field_names);

        let t_jni_total = std::time::Instant::now();
        perf_println!(
            "⏱️ PROJ_DIAG: === nativeDocBatchProjected START === {} docs, fields={:?}",
            segments_vec.len(), projected_fields
        );

        // Build doc addresses
        let addresses: Vec<(u32, u32)> = segments_vec
            .iter()
            .zip(doc_ids_vec.iter())
            .map(|(&seg, &doc)| (seg, doc))
            .collect();

        // Batch retrieve: resolve all doc addresses via fast fields, then group by file
        let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            let manifest = ctx.parquet_manifest.as_ref().unwrap();
            let storage = get_parquet_storage(ctx)?;
            let metadata_cache = &ctx.parquet_metadata_cache;
            let byte_cache = &ctx.parquet_byte_range_cache;

            let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
            let _guard = runtime.enter();

            tokio::task::block_in_place(|| {
                runtime.block_on(async {
                    let groups = resolve_doc_addresses_to_groups(ctx, &addresses).await?;

                    let t_parquet = std::time::Instant::now();
                    let result = crate::parquet_companion::arrow_to_tant::batch_parquet_to_tant_buffer_by_groups(
                        groups,
                        addresses.len(),
                        projected_fields.as_deref(),
                        manifest,
                        &storage,
                        Some(metadata_cache),
                        Some(byte_cache),
                        ctx.parquet_coalesce_config,
                    )
                    .await;
                    perf_println!(
                        "⏱️ PROJ_DIAG: batch_parquet_to_tant_buffer_by_groups took {}ms",
                        t_parquet.elapsed().as_millis()
                    );
                    result
                })
            })
        });

        match result {
            Some(Ok(tant_bytes)) => {
                let t_jni_copy = std::time::Instant::now();
                perf_println!(
                    "⏱️ PROJ_DIAG: serialized {} TANT bytes, copying to JNI byte array",
                    tant_bytes.len()
                );
                let byte_array = env.new_byte_array(tant_bytes.len() as i32)
                    .map_err(|e| anyhow::anyhow!("Failed to create byte array: {}", e))?;
                let byte_slice: &[i8] = unsafe {
                    std::slice::from_raw_parts(tant_bytes.as_ptr() as *const i8, tant_bytes.len())
                };
                env.set_byte_array_region(&byte_array, 0, byte_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to set byte array data: {}", e))?;
                perf_println!(
                    "⏱️ PROJ_DIAG: JNI byte array copy took {}ms",
                    t_jni_copy.elapsed().as_millis()
                );
                perf_println!(
                    "⏱️ PROJ_DIAG: === nativeDocBatchProjected TOTAL took {}ms ===",
                    t_jni_total.elapsed().as_millis()
                );
                Ok(byte_array.into_raw())
            }
            Some(Err(e)) => {
                perf_println!(
                    "⏱️ PROJ_DIAG: === nativeDocBatchProjected FAILED after {}ms: {} ===",
                    t_jni_total.elapsed().as_millis(), e
                );
                Err(anyhow::anyhow!("Parquet batch retrieval failed: {}", e))
            }
            None => Err(anyhow::anyhow!("Searcher context not found")),
        }
    }).unwrap_or(std::ptr::null_mut())
}

/// Arrow FFI document batch retrieval from parquet companion mode.
///
/// Instead of serializing to TANT binary format, this exports Arrow columnar data
/// directly to pre-allocated FFI_ArrowArray/FFI_ArrowSchema C structs. The Java caller
/// passes memory addresses of these structs, and the native layer writes column data
/// to them with zero-copy semantics.
///
/// Returns the number of rows written, or -1 if Arrow FFI is not supported (no parquet
/// manifest — the caller should use the TANT path instead).
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeDocBatchArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
    field_names: jni::sys::jobjectArray,
    array_addrs: jni::sys::jlongArray,
    schema_addrs: jni::sys::jlongArray,
) -> jint {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return -1;
    }

    // Check if this split has a parquet manifest (Arrow FFI only works for companion splits)
    let has_manifest = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.parquet_manifest.is_some()
    })
    .unwrap_or(false);

    if !has_manifest {
        // No parquet manifest — signal caller to fall back to TANT path
        return -1;
    }

    // Wrap core logic with convert_throwable to catch panics at the JNI boundary.
    // Panics in async parquet I/O, Arrow compute, or FFI export would otherwise cross JNI → UB.
    use crate::utils::convert_throwable;
    convert_throwable(&mut env, |env| {
        // Extract segments and doc_ids
        let (segments_vec, doc_ids_vec) = extract_jni_int_arrays(env, segments, doc_ids)
            .map_err(|msg| anyhow::anyhow!("{}", msg))?;

        // Extract field names
        let projected_fields = extract_jni_field_names(env, field_names);

        // Extract array_addrs and schema_addrs (long[] → Vec<i64>)
        let array_addrs_jni = unsafe { jni::objects::JLongArray::from_raw(array_addrs) };
        let schema_addrs_jni = unsafe { jni::objects::JLongArray::from_raw(schema_addrs) };

        let addrs_len = env.get_array_length(&array_addrs_jni)
            .map_err(|e| anyhow::anyhow!("Failed to get array_addrs length: {}", e))? as usize;

        let mut array_addrs_vec = vec![0i64; addrs_len];
        env.get_long_array_region(&array_addrs_jni, 0, &mut array_addrs_vec)
            .map_err(|e| anyhow::anyhow!("Failed to get array_addrs: {}", e))?;

        let mut schema_addrs_vec = vec![0i64; addrs_len];
        env.get_long_array_region(&schema_addrs_jni, 0, &mut schema_addrs_vec)
            .map_err(|e| anyhow::anyhow!("Failed to get schema_addrs: {}", e))?;

        let t_jni_total = std::time::Instant::now();
        perf_println!(
            "⏱️ FFI_DIAG: === nativeDocBatchArrowFfi START === {} docs, {} columns, fields={:?}",
            segments_vec.len(), addrs_len, projected_fields
        );

        // Build doc addresses
        let addresses: Vec<(u32, u32)> = segments_vec
            .iter()
            .zip(doc_ids_vec.iter())
            .map(|(&seg, &doc)| (seg, doc))
            .collect();

        // Resolve addresses, read parquet, export via FFI
        let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            let manifest = ctx.parquet_manifest.as_ref().unwrap();
            let storage = get_parquet_storage(ctx)?;
            let metadata_cache = &ctx.parquet_metadata_cache;
            let byte_cache = &ctx.parquet_byte_range_cache;

            let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
            let _guard = runtime.enter();

            tokio::task::block_in_place(|| {
                runtime.block_on(async {
                    let groups = resolve_doc_addresses_to_groups(ctx, &addresses).await?;

                    let t_ffi = std::time::Instant::now();
                    let row_count =
                        crate::parquet_companion::arrow_ffi_export::batch_parquet_to_arrow_ffi(
                            groups,
                            addresses.len(),
                            projected_fields.as_deref(),
                            manifest,
                            &storage,
                            Some(metadata_cache),
                            Some(byte_cache),
                            ctx.parquet_coalesce_config,
                            &array_addrs_vec,
                            &schema_addrs_vec,
                        )
                        .await?;
                    perf_println!(
                        "⏱️ FFI_DIAG: batch_parquet_to_arrow_ffi returned {} rows, took {}ms",
                        row_count, t_ffi.elapsed().as_millis()
                    );
                    Ok::<usize, anyhow::Error>(row_count)
                })
            })
        });

        match result {
            Some(Ok(row_count)) => {
                perf_println!(
                    "⏱️ FFI_DIAG: === nativeDocBatchArrowFfi TOTAL took {}ms, {} rows ===",
                    t_jni_total.elapsed().as_millis(), row_count
                );
                Ok(row_count as jint)
            }
            Some(Err(e)) => {
                perf_println!(
                    "⏱️ FFI_DIAG: === nativeDocBatchArrowFfi FAILED after {}ms: {} ===",
                    t_jni_total.elapsed().as_millis(), e
                );
                Err(anyhow::anyhow!("Arrow FFI batch retrieval failed: {}", e))
            }
            None => Err(anyhow::anyhow!("Searcher context not found")),
        }
    }).unwrap_or(-1)
}

// =====================================================================
// Streaming Retrieval Session JNI Methods
// =====================================================================

/// Start a streaming bulk retrieval session for companion mode.
///
/// Performs the search + resolve phases synchronously, then starts a background
/// producer task that streams batches through a bounded channel.
///
/// Returns a session handle (jlong) that Java uses to poll batches via
/// nativeNextBatch. Returns 0 on error (exception thrown) or if not a companion split.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeStartStreamingRetrieval(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: jni::objects::JString,
    field_names: jni::sys::jobjectArray,
    type_hint_strs: jni::sys::jobjectArray,
    max_docs: jni::sys::jint,
) -> jlong {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return 0;
    }

    // Check if this split has a parquet manifest (companion vs regular split)
    let has_manifest = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.parquet_manifest.is_some()
    })
    .unwrap_or(false);

    use crate::utils::convert_throwable;
    convert_throwable(&mut env, |env| {
        // Extract query string
        let query_json: String = env
            .get_string(&query_ast_json)
            .map_err(|e| anyhow::anyhow!("Failed to get query string: {}", e))?
            .into();

        // Extract field names
        let projected_fields = extract_jni_field_names(env, field_names);

        // Parse type hints (alternating pairs: [fieldName, arrowType, ...])
        let type_hints = parse_type_hints_from_env(env, type_hint_strs)?;

        let t_total = std::time::Instant::now();
        perf_println!(
            "⏱️ STREAMING_JNI: === nativeStartStreamingRetrieval START === companion={} fields={:?}",
            has_manifest, projected_fields
        );

        let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
            let _guard = runtime.enter();

            tokio::task::block_in_place(|| {
                runtime.block_on(async {
                    // Phase 1: No-score search (works for both companion and regular splits)
                    let mut doc_ids = crate::split_searcher::bulk_retrieval::perform_bulk_search(
                        ctx, &query_json,
                    ).await?;

                    perf_println!("⏱️ STREAMING_JNI: search found {} docs", doc_ids.len());

                    // Apply maxDocs limit to avoid unnecessary S3 prefetch/retrieval
                    if max_docs > 0 && (doc_ids.len() as i32) > max_docs {
                        perf_println!(
                            "⏱️ STREAMING_JNI: truncating {} docs to maxDocs={}",
                            doc_ids.len(), max_docs
                        );
                        doc_ids.truncate(max_docs as usize);
                    }

                    if has_manifest {
                        // === COMPANION PATH: parquet-based streaming ===
                        if doc_ids.is_empty() {
                            perf_println!("⏱️ STREAMING_JNI: no matches — returning empty session (companion)");
                            let (_, rx) = tokio::sync::mpsc::channel::<anyhow::Result<arrow_array::RecordBatch>>(1);
                            let manifest = ctx.parquet_manifest.as_ref().unwrap();
                            let schema = crate::parquet_companion::streaming_ffi::build_tantivy_schema_pub(
                                manifest, projected_fields.as_deref(),
                            )?;
                            let session = crate::parquet_companion::streaming_ffi::StreamingRetrievalSession::new_empty(
                                rx, schema,
                            );
                            let handle = crate::utils::arc_to_jlong(std::sync::Arc::new(std::sync::Mutex::new(session)));
                            return Ok::<jlong, anyhow::Error>(handle);
                        }

                        // Phase 2: Resolve to parquet locations
                        let groups = crate::split_searcher::bulk_retrieval::resolve_to_parquet_locations(
                            ctx, &doc_ids,
                        ).await?;

                        perf_println!(
                            "⏱️ STREAMING_JNI: resolved to {} file groups (companion)",
                            groups.len()
                        );

                        // Phase 3: Start companion streaming producer
                        let manifest = ctx.parquet_manifest.as_ref().unwrap().clone();
                        let storage = crate::split_searcher::bulk_retrieval::get_parquet_storage(ctx)?;

                        let session = crate::parquet_companion::streaming_ffi::start_streaming_retrieval(
                            groups,
                            projected_fields,
                            manifest,
                            storage,
                            Some(ctx.parquet_metadata_cache.clone()),
                            Some(ctx.parquet_byte_range_cache.clone()),
                            ctx.parquet_coalesce_config,
                        )?;

                        let handle = crate::utils::arc_to_jlong(std::sync::Arc::new(std::sync::Mutex::new(session)));
                        Ok(handle)
                    } else {
                        // === REGULAR PATH: tantivy doc store streaming ===
                        let session = crate::split_searcher::streaming_doc_retrieval::start_tantivy_streaming_retrieval(
                            ctx, doc_ids, projected_fields, type_hints,
                        ).await?;

                        let handle = crate::utils::arc_to_jlong(std::sync::Arc::new(std::sync::Mutex::new(session)));
                        Ok(handle)
                    }
                })
            })
        });

        match result {
            Some(Ok(handle)) => {
                perf_println!(
                    "⏱️ STREAMING_JNI: === session started, took {}ms ===",
                    t_total.elapsed().as_millis()
                );
                Ok(handle)
            }
            Some(Err(e)) => {
                perf_println!(
                    "⏱️ STREAMING_JNI: === FAILED after {}ms: {} ===",
                    t_total.elapsed().as_millis(), e
                );
                Err(anyhow::anyhow!("Failed to start streaming retrieval: {}", e))
            }
            None => Err(anyhow::anyhow!("Searcher context not found")),
        }
    })
    .unwrap_or(0)
}

/// Poll the next batch from a streaming session.
///
/// Writes Arrow FFI data to the provided addresses.
/// Returns: >0 = row count, 0 = end of stream, -1 = error.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeNextBatch(
    mut env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
    array_addrs: jni::sys::jlongArray,
    schema_addrs: jni::sys::jlongArray,
) -> jint {
    if session_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid session pointer"));
        return -1;
    }

    use crate::utils::convert_throwable;
    convert_throwable(&mut env, |env| {
        // Look up session in registry (safe — no raw pointer dereference)
        let session_arc = crate::utils::jlong_to_arc::<std::sync::Mutex<
            crate::parquet_companion::streaming_ffi::StreamingRetrievalSession,
        >>(session_ptr)
        .ok_or_else(|| anyhow::anyhow!("Streaming session not found or already closed (handle={})", session_ptr))?;

        // Extract FFI addresses
        let array_addrs_jni = unsafe { jni::objects::JLongArray::from_raw(array_addrs) };
        let schema_addrs_jni = unsafe { jni::objects::JLongArray::from_raw(schema_addrs) };

        let addrs_len = env
            .get_array_length(&array_addrs_jni)
            .map_err(|e| anyhow::anyhow!("Failed to get array_addrs length: {}", e))?
            as usize;

        let mut array_addrs_vec = vec![0i64; addrs_len];
        env.get_long_array_region(&array_addrs_jni, 0, &mut array_addrs_vec)
            .map_err(|e| anyhow::anyhow!("Failed to get array_addrs: {}", e))?;

        let mut schema_addrs_vec = vec![0i64; addrs_len];
        env.get_long_array_region(&schema_addrs_jni, 0, &mut schema_addrs_vec)
            .map_err(|e| anyhow::anyhow!("Failed to get schema_addrs: {}", e))?;

        // Poll next batch from channel (blocking)
        let mut session = session_arc.lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock streaming session: {}", e))?;
        match session.blocking_next() {
            Some(Ok(batch)) => {
                let row_count = crate::parquet_companion::streaming_ffi::write_batch_to_ffi(
                    &batch,
                    &array_addrs_vec,
                    &schema_addrs_vec,
                )?;
                perf_println!("⏱️ STREAMING_JNI: nextBatch returned {} rows", row_count);
                Ok(row_count as jint)
            }
            Some(Err(e)) => {
                perf_println!("⏱️ STREAMING_JNI: nextBatch error: {}", e);
                Err(anyhow::anyhow!("Streaming batch read failed: {}", e))
            }
            None => {
                perf_println!("⏱️ STREAMING_JNI: end of stream");
                Ok(0) // End of stream
            }
        }
    })
    .unwrap_or(-1)
}

/// Close and free a streaming session.
///
/// Safe to call multiple times — second call is a no-op (registry entry already removed).
/// Must be called to release native resources and stop the producer task.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeCloseStreamingSession(
    _env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) {
    if session_ptr != 0 {
        perf_println!("⏱️ STREAMING_JNI: closing session (handle={})", session_ptr);
        crate::utils::release_arc(session_ptr);
    }
}

/// Get the number of columns in a streaming session's output schema.
/// Used by Java to allocate the correct number of FFI address arrays.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeGetStreamingColumnCount(
    mut env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) -> jint {
    if session_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid session pointer"));
        return -1;
    }

    let session_arc = crate::utils::jlong_to_arc::<std::sync::Mutex<
        crate::parquet_companion::streaming_ffi::StreamingRetrievalSession,
    >>(session_ptr);

    match session_arc {
        Some(arc) => {
            match arc.lock() {
                Ok(session) => session.num_columns() as jint,
                Err(_) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to lock streaming session"));
                    -1
                }
            }
        }
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Streaming session not found or already closed"));
            -1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn test_parse_scalar_types() {
        assert_eq!(parse_arrow_type_string("i32"), Some(DataType::Int32));
        assert_eq!(parse_arrow_type_string("string"), Some(DataType::Utf8));
        assert_eq!(parse_arrow_type_string("bool"), Some(DataType::Boolean));
        assert_eq!(parse_arrow_type_string("f64"), Some(DataType::Float64));
        assert_eq!(parse_arrow_type_string("date32"), Some(DataType::Date32));
        assert_eq!(parse_arrow_type_string("unknown"), None);
    }

    #[test]
    fn test_parse_struct_type_hint() {
        let json = r#"{"struct": [["name", "string"], ["age", "i64"]]}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "name");
                assert_eq!(*fields[0].data_type(), DataType::Utf8);
                assert_eq!(fields[1].name(), "age");
                assert_eq!(*fields[1].data_type(), DataType::Int64);
            }
            _ => panic!("Expected Struct, got {:?}", dt),
        }
    }

    #[test]
    fn test_parse_list_type_hint() {
        let json = r#"{"list": "string"}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::List(inner) => {
                assert_eq!(*inner.data_type(), DataType::Utf8);
            }
            _ => panic!("Expected List, got {:?}", dt),
        }
    }

    #[test]
    fn test_parse_map_type_hint() {
        let json = r#"{"map": ["string", "i64"]}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::Map(entries, sorted) => {
                assert!(!sorted);
                match entries.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(*fields[0].data_type(), DataType::Utf8);
                        assert_eq!(*fields[1].data_type(), DataType::Int64);
                    }
                    _ => panic!("Expected Struct entries"),
                }
            }
            _ => panic!("Expected Map, got {:?}", dt),
        }
    }

    #[test]
    fn test_parse_nested_complex_type() {
        // List of structs
        let json = r#"{"list": {"struct": [["x", "f64"], ["y", "f64"]]}}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::List(inner) => {
                match inner.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                        assert_eq!(fields[0].name(), "x");
                        assert_eq!(fields[1].name(), "y");
                    }
                    _ => panic!("Expected Struct inside List"),
                }
            }
            _ => panic!("Expected List, got {:?}", dt),
        }
    }

    /// Struct field ordering matches the array-of-pairs order.
    /// Spark reads struct children by ordinal position, so ordering is critical.
    #[test]
    fn test_struct_field_ordering_preserved() {
        // "name" before "address" before "city" — would be wrong if sorted alphabetically
        let json = r#"{"struct": [["name", "string"], ["address", "string"], ["city", "string"]]}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name(), "name");
                assert_eq!(fields[1].name(), "address");
                assert_eq!(fields[2].name(), "city");
            }
            _ => panic!("Expected Struct, got {:?}", dt),
        }
    }

    /// Alphabetically-first field appears last — confirms array order, not sort order.
    #[test]
    fn test_struct_field_ordering_alpha_last() {
        let json = r#"{"struct": [["z_last", "i64"], ["a_first", "string"], ["m_middle", "f64"]]}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name(), "z_last");
                assert_eq!(*fields[0].data_type(), DataType::Int64);
                assert_eq!(fields[1].name(), "a_first");
                assert_eq!(*fields[1].data_type(), DataType::Utf8);
                assert_eq!(fields[2].name(), "m_middle");
                assert_eq!(*fields[2].data_type(), DataType::Float64);
            }
            _ => panic!("Expected Struct, got {:?}", dt),
        }
    }

    /// Nested struct also preserves field ordering.
    #[test]
    fn test_nested_struct_field_ordering() {
        let json = r#"{"struct": [["scores", {"list": "i64"}], ["name", "string"], ["address", {"struct": [["zip", "string"], ["city", "string"]]}]]}"#;
        let dt = parse_arrow_type_string(json).unwrap();
        match &dt {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name(), "scores");
                assert_eq!(fields[1].name(), "name");
                assert_eq!(fields[2].name(), "address");
                match fields[2].data_type() {
                    DataType::Struct(inner) => {
                        assert_eq!(inner[0].name(), "zip");
                        assert_eq!(inner[1].name(), "city");
                    }
                    _ => panic!("Expected nested Struct"),
                }
            }
            _ => panic!("Expected Struct, got {:?}", dt),
        }
    }
}
