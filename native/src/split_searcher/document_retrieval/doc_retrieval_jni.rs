// doc_retrieval_jni.rs - JNI entry points for document retrieval
// Extracted from document_retrieval.rs during refactoring

use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jbyteArray, jint, jlong, jobject};
use jni::JNIEnv;
use std::sync::Arc;

use crate::common::to_java_exception;
use crate::debug_println;
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

                // Retrieve documents concurrently
                let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                    let moved_searcher = cached_searcher.clone();
                    async move {
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

/// Projected document retrieval from parquet companion.
/// Returns a JSON string of { field_name: value, ... } for the requested fields.
/// If the split has no parquet manifest, falls back to standard retrieval.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeDocProjected(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segment_ord: jint,
    doc_id: jint,
    field_names: jni::sys::jobjectArray,
) -> jobject {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Extract field names from Java String array
    let projected_fields: Option<Vec<String>> = if field_names.is_null() {
        None // null means retrieve all fields
    } else {
        let field_array = unsafe { jni::objects::JObjectArray::from_raw(field_names) };
        match env.get_array_length(&field_array) {
            Ok(len) => {
                let mut fields = Vec::with_capacity(len as usize);
                for i in 0..len {
                    match env.get_object_array_element(&field_array, i) {
                        Ok(field_obj) => {
                            if !field_obj.is_null() {
                                match env.get_string((&field_obj).into()) {
                                    Ok(field_str) => fields.push(String::from(field_str)),
                                    Err(e) => {
                                        debug_println!("⚠️ PARQUET_DOC: Failed to get field name string at index {}: {}", i, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug_println!("⚠️ PARQUET_DOC: Failed to get field name at index {}: {}", i, e);
                        }
                    }
                }
                Some(fields)
            }
            Err(e) => {
                to_java_exception(&mut env, &anyhow::anyhow!("Failed to get field names array length: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    debug_println!(
        "📖 PARQUET_DOC: nativeDocProjected called - ptr:{}, seg:{}, doc:{}, fields:{:?}",
        searcher_ptr, segment_ord, doc_id, projected_fields
    );

    // Check if this split has a parquet manifest
    let has_manifest = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        ctx.parquet_manifest.is_some()
    })
    .unwrap_or(false);

    if !has_manifest {
        // No parquet manifest - fall back to standard doc retrieval
        debug_println!("📖 PARQUET_DOC: No parquet manifest, falling back to standard docNative");
        return Java_io_indextables_tantivy4java_split_SplitSearcher_docNative(
            env, _class, searcher_ptr, segment_ord, doc_id,
        );
    }

    // Retrieve document from parquet via the manifest
    let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        let manifest = ctx.parquet_manifest.as_ref().unwrap();
        // Use parquet_storage (rooted at table_root) for parquet file access
        let storage = ctx.parquet_storage.as_ref()
            .cloned()
            .unwrap_or_else(|| ctx.cached_storage.clone());
        let metadata_cache = &ctx.parquet_metadata_cache;
        let byte_cache = &ctx.parquet_byte_range_cache;

        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let _guard = runtime.enter();

        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                crate::parquet_companion::doc_retrieval::retrieve_document_from_parquet(
                    manifest,
                    segment_ord as u32,
                    doc_id as u32,
                    projected_fields.as_deref(),
                    &storage,
                    Some(metadata_cache),
                    Some(byte_cache),
                )
                .await
            })
        })
    });

    match result {
        Some(Ok(field_map)) => {
            // Convert HashMap to JSON string for Java
            match serde_json::to_string(&field_map) {
                Ok(json_str) => {
                    debug_println!("📖 PARQUET_DOC: Retrieved {} fields from parquet", field_map.len());
                    match env.new_string(&json_str) {
                        Ok(java_str) => java_str.into_raw(),
                        Err(e) => {
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Java string: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                }
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to serialize parquet document: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Some(Err(e)) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Parquet document retrieval failed: {}", e));
            std::ptr::null_mut()
        }
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Searcher context not found"));
            std::ptr::null_mut()
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
        // No parquet manifest - fall back to standard bulk retrieval with field filtering
        // Fall back to standard bulk retrieval
        return Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative(
            env, _class, searcher_ptr, segments, doc_ids,
        );
    }

    // Extract JNI arrays
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };

    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array length: {}", e));
            return std::ptr::null_mut();
        }
    };

    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array: {}", e));
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();

    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get doc_ids array: {}", e));
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();

    // Extract field names
    let projected_fields: Option<Vec<String>> = if field_names.is_null() {
        None
    } else {
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
    };

    debug_println!(
        "📖 PARQUET_BATCH: nativeDocBatchProjected called - {} docs, fields={:?}",
        array_len, projected_fields
    );

    // Build doc addresses
    let addresses: Vec<(u32, u32)> = segments_vec
        .iter()
        .zip(doc_ids_vec.iter())
        .map(|(&seg, &doc)| (seg, doc))
        .collect();

    // Batch retrieve from parquet
    let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        let manifest = ctx.parquet_manifest.as_ref().unwrap();
        // Use parquet_storage (rooted at table_root) for parquet file access
        let storage = ctx.parquet_storage.as_ref()
            .cloned()
            .unwrap_or_else(|| ctx.cached_storage.clone());
        let metadata_cache = &ctx.parquet_metadata_cache;
        let byte_cache = &ctx.parquet_byte_range_cache;

        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let _guard = runtime.enter();

        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                crate::parquet_companion::doc_retrieval::batch_retrieve_from_parquet(
                    &addresses,
                    projected_fields.as_deref(),
                    manifest,
                    &storage,
                    Some(metadata_cache),
                    Some(byte_cache),
                )
                .await
            })
        })
    });

    match result {
        Some(Ok(documents)) => {
            // Serialize as JSON array for efficient JNI crossing
            match serde_json::to_vec(&documents) {
                Ok(json_bytes) => {
                    debug_println!(
                        "📖 PARQUET_BATCH: Retrieved {} docs, serialized to {} bytes",
                        documents.len(), json_bytes.len()
                    );
                    match env.new_byte_array(json_bytes.len() as i32) {
                        Ok(byte_array) => {
                            let byte_slice: &[i8] = unsafe {
                                std::slice::from_raw_parts(json_bytes.as_ptr() as *const i8, json_bytes.len())
                            };
                            if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                                to_java_exception(&mut env, &anyhow::anyhow!("Failed to set byte array data: {}", e));
                                return std::ptr::null_mut();
                            }
                            byte_array.into_raw()
                        }
                        Err(e) => {
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create byte array: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                }
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to serialize parquet documents: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Some(Err(e)) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Parquet batch retrieval failed: {}", e));
            std::ptr::null_mut()
        }
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Searcher context not found"));
            std::ptr::null_mut()
        }
    }
}
