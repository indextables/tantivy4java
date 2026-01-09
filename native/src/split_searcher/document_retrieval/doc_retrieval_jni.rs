// doc_retrieval_jni.rs - JNI entry points for document retrieval
// Extracted from document_retrieval.rs during refactoring

use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;

use crate::common::to_java_exception;
use crate::debug_println;
use crate::runtime_manager::block_on_operation;
use crate::split_searcher::async_impl::perform_doc_retrieval_async_impl_thread_safe;

use super::batch_doc_retrieval::retrieve_documents_batch_from_split_optimized;

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
