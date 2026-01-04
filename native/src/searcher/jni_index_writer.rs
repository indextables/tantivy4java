// jni_index_writer.rs - JNI methods for IndexWriter class
// Extracted from mod.rs during refactoring
// Contains: nativeAddDocument, nativeAddJson, nativeCommit, nativeRollback,
//           nativeDeleteDocumentsByTerm, nativeDeleteDocumentsByQuery, nativeMerge, etc.

use jni::objects::{JClass, JString, JObject, JByteBuffer};
use jni::sys::jlong;
use jni::JNIEnv;

use tantivy::IndexWriter as TantivyIndexWriter;
use tantivy::schema::Schema;
use tantivy::query::Query as TantivyQuery;
use tantivy::index::SegmentId;
use std::sync::{Arc, Mutex};

use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::document::{DocumentWrapper, DocumentBuilder};
use super::batch_parsing::parse_batch_buffer;
use super::jni_searcher::extract_segment_ids;
use super::convert_jobject_to_term;

// IndexWriter native methods
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeAddDocument(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    doc_ptr: jlong,
) -> jlong {
    // First, clone the DocumentBuilder to avoid nested locks
    let doc_builder_clone = match with_arc_safe::<Mutex<DocumentWrapper>, Option<DocumentBuilder>>(doc_ptr, |wrapper_mutex| {
        let doc_wrapper = wrapper_mutex.lock().unwrap();
        match &*doc_wrapper {
            DocumentWrapper::Builder(doc_builder) => Some(doc_builder.clone()),
            DocumentWrapper::Retrieved(_) => None,
        }
    }) {
        Some(Some(doc)) => doc,
        _ => {
            handle_error(&mut env, "Invalid Document pointer or document is not in builder state");
            return 0;
        }
    };

    // Now get the schema and build the document, then add it
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        // Get schema from the writer
        let schema = writer.index().schema();
        let document = doc_builder_clone.build(&schema).map_err(|e| e.to_string())?;
        writer.add_document(document).map_err(|e| e.to_string())
    });

    match result {
        Some(Ok(opstamp)) => {
            opstamp as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Document pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeAddJson(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    json: JString,
) -> jlong {
    let json_str: String = match env.get_string(&json) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON string");
            return 0;
        }
    };

    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        // Parse JSON and add document
        let schema = writer.index().schema();
        let document = match tantivy::schema::TantivyDocument::parse_json(&schema, &json_str) {
            Ok(doc) => doc,
            Err(e) => return Err(format!("Failed to parse JSON: {}", e)),
        };

        writer.add_document(document).map_err(|e| e.to_string())
    });

    match result {
        Some(Ok(opstamp)) => opstamp as jlong,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeAddDocumentsByBuffer(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    buffer: JByteBuffer,
) -> jni::sys::jlongArray {
    // Parse the buffer according to the batch protocol
    let result = parse_batch_buffer(&mut env, ptr, buffer);

    match result {
        Ok(opstamps) => {
            // Convert Vec<u64> to Java long array
            let jlong_array = match env.new_long_array(opstamps.len() as i32) {
                Ok(arr) => arr,
                Err(_) => {
                    handle_error(&mut env, "Failed to create result array");
                    return std::ptr::null_mut();
                }
            };

            let jlong_opstamps: Vec<i64> = opstamps.into_iter().map(|op| op as i64).collect();
            if env.set_long_array_region(&jlong_array, 0, &jlong_opstamps).is_err() {
                handle_error(&mut env, "Failed to populate result array");
                return std::ptr::null_mut();
            }

            jlong_array.as_raw()
        },
        Err(err) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeCommit(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let mut writer = writer_mutex.lock().unwrap();
        writer.commit().map_err(|e| e.to_string())
    });

    match result {
        Some(Ok(opstamp)) => {
            opstamp as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeRollback(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let mut writer = writer_mutex.lock().unwrap();
        writer.rollback().map_err(|e| e.to_string())
    });

    match result {
        Some(Ok(opstamp)) => opstamp as jlong,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeGarbageCollectFiles(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<(), String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        // Use futures executor to handle the async garbage collection
        match futures::executor::block_on(writer.garbage_collect_files()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    });

    match result {
        Some(Ok(_)) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeDeleteAllDocuments(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<(), String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        let _count = writer.delete_all_documents();
        Ok(())
    });

    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeGetCommitOpstamp(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, u64>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        writer.commit_opstamp()
    });

    match result {
        Some(opstamp) => opstamp as jlong,
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeDeleteDocuments(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _field_value: JObject,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeDeleteDocumentsByTerm(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    field_value: JObject,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    // Convert the Java object to the appropriate Term outside the closure to avoid deadlocks
    let term = match (|| -> Result<_, String> {
        // Get schema first
        let schema = with_arc_safe::<Mutex<TantivyIndexWriter>, Schema>(ptr, |writer_mutex| {
            let writer = writer_mutex.lock().unwrap();
            writer.index().schema()
        }).ok_or_else(|| "Invalid IndexWriter pointer".to_string())?;

        // Get the field from the schema
        let field = schema.get_field(&field_name_str)
            .map_err(|_| format!("Field '{}' not found in schema", field_name_str))?;

        // Convert the Java object to the appropriate Term
        convert_jobject_to_term(&mut env, &field_value, field, &schema)
    })() {
        Ok(term) => term,
        Err(err) => {
            handle_error(&mut env, &err);
            return 0;
        }
    };

    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        // Delete documents by term
        let deleted_count = writer.delete_term(term);
        // Note: Tantivy's delete_term returns the number of delete operations, not necessarily
        // the number of documents that will be deleted (deletion happens during commit)
        Ok(deleted_count)
    });

    match result {
        Some(Ok(count)) => count as jlong,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeDeleteDocumentsByQuery(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query_ptr: jlong,
) -> jlong {
    // Clone the query first to avoid nested locks and deadlocks
    let query_clone = match with_arc_safe::<Box<dyn TantivyQuery>, Box<dyn TantivyQuery>>(query_ptr, |query_arc| {
        query_arc.box_clone()
    }) {
        Some(q) => q,
        None => {
            handle_error(&mut env, "Invalid Query pointer");
            return 0;
        }
    };

    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<u64, String>>(ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        // Use the pre-cloned query to avoid nested object registry access
        Ok(writer.delete_query(query_clone).map_err(|e| e.to_string())?)
    });

    match result {
        Some(Ok(count)) => count as jlong,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeWaitMergingThreads(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    // wait_merging_threads consumes the IndexWriter, so we need to remove it from the Arc registry
    let writer_arc = {
        let mut registry = crate::utils::ARC_REGISTRY.lock().unwrap();
        registry.remove(&ptr).and_then(|boxed| boxed.downcast::<Arc<Mutex<TantivyIndexWriter>>>().ok().map(|b| *b))
    };

    let result = match writer_arc {
        Some(arc) => {
            // Try to extract the IndexWriter from the Arc<Mutex<TantivyIndexWriter>>
            match Arc::try_unwrap(arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.wait_merging_threads() {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e.to_string()),
                    }
                },
                Err(_) => Err("Cannot wait on merging threads: IndexWriter is still in use".to_string()),
            }
        },
        None => Err("Invalid IndexWriter pointer".to_string()),
    };

    match result {
        Ok(()) => {},
        Err(err) => {
            handle_error(&mut env, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeMerge(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    segment_ids: JObject,
) -> jlong {
    // Extract segment IDs from Java list
    let segment_id_vec = if !segment_ids.is_null() {
        match extract_segment_ids(&mut env, &segment_ids) {
            Ok(ids) => ids,
            Err(e) => {
                handle_error(&mut env, &e);
                return 0;
            }
        }
    } else {
        Vec::new()
    };

    let result = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<tantivy::SegmentMeta, String>>(ptr, |writer_mutex| {
        let mut writer = writer_mutex.lock().unwrap();
        // Convert segment IDs to tantivy SegmentIds
        let tantivy_segment_ids: Result<Vec<SegmentId>, String> = segment_id_vec
            .iter()
            .map(|id_str| {
                SegmentId::from_uuid_string(id_str)
                    .map_err(|e| format!("Invalid segment ID '{}': {}", id_str, e))
            })
            .collect();

        let segment_ids = tantivy_segment_ids?;

        // Perform the merge operation - this returns a future
        let merge_future = writer.merge(&segment_ids);

        // Use futures executor to handle the async merge
        match futures::executor::block_on(merge_future) {
            Ok(Some(segment_meta)) => Ok(segment_meta),
            Ok(None) => Err("Merge operation failed - no segment metadata returned".to_string()),
            Err(e) => Err(format!("Merge operation failed: {}", e)),
        }
    });

    match result {
        Some(Ok(segment_meta)) => {
            // Register the resulting segment metadata and return pointer
            let segment_meta_arc = Arc::new(segment_meta);
            arc_to_jlong(segment_meta_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid IndexWriter pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}
