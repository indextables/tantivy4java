// jni_searcher.rs - JNI methods for Searcher class
// Extracted from mod.rs during refactoring
// Contains: nativeSearch, nativeDoc, nativeDocBatch, nativeGetNumDocs,
//           nativeGetNumSegments, nativeGetSegmentIds, nativeClose

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jint, jobject};
use jni::JNIEnv;
use jni::sys::jlongArray as JLongArray;

use tantivy::{Searcher as TantivySearcher, DocAddress as TantivyDocAddress};
use tantivy::query::Query as TantivyQuery;
use std::sync::{Arc, Mutex};

use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::document::{RetrievedDocument, DocumentWrapper};

// Helper function to extract segment IDs from Java List<String>
pub(crate) fn extract_segment_ids(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };

    let mut segment_ids = Vec::with_capacity(size as usize);

    // Extract each segment ID string from the list
    for i in 0..size {
        let element = match env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[i.into()]) {
            Ok(result) => result.l().map_err(|_| "Failed to get object from list")?,
            Err(_) => return Err("Failed to call get() on list".to_string()),
        };

        let string_obj = match env.call_method(&element, "toString", "()Ljava/lang/String;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to convert to string")?,
            Err(_) => return Err("Failed to call toString() on list element".to_string()),
        };

        let java_string = JString::from(string_obj);
        let rust_string: String = match env.get_string(&java_string) {
            Ok(s) => s.into(),
            Err(_) => return Err("Failed to convert Java string to Rust string".to_string()),
        };

        segment_ids.push(rust_string);
    }

    Ok(segment_ids)
}

// Searcher native methods
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeSearch(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query_ptr: jlong,
    limit: jint,
    _count: jboolean,
    _order_by_field: JString,
    _offset: jint,
    _order: jint,
) -> jlong {
    // Clone the query using Arc registry
    let query_clone = match with_arc_safe::<Box<dyn TantivyQuery>, Box<dyn TantivyQuery>>(query_ptr, |query| {
        query.box_clone()
    }) {
        Some(q) => q,
        None => {
            handle_error(&mut env, "Invalid Query pointer");
            return 0;
        }
    };

    let result = with_arc_safe::<Mutex<TantivySearcher>, Result<Vec<(f32, tantivy::DocAddress)>, String>>(ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        let collector = tantivy::collector::TopDocs::with_limit(limit as usize);
        let search_result = searcher.search(query_clone.as_ref(), &collector).map_err(|e| e.to_string())?;
        Ok(search_result)
    });

    match result {
        Some(Ok(top_docs)) => {
            let top_docs_arc = Arc::new(top_docs);
            arc_to_jlong(top_docs_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Query pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeAggregate(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query_ptr: jlong,
    _agg_query: JObject,
) -> jobject {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeGetNumDocs(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_arc_safe::<Mutex<TantivySearcher>, jint>(ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        searcher.num_docs() as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeGetNumSegments(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_arc_safe::<Mutex<TantivySearcher>, jint>(ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        searcher.segment_readers().len() as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeDoc(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    doc_address_ptr: jlong,
) -> jlong {
    // Get the DocAddress object
    let tantivy_doc_address = match with_arc_safe::<TantivyDocAddress, Option<TantivyDocAddress>>(doc_address_ptr, |doc_address| {
        Some(**doc_address)
    }) {
        Some(Some(addr)) => addr,
        _ => {
            handle_error(&mut env, "Invalid DocAddress pointer");
            return 0;
        }
    };

    // Get the document from the searcher
    let result = with_arc_safe::<Mutex<TantivySearcher>, Result<tantivy::schema::TantivyDocument, String>>(searcher_ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        match searcher.doc(tantivy_doc_address) {
            Ok(doc) => Ok(doc),
            Err(e) => Err(e.to_string()),
        }
    });

    match result {
        Some(Ok(document)) => {
            // Get the schema from the searcher to convert the document properly
            let schema_result = with_arc_safe::<Mutex<TantivySearcher>, Option<tantivy::schema::Schema>>(searcher_ptr, |searcher_mutex| {
                let searcher = searcher_mutex.lock().unwrap();
                Some(searcher.schema().clone())
            });

            match schema_result {
                Some(Some(schema)) => {
                    // Convert TantivyDocument to RetrievedDocument with proper field access
                    let retrieved_doc = RetrievedDocument::new_with_schema(document, &schema);
                    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                    let wrapper_arc = Arc::new(Mutex::new(wrapper));
                    arc_to_jlong(wrapper_arc)
                },
                _ => {
                    handle_error(&mut env, "Failed to get schema from searcher");
                    0
                }
            }
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Searcher pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeDocBatch(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    doc_address_ptrs: JLongArray,
) -> JLongArray {
    // Convert JNI array to proper JArray type
    let doc_addresses_array = unsafe { jni::objects::JLongArray::from_raw(doc_address_ptrs) };

    // Get the array of document address pointers
    let array_len = match env.get_array_length(&doc_addresses_array) {
        Ok(len) => len as usize,
        Err(e) => {
            handle_error(&mut env, &format!("Failed to get array length: {}", e));
            return std::ptr::null_mut();
        }
    };

    let mut addresses = vec![0i64; array_len];
    if let Err(e) = env.get_long_array_region(&doc_addresses_array, 0, &mut addresses) {
        handle_error(&mut env, &format!("Failed to get array elements: {}", e));
        return std::ptr::null_mut();
    }

    // Convert to Vec of DocAddress objects
    let mut tantivy_addresses = Vec::new();
    for &addr_ptr in addresses.iter() {
        let tantivy_doc_address = match with_arc_safe::<TantivyDocAddress, Option<TantivyDocAddress>>(addr_ptr, |doc_address| {
            Some(**doc_address)
        }) {
            Some(Some(addr)) => addr,
            _ => {
                handle_error(&mut env, &format!("Invalid DocAddress pointer at index {}", tantivy_addresses.len()));
                return std::ptr::null_mut();
            }
        };
        tantivy_addresses.push(tantivy_doc_address);
    }

    // Get the searcher and schema once
    let result = with_arc_safe::<Mutex<TantivySearcher>, Result<(Vec<tantivy::schema::TantivyDocument>, tantivy::schema::Schema), String>>(
        searcher_ptr,
        |searcher_mutex| {
            let searcher = searcher_mutex.lock().unwrap();
            let schema = searcher.schema().clone();

            // Sort addresses by segment for better cache locality
            let mut indexed_addresses: Vec<(usize, TantivyDocAddress)> = tantivy_addresses
                .into_iter()
                .enumerate()
                .collect();
            indexed_addresses.sort_by_key(|(_, addr)| (addr.segment_ord, addr.doc_id));

            // Retrieve all documents efficiently
            let mut documents = vec![None; indexed_addresses.len()];
            for (original_index, addr) in indexed_addresses {
                match searcher.doc(addr) {
                    Ok(doc) => documents[original_index] = Some(doc),
                    Err(e) => return Err(format!("Failed to retrieve document at index {}: {}", original_index, e)),
                }
            }

            // Convert Option<Document> to Document, returning error if any failed
            let documents: Result<Vec<_>, _> = documents
                .into_iter()
                .enumerate()
                .map(|(i, opt)| opt.ok_or_else(|| format!("Document at index {} was not retrieved", i)))
                .collect();

            match documents {
                Ok(docs) => Ok((docs, schema)),
                Err(e) => Err(e),
            }
        }
    );

    match result {
        Some(Ok((documents, schema))) => {
            // Convert documents to Document wrappers and get their pointers
            let doc_ptrs: Vec<jlong> = documents
                .into_iter()
                .map(|document| {
                    let retrieved_doc = RetrievedDocument::new_with_schema(document, &schema);
                    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                    let wrapper_arc = Arc::new(Mutex::new(wrapper));
                    arc_to_jlong(wrapper_arc)
                })
                .collect();

            // Create Java long array with document pointers
            match env.new_long_array(doc_ptrs.len() as i32) {
                Ok(array) => {
                    if let Err(e) = env.set_long_array_region(&array, 0, &doc_ptrs) {
                        handle_error(&mut env, &format!("Failed to set array region: {}", e));
                        std::ptr::null_mut()
                    } else {
                        array.into_raw()
                    }
                }
                Err(e) => {
                    handle_error(&mut env, &format!("Failed to create long array: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        },
        None => {
            handle_error(&mut env, "Invalid Searcher pointer");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeDocFreq(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _field_value: JObject,
) -> jint {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeGetSegmentIds(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    let result = with_arc_safe::<Mutex<TantivySearcher>, Result<Vec<String>, String>>(ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        let segment_ids: Vec<String> = searcher
            .segment_readers()
            .iter()
            .map(|segment_reader| segment_reader.segment_id().uuid_string())
            .collect();
        Ok(segment_ids)
    });

    match result {
        Some(Ok(segment_ids)) => {
            // Create a Java ArrayList
            match (|| -> Result<jobject, String> {
                let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
                let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;

                for segment_id in segment_ids {
                    let java_string = env.new_string(&segment_id).map_err(|e| e.to_string())?;
                    env.call_method(
                        &array_list,
                        "add",
                        "(Ljava/lang/Object;)Z",
                        &[(&java_string).into()]
                    ).map_err(|e| e.to_string())?;
                }

                Ok(array_list.into_raw())
            })() {
                Ok(list) => list,
                Err(err) => {
                    handle_error(&mut env, &err);
                    std::ptr::null_mut()
                }
            }
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        },
        None => {
            handle_error(&mut env, "Invalid Searcher pointer");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}
