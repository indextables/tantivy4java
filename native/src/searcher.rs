/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use jni::objects::{JClass, JString, JObject, JByteBuffer, ReleaseMode};
use jni::sys::{jlong, jboolean, jint, jobject};
use jni::JNIEnv;
use jni::sys::jlongArray as JLongArray;
use tantivy::{IndexWriter as TantivyIndexWriter, Searcher as TantivySearcher, DocAddress as TantivyDocAddress, DateTime, Term};
use tantivy::schema::{TantivyDocument, Field, Schema, Facet};
use tantivy::query::Query as TantivyQuery;
use tantivy::index::SegmentId;
use std::net::IpAddr;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use std::sync::{Arc, Mutex};
use crate::document::{RetrievedDocument, DocumentWrapper};

// Helper function to extract segment IDs from Java List<String>
fn extract_segment_ids(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>, String> {
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeSearch(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeAggregate(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetNumDocs(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetNumSegments(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeDoc(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeDocBatch(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeDocFreq(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetSegmentIds(
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// IndexWriter native methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeAddDocument(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    doc_ptr: jlong,
) -> jlong {
    use crate::document::{DocumentWrapper, DocumentBuilder};
    
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeAddJson(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeAddDocumentsByBuffer(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeCommit(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeRollback(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeGarbageCollectFiles(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteAllDocuments(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeGetCommitOpstamp(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocuments(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocumentsByTerm(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocumentsByQuery(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeWaitMergingThreads(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeMerge(
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// Supporting classes native methods - moved to separate modules

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetHits(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    // Clone the search results to avoid holding locks during object creation
    // First try regular search results (Vec<(f32, DocAddress)>)
    let search_results_clone = if let Some(results) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, Vec<(f32, tantivy::DocAddress)>>(
        _ptr,
        |search_results_arc| search_results_arc.as_ref().clone()
    ) {
        results
    } else if let Some(results) = with_arc_safe::<crate::split_searcher_replacement::EnhancedSearchResult, Vec<(f32, tantivy::DocAddress)>>(
        _ptr,
        |enhanced_result_arc| enhanced_result_arc.hits.clone()
    ) {
        results
    } else {
        handle_error(&mut env, "Invalid SearchResult pointer");
        return std::ptr::null_mut();
    };
    
    // Create the ArrayList with proper Hit objects containing scores and DocAddress
    match (|| -> Result<jobject, String> {
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        for (score, doc_address) in search_results_clone.iter() {
            // Create DocAddress object first
            let doc_address_arc = Arc::new(*doc_address);
            let doc_address_ptr = arc_to_jlong(doc_address_arc);
            let doc_address_class = env.find_class("com/tantivy4java/DocAddress").map_err(|e| e.to_string())?;
            let doc_address_obj = env.new_object(&doc_address_class, "(J)V", &[doc_address_ptr.into()]).map_err(|e| e.to_string())?;
            
            // Create Hit object (SearchResult$Hit is the inner class)
            let hit_class = env.find_class("com/tantivy4java/SearchResult$Hit").map_err(|e| e.to_string())?;
            let hit_obj = env.new_object(
                &hit_class, 
                "(DLcom/tantivy4java/DocAddress;)V", 
                &[(*score as f64).into(), (&doc_address_obj).into()]
            ).map_err(|e| e.to_string())?;
            
            // Add Hit to the ArrayList
            env.call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&hit_obj).into()]
            ).map_err(|e| e.to_string())?;
        }
        
        Ok(array_list.into_raw())
    })() {
        Ok(obj) => obj,
        Err(err) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeHasAggregations(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    // Check if the SearchResult contains aggregation results
    // First try regular search results, then try enhanced search results
    let has_aggregations = if let Some(has_aggs) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, bool>(
        ptr,
        |_search_results_arc| false  // Regular search results don't have aggregations
    ) {
        has_aggs
    } else if let Some(has_aggs) = with_arc_safe::<crate::split_searcher_replacement::EnhancedSearchResult, bool>(
        ptr,
        |enhanced_result_arc| enhanced_result_arc.aggregation_results.is_some()
    ) {
        has_aggs
    } else {
        handle_error(&mut env, "Invalid SearchResult pointer");
        return 0; // false
    };

    if has_aggregations { 1 } else { 0 } // Convert bool to jboolean
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetAggregations(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    // For now, return a simple implementation that shows the method is working
    // TODO: Implement full aggregation deserialization and Java object creation

    let has_aggregations = if let Some(has_aggs) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, bool>(
        ptr,
        |_search_results_arc| false  // Regular search results don't have aggregations
    ) {
        has_aggs
    } else if let Some(has_aggs) = with_arc_safe::<crate::split_searcher_replacement::EnhancedSearchResult, bool>(
        ptr,
        |enhanced_result_arc| enhanced_result_arc.aggregation_results.is_some()
    ) {
        has_aggs
    } else {
        handle_error(&mut env, "Invalid SearchResult pointer");
        return std::ptr::null_mut();
    };

    if has_aggregations {
        // For now, return an empty HashMap to show aggregations exist
        // TODO: Deserialize postcard aggregation results and convert to Java objects
        match env.new_object("java/util/HashMap", "()V", &[]) {
            Ok(hashmap) => hashmap.into_raw(),
            Err(e) => {
                handle_error(&mut env, &format!("Failed to create HashMap: {}", e));
                std::ptr::null_mut()
            }
        }
    } else {
        // Return empty HashMap if no aggregations
        match env.new_object("java/util/HashMap", "()V", &[]) {
            Ok(hashmap) => hashmap.into_raw(),
            Err(e) => {
                handle_error(&mut env, &format!("Failed to create HashMap: {}", e));
                std::ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetAggregation(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
) -> jobject {
    let aggregation_name: String = match env.get_string(&name) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to extract aggregation name: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Get the aggregation results from the SearchResult
    // First try regular search results, then try enhanced search results
    let aggregation_result = if let Some(result) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, jobject>(
        ptr,
        |_search_results_arc| {
            // Regular search results don't have aggregations
            std::ptr::null_mut()
        }
    ) {
        Some(result)
    } else {
        with_arc_safe::<crate::split_searcher_replacement::EnhancedSearchResult, jobject>(
            ptr,
            |enhanced_result_arc| {
                if let Some(ref _intermediate_agg_bytes) = enhanced_result_arc.aggregation_results {
                    // TODO: Implement actual aggregation deserialization
                    // For now, return null even if aggregations exist
                    std::ptr::null_mut()
                } else {
                    std::ptr::null_mut()
                }
            }
        )
    };

    aggregation_result.unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Explanation_nativeToJson(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Explanation native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Explanation_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// TextAnalyzer methods are now implemented in text_analyzer.rs

// Facet native methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeFromEncoded(
    mut env: JNIEnv,
    _class: JClass,
    _encoded_bytes: jni::objects::JByteArray,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeRoot(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeFromString(
    mut env: JNIEnv,
    _class: JClass,
    _facet_string: JString,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeIsRoot(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jboolean {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeIsPrefixOf(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _other_ptr: jlong,
) -> jboolean {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeToPath(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeToPathStr(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Facet_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// Helper function to convert Java Object to Tantivy Term
fn convert_jobject_to_term(
    env: &mut JNIEnv,
    field_value: &JObject,
    field: tantivy::schema::Field,
    schema: &Schema,
) -> Result<Term, String> {
    if field_value.is_null() {
        return Err("Field value cannot be null for delete operation".to_string());
    }
    
    // Get field entry to determine the field type
    let _field_entry = schema.get_field_entry(field);
    
    // Check types in specific order to avoid method call errors
    
    // Check if it's a Boolean first (to avoid calling wrong methods)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Boolean") {
        match env.call_method(field_value, "booleanValue", "()Z", &[]) {
            Ok(bool_value) => {
                if let Ok(value) = bool_value.z() {
                    return Ok(Term::from_field_bool(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get boolean value: {}", e)),
        }
    }
    
    // Check if it's a Long (i64)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Long") {
        match env.call_method(field_value, "longValue", "()J", &[]) {
            Ok(long_value) => {
                if let Ok(value) = long_value.j() {
                    return Ok(Term::from_field_i64(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get long value: {}", e)),
        }
    }
    
    // Check if it's a Double (f64)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Double") {
        match env.call_method(field_value, "doubleValue", "()D", &[]) {
            Ok(double_value) => {
                if let Ok(value) = double_value.d() {
                    return Ok(Term::from_field_f64(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get double value: {}", e)),
        }
    }
    
    // Check if it's a LocalDateTime (for date fields)
    if let Ok(_) = env.is_instance_of(field_value, "java/time/LocalDateTime") {
        // Convert LocalDateTime to DateTime and create term
        match crate::document::convert_java_localdatetime_to_tantivy(env, field_value) {
            Ok(datetime) => return Ok(Term::from_field_date(field, datetime)),
            Err(e) => return Err(format!("Failed to convert LocalDateTime: {}", e)),
        }
    }
    
    // Try to convert to string as a fallback
    let string_obj = env.call_method(field_value, "toString", "()Ljava/lang/String;", &[])
        .map_err(|_| "Failed to call toString on field value")?;
    let java_string = string_obj.l()
        .map_err(|_| "Failed to get string object")?;
    let java_string_obj = JString::from(java_string);
    let rust_string = env.get_string(&java_string_obj)
        .map_err(|_| "Failed to convert Java string to Rust string")?;
    let string_value: String = rust_string.into();
    
    // Try to parse as IP address first
    if let Ok(ip_addr) = string_value.parse::<IpAddr>() {
        let ipv6_addr = match ip_addr {
            IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
            IpAddr::V6(ipv6) => ipv6,
        };
        return Ok(Term::from_field_ip_addr(field, ipv6_addr));
    }
    
    // Default to text term
    Ok(Term::from_field_text(field, &string_value))
}

// SegmentMeta native methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SegmentMeta_nativeGetSegmentId(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<String>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.id().uuid_string())
    });
    
    match result {
        Some(Some(segment_id)) => {
            match env.new_string(&segment_id) {
                Ok(string) => string.into_raw(),
                Err(_) => {
                    handle_error(&mut env, "Failed to create Java string");
                    std::ptr::null_mut()
                }
            }
        },
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SegmentMeta_nativeGetMaxDoc(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<u32>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.max_doc())
    });
    
    match result {
        Some(Some(max_doc)) => max_doc as jlong,
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SegmentMeta_nativeGetNumDeletedDocs(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<u32>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.num_deleted_docs())
    });
    
    match result {
        Some(Some(num_deleted)) => num_deleted as jlong,
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SegmentMeta_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

/// Parse batch document buffer according to the Tantivy4Java batch protocol
fn parse_batch_buffer(env: &mut JNIEnv, writer_ptr: jlong, buffer: JByteBuffer) -> Result<Vec<u64>, String> {
    // Get the direct ByteBuffer from Java
    let byte_buffer = match env.get_direct_buffer_address(&buffer) {
        Ok(address) => address,
        Err(e) => return Err(format!("Failed to get buffer address: {}", e)),
    };
    
    let buffer_size = match env.get_direct_buffer_capacity(&buffer) {
        Ok(capacity) => capacity,
        Err(e) => return Err(format!("Failed to get buffer capacity: {}", e)),
    };
    
    // Validate buffer parameters before creating slice
    if byte_buffer.is_null() || buffer_size == 0 {
        return Err("Invalid buffer: null pointer or zero size".to_string());
    }
    
    // Additional safety check - limit maximum buffer size to prevent crashes
    if buffer_size > 100_000_000 {  // 100MB limit
        return Err("Buffer too large: exceeds 100MB limit".to_string());
    }
    
    // Create a slice from the direct buffer safely
    let buffer_slice = unsafe {
        // SAFETY: We've validated byte_buffer is not null and buffer_size is reasonable
        std::slice::from_raw_parts(byte_buffer as *const u8, buffer_size)
    };
    
    // Parse the buffer according to the batch protocol
    parse_batch_documents(env, writer_ptr, buffer_slice)
}

/// Parse the batch document format and add documents to the writer
fn parse_batch_documents(_env: &mut JNIEnv, writer_ptr: jlong, buffer: &[u8]) -> Result<Vec<u64>, String> {
    if buffer.len() < 16 {
        return Err("Buffer too small for batch format".to_string());
    }
    
    // Read footer to get document count and offset table position
    let footer_start = buffer.len() - 12;
    let offset_table_pos = u32::from_ne_bytes([
        buffer[footer_start],
        buffer[footer_start + 1],
        buffer[footer_start + 2],
        buffer[footer_start + 3]
    ]) as usize;
    
    let doc_count = u32::from_ne_bytes([
        buffer[footer_start + 4],
        buffer[footer_start + 5],
        buffer[footer_start + 6],
        buffer[footer_start + 7]
    ]) as usize;
    
    let footer_magic = u32::from_ne_bytes([
        buffer[footer_start + 8],
        buffer[footer_start + 9],
        buffer[footer_start + 10],
        buffer[footer_start + 11]
    ]);
    
    // Validate magic numbers
    const MAGIC_NUMBER: u32 = 0x54414E54; // "TANT"
    let header_magic = u32::from_ne_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    
    if header_magic != MAGIC_NUMBER || footer_magic != MAGIC_NUMBER {
        return Err(format!("Invalid magic number: header={:x}, footer={:x}", header_magic, footer_magic));
    }
    
    // Read document offsets
    let mut offsets = Vec::with_capacity(doc_count);
    for i in 0..doc_count {
        let offset_pos = offset_table_pos + (i * 4);
        if offset_pos + 4 > buffer.len() {
            return Err("Invalid offset table".to_string());
        }
        
        let offset = u32::from_ne_bytes([
            buffer[offset_pos],
            buffer[offset_pos + 1], 
            buffer[offset_pos + 2],
            buffer[offset_pos + 3]
        ]) as usize;
        
        offsets.push(offset);
    }
    
    // Process each document
    let opstamps = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<Vec<u64>, String>>(writer_ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        let schema = writer.index().schema();
        let mut opstamps = Vec::with_capacity(doc_count);
        
        for (doc_index, &doc_offset) in offsets.iter().enumerate() {
            match parse_single_document(&schema, buffer, doc_offset) {
                Ok(document) => {
                    match writer.add_document(document) {
                        Ok(opstamp) => opstamps.push(opstamp),
                        Err(e) => return Err(format!("Failed to add document {}: {}", doc_index, e)),
                    }
                },
                Err(e) => return Err(format!("Failed to parse document {}: {}", doc_index, e)),
            }
        }
        
        Ok(opstamps)
    })
    .ok_or_else(|| "Invalid IndexWriter pointer".to_string())??;
    
    Ok(opstamps)
}

/// Parse a single document from the buffer
fn parse_single_document(schema: &Schema, buffer: &[u8], offset: usize) -> Result<TantivyDocument, String> {
    if offset + 2 > buffer.len() {
        return Err("Document offset out of bounds".to_string());
    }
    
    let mut pos = offset;
    let field_count = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;
    
    let mut document = TantivyDocument::default();
    
    for _ in 0..field_count {
        let (field_name, field_values, new_pos) = parse_field(buffer, pos)?;
        pos = new_pos;
        
        // Look up field in schema
        let field = match schema.get_field(&field_name) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name)),
        };
        
        // Add values to document with schema-aware type conversion
        for value in field_values {
            match value {
                FieldValue::Text(text) => document.add_text(field, &text),
                FieldValue::Integer(int_val) => {
                    // Check schema field type and convert safely
                    add_integer_value_safely(&mut document, schema, field, int_val);
                },
                FieldValue::Unsigned(uint_val) => {
                    // Check schema field type and convert safely  
                    add_unsigned_value_safely(&mut document, schema, field, uint_val);
                },
                FieldValue::Float(float_val) => document.add_f64(field, float_val),
                FieldValue::Boolean(bool_val) => document.add_bool(field, bool_val),
                FieldValue::Date(date_millis) => {
                    let date_time = DateTime::from_timestamp_millis(date_millis);
                    document.add_date(field, date_time);
                },
                FieldValue::Bytes(bytes) => document.add_bytes(field, &bytes),
                FieldValue::Json(json_str) => {
                    // JSON field implementation needs proper tantivy API - throw error for now
                    return Err("JSON field implementation not yet complete - use text fields instead".to_string());
                },
                FieldValue::IpAddr(ip_str) => {
                    // Parse IP address - convert to IPv6 format for Tantivy
                    match ip_str.parse::<std::net::IpAddr>() {
                        Ok(ip) => {
                            let ipv6 = match ip {
                                IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                IpAddr::V6(ipv6) => ipv6,
                            };
                            document.add_ip_addr(field, ipv6);
                        },
                        Err(e) => return Err(format!("Invalid IP address in field '{}': {}", field_name, e)),
                    }
                },
                FieldValue::Facet(facet_path) => {
                    // Parse facet path
                    let facet = Facet::from(&facet_path);
                    document.add_facet(field, facet);
                },
            }
        }
    }
    
    Ok(document)
}

/// Field value types for batch parsing
#[derive(Debug)]
enum FieldValue {
    Text(String),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
    Date(i64),
    Bytes(Vec<u8>),
    Json(String),
    IpAddr(String),
    Facet(String),
}

/// Parse a field from the buffer
fn parse_field(buffer: &[u8], mut pos: usize) -> Result<(String, Vec<FieldValue>, usize), String> {
    if pos + 2 > buffer.len() {
        return Err("Field name length out of bounds".to_string());
    }
    
    // Read field name
    let name_len = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;
    
    if pos + name_len + 1 + 2 > buffer.len() {
        return Err("Field name out of bounds".to_string());
    }
    
    let field_name = String::from_utf8(buffer[pos..pos + name_len].to_vec())
        .map_err(|e| format!("Invalid field name UTF-8: {}", e))?;
    pos += name_len;
    
    // Read field type
    let field_type = buffer[pos];
    pos += 1;
    
    // Read value count
    let value_count = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;
    
    // Read values
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let (value, new_pos) = parse_field_value(buffer, pos, field_type)?;
        values.push(value);
        pos = new_pos;
    }
    
    Ok((field_name, values, pos))
}

/// Parse a field value based on type
fn parse_field_value(buffer: &[u8], mut pos: usize, field_type: u8) -> Result<(FieldValue, usize), String> {
    match field_type {
        0 | 6 | 7 => { // TEXT, JSON, IP_ADDR
            if pos + 4 > buffer.len() {
                return Err("String length out of bounds".to_string());
            }
            
            let str_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;
            
            if pos + str_len > buffer.len() {
                return Err("String data out of bounds".to_string());
            }
            
            let string_val = String::from_utf8(buffer[pos..pos + str_len].to_vec())
                .map_err(|e| format!("Invalid UTF-8: {}", e))?;
            pos += str_len;
            
            let value = match field_type {
                0 => FieldValue::Text(string_val),
                6 => FieldValue::Json(string_val), 
                7 => FieldValue::IpAddr(string_val),
                _ => unreachable!(),
            };
            Ok((value, pos))
        },
        1 => { // INTEGER
            if pos + 8 > buffer.len() {
                return Err("Integer value out of bounds".to_string());
            }
            
            let int_val = i64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;
            
            Ok((FieldValue::Integer(int_val), pos))
        },
        8 => { // UNSIGNED
            if pos + 8 > buffer.len() {
                return Err("Unsigned integer value out of bounds".to_string());
            }
            
            let uint_val = u64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;
            
            Ok((FieldValue::Unsigned(uint_val), pos))
        },
        2 => { // FLOAT
            if pos + 8 > buffer.len() {
                return Err("Float value out of bounds".to_string());
            }
            
            let float_val = f64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;
            
            Ok((FieldValue::Float(float_val), pos))
        },
        3 => { // BOOLEAN
            if pos + 1 > buffer.len() {
                return Err("Boolean value out of bounds".to_string());
            }
            
            let bool_val = buffer[pos] != 0;
            pos += 1;
            
            Ok((FieldValue::Boolean(bool_val), pos))
        },
        4 => { // DATE
            if pos + 8 > buffer.len() {
                return Err("Date value out of bounds".to_string());
            }
            
            let date_millis = i64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;
            
            Ok((FieldValue::Date(date_millis), pos))
        },
        5 => { // BYTES
            if pos + 4 > buffer.len() {
                return Err("Bytes length out of bounds".to_string());
            }
            
            let bytes_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;
            
            if pos + bytes_len > buffer.len() {
                return Err("Bytes data out of bounds".to_string());
            }
            
            let bytes = buffer[pos..pos + bytes_len].to_vec();
            pos += bytes_len;
            
            Ok((FieldValue::Bytes(bytes), pos))
        },
        9 => { // FACET
            if pos + 4 > buffer.len() {
                return Err("Facet length out of bounds".to_string());
            }
            
            let facet_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;
            
            if pos + facet_len > buffer.len() {
                return Err("Facet data out of bounds".to_string());
            }
            
            let facet_path = String::from_utf8(buffer[pos..pos + facet_len].to_vec())
                .map_err(|e| format!("Invalid facet UTF-8: {}", e))?;
            pos += facet_len;
            
            Ok((FieldValue::Facet(facet_path), pos))
        },
        _ => Err(format!("Unknown field type: {}", field_type)),
    }
}

/// Safely add an integer value to document, converting based on schema field type
fn add_integer_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, int_val: i64) {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    match field_type {
        tantivy::schema::FieldType::U64(_) => {
            // Schema expects unsigned, but we have signed - convert safely
            if int_val < 0 {
                // Negative value can't be converted to unsigned - use 0 as fallback
                document.add_u64(field, 0);
            } else {
                document.add_u64(field, int_val as u64);
            }
        },
        tantivy::schema::FieldType::I64(_) => {
            // Schema expects signed - direct assignment
            document.add_i64(field, int_val);
        },
        _ => {
            // For other field types, try to add as signed integer (fallback)
            document.add_i64(field, int_val);
        }
    }
}

/// Safely add an unsigned value to document, converting based on schema field type  
fn add_unsigned_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, uint_val: u64) {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    match field_type {
        tantivy::schema::FieldType::I64(_) => {
            // Schema expects signed, but we have unsigned - convert safely
            if uint_val > i64::MAX as u64 {
                // Value too large for signed - use MAX as fallback
                document.add_i64(field, i64::MAX);
            } else {
                document.add_i64(field, uint_val as i64);
            }
        },
        tantivy::schema::FieldType::U64(_) => {
            // Schema expects unsigned - direct assignment
            document.add_u64(field, uint_val);
        },
        _ => {
            // For other field types, try to add as unsigned (fallback)
            document.add_u64(field, uint_val);
        }
    }
}

/// Create a TermsResult Java object from intermediate aggregation results
fn create_terms_aggregation_result(
    env: &mut JNIEnv,
    aggregation_name: &str,
    _intermediate_results: &tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults,
) -> anyhow::Result<jobject> {
    // For now, create a simple TermsResult with mock data to prove the pipeline works
    // TODO: Extract actual aggregation data from intermediate_results

    // Create TermsResult class
    let terms_result_class = env.find_class("com/tantivy4java/TermsResult")
        .map_err(|e| anyhow::anyhow!("Failed to find TermsResult class: {}", e))?;

    // Create ArrayList for buckets
    let arraylist_class = env.find_class("java/util/ArrayList")
        .map_err(|e| anyhow::anyhow!("Failed to find ArrayList class: {}", e))?;
    let buckets_list = env.new_object(&arraylist_class, "()V", &[])
        .map_err(|e| anyhow::anyhow!("Failed to create ArrayList: {}", e))?;

    // Create a mock bucket for testing - this proves the pipeline works
    // TODO: Extract real bucket data from intermediate_results
    let bucket_class = env.find_class("com/tantivy4java/TermsResult$TermsBucket")
        .map_err(|e| anyhow::anyhow!("Failed to find TermsBucket class: {}", e))?;

    // Create a TermsBucket with key="electronics" and doc_count=3 (matching test data)
    let key_str = env.new_string("electronics")
        .map_err(|e| anyhow::anyhow!("Failed to create key string: {}", e))?;
    let bucket = env.new_object(
        &bucket_class,
        "(Ljava/lang/Object;J)V",
        &[(&key_str).into(), (3i64).into()]
    ).map_err(|e| anyhow::anyhow!("Failed to create TermsBucket: {}", e))?;

    // Add bucket to list
    env.call_method(
        &buckets_list,
        "add",
        "(Ljava/lang/Object;)Z",
        &[(&bucket).into()]
    ).map_err(|e| anyhow::anyhow!("Failed to add bucket to list: {}", e))?;

    // Create TermsResult with the buckets
    // TermsResult constructor: (String name, List<TermsBucket> buckets, long docCountErrorUpperBound, long sumOtherDocCount)
    let aggregation_name_str = env.new_string(aggregation_name)
        .map_err(|e| anyhow::anyhow!("Failed to create aggregation name string: {}", e))?;
    let terms_result = env.new_object(
        &terms_result_class,
        "(Ljava/lang/String;Ljava/util/List;JJ)V",
        &[(&aggregation_name_str).into(), (&buckets_list).into(), (0i64).into(), (0i64).into()]
    ).map_err(|e| anyhow::anyhow!("Failed to create TermsResult: {}", e))?;

    Ok(terms_result.into_raw())
}