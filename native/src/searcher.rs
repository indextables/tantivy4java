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

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jint, jobject};
use jni::JNIEnv;
use tantivy::{IndexWriter as TantivyIndexWriter, Searcher as TantivySearcher, DocAddress as TantivyDocAddress};
use tantivy::query::Query as TantivyQuery;
use tantivy::Term;
use tantivy::schema::Schema;
use std::net::IpAddr;
use crate::utils::{register_object, remove_object, with_object, with_object_mut, handle_error};
use crate::document::{RetrievedDocument, DocumentWrapper};

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
    // Clone the query first to avoid nested locks
    let query_clone = match with_object::<Box<dyn TantivyQuery>, Box<dyn TantivyQuery>>(query_ptr as u64, |query| {
        query.box_clone()
    }) {
        Some(q) => q,
        None => {
            handle_error(&mut env, "Invalid Query pointer");
            return 0;
        }
    };
    
    let result = with_object::<TantivySearcher, Result<Vec<(f32, tantivy::DocAddress)>, String>>(ptr as u64, |searcher| {
        let collector = tantivy::collector::TopDocs::with_limit(limit as usize);
        let search_result = searcher.search(query_clone.as_ref(), &collector).map_err(|e| e.to_string())?;
        Ok(search_result)
    });
    
    match result {
        Some(Ok(top_docs)) => {
            register_object(top_docs) as jlong
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
    with_object::<TantivySearcher, jint>(ptr as u64, |searcher| {
        searcher.num_docs() as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetNumSegments(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_object::<TantivySearcher, jint>(ptr as u64, |searcher| {
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
    let tantivy_doc_address = match with_object::<TantivyDocAddress, Option<TantivyDocAddress>>(doc_address_ptr as u64, |doc_address| {
        Some(*doc_address)
    }) {
        Some(Some(addr)) => addr,
        _ => {
            handle_error(&mut env, "Invalid DocAddress pointer");
            return 0;
        }
    };
    
    // Get the document from the searcher
    let result = with_object::<TantivySearcher, Result<tantivy::schema::TantivyDocument, String>>(searcher_ptr as u64, |searcher| {
        match searcher.doc(tantivy_doc_address) {
            Ok(doc) => Ok(doc),
            Err(e) => Err(e.to_string()),
        }
    });
    
    match result {
        Some(Ok(document)) => {
            // Get the schema from the searcher to convert the document properly
            let schema_result = with_object::<TantivySearcher, Option<tantivy::schema::Schema>>(searcher_ptr as u64, |searcher| {
                Some(searcher.schema().clone())
            });
            
            match schema_result {
                Some(Some(schema)) => {
                    // Convert TantivyDocument to RetrievedDocument with proper field access
                    let retrieved_doc = RetrievedDocument::new_with_schema(document, &schema);
                    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                    register_object(wrapper) as jlong
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
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
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
    let doc_builder_clone = match with_object::<DocumentWrapper, Option<DocumentBuilder>>(doc_ptr as u64, |doc_wrapper| {
        match doc_wrapper {
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
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
    
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeCommit(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
    let result = with_object::<TantivyIndexWriter, Result<(), String>>(ptr as u64, |writer| {
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
    let result = with_object_mut::<TantivyIndexWriter, Result<(), String>>(ptr as u64, |writer| {
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
    let result = with_object::<TantivyIndexWriter, u64>(ptr as u64, |writer| {
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
        let schema = with_object::<TantivyIndexWriter, Schema>(ptr as u64, |writer| {
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
    
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
    let query_clone = match with_object::<Box<dyn TantivyQuery>, Box<dyn TantivyQuery>>(query_ptr as u64, |query| {
        query.box_clone()
    }) {
        Some(q) => q,
        None => {
            handle_error(&mut env, "Invalid Query pointer");
            return 0;
        }
    };
    
    let result = with_object_mut::<TantivyIndexWriter, Result<u64, String>>(ptr as u64, |writer| {
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
    // wait_merging_threads consumes the IndexWriter, so we need to remove it from the registry
    let writer_opt = {
        let mut registry = crate::utils::OBJECT_REGISTRY.lock().unwrap();
        registry.remove(&(ptr as u64)).and_then(|boxed| boxed.downcast::<TantivyIndexWriter>().ok())
    };
    
    let result = match writer_opt {
        Some(writer) => {
            match writer.wait_merging_threads() {
                Ok(()) => Ok(()),
                Err(e) => Err(e.to_string()),
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
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

// Supporting classes native methods - moved to separate modules

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetHits(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    // Clone the search results to avoid holding locks during object creation
    let search_results_clone = match with_object::<Vec<(f32, tantivy::DocAddress)>, Option<Vec<(f32, tantivy::DocAddress)>>>(
        _ptr as u64, 
        |search_results| Some(search_results.clone())
    ) {
        Some(Some(results)) => results,
        _ => {
            handle_error(&mut env, "Invalid SearchResult pointer");
            return std::ptr::null_mut();
        }
    };
    
    // Create the ArrayList with proper Hit objects containing scores and DocAddress
    match (|| -> Result<jobject, String> {
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        for (score, doc_address) in search_results_clone.iter() {
            // Create DocAddress object first
            let doc_address_ptr = register_object(*doc_address) as jlong;
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
    remove_object(ptr as u64);
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
    remove_object(ptr as u64);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeAnalyze(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _text: JString,
) -> jobject {
    handle_error(&mut env, "TextAnalyzer native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

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
    remove_object(ptr as u64);
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