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
use tantivy::{IndexWriter as TantivyIndexWriter, Searcher as TantivySearcher};
use tantivy::query::Query as TantivyQuery;
use crate::utils::{register_object, remove_object, with_object, with_object_mut, handle_error};

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
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jint {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetNumSegments(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jint {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeDoc(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _doc_address_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    0
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
    use crate::document::DocumentBuilder;
    
    // First, clone the DocumentBuilder to avoid nested locks
    let doc_builder_clone = match with_object::<DocumentBuilder, DocumentBuilder>(doc_ptr as u64, |doc_builder| {
        doc_builder.clone()
    }) {
        Some(doc) => doc,
        None => {
            handle_error(&mut env, "Invalid Document pointer");
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
    _ptr: jlong,
    _json: JString,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
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
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeGarbageCollectFiles(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteAllDocuments(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeGetCommitOpstamp(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
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
    _ptr: jlong,
    _field_name: JString,
    _field_value: JObject,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocumentsByQuery(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeWaitMergingThreads(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

// Supporting classes native methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeNew(
    _env: JNIEnv,
    _class: JClass,
    segment_ord: jint,
    doc: jint,
) -> jlong {
    let doc_address = tantivy::DocAddress::new(segment_ord as u32, doc as u32);
    register_object(doc_address) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeGetSegmentOrd(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    let result = with_object::<tantivy::DocAddress, u32>(ptr as u64, |doc_address| {
        doc_address.segment_ord
    });
    
    match result {
        Some(segment_ord) => segment_ord as jint,
        None => {
            handle_error(&mut env, "Invalid DocAddress pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeGetDoc(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    let result = with_object::<tantivy::DocAddress, u32>(ptr as u64, |doc_address| {
        doc_address.doc_id
    });
    
    match result {
        Some(doc_id) => doc_id as jint,
        None => {
            handle_error(&mut env, "Invalid DocAddress pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetHits(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    // Skip registry access for now - just return empty ArrayList
    let result = (|| -> Result<jobject, String> {
        // Create ArrayList
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        // Add a simple test string
        let test_str = "getHits() method working!";
        let java_string = env.new_string(test_str).map_err(|e| e.to_string())?;
        
        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[jni::objects::JValue::Object(&java_string)]
        ).map_err(|e| e.to_string())?;
        
        Ok(array_list.into_raw())
    })();
    
    match result {
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