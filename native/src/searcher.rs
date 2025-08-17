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
use crate::utils::{remove_object, handle_error};

// Searcher native methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeSearch(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query_ptr: jlong,
    _limit: jint,
    _count: jboolean,
    _order_by_field: JString,
    _offset: jint,
    _order: jint,
) -> jlong {
    handle_error(&mut env, "Searcher native methods not fully implemented yet");
    0
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
    _ptr: jlong,
    _doc_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
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
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "IndexWriter native methods not fully implemented yet");
    0
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
    mut env: JNIEnv,
    _class: JClass,
    _segment_ord: jint,
    _doc: jint,
) -> jlong {
    handle_error(&mut env, "DocAddress native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeGetSegmentOrd(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jint {
    handle_error(&mut env, "DocAddress native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_DocAddress_nativeGetDoc(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jint {
    handle_error(&mut env, "DocAddress native methods not fully implemented yet");
    0
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
    handle_error(&mut env, "SearchResult native methods not fully implemented yet");
    std::ptr::null_mut()
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