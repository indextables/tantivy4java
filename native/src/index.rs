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
use jni::sys::{jlong, jboolean, jint};
use jni::JNIEnv;
use crate::utils::{remove_object, handle_error};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeNew(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _path: JString,
    _reuse: jboolean,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    _path: JString,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeExists(
    mut env: JNIEnv,
    _class: JClass,
    _path: JString,
) -> jboolean {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeWriter(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _heap_size: jint,
    _num_threads: jint,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeConfigReader(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _reload_policy: JString,
    _num_warmers: jint,
) {
    handle_error(&mut env, "Index native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeSearcher(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeReload(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) {
    handle_error(&mut env, "Index native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeParseQuery(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query: JString,
    _default_field_names: JObject,
    _field_boosts: JObject,
    _fuzzy_fields: JObject,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeParseQueryLenient(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query: JString,
    _default_field_names: JObject,
    _field_boosts: JObject,
    _fuzzy_fields: JObject,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeRegisterTokenizer(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _text_analyzer_ptr: jlong,
) {
    handle_error(&mut env, "Index native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}