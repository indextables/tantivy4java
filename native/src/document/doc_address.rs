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

use jni::objects::JClass;
use jni::sys::{jlong, jint};
use jni::JNIEnv;
use tantivy::DocAddress as TantivyDocAddress;
use crate::utils::{arc_to_jlong, release_arc, with_arc_safe};
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_DocAddress_nativeNew(
    _env: JNIEnv,
    _class: JClass,
    segment_ord: jint,
    doc: jint,
) -> jlong {
    let doc_address = TantivyDocAddress::new(segment_ord as u32, doc as u32);
    let doc_address_arc = Arc::new(doc_address);
    arc_to_jlong(doc_address_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_DocAddress_nativeGetSegmentOrd(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_arc_safe::<TantivyDocAddress, jint>(ptr, |doc_address_arc| {
        doc_address_arc.segment_ord as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_DocAddress_nativeGetDoc(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_arc_safe::<TantivyDocAddress, jint>(ptr, |doc_address_arc| {
        doc_address_arc.doc_id as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_DocAddress_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}