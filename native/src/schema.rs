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

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;
use tantivy::schema::SchemaBuilder;
use crate::utils::{register_object, remove_object, with_object, handle_error};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let builder = SchemaBuilder::new();
    register_object(builder) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeIsValidFieldName(
    mut env: JNIEnv,
    _class: JClass,
    name: JString,
) -> jboolean {
    let _field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    1 as jboolean
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddTextField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    _stored: jboolean,
    _fast: jboolean,
    _tokenizer_name: JString,
    _index_option: JString,
) {
    let result = with_object::<SchemaBuilder, ()>(ptr as u64, |_builder| {
        // Note: This is a simplified implementation
        // In a complete implementation, we would need to:
        // 1. Properly handle the tokenizer_name and index_option parameters
        // 2. Create appropriate TextOptions with the specified settings
        // 3. Add the field to the builder
    });
    
    if result.is_none() {
        handle_error(&mut env, "Invalid SchemaBuilder pointer");
        return;
    }
    
    let _field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddIntegerField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddFloatField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddUnsignedField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddBooleanField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddDateField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddJsonField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _tokenizer_name: JString,
    _index_option: JString,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddFacetField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddBytesField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
    _index_option: JString,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddIpAddrField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeBuild(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jlong {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}