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

use jni::objects::{JClass, JString, JByteArray, JObject};
use jni::sys::{jlong, jboolean, jint, jobject};
use jni::JNIEnv;
use tantivy::TantivyDocument;
use tantivy::schema::{Schema, OwnedValue};
use std::collections::BTreeMap;
use crate::utils::{register_object, remove_object, with_object_mut, handle_error};

/// Intermediate document structure that stores field values by name
#[derive(Clone)]
pub struct DocumentBuilder {
    field_values: BTreeMap<String, Vec<OwnedValue>>,
}

impl DocumentBuilder {
    pub fn new() -> Self {
        Self {
            field_values: BTreeMap::new(),
        }
    }
    
    pub fn add_text(&mut self, field_name: String, text: String) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Str(text));
    }
    
    pub fn build(self, schema: &Schema) -> Result<TantivyDocument, String> {
        let mut doc = TantivyDocument::new();
        
        for (field_name, values) in self.field_values {
            let field = schema.get_field(&field_name)
                .map_err(|_| format!("Field '{}' not found in schema", field_name))?;
            
            for value in values {
                match value {
                    OwnedValue::Str(text) => doc.add_text(field, &text),
                    _ => return Err(format!("Unsupported value type for field '{}'", field_name)),
                }
            }
        }
        
        Ok(doc)
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let document_builder = DocumentBuilder::new();
    register_object(document_builder) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeFromMap(
    mut env: JNIEnv,
    _class: JClass,
    _fields: JObject,
    _schema_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGet(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
) -> jobject {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeToMap(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeExtend(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _fields: JObject,
    _schema_ptr: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddText(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    text: JString,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid text value");
            return;
        }
    };
    
    let result = with_object_mut::<DocumentBuilder, Result<(), String>>(ptr as u64, |doc_builder| {
        doc_builder.add_text(field_name_str, text_str);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddUnsigned(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _value: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddInteger(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _value: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddFloat(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _value: f64,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddBoolean(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _value: jboolean,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddDate(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _date: JObject,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddFacet(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _facet_ptr: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddBytes(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _bytes: JByteArray,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddJson(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _value: JObject,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddIpAddr(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _ip_addr: JString,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetNumFields(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jint {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeIsEmpty(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jboolean {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}