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
use tantivy::schema::TantivyDocument;
use tantivy::schema::{Schema, OwnedValue, Value, Document};
use std::collections::BTreeMap;
use crate::utils::{register_object, remove_object, with_object_mut, with_object, handle_error};

/// Unified document type that can handle both creation and retrieval
#[derive(Clone)]
pub enum DocumentWrapper {
    Builder(DocumentBuilder),
    Retrieved(RetrievedDocument),
}

/// Intermediate document structure that stores field values by name
#[derive(Clone)]
pub struct DocumentBuilder {
    field_values: BTreeMap<String, Vec<OwnedValue>>,
}

/// Document retrieved from search results with field values
#[derive(Clone)]
pub struct RetrievedDocument {
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
    
    pub fn add_integer(&mut self, field_name: String, value: i64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::I64(value));
    }
    
    pub fn add_float(&mut self, field_name: String, value: f64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::F64(value));
    }
    
    pub fn add_unsigned(&mut self, field_name: String, value: u64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::U64(value));
    }
    
    pub fn add_boolean(&mut self, field_name: String, value: bool) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Bool(value));
    }
    
    pub fn build(self, schema: &Schema) -> Result<TantivyDocument, String> {
        let mut doc = TantivyDocument::new();
        
        for (field_name, values) in self.field_values {
            let field = schema.get_field(&field_name)
                .map_err(|_| format!("Field '{}' not found in schema", field_name))?;
            
            for value in values {
                match value {
                    OwnedValue::Str(text) => doc.add_text(field, &text),
                    OwnedValue::I64(num) => doc.add_i64(field, num),
                    OwnedValue::F64(num) => doc.add_f64(field, num),
                    OwnedValue::U64(num) => doc.add_u64(field, num),
                    OwnedValue::Bool(b) => doc.add_bool(field, b),
                    _ => return Err(format!("Unsupported value type for field '{}'", field_name)),
                }
            }
        }
        
        Ok(doc)
    }
}

impl DocumentWrapper {
    pub fn get_field_values(&self, field_name: &str) -> Option<&Vec<OwnedValue>> {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.get(field_name),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.get(field_name),
        }
    }
    
    pub fn get_all_fields(&self) -> &BTreeMap<String, Vec<OwnedValue>> {
        match self {
            DocumentWrapper::Builder(builder) => &builder.field_values,
            DocumentWrapper::Retrieved(retrieved) => &retrieved.field_values,
        }
    }
    
    pub fn is_empty(&self) -> bool {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.is_empty(),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.is_empty(),
        }
    }
    
    pub fn num_fields(&self) -> usize {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.len(),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.len(),
        }
    }
}

impl RetrievedDocument {
    pub fn new(_document: TantivyDocument) -> Self {
        // For now, create an empty document - we'll need schema to convert properly
        // This is a simplified implementation that will be enhanced later
        Self {
            field_values: BTreeMap::new(),
        }
    }
    
    pub fn new_with_schema(document: TantivyDocument, schema: &tantivy::schema::Schema) -> Self {
        // Following the Python implementation: doc.to_named_doc(schema)
        let named_doc = document.to_named_doc(schema);
        Self {
            field_values: named_doc.0,
        }
    }
    
    pub fn get_field_values(&self, field_name: &str) -> Option<&Vec<OwnedValue>> {
        self.field_values.get(field_name)
    }
    
    pub fn get_all_fields(&self) -> &BTreeMap<String, Vec<OwnedValue>> {
        &self.field_values
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let document_builder = DocumentBuilder::new();
    let wrapper = DocumentWrapper::Builder(document_builder);
    register_object(wrapper) as jlong
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
    ptr: jlong,
    field_name: JString,
) -> jobject {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return std::ptr::null_mut();
        }
    };
    
    let result = with_object::<DocumentWrapper, Result<jobject, String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper.get_field_values(&field_name_str) {
            Some(values) => {
                // Create ArrayList to return the values
                let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
                let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
                
                // Add each value to the ArrayList
                for value in values {
                    let java_value = match value {
                        OwnedValue::Str(s) => {
                            let java_string = env.new_string(s).map_err(|e| e.to_string())?;
                            java_string.into()
                        },
                        OwnedValue::I64(i) => {
                            let long_class = env.find_class("java/lang/Long").map_err(|e| e.to_string())?;
                            env.new_object(&long_class, "(J)V", &[(*i).into()]).map_err(|e| e.to_string())?
                        },
                        OwnedValue::F64(f) => {
                            let double_class = env.find_class("java/lang/Double").map_err(|e| e.to_string())?;
                            env.new_object(&double_class, "(D)V", &[(*f).into()]).map_err(|e| e.to_string())?
                        },
                        OwnedValue::U64(u) => {
                            let long_class = env.find_class("java/lang/Long").map_err(|e| e.to_string())?;
                            env.new_object(&long_class, "(J)V", &[(*u as i64).into()]).map_err(|e| e.to_string())?
                        },
                        OwnedValue::Bool(b) => {
                            let boolean_class = env.find_class("java/lang/Boolean").map_err(|e| e.to_string())?;
                            env.new_object(&boolean_class, "(Z)V", &[(*b).into()]).map_err(|e| e.to_string())?
                        },
                        _ => {
                            // For other types, convert to string for now
                            let string_value = format!("{:?}", value);
                            let java_string = env.new_string(&string_value).map_err(|e| e.to_string())?;
                            java_string.into()
                        }
                    };
                    
                    env.call_method(&array_list, "add", "(Ljava/lang/Object;)Z", &[(&java_value).into()])
                        .map_err(|e| e.to_string())?;
                }
                
                Ok(array_list.as_raw())
            },
            None => {
                // Return empty ArrayList for non-existent fields
                let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
                let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
                Ok(array_list.as_raw())
            }
        }
    });
    
    match result {
        Some(Ok(list_ptr)) => list_ptr,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        },
        None => {
            handle_error(&mut env, "Invalid Document pointer");
            std::ptr::null_mut()
        }
    }
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
    
    let result = with_object_mut::<DocumentWrapper, Result<(), String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper {
            DocumentWrapper::Builder(ref mut doc_builder) => {
                doc_builder.add_text(field_name_str, text_str);
                Ok(())
            },
            DocumentWrapper::Retrieved(_) => {
                Err("Cannot add text to a retrieved document".to_string())
            }
        }
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
    ptr: jlong,
    field_name: JString,
    value: jlong,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<DocumentWrapper, Result<(), String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper {
            DocumentWrapper::Builder(ref mut doc_builder) => {
                doc_builder.add_unsigned(field_name_str, value as u64);
                Ok(())
            },
            DocumentWrapper::Retrieved(_) => {
                Err("Cannot add unsigned to a retrieved document".to_string())
            }
        }
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
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddInteger(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    value: jlong,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<DocumentWrapper, Result<(), String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper {
            DocumentWrapper::Builder(ref mut doc_builder) => {
                doc_builder.add_integer(field_name_str, value as i64);
                Ok(())
            },
            DocumentWrapper::Retrieved(_) => {
                Err("Cannot add integer to a retrieved document".to_string())
            }
        }
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
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddFloat(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    value: f64,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<DocumentWrapper, Result<(), String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper {
            DocumentWrapper::Builder(ref mut doc_builder) => {
                doc_builder.add_float(field_name_str, value);
                Ok(())
            },
            DocumentWrapper::Retrieved(_) => {
                Err("Cannot add float to a retrieved document".to_string())
            }
        }
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
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddBoolean(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    value: jboolean,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<DocumentWrapper, Result<(), String>>(ptr as u64, |doc_wrapper| {
        match doc_wrapper {
            DocumentWrapper::Builder(ref mut doc_builder) => {
                doc_builder.add_boolean(field_name_str, value != 0);
                Ok(())
            },
            DocumentWrapper::Retrieved(_) => {
                Err("Cannot add boolean to a retrieved document".to_string())
            }
        }
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
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_object::<DocumentWrapper, jint>(ptr as u64, |doc_wrapper| {
        doc_wrapper.num_fields() as jint
    }).unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeIsEmpty(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    with_object::<DocumentWrapper, jboolean>(ptr as u64, |doc_wrapper| {
        if doc_wrapper.is_empty() { 1 } else { 0 }
    }).unwrap_or(1)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}