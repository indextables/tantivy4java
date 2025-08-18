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
use tantivy::{Index as TantivyIndex, IndexSettings, IndexWriter as TantivyIndexWriter};
use tantivy::schema::Schema;
use tantivy::query::{Query as TantivyQuery, QueryParser};
use tantivy::directory::MmapDirectory;
use crate::utils::{register_object, remove_object, with_object, handle_error};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeNew(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _path: JString,
    _reuse: jboolean,
) -> jlong {
    let path_str: String = match env.get_string(&_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid path");
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<TantivyIndex, String>>(_schema_ptr as u64, |schema| {
        if path_str.is_empty() {
            // Create in-memory index
            Ok(TantivyIndex::create_in_ram(schema.clone()))
        } else {
            // Create index in directory
            let dir = MmapDirectory::open(&path_str).map_err(|e| e.to_string())?;
            let settings = IndexSettings::default();
            TantivyIndex::create(dir, schema.clone(), settings).map_err(|e| e.to_string())
        }
    });
    
    match result {
        Some(Ok(index)) => {
            register_object(index) as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Schema pointer");
            0
        }
    }
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
    ptr: jlong,
    heap_size: jint,
    num_threads: jint,
) -> jlong {
    let result = with_object::<TantivyIndex, Result<TantivyIndexWriter, String>>(ptr as u64, |index| {
        let heap_size_mb = if heap_size > 0 { heap_size as usize } else { 50 };
        let num_threads_val = if num_threads > 0 { num_threads as usize } else { 1 };
        
        index.writer_with_num_threads(num_threads_val, heap_size_mb * 1_000_000)
            .map_err(|e| e.to_string())
    });
    
    match result {
        Some(Ok(writer)) => {
            register_object(writer) as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
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
    ptr: jlong,
) -> jlong {
    let result = with_object::<TantivyIndex, Result<tantivy::Searcher, String>>(ptr as u64, |index| {
        let reader = index.reader().map_err(|e| e.to_string())?;
        Ok(reader.searcher())
    });
    
    match result {
        Some(Ok(searcher)) => {
            register_object(searcher) as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
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
    ptr: jlong,
) {
    let result = with_object::<TantivyIndex, Result<(), String>>(ptr as u64, |_index| {
        // In Tantivy, we don't need to explicitly reload the index
        // The reader will automatically pick up changes when a new searcher is created
        // But we can call this to ensure any pending changes are committed
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeParseQuery(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query: JString,
    default_field_names: JObject,
    _field_boosts: JObject,
    _fuzzy_fields: JObject,
) -> jlong {
    let query_str: String = match env.get_string(&query) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid query string");
            return 0;
        }
    };
    
    // Extract default field names from the Java List
    let field_names = if !default_field_names.is_null() {
        match extract_string_list(&mut env, &default_field_names) {
            Ok(names) => names,
            Err(e) => {
                handle_error(&mut env, &e);
                return 0;
            }
        }
    } else {
        Vec::new()
    };
    
    let result = with_object::<TantivyIndex, Result<Box<dyn TantivyQuery>, String>>(ptr as u64, |index| {
        let schema = index.schema();
        
        // Get default fields
        let default_fields: Vec<_> = if field_names.is_empty() {
            // Use all indexed fields if no default fields specified
            schema.fields()
                .filter(|(_, field_entry)| field_entry.is_indexed())
                .map(|(field, _)| field)
                .collect()
        } else {
            // Convert field names to field handles
            field_names.iter()
                .map(|name| {
                    schema.get_field(name).map_err(|_| {
                        format!("Field '{}' is not defined in the schema", name)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        
        if default_fields.is_empty() {
            return Err("No indexed fields available for query parsing".to_string());
        }
        
        // Create query parser
        let query_parser = QueryParser::for_index(index, default_fields);
        
        // Parse the query
        let parsed_query = query_parser.parse_query(&query_str)
            .map_err(|e| format!("Query parsing error: {}", e))?;
        
        Ok(parsed_query)
    });
    
    match result {
        Some(Ok(query)) => {
            register_object(query) as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
}

// Helper function to extract string list from Java List
fn extract_string_list(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut strings = Vec::with_capacity(size as usize);
    
    // Extract each string from the list
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
        
        strings.push(rust_string);
    }
    
    Ok(strings)
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