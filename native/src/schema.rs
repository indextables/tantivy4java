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
use jni::sys::{jboolean, jlong, jint, jobject};
use jni::JNIEnv;
use tantivy::schema::{SchemaBuilder, TextOptions, NumericOptions, DateOptions, IpAddrOptions};
use tantivy::schema::{IndexRecordOption, TextFieldIndexing};
use crate::utils::{register_object, remove_object, with_object_mut, handle_error};

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
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    tantivy::schema::is_valid_field_name(&field_name) as jboolean
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
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let tokenizer_name_str: String = match env.get_string(&_tokenizer_name) {
        Ok(s) => s.into(),
        Err(_) => "default".to_string(),
    };
    
    let index_option_str: String = match env.get_string(&_index_option) {
        Ok(s) => s.into(),
        Err(_) => "position".to_string(),
    };
    
    let result = with_object_mut::<SchemaBuilder, Result<(), String>>(ptr as u64, |builder| {
        // Build text options
        let mut text_options = TextOptions::default();
        
        if _stored != 0 {
            text_options = text_options.set_stored();
        }
        
        if _fast != 0 {
            text_options = text_options.set_fast(Some(&tokenizer_name_str));
        }
        
        // Text fields are indexed by default if they have tokenizer and index options
        let should_index = true;
        if should_index {
            let indexing = match index_option_str.as_str() {
                "position" => TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name_str)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                "freq" => TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name_str)
                    .set_index_option(IndexRecordOption::WithFreqs),
                "basic" => TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name_str)
                    .set_index_option(IndexRecordOption::Basic),
                _ => TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name_str)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            };
            
            text_options = text_options.set_indexing_options(indexing);
        }
        
        // Add the field to the builder
        builder.add_text_field(&field_name, text_options);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddIntegerField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<SchemaBuilder, Result<(), String>>(ptr as u64, |builder| {
        let mut options = NumericOptions::default();
        
        if _stored != 0 {
            options = options.set_stored();
        }
        
        if _indexed != 0 {
            options = options.set_indexed();
        }
        
        if _fast != 0 {
            options = options.set_fast();
        }
        
        builder.add_i64_field(&field_name, options);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddFloatField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<SchemaBuilder, Result<(), String>>(ptr as u64, |builder| {
        let mut options = NumericOptions::default();
        
        if _stored != 0 {
            options = options.set_stored();
        }
        
        if _indexed != 0 {
            options = options.set_indexed();
        }
        
        if _fast != 0 {
            options = options.set_fast();
        }
        
        builder.add_f64_field(&field_name, options);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddUnsignedField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<SchemaBuilder, Result<(), String>>(ptr as u64, |builder| {
        let mut options = NumericOptions::default();
        
        if _stored != 0 {
            options = options.set_stored();
        }
        
        if _indexed != 0 {
            options = options.set_indexed();
        }
        
        if _fast != 0 {
            options = options.set_fast();
        }
        
        builder.add_u64_field(&field_name, options);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddBooleanField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    _stored: jboolean,
    _indexed: jboolean,
    _fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    let result = with_object_mut::<SchemaBuilder, Result<(), String>>(ptr as u64, |builder| {
        let mut options = NumericOptions::default();
        
        if _stored != 0 {
            options = options.set_stored();
        }
        
        if _indexed != 0 {
            options = options.set_indexed();
        }
        
        if _fast != 0 {
            options = options.set_fast();
        }
        
        builder.add_bool_field(&field_name, options);
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddDateField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    stored: jboolean,
    indexed: jboolean,
    fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    with_object_mut::<SchemaBuilder, ()>(ptr as u64, |builder| {
        let mut date_options = DateOptions::default();
        
        if stored != 0 {
            date_options = date_options.set_stored();
        }
        
        if indexed != 0 {
            date_options = date_options.set_indexed();
        }
        
        if fast != 0 {
            date_options = date_options.set_fast();
        }
        
        builder.add_date_field(&field_name, date_options);
    });
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
    ptr: jlong,
    name: JString,
    stored: jboolean,
    indexed: jboolean,
    fast: jboolean,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };
    
    with_object_mut::<SchemaBuilder, ()>(ptr as u64, |builder| {
        let mut ip_options = IpAddrOptions::default();
        
        if stored != 0 {
            ip_options = ip_options.set_stored();
        }
        
        if indexed != 0 {
            ip_options = ip_options.set_indexed();
        }
        
        if fast != 0 {
            ip_options = ip_options.set_fast();
        }
        
        builder.add_ip_addr_field(&field_name, ip_options);
    });
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeBuild(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    // For build, we need to consume the SchemaBuilder, so we need to take it out of the registry
    let builder_opt = {
        let mut registry = crate::utils::OBJECT_REGISTRY.lock().unwrap();
        registry.remove(&(ptr as u64)).and_then(|boxed| boxed.downcast::<SchemaBuilder>().ok())
    };
    
    match builder_opt {
        Some(builder) => {
            let schema = builder.build();
            register_object(schema) as jlong
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
            0
        }
    }
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

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetFieldNames(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    use crate::utils::with_object;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        // Create ArrayList to hold field names
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        // Get all fields and add their names to the list
        for (field, _field_entry) in schema.fields() {
            let field_name = schema.get_field_name(field);
            let java_string = env.new_string(field_name).map_err(|e| e.to_string())?;
            
            env.call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&java_string).into()],
            ).map_err(|e| e.to_string())?;
        }
        
        Ok(array_list.into_raw())
    });
    
    match result {
        Some(Ok(list)) => list,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get field names");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeHasField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
) -> jboolean {
    use crate::utils::with_object;
    
    if ptr == 0 {
        return 0;
    }
    
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => return 0,
    };
    
    let result = with_object::<tantivy::schema::Schema, bool>(ptr as u64, |schema| {
        schema.get_field(&field_name_str).is_ok()
    });
    
    result.unwrap_or(false) as jboolean
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetFieldCount(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    use crate::utils::with_object;
    use jni::sys::jint;
    
    if ptr == 0 {
        return 0;
    }
    
    let result = with_object::<tantivy::schema::Schema, usize>(ptr as u64, |schema| {
        schema.fields().count()
    });
    
    result.unwrap_or(0) as jint
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetFieldNamesByType(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_type: jint,
) -> jobject {
    use crate::utils::with_object;
    use tantivy::schema::FieldType as TantivyFieldType;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        // Create ArrayList to hold field names
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        // Map Java FieldType enum values to Tantivy FieldType
        // Java enum values: TEXT(1), UNSIGNED(2), INTEGER(3), FLOAT(4), BOOLEAN(5), DATE(6), FACET(7), BYTES(8), JSON(9), IP_ADDR(10)
        for (field, field_entry) in schema.fields() {
            let tantivy_field_type = field_entry.field_type();
            let matches_type = match field_type {
                1 => matches!(tantivy_field_type, TantivyFieldType::Str(_)), // TEXT
                2 => matches!(tantivy_field_type, TantivyFieldType::U64(_)), // UNSIGNED
                3 => matches!(tantivy_field_type, TantivyFieldType::I64(_)), // INTEGER
                4 => matches!(tantivy_field_type, TantivyFieldType::F64(_)), // FLOAT
                5 => matches!(tantivy_field_type, TantivyFieldType::Bool(_)), // BOOLEAN
                6 => matches!(tantivy_field_type, TantivyFieldType::Date(_)), // DATE
                7 => matches!(tantivy_field_type, TantivyFieldType::Facet(_)), // FACET
                8 => matches!(tantivy_field_type, TantivyFieldType::Bytes(_)), // BYTES
                9 => matches!(tantivy_field_type, TantivyFieldType::JsonObject(_)), // JSON
                10 => matches!(tantivy_field_type, TantivyFieldType::IpAddr(_)), // IP_ADDR
                _ => false,
            };
            
            if matches_type {
                let field_name = schema.get_field_name(field);
                let java_string = env.new_string(field_name).map_err(|e| e.to_string())?;
                
                env.call_method(
                    &array_list,
                    "add",
                    "(Ljava/lang/Object;)Z",
                    &[(&java_string).into()],
                ).map_err(|e| e.to_string())?;
            }
        }
        
        Ok(array_list.into_raw())
    });
    
    match result {
        Some(Ok(list)) => list,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get field names by type");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetFieldNamesByCapabilities(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    stored: jint,
    indexed: jint,
    fast: jint,
) -> jobject {
    use crate::utils::with_object;
    use tantivy::schema::FieldType as TantivyFieldType;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        // Create ArrayList to hold field names
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        for (field, field_entry) in schema.fields() {
            let field_type = field_entry.field_type();
            
            // Check stored capability
            let is_stored = match field_type {
                TantivyFieldType::Str(text_options) => text_options.is_stored(),
                TantivyFieldType::U64(numeric_options) => numeric_options.is_stored(),
                TantivyFieldType::I64(numeric_options) => numeric_options.is_stored(),
                TantivyFieldType::F64(numeric_options) => numeric_options.is_stored(),
                TantivyFieldType::Bool(numeric_options) => numeric_options.is_stored(),
                TantivyFieldType::Date(date_options) => date_options.is_stored(),
                TantivyFieldType::Facet(facet_options) => facet_options.is_stored(),
                TantivyFieldType::Bytes(bytes_options) => bytes_options.is_stored(),
                TantivyFieldType::JsonObject(json_options) => json_options.is_stored(),
                TantivyFieldType::IpAddr(ip_options) => ip_options.is_stored(),
            };
            
            // Check indexed capability
            let is_indexed = match field_type {
                TantivyFieldType::Str(text_options) => text_options.get_indexing_options().is_some(),
                TantivyFieldType::U64(numeric_options) => numeric_options.is_indexed(),
                TantivyFieldType::I64(numeric_options) => numeric_options.is_indexed(),
                TantivyFieldType::F64(numeric_options) => numeric_options.is_indexed(),
                TantivyFieldType::Bool(numeric_options) => numeric_options.is_indexed(),
                TantivyFieldType::Date(date_options) => date_options.is_indexed(),
                TantivyFieldType::Facet(_) => true, // Facets are always indexed
                TantivyFieldType::Bytes(bytes_options) => bytes_options.is_indexed(),
                TantivyFieldType::JsonObject(_json_options) => false, // JSON objects don't have indexed capability in this version
                TantivyFieldType::IpAddr(ip_options) => ip_options.is_indexed(),
            };
            
            // Check fast capability
            let is_fast = match field_type {
                TantivyFieldType::Str(text_options) => text_options.is_fast(),
                TantivyFieldType::U64(numeric_options) => numeric_options.is_fast(),
                TantivyFieldType::I64(numeric_options) => numeric_options.is_fast(),
                TantivyFieldType::F64(numeric_options) => numeric_options.is_fast(),
                TantivyFieldType::Bool(numeric_options) => numeric_options.is_fast(),
                TantivyFieldType::Date(date_options) => date_options.is_fast(),
                TantivyFieldType::Facet(_) => false, // Facets are not fast fields
                TantivyFieldType::Bytes(bytes_options) => bytes_options.is_fast(),
                TantivyFieldType::JsonObject(_) => false, // JSON objects are not fast fields
                TantivyFieldType::IpAddr(ip_options) => ip_options.is_fast(),
            };
            
            // Check if this field matches the filter criteria
            let matches_stored = stored == -1 || (stored == 1 && is_stored) || (stored == 0 && !is_stored);
            let matches_indexed = indexed == -1 || (indexed == 1 && is_indexed) || (indexed == 0 && !is_indexed);
            let matches_fast = fast == -1 || (fast == 1 && is_fast) || (fast == 0 && !is_fast);
            
            if matches_stored && matches_indexed && matches_fast {
                let field_name = schema.get_field_name(field);
                let java_string = env.new_string(field_name).map_err(|e| e.to_string())?;
                
                env.call_method(
                    &array_list,
                    "add",
                    "(Ljava/lang/Object;)Z",
                    &[(&java_string).into()],
                ).map_err(|e| e.to_string())?;
            }
        }
        
        Ok(array_list.into_raw())
    });
    
    match result {
        Some(Ok(list)) => list,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get field names by capabilities");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetFieldInfo(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
) -> jobject {
    use crate::utils::with_object;
    use tantivy::schema::FieldType as TantivyFieldType;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Ok(std::ptr::null_mut()),
        };
        
        let field_entry = schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        
        // Determine Java FieldType enum value
        let (java_field_type, stored, indexed, fast, tokenizer_name, index_option) = match field_type {
            TantivyFieldType::Str(text_options) => {
                let indexing_options = text_options.get_indexing_options();
                let tokenizer = indexing_options.as_ref()
                    .and_then(|opts| Some(opts.tokenizer().to_string()))
                    .unwrap_or_else(|| "default".to_string());
                let index_opt = indexing_options.as_ref()
                    .map(|opts| match opts.index_option() {
                        tantivy::schema::IndexRecordOption::Basic => "basic",
                        tantivy::schema::IndexRecordOption::WithFreqs => "freq", 
                        tantivy::schema::IndexRecordOption::WithFreqsAndPositions => "position",
                    })
                    .unwrap_or("basic")
                    .to_string();
                (1, text_options.is_stored(), indexing_options.is_some(), text_options.is_fast(), Some(tokenizer), Some(index_opt))
            },
            TantivyFieldType::U64(numeric_options) => (2, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
            TantivyFieldType::I64(numeric_options) => (3, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
            TantivyFieldType::F64(numeric_options) => (4, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
            TantivyFieldType::Bool(numeric_options) => (5, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
            TantivyFieldType::Date(date_options) => (6, date_options.is_stored(), date_options.is_indexed(), date_options.is_fast(), None, None),
            TantivyFieldType::Facet(facet_options) => (7, facet_options.is_stored(), true, false, None, None),
            TantivyFieldType::Bytes(bytes_options) => (8, bytes_options.is_stored(), bytes_options.is_indexed(), bytes_options.is_fast(), None, None),
            TantivyFieldType::JsonObject(json_options) => (9, json_options.is_stored(), false, false, None, None),
            TantivyFieldType::IpAddr(ip_options) => (10, ip_options.is_stored(), ip_options.is_indexed(), ip_options.is_fast(), None, None),
        };
        
        // Create FieldType enum by getting the enum value through valueOf
        let field_type_class = env.find_class("com/tantivy4java/FieldType").map_err(|e| e.to_string())?;
        let field_type_name = match java_field_type {
            1 => "TEXT",
            2 => "UNSIGNED", 
            3 => "INTEGER",
            4 => "FLOAT",
            5 => "BOOLEAN",
            6 => "DATE",
            7 => "FACET",
            8 => "BYTES",
            9 => "JSON",
            10 => "IP_ADDR",
            _ => "TEXT",
        };
        let field_type_name_str = env.new_string(field_type_name).map_err(|e| e.to_string())?;
        let field_type_obj = env.call_static_method(
            &field_type_class,
            "valueOf",
            "(Ljava/lang/String;)Lcom/tantivy4java/FieldType;",
            &[(&field_type_name_str).into()]
        ).map_err(|e| e.to_string())?.l().map_err(|e| e.to_string())?;
        
        // Create FieldInfo object
        let field_info_class = env.find_class("com/tantivy4java/FieldInfo").map_err(|e| e.to_string())?;
        
        let field_info = if let (Some(tokenizer), Some(index_opt)) = (tokenizer_name, index_option) {
            // Text field constructor with tokenizer and index option
            let tokenizer_str = env.new_string(tokenizer).map_err(|e| e.to_string())?;
            let index_opt_str = env.new_string(index_opt).map_err(|e| e.to_string())?;
            let name_str = env.new_string(&field_name_str).map_err(|e| e.to_string())?;
            
            env.new_object(
                &field_info_class,
                "(Ljava/lang/String;Lcom/tantivy4java/FieldType;ZZZLjava/lang/String;Ljava/lang/String;)V",
                &[
                    (&name_str).into(),
                    (&field_type_obj).into(),
                    (stored as jboolean).into(),
                    (indexed as jboolean).into(),
                    (fast as jboolean).into(),
                    (&tokenizer_str).into(),
                    (&index_opt_str).into(),
                ]
            ).map_err(|e| e.to_string())?
        } else {
            // Non-text field constructor
            let name_str = env.new_string(&field_name_str).map_err(|e| e.to_string())?;
            
            env.new_object(
                &field_info_class,
                "(Ljava/lang/String;Lcom/tantivy4java/FieldType;ZZZ)V",
                &[
                    (&name_str).into(),
                    (&field_type_obj).into(),
                    (stored as jboolean).into(),
                    (indexed as jboolean).into(),
                    (fast as jboolean).into(),
                ]
            ).map_err(|e| e.to_string())?
        };
        
        Ok(field_info.into_raw())
    });
    
    match result {
        Some(Ok(field_info)) => field_info,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get field info");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetAllFieldInfo(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    use crate::utils::with_object;
    use tantivy::schema::FieldType as TantivyFieldType;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        // Create ArrayList to hold FieldInfo objects
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        for (field, field_entry) in schema.fields() {
            let field_name = schema.get_field_name(field);
            let field_type = field_entry.field_type();
            
            // Determine Java FieldType enum value and field properties
            let (java_field_type, stored, indexed, fast, tokenizer_name, index_option) = match field_type {
                TantivyFieldType::Str(text_options) => {
                    let indexing_options = text_options.get_indexing_options();
                    let tokenizer = indexing_options.as_ref()
                        .and_then(|opts| Some(opts.tokenizer().to_string()))
                        .unwrap_or_else(|| "default".to_string());
                    let index_opt = indexing_options.as_ref()
                        .map(|opts| match opts.index_option() {
                            tantivy::schema::IndexRecordOption::Basic => "basic",
                            tantivy::schema::IndexRecordOption::WithFreqs => "freq", 
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions => "position",
                        })
                        .unwrap_or("basic")
                        .to_string();
                    (1, text_options.is_stored(), indexing_options.is_some(), text_options.is_fast(), Some(tokenizer), Some(index_opt))
                },
                TantivyFieldType::U64(numeric_options) => (2, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
                TantivyFieldType::I64(numeric_options) => (3, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
                TantivyFieldType::F64(numeric_options) => (4, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
                TantivyFieldType::Bool(numeric_options) => (5, numeric_options.is_stored(), numeric_options.is_indexed(), numeric_options.is_fast(), None, None),
                TantivyFieldType::Date(date_options) => (6, date_options.is_stored(), date_options.is_indexed(), date_options.is_fast(), None, None),
                TantivyFieldType::Facet(facet_options) => (7, facet_options.is_stored(), true, false, None, None),
                TantivyFieldType::Bytes(bytes_options) => (8, bytes_options.is_stored(), bytes_options.is_indexed(), bytes_options.is_fast(), None, None),
                TantivyFieldType::JsonObject(json_options) => (9, json_options.is_stored(), false, false, None, None),
                TantivyFieldType::IpAddr(ip_options) => (10, ip_options.is_stored(), ip_options.is_indexed(), ip_options.is_fast(), None, None),
            };
            
            // Create FieldType enum by getting the enum value through valueOf
            let field_type_class = env.find_class("com/tantivy4java/FieldType").map_err(|e| e.to_string())?;
            let field_type_name = match java_field_type {
                1 => "TEXT",
                2 => "UNSIGNED", 
                3 => "INTEGER",
                4 => "FLOAT",
                5 => "BOOLEAN",
                6 => "DATE",
                7 => "FACET",
                8 => "BYTES",
                9 => "JSON",
                10 => "IP_ADDR",
                _ => "TEXT",
            };
            let field_type_name_str = env.new_string(field_type_name).map_err(|e| e.to_string())?;
            let field_type_obj = env.call_static_method(
                &field_type_class,
                "valueOf",
                "(Ljava/lang/String;)Lcom/tantivy4java/FieldType;",
                &[(&field_type_name_str).into()]
            ).map_err(|e| e.to_string())?.l().map_err(|e| e.to_string())?;
            
            // Create FieldInfo object
            let field_info_class = env.find_class("com/tantivy4java/FieldInfo").map_err(|e| e.to_string())?;
            
            let field_info = if let (Some(tokenizer), Some(index_opt)) = (tokenizer_name, index_option) {
                // Text field constructor with tokenizer and index option
                let tokenizer_str = env.new_string(tokenizer).map_err(|e| e.to_string())?;
                let index_opt_str = env.new_string(index_opt).map_err(|e| e.to_string())?;
                let name_str = env.new_string(field_name).map_err(|e| e.to_string())?;
                
                env.new_object(
                    &field_info_class,
                    "(Ljava/lang/String;Lcom/tantivy4java/FieldType;ZZZLjava/lang/String;Ljava/lang/String;)V",
                    &[
                        (&name_str).into(),
                        (&field_type_obj).into(),
                        (stored as jboolean).into(),
                        (indexed as jboolean).into(),
                        (fast as jboolean).into(),
                        (&tokenizer_str).into(),
                        (&index_opt_str).into(),
                    ]
                ).map_err(|e| e.to_string())?
            } else {
                // Non-text field constructor
                let name_str = env.new_string(field_name).map_err(|e| e.to_string())?;
                
                env.new_object(
                    &field_info_class,
                    "(Ljava/lang/String;Lcom/tantivy4java/FieldType;ZZZ)V",
                    &[
                        (&name_str).into(),
                        (&field_type_obj).into(),
                        (stored as jboolean).into(),
                        (indexed as jboolean).into(),
                        (fast as jboolean).into(),
                    ]
                ).map_err(|e| e.to_string())?
            };
            
            // Add FieldInfo to the list
            env.call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&field_info).into()],
            ).map_err(|e| e.to_string())?;
        }
        
        Ok(array_list.into_raw())
    });
    
    match result {
        Some(Ok(list)) => list,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get all field info");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetSchemaSummary(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    use crate::utils::with_object;
    use jni::sys::jobject;
    
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    let result = with_object::<tantivy::schema::Schema, Result<jobject, String>>(ptr as u64, |schema| {
        let mut summary = String::new();
        summary.push_str(&format!("Schema with {} fields:\n", schema.fields().count()));
        
        for (field, field_entry) in schema.fields() {
            let field_name = schema.get_field_name(field);
            summary.push_str(&format!("  - {}: {:?}\n", field_name, field_entry.field_type()));
        }
        
        let java_string = env.new_string(summary).map_err(|e| e.to_string())?;
        Ok(java_string.into_raw())
    });
    
    match result {
        Some(Ok(summary)) => summary,
        Some(Err(_)) | None => {
            handle_error(&mut env, "Failed to get schema summary");
            std::ptr::null_mut()
        }
    }
}