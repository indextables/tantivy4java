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
        
        // Set indexing options
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
    use jni::objects::JObject;
    use jni::sys::jobject;
    
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