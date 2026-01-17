// jni_builder.rs - SchemaBuilder JNI functions
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, jint};
use jni::JNIEnv;
use tantivy::schema::{SchemaBuilder, TextOptions, NumericOptions, DateOptions, IpAddrOptions, JsonObjectOptions, BytesOptions};
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, DateTimePrecision};
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use std::sync::{Arc, Mutex};

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let builder = SchemaBuilder::new();
    let builder_arc = Arc::new(Mutex::new(builder));
    arc_to_jlong(builder_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeIsValidFieldName(
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddTextField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddIntegerField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddFloatField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddUnsignedField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddBooleanField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddDateField(
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

    with_arc_safe::<Mutex<SchemaBuilder>, ()>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
        let mut date_options = DateOptions::default();

        if stored != 0 {
            date_options = date_options.set_stored();
        }

        if indexed != 0 {
            date_options = date_options.set_indexed();
        }

        // Set microsecond precision for ALL date fields (not just fast fields)
        // This ensures precision is preserved regardless of storage mode.
        // Note: Inverted index precision remains at seconds (hardcoded in Tantivy core),
        // but this affects fast fields and may affect stored field serialization.
        date_options = date_options.set_precision(DateTimePrecision::Microseconds);

        if fast != 0 {
            date_options = date_options.set_fast();
        }

        builder.add_date_field(&field_name, date_options);
    });
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddJsonField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    stored: jboolean,
    tokenizer_name: JString,
    index_option: JString,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let tokenizer_str: String = match env.get_string(&tokenizer_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid tokenizer name");
            return;
        }
    };

    let index_option_str: String = match env.get_string(&index_option) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid index option");
            return;
        }
    };

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();

        // Create JSON field options
        let mut json_options = JsonObjectOptions::default();

        if stored != 0 {
            json_options = json_options.set_stored();
        }

        // Set up indexing with the specified tokenizer
        let index_record_option = match index_option_str.as_str() {
            "position" => IndexRecordOption::WithFreqsAndPositions,
            "freq" => IndexRecordOption::WithFreqs,
            "basic" => IndexRecordOption::Basic,
            _ => IndexRecordOption::Basic,
        };

        let indexing = TextFieldIndexing::default()
            .set_tokenizer(&tokenizer_str)
            .set_index_option(index_record_option);

        json_options = json_options.set_indexing_options(indexing);

        // Add the JSON field to the builder
        builder.add_json_field(&field_name, json_options);
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddJsonFieldWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    stored: jboolean,
    indexed: jboolean,
    fast: jboolean,
    fast_tokenizer: JString,
    expand_dots: jboolean,
    tokenizer_name: JString,
    record_option: jint,
) -> jint {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return -1;
        }
    };

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<tantivy::schema::Field, String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();

        // Create JSON field options
        let mut json_options = JsonObjectOptions::default();

        // Set stored flag
        if stored != 0 {
            json_options = json_options.set_stored();
        }

        // Set indexing options if enabled
        if indexed != 0 {
            let tokenizer_str: String = if !tokenizer_name.is_null() {
                match env.get_string(&tokenizer_name) {
                    Ok(s) => s.into(),
                    Err(_) => "default".to_string(),
                }
            } else {
                "default".to_string()
            };

            let index_record_option = match record_option {
                0 => IndexRecordOption::Basic,
                1 => IndexRecordOption::WithFreqs,
                2 => IndexRecordOption::WithFreqsAndPositions,
                _ => IndexRecordOption::WithFreqsAndPositions,
            };

            let indexing = TextFieldIndexing::default()
                .set_tokenizer(&tokenizer_str)
                .set_index_option(index_record_option);

            json_options = json_options.set_indexing_options(indexing);
        }

        // Set fast field options if enabled
        if fast != 0 {
            if !fast_tokenizer.is_null() {
                match env.get_string(&fast_tokenizer) {
                    Ok(s) => {
                        let fast_tok_str: String = s.into();
                        json_options = json_options.set_fast(Some(&fast_tok_str));
                    },
                    Err(_) => {
                        json_options = json_options.set_fast(None);
                    }
                }
            } else {
                json_options = json_options.set_fast(None);
            }
        }

        // Set expand_dots flag
        if expand_dots != 0 {
            json_options = json_options.set_expand_dots_enabled();
        }

        // Add the JSON field to the builder and return the field
        let field = builder.add_json_field(&field_name, json_options);
        Ok(field)
    });

    match result {
        Some(Ok(field)) => field.field_id() as jint,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            -1
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
            -1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddFacetField(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
) {
    handle_error(&mut env, "SchemaBuilder native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddBytesField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
    stored: jboolean,
    indexed: jboolean,
    fast: jboolean,
    _index_option: JString,
) {
    let field_name: String = match env.get_string(&name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
        let mut options = BytesOptions::default();

        if stored != 0 {
            options = options.set_stored();
        }

        if indexed != 0 {
            options = options.set_indexed();
        }

        if fast != 0 {
            options = options.set_fast();
        }

        builder.add_bytes_field(&field_name, options);
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddIpAddrField(
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

    with_arc_safe::<Mutex<SchemaBuilder>, ()>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeBuild(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    // For build, we need to consume the SchemaBuilder, so we need to take it out of the Arc registry
    let builder_arc = {
        let mut registry = crate::utils::ARC_REGISTRY.lock().unwrap();
        registry.remove(&ptr).and_then(|boxed| boxed.downcast::<Arc<Mutex<SchemaBuilder>>>().ok().map(|b| *b))
    };

    match builder_arc {
        Some(arc) => {
            // Try to extract the SchemaBuilder from the Arc<Mutex<SchemaBuilder>>
            match Arc::try_unwrap(arc) {
                Ok(mutex) => {
                    let builder = mutex.into_inner().unwrap();
                    let schema = builder.build();
                    // Use Arc registry for the built schema
                    let schema_arc = Arc::new(schema);
                    arc_to_jlong(schema_arc)
                },
                Err(_) => {
                    handle_error(&mut env, "Cannot build schema: SchemaBuilder is still in use");
                    0
                }
            }
        },
        None => {
            handle_error(&mut env, "Invalid SchemaBuilder pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddStringField(
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

    let result = with_arc_safe::<Mutex<SchemaBuilder>, Result<(), String>>(ptr, |builder_mutex| {
        let mut builder = builder_mutex.lock().unwrap();
        let mut text_options = TextOptions::default();

        if stored != 0 {
            text_options = text_options.set_stored();
        }

        if fast != 0 {
            // For string fields, we use "raw" tokenizer for exact matching
            text_options = text_options.set_fast(Some("raw"));
        }

        if indexed != 0 {
            // For string fields, use "raw" tokenizer which doesn't tokenize,
            // providing exact string matching behavior
            let indexing = TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic);

            text_options = text_options.set_indexing_options(indexing);
        }

        // Add the field to the builder as a text field with raw tokenizer
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
