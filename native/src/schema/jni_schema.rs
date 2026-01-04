// jni_schema.rs - Schema introspection JNI functions
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, jint, jobject};
use jni::JNIEnv;
use tantivy::schema::FieldType as TantivyFieldType;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::debug_println;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    // Release from Arc registry (all schemas should now be in Arc registry)
    release_arc(ptr);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetFieldNames(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    // Debug logging
    debug_println!("RUST DEBUG: nativeGetFieldNames called with pointer: {}", ptr);

    // Try Arc-based access first (for SplitSearcher schemas)
    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        debug_println!("RUST DEBUG: Arc-based access successful for pointer: {}", ptr);
        let schema = schema_arc.as_ref();
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

    // If Arc-based access failed, try regular object access (for SchemaBuilder schemas)
    let result = match result {
        Some(result) => Some(result),
        None => with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
            let schema = schema_arc.as_ref();
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
        })
    };

    match result {
        Some(Ok(list)) => {
            debug_println!("RUST DEBUG: nativeGetFieldNames - successfully created field names list");
            list
        },
        Some(Err(e)) => {
            debug_println!("RUST DEBUG: nativeGetFieldNames - with_arc_safe returned error: {}", e);
            handle_error(&mut env, "Failed to get field names");
            std::ptr::null_mut()
        },
        None => {
            debug_println!("RUST DEBUG: nativeGetFieldNames - with_arc_safe returned None, jlong_to_arc failed");
            handle_error(&mut env, "Failed to get field names");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeHasField(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
) -> jboolean {
    if ptr == 0 {
        return 0;
    }

    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => return 0,
    };

    // Try Arc-based access first (for SplitSearcher schemas), then regular object access
    let result = with_arc_safe::<tantivy::schema::Schema, bool>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        schema.get_field(&field_name_str).is_ok()
    }).or_else(|| with_arc_safe::<tantivy::schema::Schema, bool>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        schema.get_field(&field_name_str).is_ok()
    }));

    result.unwrap_or(false) as jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetFieldCount(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    if ptr == 0 {
        return 0;
    }

    // Try Arc-based access first (for SplitSearcher schemas), then regular object access
    let result = with_arc_safe::<tantivy::schema::Schema, usize>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        schema.fields().count()
    }).or_else(|| with_arc_safe::<tantivy::schema::Schema, usize>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        schema.fields().count()
    }));

    result.unwrap_or(0) as jint
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetFieldNamesByType(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_type: jint,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetFieldNamesByCapabilities(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    stored: jint,
    indexed: jint,
    fast: jint,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetFieldInfo(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
        let field_type_class = env.find_class("io/indextables/tantivy4java/core/FieldType").map_err(|e| e.to_string())?;
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
            "(Ljava/lang/String;)Lio/indextables/tantivy4java/core/FieldType;",
            &[(&field_type_name_str).into()]
        ).map_err(|e| e.to_string())?.l().map_err(|e| e.to_string())?;

        // Create FieldInfo object
        let field_info_class = env.find_class("io/indextables/tantivy4java/core/FieldInfo").map_err(|e| e.to_string())?;

        let field_info = if let (Some(tokenizer), Some(index_opt)) = (tokenizer_name, index_option) {
            // Text field constructor with tokenizer and index option
            let tokenizer_str = env.new_string(tokenizer).map_err(|e| e.to_string())?;
            let index_opt_str = env.new_string(index_opt).map_err(|e| e.to_string())?;
            let name_str = env.new_string(&field_name_str).map_err(|e| e.to_string())?;

            env.new_object(
                &field_info_class,
                "(Ljava/lang/String;Lio/indextables/tantivy4java/core/FieldType;ZZZLjava/lang/String;Ljava/lang/String;)V",
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
                "(Ljava/lang/String;Lio/indextables/tantivy4java/core/FieldType;ZZZ)V",
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetAllFieldInfo(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
            let field_type_class = env.find_class("io/indextables/tantivy4java/core/FieldType").map_err(|e| e.to_string())?;
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
                "(Ljava/lang/String;)Lio/indextables/tantivy4java/core/FieldType;",
                &[(&field_type_name_str).into()]
            ).map_err(|e| e.to_string())?.l().map_err(|e| e.to_string())?;

            // Create FieldInfo object
            let field_info_class = env.find_class("io/indextables/tantivy4java/core/FieldInfo").map_err(|e| e.to_string())?;

            let field_info = if let (Some(tokenizer), Some(index_opt)) = (tokenizer_name, index_option) {
                // Text field constructor with tokenizer and index option
                let tokenizer_str = env.new_string(tokenizer).map_err(|e| e.to_string())?;
                let index_opt_str = env.new_string(index_opt).map_err(|e| e.to_string())?;
                let name_str = env.new_string(field_name).map_err(|e| e.to_string())?;

                env.new_object(
                    &field_info_class,
                    "(Ljava/lang/String;Lio/indextables/tantivy4java/core/FieldType;ZZZLjava/lang/String;Ljava/lang/String;)V",
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
                    "(Ljava/lang/String;Lio/indextables/tantivy4java/core/FieldType;ZZZ)V",
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
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeGetSchemaSummary(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jobject, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Schema_nativeClone(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    if ptr == 0 {
        handle_error(&mut env, "Cannot clone: Invalid Schema pointer");
        return 0;
    }

    debug_println!("RUST DEBUG: nativeClone called with pointer: {}", ptr);

    let result = with_arc_safe::<tantivy::schema::Schema, Result<jlong, String>>(ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Clone the schema - Tantivy's Schema implements Clone
        let cloned_schema = schema.clone();

        debug_println!("RUST DEBUG: Schema cloned successfully, original fields: {}, cloned fields: {}",
                     schema.fields().count(), cloned_schema.fields().count());

        // Create a new Arc for the cloned schema and register it
        let cloned_arc = Arc::new(cloned_schema);
        let new_ptr = arc_to_jlong(cloned_arc);

        debug_println!("RUST DEBUG: Cloned schema registered with new pointer: {}", new_ptr);

        Ok(new_ptr)
    });

    match result {
        Some(Ok(cloned_ptr)) => {
            debug_println!("RUST DEBUG: nativeClone completed successfully, returning pointer: {}", cloned_ptr);
            cloned_ptr
        },
        Some(Err(e)) => {
            debug_println!("RUST DEBUG: nativeClone failed with error: {}", e);
            handle_error(&mut env, &format!("Failed to clone schema: {}", e));
            0
        },
        None => {
            debug_println!("RUST DEBUG: nativeClone failed - invalid pointer or Arc access failed");
            handle_error(&mut env, "Cannot clone: Invalid Schema pointer or schema already closed");
            0
        }
    }
}
