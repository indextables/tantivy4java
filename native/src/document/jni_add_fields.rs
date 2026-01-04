// jni_add_fields.rs - JNI functions for adding field values to documents
// Extracted from document.rs during refactoring

use std::net::IpAddr;
use std::sync::Mutex;

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;

use crate::utils::{handle_error, with_arc_safe};

use super::helpers::convert_java_localdatetime_to_tantivy;
use super::types::DocumentWrapper;

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeExtend(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _fields: JObject,
    _schema_ptr: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddText(
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

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    doc_builder.add_text(field_name_str, text_str);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add text to a retrieved document".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddUnsigned(
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

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    doc_builder.add_unsigned(field_name_str, value as u64);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add unsigned to a retrieved document".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddInteger(
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

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    doc_builder.add_integer(field_name_str, value as i64);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add integer to a retrieved document".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddFloat(
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

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    doc_builder.add_float(field_name_str, value);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add float to a retrieved document".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddBoolean(
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

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    doc_builder.add_boolean(field_name_str, value != 0);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add boolean to a retrieved document".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddDate(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    date: JObject,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    // Convert Java LocalDateTime to DateTime
    let tantivy_datetime = match convert_java_localdatetime_to_tantivy(&mut env, &date) {
        Ok(dt) => dt,
        Err(e) => {
            handle_error(&mut env, &e);
            return;
        }
    };

    with_arc_safe::<Mutex<DocumentWrapper>, ()>(ptr, |wrapper_mutex| {
        let mut wrapper = wrapper_mutex.lock().unwrap();
        match &mut *wrapper {
            DocumentWrapper::Builder(doc_builder) => {
                doc_builder.add_date(field_name_str, tantivy_datetime);
            }
            DocumentWrapper::Retrieved(_) => {
                // Retrieved documents are read-only
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddFacet(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _facet_ptr: jlong,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddBytes(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _field_name: JString,
    _bytes: JByteArray,
) {
    handle_error(&mut env, "Document native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddJson(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    json_string: JString,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let json_str: String = match env.get_string(&json_string) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON string");
            return;
        }
    };

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    // Parse JSON string into a BTreeMap<String, OwnedValue>
                    let json_value: std::collections::BTreeMap<String, tantivy::schema::OwnedValue> =
                        match serde_json::from_str(&json_str) {
                            Ok(value) => value,
                            Err(e) => return Err(format!("Invalid JSON format: {}", e)),
                        };

                    // Add JSON object using the helper method
                    doc_builder.add_json(field_name_str, json_value);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add values to retrieved documents".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddJsonFromString(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    json_string: JString,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let json_str: String = match env.get_string(&json_string) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON string");
            return;
        }
    };

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    // Parse JSON string to BTreeMap<String, OwnedValue>
                    let json_value: std::collections::BTreeMap<String, tantivy::schema::OwnedValue> =
                        match serde_json::from_str(&json_str) {
                            Ok(value) => value,
                            Err(e) => return Err(format!("Invalid JSON format: {}", e)),
                        };

                    // Add JSON object using the helper method
                    doc_builder.add_json(field_name_str, json_value);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add values to retrieved documents".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddIpAddr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    ip_addr: JString,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let ip_addr_str: String = match env.get_string(&ip_addr) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid IP address string");
            return;
        }
    };

    // Parse the IP address
    let ip_addr_parsed = match ip_addr_str.parse::<IpAddr>() {
        Ok(ip) => ip,
        Err(_) => {
            handle_error(
                &mut env,
                &format!("Invalid IP address format: {}", ip_addr_str),
            );
            return;
        }
    };

    with_arc_safe::<Mutex<DocumentWrapper>, ()>(ptr, |wrapper_mutex| {
        let mut wrapper = wrapper_mutex.lock().unwrap();
        match &mut *wrapper {
            DocumentWrapper::Builder(doc_builder) => {
                doc_builder.add_ip_addr(field_name_str, ip_addr_parsed);
            }
            DocumentWrapper::Retrieved(_) => {
                // Retrieved documents are read-only
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeAddString(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    field_name: JString,
    value: JString,
) {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return;
        }
    };

    let value_str: String = match env.get_string(&value) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid string value");
            return;
        }
    };

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<(), String>>(ptr, |wrapper_mutex| {
            let mut doc_wrapper = wrapper_mutex.lock().unwrap();
            match &mut *doc_wrapper {
                DocumentWrapper::Builder(ref mut doc_builder) => {
                    // For string fields, we store them as text values
                    // The distinction between string and text fields is handled at the schema level
                    doc_builder.add_text(field_name_str, value_str);
                    Ok(())
                }
                DocumentWrapper::Retrieved(_) => {
                    Err("Cannot add values to retrieved documents".to_string())
                }
            }
        });

    match result {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
        }
    }
}
