// jni_getters.rs - JNI functions for document retrieval and metadata
// Extracted from document.rs during refactoring

use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, Timelike};
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jint, jlong, jobject};
use jni::JNIEnv;
use tantivy::schema::OwnedValue;

use crate::debug_println;
use crate::utils::{arc_to_jlong, handle_error, release_arc, with_arc_safe};

use super::types::{DocumentBuilder, DocumentWrapper};

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let document_builder = DocumentBuilder::new();
    let wrapper = DocumentWrapper::Builder(document_builder);
    let wrapper_arc = Arc::new(Mutex::new(wrapper));
    arc_to_jlong(wrapper_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeFromMap(
    mut env: JNIEnv,
    _class: JClass,
    _fields: JObject,
    _schema_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeGet(
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

    debug_println!(
        "🔍 DOCUMENT_GET: nativeGet called with ptr={}, field_name={}",
        ptr,
        field_name_str
    );

    let result =
        with_arc_safe::<Mutex<DocumentWrapper>, Result<jobject, String>>(ptr, |wrapper_mutex| {
            debug_println!("🔍 DOCUMENT_GET: Arc found in registry, accessing DocumentWrapper");
            let doc_wrapper = wrapper_mutex.lock().unwrap();
            debug_println!(
                "🔍 DOCUMENT_GET: Got lock on DocumentWrapper, attempting to get field: {}",
                field_name_str
            );
            match doc_wrapper.get_field_values(&field_name_str) {
                Some(values) => {
                    // Create ArrayList to return the values
                    let array_list_class =
                        env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
                    let array_list = env
                        .new_object(&array_list_class, "()V", &[])
                        .map_err(|e| e.to_string())?;

                    // Add each value to the ArrayList
                    for value in values {
                        let java_value = match value {
                            OwnedValue::Str(s) => {
                                let java_string = env.new_string(s).map_err(|e| e.to_string())?;
                                java_string.into()
                            }
                            OwnedValue::I64(i) => {
                                // Always return Long objects for consistency with test expectations
                                let long_class =
                                    env.find_class("java/lang/Long").map_err(|e| e.to_string())?;
                                env.new_object(&long_class, "(J)V", &[(*i).into()])
                                    .map_err(|e| e.to_string())?
                            }
                            OwnedValue::F64(f) => {
                                let double_class = env
                                    .find_class("java/lang/Double")
                                    .map_err(|e| e.to_string())?;
                                env.new_object(&double_class, "(D)V", &[(*f).into()])
                                    .map_err(|e| e.to_string())?
                            }
                            OwnedValue::U64(u) => {
                                let long_class =
                                    env.find_class("java/lang/Long").map_err(|e| e.to_string())?;
                                env.new_object(&long_class, "(J)V", &[(*u as i64).into()])
                                    .map_err(|e| e.to_string())?
                            }
                            OwnedValue::Bool(b) => {
                                let boolean_class = env
                                    .find_class("java/lang/Boolean")
                                    .map_err(|e| e.to_string())?;
                                env.new_object(&boolean_class, "(Z)V", &[(*b).into()])
                                    .map_err(|e| e.to_string())?
                            }
                            OwnedValue::Date(dt) => {
                                // Convert DateTime to Java LocalDateTime with nanosecond precision
                                let timestamp_nanos = dt.into_timestamp_nanos();
                                let chrono_dt = chrono::DateTime::from_timestamp_nanos(timestamp_nanos);
                                let naive_dt = chrono_dt.naive_utc();

                                // Extract components including nanoseconds
                                let year = naive_dt.year();
                                let month = naive_dt.month() as i32;
                                let day = naive_dt.day() as i32;
                                let hour = naive_dt.hour() as i32;
                                let minute = naive_dt.minute() as i32;
                                let second = naive_dt.second() as i32;
                                let nano = naive_dt.nanosecond() as i32;

                                // Create Java LocalDateTime object with nanosecond precision
                                // Signature: LocalDateTime.of(int year, int month, int dayOfMonth, int hour, int minute, int second, int nanoOfSecond)
                                let localdatetime_class = env
                                    .find_class("java/time/LocalDateTime")
                                    .map_err(|e| e.to_string())?;
                                let localdatetime_result = env
                                    .call_static_method(
                                        &localdatetime_class,
                                        "of",
                                        "(IIIIIII)Ljava/time/LocalDateTime;",
                                        &[
                                            year.into(),
                                            month.into(),
                                            day.into(),
                                            hour.into(),
                                            minute.into(),
                                            second.into(),
                                            nano.into(),
                                        ],
                                    )
                                    .map_err(|e| e.to_string())?;
                                localdatetime_result.l().map_err(|e| e.to_string())?
                            }
                            OwnedValue::IpAddr(ipv6) => {
                                // Convert IPv6 address back to original format if it was IPv4-mapped
                                let ip_addr = IpAddr::V6(*ipv6);
                                let ip_string = if let Some(ipv4) = ipv6.to_ipv4_mapped() {
                                    IpAddr::V4(ipv4).to_string()
                                } else {
                                    ip_addr.to_string()
                                };
                                let java_string =
                                    env.new_string(&ip_string).map_err(|e| e.to_string())?;
                                java_string.into()
                            }
                            OwnedValue::Object(_) | OwnedValue::Array(_) => {
                                // Serialize JSON objects and arrays to proper JSON string
                                let json_string = serde_json::to_string(value).unwrap_or_else(|e| {
                                    debug_println!("Failed to serialize JSON value: {}", e);
                                    "{}".to_string()
                                });
                                let java_string =
                                    env.new_string(&json_string).map_err(|e| e.to_string())?;
                                java_string.into()
                            }
                            _ => {
                                // For any other unsupported types, use debug format with warning
                                debug_println!(
                                    "WARNING: Unsupported OwnedValue type encountered, using debug format"
                                );
                                let string_value = format!("{:?}", value);
                                let java_string =
                                    env.new_string(&string_value).map_err(|e| e.to_string())?;
                                java_string.into()
                            }
                        };

                        env.call_method(
                            &array_list,
                            "add",
                            "(Ljava/lang/Object;)Z",
                            &[(&java_value).into()],
                        )
                        .map_err(|e| e.to_string())?;
                    }

                    Ok(array_list.as_raw())
                }
                None => {
                    // Return empty ArrayList for non-existent fields
                    let array_list_class =
                        env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
                    let array_list = env
                        .new_object(&array_list_class, "()V", &[])
                        .map_err(|e| e.to_string())?;
                    Ok(array_list.as_raw())
                }
            }
        });

    match result {
        Some(Ok(list_ptr)) => list_ptr,
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        }
        None => {
            handle_error(&mut env, "Invalid Document pointer");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeToMap(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Document native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeGetNumFields(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    with_arc_safe::<Mutex<DocumentWrapper>, jint>(ptr, |wrapper_mutex| {
        let doc_wrapper = wrapper_mutex.lock().unwrap();
        doc_wrapper.num_fields() as jint
    })
    .unwrap_or(0)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeIsEmpty(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    with_arc_safe::<Mutex<DocumentWrapper>, jboolean>(ptr, |wrapper_mutex| {
        let doc_wrapper = wrapper_mutex.lock().unwrap();
        if doc_wrapper.is_empty() {
            1
        } else {
            0
        }
    })
    .unwrap_or(1)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Document_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}
