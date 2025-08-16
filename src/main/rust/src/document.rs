use jni::objects::{JClass, JString, JByteArray};
use jni::sys::{jlong, jdouble};
use jni::JNIEnv;
use tantivy::{TantivyDocument, schema::{Field, Value}};

use crate::{drop_handle, ptr_to_handle, handle_to_ptr};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let document = Box::new(TantivyDocument::default());
    handle_to_ptr(document)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddText(
    env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: JString,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Ok(text_value) = env.get_string(value) {
            let text_value: String = text_value.into();
            document.add_text(*field, &text_value);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jlong,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        document.add_i64(*field, value);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddUInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jlong,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        document.add_u64(*field, value as u64);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddFloat(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jdouble,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        document.add_f64(*field, value);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddDate(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    timestamp: jlong,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        use tantivy::DateTime;
        let datetime = DateTime::from_timestamp_secs(timestamp);
        document.add_date(*field, datetime);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddBytes(
    env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: JByteArray,
) {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Ok(bytes) = env.convert_byte_array(value) {
            document.add_bytes(*field, &bytes);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetText(
    env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jni::objects::jstring {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(text) = value.as_text() {
                if let Ok(jstring) = env.new_string(text) {
                    return jstring.into_raw();
                }
            }
        }
    }
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(int_val) = value.as_i64() {
                return int_val;
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetUInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(uint_val) = value.as_u64() {
                return uint_val as jlong;
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetFloat(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jdouble {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(float_val) = value.as_f64() {
                return float_val;
            }
        }
    }
    0.0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetDate(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(date_val) = value.as_date() {
                return date_val.into_timestamp_secs();
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetBytes(
    env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> JByteArray {
    if let (Some(document), Some(field)) = (
        ptr_to_handle::<TantivyDocument>(doc_handle),
        ptr_to_handle::<Field>(field_handle),
    ) {
        if let Some(value) = document.get_first(*field) {
            if let Some(bytes) = value.as_bytes() {
                if let Ok(byte_array) = env.byte_array_from_slice(bytes) {
                    return byte_array;
                }
            }
        }
    }
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
) {
    drop_handle::<TantivyDocument>(doc_handle);
}