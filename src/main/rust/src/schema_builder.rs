use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tantivy::schema::*;

use crate::{drop_handle, ptr_to_handle};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeFromJson(
    env: JNIEnv,
    _class: JClass,
    json: JString,
) -> jlong {
    if let Ok(json_str) = env.get_string(json) {
        let json_str: String = json_str.into();
        if let Ok(schema) = serde_json::from_str::<Schema>(&json_str) {
            let schema_box = Box::new(schema);
            return crate::handle_to_ptr(schema_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeToJson(
    env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
) -> jni::objects::jstring {
    if let Some(schema) = ptr_to_handle::<Schema>(schema_handle) {
        if let Ok(json) = serde_json::to_string(schema) {
            if let Ok(jstring) = env.new_string(json) {
                return jstring.into_raw();
            }
        }
    }
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetField(
    env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
    field_name: JString,
) -> jlong {
    if let Some(schema) = ptr_to_handle::<Schema>(schema_handle) {
        if let Ok(name_str) = env.get_string(field_name) {
            let name_str: String = name_str.into();
            if let Ok(field) = schema.get_field(&name_str) {
                let field_box = Box::new(field);
                return crate::handle_to_ptr(field_box);
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
) {
    drop_handle::<Schema>(schema_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Field_nativeGetName(
    env: JNIEnv,
    _class: JClass,
    field_handle: jlong,
) -> jni::objects::jstring {
    if let Some(field) = ptr_to_handle::<tantivy::schema::Field>(field_handle) {
        if let Ok(jstring) = env.new_string("field_name") {
            return jstring.into_raw();
        }
    }
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Field_nativeGetType(
    _env: JNIEnv,
    _class: JClass,
    field_handle: jlong,
) -> jni::sys::jint {
    if let Some(_field) = ptr_to_handle::<tantivy::schema::Field>(field_handle) {
        return 0;
    }
    -1
}