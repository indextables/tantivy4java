// common.rs - Shared JNI helpers used across delta_reader, iceberg_reader, and parquet_reader.
//
// Consolidates extract_string(), buffer_to_jbytearray(), extract_hashmap(),
// extract_string_list(), and build_storage_config() to avoid duplication.

use std::collections::HashMap;

use jni::objects::{JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::delta_reader::engine::DeltaStorageConfig;

/// Convert Rust error to Java exception.
pub fn to_java_exception(env: &mut JNIEnv, error: &anyhow::Error) {
    let error_message = format!("{}", error);
    let _ = env.throw_new("java/lang/RuntimeException", error_message);
}

/// Extract a String value from a Java HashMap<String,String> by key.
pub fn extract_string(env: &mut JNIEnv, map: &JObject, key: &str) -> Option<String> {
    let key_jstr = env.new_string(key).ok()?;
    let value = env
        .call_method(
            map,
            "get",
            "(Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&key_jstr).into()],
        )
        .ok()?
        .l()
        .ok()?;
    if value.is_null() {
        return None;
    }
    let value_jstr = JString::from(value);
    let value_str = env.get_string(&value_jstr).ok()?;
    Some(value_str.to_string_lossy().to_string())
}

/// Copy a byte slice into a new Java byte array (jbyteArray).
///
/// On failure, throws a Java RuntimeException and returns null.
pub fn buffer_to_jbytearray(env: &mut JNIEnv, buffer: &[u8]) -> jbyteArray {
    match env.new_byte_array(buffer.len() as i32) {
        Ok(byte_array) => {
            let byte_slice: &[i8] = unsafe {
                std::slice::from_raw_parts(buffer.as_ptr() as *const i8, buffer.len())
            };
            if let Err(e) = env.set_byte_array_region(&byte_array, 0, byte_slice) {
                to_java_exception(
                    env,
                    &anyhow::anyhow!("Failed to copy byte array: {}", e),
                );
                return std::ptr::null_mut();
            }
            byte_array.into_raw()
        }
        Err(e) => {
            to_java_exception(
                env,
                &anyhow::anyhow!("Failed to allocate byte array: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

/// Extract a full Java HashMap<String,String> into a Rust HashMap.
pub fn extract_hashmap(env: &mut JNIEnv, map: &JObject) -> Result<HashMap<String, String>, String> {
    if map.is_null() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::new();

    let entry_set = env
        .call_method(map, "entrySet", "()Ljava/util/Set;", &[])
        .map_err(|e| format!("Failed to call entrySet(): {}", e))?
        .l()
        .map_err(|e| format!("entrySet() not an object: {}", e))?;

    let iterator = env
        .call_method(&entry_set, "iterator", "()Ljava/util/Iterator;", &[])
        .map_err(|e| format!("Failed to call iterator(): {}", e))?
        .l()
        .map_err(|e| format!("iterator() not an object: {}", e))?;

    loop {
        let has_next = env
            .call_method(&iterator, "hasNext", "()Z", &[])
            .map_err(|e| format!("Failed to call hasNext(): {}", e))?
            .z()
            .map_err(|e| format!("hasNext() not boolean: {}", e))?;

        if !has_next {
            break;
        }

        let entry = env
            .call_method(&iterator, "next", "()Ljava/lang/Object;", &[])
            .map_err(|e| format!("Failed to call next(): {}", e))?
            .l()
            .map_err(|e| format!("next() not an object: {}", e))?;

        let key = env
            .call_method(&entry, "getKey", "()Ljava/lang/Object;", &[])
            .map_err(|e| format!("Failed to call getKey(): {}", e))?
            .l()
            .map_err(|e| format!("getKey() not an object: {}", e))?;

        let value = env
            .call_method(&entry, "getValue", "()Ljava/lang/Object;", &[])
            .map_err(|e| format!("Failed to call getValue(): {}", e))?
            .l()
            .map_err(|e| format!("getValue() not an object: {}", e))?;

        if !key.is_null() && !value.is_null() {
            let key_jstr = JString::from(key);
            let value_jstr = JString::from(value);

            let key_str = env
                .get_string(&key_jstr)
                .map_err(|e| format!("Failed to read key string: {}", e))?
                .to_string_lossy()
                .to_string();
            let value_str = env
                .get_string(&value_jstr)
                .map_err(|e| format!("Failed to read value string: {}", e))?
                .to_string_lossy()
                .to_string();

            result.insert(key_str, value_str);
        }
    }

    Ok(result)
}

/// Extract a Java List<String> into a Vec<String>.
pub fn extract_string_list(env: &mut JNIEnv, list: &JObject) -> Result<Vec<String>, anyhow::Error> {
    if list.is_null() {
        return Ok(Vec::new());
    }

    let size = env
        .call_method(list, "size", "()I", &[])
        .map_err(|e| anyhow::anyhow!("Failed to call size(): {}", e))?
        .i()
        .map_err(|e| anyhow::anyhow!("Failed to get size as int: {}", e))?;

    let mut result = Vec::with_capacity(size as usize);
    for i in 0..size {
        let elem = env
            .call_method(list, "get", "(I)Ljava/lang/Object;", &[jni::objects::JValue::Int(i)])
            .map_err(|e| anyhow::anyhow!("Failed to call get({}): {}", i, e))?
            .l()
            .map_err(|e| anyhow::anyhow!("Failed to get element as object: {}", e))?;

        if elem.is_null() {
            continue;
        }

        let jstr = JString::from(elem);
        let s = env
            .get_string(&jstr)
            .map_err(|e| anyhow::anyhow!("Failed to get string: {}", e))?;
        result.push(s.to_string_lossy().to_string());
    }

    Ok(result)
}

/// Build a DeltaStorageConfig from a Java HashMap<String,String>.
///
/// Used by delta_reader and parquet_reader JNI entry points.
pub fn build_storage_config(env: &mut JNIEnv, config_map: &JObject) -> DeltaStorageConfig {
    if config_map.is_null() {
        return DeltaStorageConfig::default();
    }

    DeltaStorageConfig {
        aws_access_key: extract_string(env, config_map, "aws_access_key_id"),
        aws_secret_key: extract_string(env, config_map, "aws_secret_access_key"),
        aws_session_token: extract_string(env, config_map, "aws_session_token"),
        aws_region: extract_string(env, config_map, "aws_region"),
        aws_endpoint: extract_string(env, config_map, "aws_endpoint"),
        aws_force_path_style: extract_string(env, config_map, "aws_force_path_style")
            .map(|s| s == "true")
            .unwrap_or(false),
        azure_account_name: extract_string(env, config_map, "azure_account_name"),
        azure_access_key: extract_string(env, config_map, "azure_access_key"),
        azure_bearer_token: extract_string(env, config_map, "azure_bearer_token"),
    }
}
