// parquet_reader/jni.rs - JNI entry points for Hive-style parquet directory reading
//
// Bridges Java ParquetTableReader native methods to the Rust parquet_reader
// module, extracting a Java HashMap<String,String> and returning results
// as TANT byte buffers (jbyteArray).

use jni::objects::{JClass, JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::common::to_java_exception;
use crate::debug_println;

use super::distributed::{get_parquet_table_info, list_partition_files};
use super::serialization::{serialize_parquet_table_info, serialize_parquet_file_entries};

/// Helper to extract a String value from a Java HashMap<String,String>.
fn extract_string(env: &mut JNIEnv, map: &JObject, key: &str) -> Option<String> {
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

/// Build a DeltaStorageConfig from a Java HashMap<String,String>.
fn build_config(
    env: &mut JNIEnv,
    config_map: &JObject,
) -> crate::delta_reader::engine::DeltaStorageConfig {
    use crate::delta_reader::engine::DeltaStorageConfig;

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

/// Helper to return a TANT byte buffer as a jbyteArray.
fn buffer_to_jbytearray(env: &mut JNIEnv, buffer: &[u8]) -> jbyteArray {
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

// ── JNI entry points ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetTableReader_nativeGetTableInfo(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 PARQUET_JNI: nativeGetTableInfo called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    debug_println!(
        "🔧 PARQUET_JNI: getTableInfo url={}, has_aws={}, has_azure={}",
        url_str,
        config.aws_access_key.is_some(),
        config.azure_account_name.is_some()
    );

    match get_parquet_table_info(&url_str, &config) {
        Ok(info) => {
            debug_println!(
                "🔧 PARQUET_JNI: TableInfo: {} partition dirs, {} root files, partitioned={}",
                info.partition_directories.len(),
                info.root_parquet_files.len(),
                info.is_partitioned
            );
            let buffer = serialize_parquet_table_info(&info);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetTableReader_nativeListPartitionFiles(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
    partition_prefix: JString,
) -> jbyteArray {
    debug_println!("🔧 PARQUET_JNI: nativeListPartitionFiles called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let prefix_str = match env.get_string(&partition_prefix) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read partition prefix: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    debug_println!(
        "🔧 PARQUET_JNI: listPartitionFiles url={}, prefix={}",
        url_str, prefix_str
    );

    match list_partition_files(&url_str, &config, &prefix_str) {
        Ok(entries) => {
            debug_println!(
                "🔧 PARQUET_JNI: Listed {} parquet files",
                entries.len()
            );
            let buffer = serialize_parquet_file_entries(&entries);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}
