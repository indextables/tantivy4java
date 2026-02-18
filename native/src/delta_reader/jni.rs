// delta_reader/jni.rs - JNI entry point for Delta table file listing
//
// Bridges Java DeltaTableReader.nativeListFiles() to the Rust delta_reader
// module, extracting credentials from a Java HashMap<String,String> and
// returning file entries as a TANT byte buffer (jbyteArray).

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jlong};
use jni::JNIEnv;

use crate::common::to_java_exception;
use crate::debug_println;

use super::engine::DeltaStorageConfig;
use super::scan::{list_delta_files, read_delta_schema};
use super::serialization::{serialize_delta_entries, serialize_delta_schema, serialize_snapshot_info, serialize_log_changes};
use super::distributed::{get_snapshot_info, read_checkpoint_part, read_post_checkpoint_changes};

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
fn build_config(env: &mut JNIEnv, config_map: &JObject) -> DeltaStorageConfig {
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

/// Extract a Java List<String> into a Vec<String>.
fn extract_string_list(env: &mut JNIEnv, list: &JObject) -> Result<Vec<String>, anyhow::Error> {
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

// ─── Existing JNI entry points ──────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeListFiles(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    version: jlong,
    config_map: JObject,
    compact: jboolean,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeListFiles called (compact={})", compact != 0);

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);
    let version_opt = if version < 0 { None } else { Some(version as u64) };

    debug_println!(
        "🔧 DELTA_JNI: url={}, version={:?}, has_aws={}, has_azure={}",
        url_str,
        version_opt,
        config.aws_access_key.is_some(),
        config.azure_account_name.is_some()
    );

    match list_delta_files(&url_str, &config, version_opt) {
        Ok((entries, actual_version)) => {
            debug_println!(
                "🔧 DELTA_JNI: Listed {} files at version {}",
                entries.len(),
                actual_version
            );
            let buffer = serialize_delta_entries(&entries, actual_version, compact != 0);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeReadSchema(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    version: jlong,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeReadSchema called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);
    let version_opt = if version < 0 { None } else { Some(version as u64) };

    debug_println!(
        "🔧 DELTA_JNI: readSchema url={}, version={:?}",
        url_str,
        version_opt
    );

    match read_delta_schema(&url_str, &config, version_opt) {
        Ok((fields, schema_json, actual_version)) => {
            debug_println!(
                "🔧 DELTA_JNI: Schema has {} fields at version {}",
                fields.len(),
                actual_version
            );
            let buffer = serialize_delta_schema(&fields, &schema_json, actual_version);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

// ─── Distributed scanning JNI entry points ──────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeGetSnapshotInfo(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeGetSnapshotInfo called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    match get_snapshot_info(&url_str, &config) {
        Ok(info) => {
            debug_println!(
                "🔧 DELTA_JNI: SnapshotInfo version={}, {} checkpoint parts, {} commits",
                info.version,
                info.checkpoint_part_paths.len(),
                info.commit_file_paths.len()
            );
            let buffer = serialize_snapshot_info(&info);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeReadCheckpointPart(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
    part_path: JString,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeReadCheckpointPart called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let part = match env.get_string(&part_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read part path: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    match read_checkpoint_part(&url_str, &config, &part) {
        Ok(entries) => {
            debug_println!("🔧 DELTA_JNI: Read {} entries from checkpoint part", entries.len());
            let buffer = serialize_delta_entries(&entries, 0, false);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeReadPostCheckpointChanges(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
    commit_paths_array: JObject,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeReadPostCheckpointChanges called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let config = build_config(&mut env, &config_map);

    let commit_paths = match extract_string_list(&mut env, &commit_paths_array) {
        Ok(paths) => paths,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    match read_post_checkpoint_changes(&url_str, &config, &commit_paths) {
        Ok(changes) => {
            debug_println!(
                "🔧 DELTA_JNI: {} added, {} removed",
                changes.added_files.len(),
                changes.removed_paths.len()
            );
            let buffer = serialize_log_changes(&changes);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}
