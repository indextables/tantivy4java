// delta_reader/jni.rs - JNI entry points for Delta table reading
//
// Bridges Java DeltaTableReader native methods to the Rust delta_reader
// module. Uses shared JNI helpers from common.rs.

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jlong};
use jni::JNIEnv;

use crate::common::{to_java_exception, build_storage_config, buffer_to_jbytearray, extract_string_list};
use crate::debug_println;

use super::scan::{list_delta_files, read_delta_schema};
use super::serialization::{serialize_delta_entries, serialize_delta_schema, serialize_snapshot_info, serialize_log_changes};
use super::distributed::{get_snapshot_info, read_checkpoint_part, read_post_checkpoint_changes};

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

    let config = build_storage_config(&mut env, &config_map);
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

    let config = build_storage_config(&mut env, &config_map);
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

    let config = build_storage_config(&mut env, &config_map);

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

    let config = build_storage_config(&mut env, &config_map);

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

    let config = build_storage_config(&mut env, &config_map);

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
