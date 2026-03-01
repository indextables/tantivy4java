// delta_reader/jni.rs - JNI entry points for Delta table reading
//
// Bridges Java DeltaTableReader native methods to the Rust delta_reader
// module. Uses shared JNI helpers from common.rs.

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jlongArray};
use jni::JNIEnv;

use crate::common::{
    to_java_exception, build_storage_config, buffer_to_jbytearray, extract_string_list,
    extract_optional_jstring, parse_optional_predicate, filter_by_predicate,
};
use crate::debug_println;

use super::scan::{list_delta_files, read_delta_schema};
use super::serialization::{serialize_delta_entries, serialize_delta_schema, serialize_snapshot_info, serialize_log_changes};
use super::distributed::{get_snapshot_info, read_checkpoint_part, read_post_checkpoint_changes, read_checkpoint_part_arrow_ffi, parse_column_mapping_json};

// ─── Existing JNI entry points ──────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeListFiles(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    version: jlong,
    config_map: JObject,
    compact: jboolean,
    predicate_json: JString,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeListFiles called (compact={})", compact != 0);

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let config = build_storage_config(&mut env, &config_map);
    let version_opt = if version < 0 { None } else { Some(version as u64) };

    debug_println!(
        "🔧 DELTA_JNI: url={}, version={:?}, has_aws={}, has_azure={}, has_predicate={}",
        url_str,
        version_opt,
        config.aws_access_key.is_some(),
        config.azure_account_name.is_some(),
        predicate.is_some()
    );

    match list_delta_files(&url_str, &config, version_opt) {
        Ok((entries, actual_version)) => {
            let entries = filter_by_predicate(entries, &predicate, |e| &e.partition_values);
            debug_println!(
                "🔧 DELTA_JNI: Listed {} files at version {} (after filtering)",
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
    predicate_json: JString,
    column_mapping_json: JString,
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

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let cm_str = extract_optional_jstring(&mut env, &column_mapping_json);
    let column_mapping = parse_column_mapping_json(cm_str.as_deref());

    let config = build_storage_config(&mut env, &config_map);

    match read_checkpoint_part(&url_str, &config, &part, &column_mapping) {
        Ok(entries) => {
            let entries = filter_by_predicate(entries, &predicate, |e| &e.partition_values);
            debug_println!("🔧 DELTA_JNI: Read {} entries from checkpoint part (after filtering)", entries.len());
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
    predicate_json: JString,
    column_mapping_json: JString,
) -> jbyteArray {
    debug_println!("🔧 DELTA_JNI: nativeReadPostCheckpointChanges called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return std::ptr::null_mut();
        }
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let cm_str = extract_optional_jstring(&mut env, &column_mapping_json);
    let column_mapping = parse_column_mapping_json(cm_str.as_deref());

    let config = build_storage_config(&mut env, &config_map);

    let commit_paths = match extract_string_list(&mut env, &commit_paths_array) {
        Ok(paths) => paths,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    match read_post_checkpoint_changes(&url_str, &config, &commit_paths, &column_mapping) {
        Ok(mut changes) => {
            changes.added_files = filter_by_predicate(
                changes.added_files, &predicate, |e| &e.partition_values,
            );
            debug_println!(
                "🔧 DELTA_JNI: {} added, {} removed (after filtering)",
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

// ─── Arrow FFI entry point ───────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_delta_DeltaTableReader_nativeReadCheckpointPartArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
    part_path: JString,
    predicate_json: JString,
    column_mapping_json: JString,
    array_addrs: jlongArray,
    schema_addrs: jlongArray,
) -> jint {
    debug_println!("🔧 DELTA_JNI: nativeReadCheckpointPartArrowFfi called");

    let url_str = match env.get_string(&table_url) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table URL: {}", e));
            return -1;
        }
    };

    let part = match env.get_string(&part_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read part path: {}", e));
            return -1;
        }
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return -1;
        }
    };

    let cm_str = extract_optional_jstring(&mut env, &column_mapping_json);
    let column_mapping = parse_column_mapping_json(cm_str.as_deref());

    let config = build_storage_config(&mut env, &config_map);

    // Extract long[] arrays from JNI
    let arr_addrs = match extract_jlong_array(&mut env, &array_addrs) {
        Ok(a) => a,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return -1;
        }
    };
    let sch_addrs = match extract_jlong_array(&mut env, &schema_addrs) {
        Ok(a) => a,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return -1;
        }
    };

    match read_checkpoint_part_arrow_ffi(
        &url_str,
        &config,
        &part,
        predicate.as_ref(),
        &column_mapping,
        &arr_addrs,
        &sch_addrs,
    ) {
        Ok(num_rows) => {
            debug_println!("🔧 DELTA_JNI: Arrow FFI exported {} rows", num_rows);
            num_rows as jint
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            -1
        }
    }
}

/// Extract a Java long[] into a Vec<i64>.
fn extract_jlong_array(env: &mut JNIEnv, arr: &jlongArray) -> anyhow::Result<Vec<i64>> {
    let safe_arr = unsafe { jni::objects::JLongArray::from_raw(*arr) };
    let len = env
        .get_array_length(&safe_arr)
        .map_err(|e| anyhow::anyhow!("Failed to get array length: {}", e))?;
    let mut buf = vec![0i64; len as usize];
    env.get_long_array_region(&safe_arr, 0, &mut buf)
        .map_err(|e| anyhow::anyhow!("Failed to read long array: {}", e))?;
    // Prevent the safe wrapper from freeing the original Java array reference
    std::mem::forget(safe_arr);
    Ok(buf)
}
