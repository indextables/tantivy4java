// iceberg_reader/jni.rs - JNI entry points for Iceberg table reading
//
// Bridges Java IcebergTableReader native methods to the Rust iceberg_reader
// module. Uses shared JNI helpers from common.rs.

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jlongArray};
use jni::JNIEnv;

use crate::common::{
    to_java_exception, extract_hashmap, buffer_to_jbytearray,
    extract_optional_jstring, parse_optional_predicate, filter_by_predicate,
};
use crate::debug_println;

use super::scan::{list_iceberg_files, read_iceberg_schema, list_iceberg_snapshots};
use super::distributed::{get_iceberg_snapshot_info, read_iceberg_manifest, read_iceberg_manifest_arrow_ffi};
use super::serialization::{
    serialize_iceberg_entries, serialize_iceberg_schema, serialize_iceberg_snapshots,
    serialize_iceberg_snapshot_info,
};

/// Helper: extract a JString, throwing a Java exception on failure.
fn get_jstring(env: &mut JNIEnv, s: &JString, name: &str) -> Result<String, ()> {
    match env.get_string(s) {
        Ok(s) => Ok(s.to_string_lossy().to_string()),
        Err(e) => {
            to_java_exception(env, &anyhow::anyhow!("Failed to read {}: {}", name, e));
            Err(())
        }
    }
}

// ── JNI entry points ──────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeListFiles(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    snapshot_id: jlong,
    config_map: JObject,
    compact: jboolean,
    predicate_json: JString,
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeListFiles called (compact={})", compact != 0);

    let catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    let snap_opt = if snapshot_id < 0 { None } else { Some(snapshot_id) };

    match list_iceberg_files(&catalog_str, &config, &namespace_str, &table_str, snap_opt) {
        Ok((entries, actual_snap_id)) => {
            let entries = filter_by_predicate(entries, &predicate, |e| &e.partition_values);
            debug_println!(
                "🔧 ICEBERG_JNI: Listed {} files at snapshot {} (after filtering)",
                entries.len(), actual_snap_id
            );
            let buffer = serialize_iceberg_entries(&entries, actual_snap_id, compact != 0);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeReadSchema(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    snapshot_id: jlong,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeReadSchema called");

    let catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    let snap_opt = if snapshot_id < 0 { None } else { Some(snapshot_id) };

    debug_println!(
        "🔧 ICEBERG_JNI: readSchema catalog={}, ns={}, table={}, snapshot={:?}",
        catalog_str, namespace_str, table_str, snap_opt
    );

    match read_iceberg_schema(&catalog_str, &config, &namespace_str, &table_str, snap_opt) {
        Ok((fields, schema_json, actual_snap_id)) => {
            debug_println!(
                "🔧 ICEBERG_JNI: Schema has {} fields at snapshot {}",
                fields.len(), actual_snap_id
            );
            let buffer = serialize_iceberg_schema(&fields, &schema_json, actual_snap_id);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeListSnapshots(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeListSnapshots called");

    let catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!(
        "🔧 ICEBERG_JNI: listSnapshots catalog={}, ns={}, table={}",
        catalog_str, namespace_str, table_str
    );

    match list_iceberg_snapshots(&catalog_str, &config, &namespace_str, &table_str) {
        Ok(snapshots) => {
            debug_println!(
                "🔧 ICEBERG_JNI: Found {} snapshots",
                snapshots.len()
            );
            let buffer = serialize_iceberg_snapshots(&snapshots);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

// ── Distributed scanning JNI entry points ────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeGetSnapshotInfo(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    snapshot_id: jlong,
    config_map: JObject,
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeGetSnapshotInfo called");

    let catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    let snap_opt = if snapshot_id < 0 { None } else { Some(snapshot_id) };

    debug_println!(
        "🔧 ICEBERG_JNI: getSnapshotInfo catalog={}, ns={}, table={}, snapshot={:?}",
        catalog_str, namespace_str, table_str, snap_opt
    );

    match get_iceberg_snapshot_info(&catalog_str, &config, &namespace_str, &table_str, snap_opt) {
        Ok(info) => {
            debug_println!(
                "🔧 ICEBERG_JNI: SnapshotInfo snapshot_id={}, {} manifests",
                info.snapshot_id, info.manifest_entries.len()
            );
            let buffer = serialize_iceberg_snapshot_info(&info);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeReadManifestFile(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    manifest_path: JString,
    config_map: JObject,
    compact: jboolean,
    predicate_json: JString,
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeReadManifestFile called");

    // catalog_name, namespace, table_name are accepted for API consistency with
    // listFiles/getSnapshotInfo but not needed — manifest path + config are sufficient.
    // We still extract them to validate the JNI strings aren't corrupted.
    let _catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let _namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let _table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let manifest_str = match get_jstring(&mut env, &manifest_path, "manifest path") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    match read_iceberg_manifest(&config, &manifest_str) {
        Ok(entries) => {
            let entries = filter_by_predicate(entries, &predicate, |e| &e.partition_values);
            debug_println!(
                "🔧 ICEBERG_JNI: Read {} entries from manifest (after filtering)",
                entries.len()
            );
            let buffer = serialize_iceberg_entries(&entries, 0, compact != 0);
            buffer_to_jbytearray(&mut env, &buffer)
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

// ── Arrow FFI entry point ─────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_iceberg_IcebergTableReader_nativeReadManifestFileArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    catalog_name: JString,
    namespace: JString,
    table_name: JString,
    manifest_path: JString,
    config_map: JObject,
    predicate_json: JString,
    array_addrs: jlongArray,
    schema_addrs: jlongArray,
) -> jint {
    debug_println!("🔧 ICEBERG_JNI: nativeReadManifestFileArrowFfi called");

    // catalog_name, namespace, table_name accepted for API consistency but not needed
    // for manifest reading — manifest path + config are sufficient.
    let _catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return -1,
    };
    let _namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return -1,
    };
    let _table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return -1,
    };
    let manifest_str = match get_jstring(&mut env, &manifest_path, "manifest path") {
        Ok(s) => s, Err(()) => return -1,
    };

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return -1;
        }
    };

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return -1;
        }
    };

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

    match read_iceberg_manifest_arrow_ffi(
        &config,
        &manifest_str,
        predicate.as_ref(),
        &arr_addrs,
        &sch_addrs,
    ) {
        Ok(num_rows) => {
            debug_println!("🔧 ICEBERG_JNI: Arrow FFI exported {} rows", num_rows);
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
