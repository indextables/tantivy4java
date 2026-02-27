// iceberg_reader/jni.rs - JNI entry points for Iceberg table reading
//
// Bridges Java IcebergTableReader native methods to the Rust iceberg_reader
// module. Uses shared JNI helpers from common.rs.

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jlong};
use jni::JNIEnv;

use crate::common::{to_java_exception, extract_hashmap, buffer_to_jbytearray};
use crate::debug_println;

use super::scan::{list_iceberg_files, read_iceberg_schema, list_iceberg_snapshots};
use super::distributed::{get_iceberg_snapshot_info, read_iceberg_manifest};
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

    let config = match extract_hashmap(&mut env, &config_map) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract config map: {}", e));
            return std::ptr::null_mut();
        }
    };

    let snap_opt = if snapshot_id < 0 { None } else { Some(snapshot_id) };

    debug_println!(
        "🔧 ICEBERG_JNI: catalog={}, ns={}, table={}, snapshot={:?}, config_keys={}",
        catalog_str, namespace_str, table_str, snap_opt, config.len()
    );

    match list_iceberg_files(&catalog_str, &config, &namespace_str, &table_str, snap_opt) {
        Ok((entries, actual_snap_id)) => {
            debug_println!(
                "🔧 ICEBERG_JNI: Listed {} files at snapshot {}",
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
) -> jbyteArray {
    debug_println!("🔧 ICEBERG_JNI: nativeReadManifestFile called");

    let catalog_str = match get_jstring(&mut env, &catalog_name, "catalog name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let namespace_str = match get_jstring(&mut env, &namespace, "namespace") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let table_str = match get_jstring(&mut env, &table_name, "table name") {
        Ok(s) => s, Err(()) => return std::ptr::null_mut(),
    };
    let manifest_str = match get_jstring(&mut env, &manifest_path, "manifest path") {
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
        "🔧 ICEBERG_JNI: readManifestFile catalog={}, ns={}, table={}, manifest={}",
        catalog_str, namespace_str, table_str, manifest_str
    );

    match read_iceberg_manifest(&config, &manifest_str) {
        Ok(entries) => {
            debug_println!(
                "🔧 ICEBERG_JNI: Read {} entries from manifest",
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
