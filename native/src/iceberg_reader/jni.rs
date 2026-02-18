// iceberg_reader/jni.rs - JNI entry points for Iceberg table reading
//
// Bridges Java IcebergTableReader native methods to the Rust iceberg_reader
// module. Extracts a Java HashMap<String,String> into a Rust HashMap and
// returns results as TANT byte buffers (jbyteArray).

use std::collections::HashMap;

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jlong};
use jni::JNIEnv;

use crate::common::to_java_exception;
use crate::debug_println;

use super::scan::{list_iceberg_files, read_iceberg_schema, list_iceberg_snapshots};
use super::distributed::{get_iceberg_snapshot_info, read_iceberg_manifest};
use super::serialization::{
    serialize_iceberg_entries, serialize_iceberg_schema, serialize_iceberg_snapshots,
    serialize_iceberg_snapshot_info,
};

/// Extract a full Java HashMap<String,String> into a Rust HashMap.
fn extract_hashmap(env: &mut JNIEnv, map: &JObject) -> Result<HashMap<String, String>, String> {
    if map.is_null() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::new();

    // Get entrySet
    let entry_set = env
        .call_method(map, "entrySet", "()Ljava/util/Set;", &[])
        .map_err(|e| format!("Failed to call entrySet(): {}", e))?
        .l()
        .map_err(|e| format!("entrySet() not an object: {}", e))?;

    // Get iterator
    let iterator = env
        .call_method(&entry_set, "iterator", "()Ljava/util/Iterator;", &[])
        .map_err(|e| format!("Failed to call iterator(): {}", e))?
        .l()
        .map_err(|e| format!("iterator() not an object: {}", e))?;

    // Iterate entries
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

/// Helper: copy a Vec<u8> buffer to a new jbyteArray.
fn buffer_to_jbytearray(env: &mut JNIEnv, buffer: &[u8]) -> Result<jbyteArray, anyhow::Error> {
    let byte_array = env
        .new_byte_array(buffer.len() as i32)
        .map_err(|e| anyhow::anyhow!("Failed to allocate byte array: {}", e))?;
    let byte_slice: &[i8] =
        unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const i8, buffer.len()) };
    env.set_byte_array_region(&byte_array, 0, byte_slice)
        .map_err(|e| anyhow::anyhow!("Failed to copy byte array: {}", e))?;
    Ok(byte_array.into_raw())
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

    // Extract strings
    let catalog_str = match env.get_string(&catalog_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read catalog name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let namespace_str = match env.get_string(&namespace) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read namespace: {}", e));
            return std::ptr::null_mut();
        }
    };
    let table_str = match env.get_string(&table_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table name: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Extract config HashMap
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

            match buffer_to_jbytearray(&mut env, &buffer) {
                Ok(arr) => arr,
                Err(e) => {
                    to_java_exception(&mut env, &e);
                    std::ptr::null_mut()
                }
            }
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

    let catalog_str = match env.get_string(&catalog_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read catalog name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let namespace_str = match env.get_string(&namespace) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read namespace: {}", e));
            return std::ptr::null_mut();
        }
    };
    let table_str = match env.get_string(&table_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table name: {}", e));
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

            match buffer_to_jbytearray(&mut env, &buffer) {
                Ok(arr) => arr,
                Err(e) => {
                    to_java_exception(&mut env, &e);
                    std::ptr::null_mut()
                }
            }
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

    let catalog_str = match env.get_string(&catalog_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read catalog name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let namespace_str = match env.get_string(&namespace) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read namespace: {}", e));
            return std::ptr::null_mut();
        }
    };
    let table_str = match env.get_string(&table_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table name: {}", e));
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

            match buffer_to_jbytearray(&mut env, &buffer) {
                Ok(arr) => arr,
                Err(e) => {
                    to_java_exception(&mut env, &e);
                    std::ptr::null_mut()
                }
            }
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

    let catalog_str = match env.get_string(&catalog_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read catalog name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let namespace_str = match env.get_string(&namespace) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read namespace: {}", e));
            return std::ptr::null_mut();
        }
    };
    let table_str = match env.get_string(&table_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table name: {}", e));
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

            match buffer_to_jbytearray(&mut env, &buffer) {
                Ok(arr) => arr,
                Err(e) => {
                    to_java_exception(&mut env, &e);
                    std::ptr::null_mut()
                }
            }
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

    let catalog_str = match env.get_string(&catalog_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read catalog name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let namespace_str = match env.get_string(&namespace) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read namespace: {}", e));
            return std::ptr::null_mut();
        }
    };
    let table_str = match env.get_string(&table_name) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table name: {}", e));
            return std::ptr::null_mut();
        }
    };
    let manifest_str = match env.get_string(&manifest_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read manifest path: {}", e));
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

    debug_println!(
        "🔧 ICEBERG_JNI: readManifestFile catalog={}, ns={}, table={}, manifest={}",
        catalog_str, namespace_str, table_str, manifest_str
    );

    match read_iceberg_manifest(&catalog_str, &config, &namespace_str, &table_str, &manifest_str) {
        Ok(entries) => {
            debug_println!(
                "🔧 ICEBERG_JNI: Read {} entries from manifest",
                entries.len()
            );

            let buffer = serialize_iceberg_entries(&entries, 0, compact != 0);

            match buffer_to_jbytearray(&mut env, &buffer) {
                Ok(arr) => arr,
                Err(e) => {
                    to_java_exception(&mut env, &e);
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}
