// txlog/jni.rs - JNI entry points for transaction log operations
//
// Java package: io.indextables.jni.txlog
// JNI prefix: io_indextables_jni_txlog_

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;

use crate::common::{buffer_to_jbytearray, build_storage_config, extract_optional_jstring, extract_string, to_java_exception};
use crate::runtime_manager::block_on_operation;

use super::distributed;
use super::serialization;

/// Extract cache-related config from the Java Map into a Rust HashMap.
fn extract_cache_config(env: &mut JNIEnv, config_map: &JObject) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    if let Some(ttl) = extract_string(env, config_map, "cache.ttl.ms") {
        map.insert("cache.ttl.ms".to_string(), ttl);
    }
    if let Some(ttl) = extract_string(env, config_map, "cache_ttl_ms") {
        map.insert("cache_ttl_ms".to_string(), ttl);
    }
    map
}

// ============================================================================
// DISTRIBUTABLE PRIMITIVES (stateless)
// ============================================================================

/// Driver: get snapshot info. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetSnapshotInfo(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to read table_path: {}", e));
            return std::ptr::null_mut();
        }
    };
    let config = build_storage_config(&mut env, &config_map);
    let cache_config = extract_cache_config(&mut env, &config_map);

    match block_on_operation(async move {
        distributed::get_txlog_snapshot_info_with_cache(&path, &config, &cache_config).await
    }) {
        Ok(info) => {
            let buf = serialization::serialize_snapshot_info(&info);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Executor: read one manifest. Returns TANT byte buffer of FileEntries.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadManifest(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    state_dir: JString,
    manifest_path: JString,
    metadata_config_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let sd = match env.get_string(&state_dir) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let mp = match env.get_string(&manifest_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    let mc_str = extract_optional_jstring(&mut env, &metadata_config_json);
    let metadata_config: std::collections::HashMap<String, String> = mc_str
        .and_then(|s| {
            match serde_json::from_str(&s) {
                Ok(map) => Some(map),
                Err(e) => {
                    crate::debug_println!("⚠️ TXLOG_JNI: Failed to parse metadata_config_json: {}", e);
                    None
                }
            }
        })
        .unwrap_or_default();

    match block_on_operation(async move {
        distributed::read_manifest(&path, &config, &sd, &mp, &metadata_config).await
    }) {
        Ok(entries) => {
            let buf = serialization::serialize_file_entries(&entries);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Driver: read post-checkpoint changes. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadPostCheckpointChanges(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    version_paths_json: JString,
    metadata_config_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    let vp_str = extract_optional_jstring(&mut env, &version_paths_json);
    let version_paths: Vec<String> = vp_str
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    let mc_str = extract_optional_jstring(&mut env, &metadata_config_json);
    let metadata_config: std::collections::HashMap<String, String> = mc_str
        .and_then(|s| {
            match serde_json::from_str(&s) {
                Ok(map) => Some(map),
                Err(e) => {
                    crate::debug_println!("⚠️ TXLOG_JNI: Failed to parse metadata_config_json: {}", e);
                    None
                }
            }
        })
        .unwrap_or_default();

    match block_on_operation(async move {
        distributed::read_post_checkpoint_changes(&path, &config, &version_paths, &metadata_config).await
    }) {
        Ok(changes) => {
            let buf = serialization::serialize_changes(&changes);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Get current version number.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetCurrentVersion(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
) -> jlong {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        distributed::get_current_version(&path, &config).await
    }) {
        Ok(v) => v,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            -1
        }
    }
}

/// List all version numbers. Returns JSON array.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListVersions(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        distributed::list_versions(&path, &config).await
    }) {
        Ok(versions) => {
            let json = serde_json::to_string(&versions).unwrap_or_else(|_| "[]".to_string());
            let jstr = match env.new_string(&json) {
                Ok(s) => s,
                Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
            };
            jstr.into_raw() as jbyteArray // Return as jstring (compatible pointer type)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Read raw JSON-lines from a specific version file.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadVersion(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    version: jlong,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        distributed::read_version_raw(&path, &config, version).await
    }) {
        Ok(content) => {
            let jstr = match env.new_string(&content) {
                Ok(s) => s,
                Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
            };
            jstr.into_raw() as jbyteArray
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

// ============================================================================
// WRITE OPERATIONS
// ============================================================================

/// Write a version file with arbitrary mixed actions and automatic retry.
/// actionsJson is JSON-lines format (one action per line).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeWriteVersion(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    actions_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let actions_str = match env.get_string(&actions_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };

    // Parse JSON-lines: one ActionEnvelope per line
    let mut actions = Vec::new();
    for line in actions_str.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let envelope: super::actions::ActionEnvelope = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) => {
                to_java_exception(&mut env, &anyhow::anyhow!("Parse action line: {}", e));
                return std::ptr::null_mut();
            }
        };
        if let Some(action) = envelope.into_action() {
            actions.push(action);
        }
    }

    match block_on_operation(async move {
        distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await
    }) {
        Ok(result) => {
            let buf = serialization::serialize_write_result(&result);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Write a version file with a single attempt (no retry).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeWriteVersionOnce(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    actions_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let actions_str = match env.get_string(&actions_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };

    let mut actions = Vec::new();
    for line in actions_str.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let envelope: super::actions::ActionEnvelope = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) => {
                to_java_exception(&mut env, &anyhow::anyhow!("Parse action line: {}", e));
                return std::ptr::null_mut();
            }
        };
        if let Some(action) = envelope.into_action() {
            actions.push(action);
        }
    }

    match block_on_operation(async move {
        distributed::write_version_once(&path, &config, actions).await
    }) {
        Ok(result) => {
            let buf = serialization::serialize_write_result(&result);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Initialize a new table (write version 0 with Protocol + Metadata).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeInitializeTable(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    protocol_json: JString,
    metadata_json: JString,
) -> () {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return; }
    };
    let config = build_storage_config(&mut env, &config_map);
    let proto_str = match env.get_string(&protocol_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return; }
    };
    let meta_str = match env.get_string(&metadata_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return; }
    };

    let protocol: super::actions::ProtocolAction = match serde_json::from_str(&proto_str) {
        Ok(p) => p,
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Parse protocol: {}", e)); return; }
    };
    let metadata: super::actions::MetadataAction = match serde_json::from_str(&meta_str) {
        Ok(m) => m,
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Parse metadata: {}", e)); return; }
    };

    if let Err(e) = block_on_operation(async move {
        distributed::initialize_table(&path, &config, protocol, metadata).await
    }) {
        to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
    }
}

/// Add files. Returns TANT buffer with WriteResult.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    adds_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    let adds_str = match env.get_string(&adds_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };

    let adds: Vec<super::actions::AddAction> = match serde_json::from_str(&adds_str) {
        Ok(a) => a,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to parse adds JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    let actions: Vec<super::actions::Action> = adds.into_iter()
        .map(super::actions::Action::Add)
        .collect();

    match block_on_operation(async move {
        distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await
    }) {
        Ok(result) => {
            let buf = serialization::serialize_write_result(&result);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Remove file by path.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeRemoveFile(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    file_path: JString,
) -> jlong {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };
    let fp = match env.get_string(&file_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };
    let config = build_storage_config(&mut env, &config_map);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let actions = vec![super::actions::Action::Remove(super::actions::RemoveAction {
        path: fp,
        deletion_timestamp: Some(now),
        data_change: true,
        partition_values: None,
        size: None,
    })];

    match block_on_operation(async move {
        distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await
    }) {
        Ok(result) => result.version,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            -1
        }
    }
}

/// Record skip action.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeSkipFile(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    skip_json: JString,
) -> jlong {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };
    let config = build_storage_config(&mut env, &config_map);

    let skip_str = match env.get_string(&skip_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };

    let skip: super::actions::SkipAction = match serde_json::from_str(&skip_str) {
        Ok(s) => s,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to parse skip JSON: {}", e));
            return -1;
        }
    };

    let actions = vec![super::actions::Action::MergeSkip(skip)];

    match block_on_operation(async move {
        distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await
    }) {
        Ok(result) => result.version,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            -1
        }
    }
}

/// Create checkpoint. Returns TANT buffer with LastCheckpointInfo.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeCreateCheckpoint(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    entries_json: JString,
    metadata_json: JString,
    protocol_json: JString,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    let entries_str = match env.get_string(&entries_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let meta_str = match env.get_string(&metadata_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let proto_str = match env.get_string(&protocol_json) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };

    // Parse file entries: accept either FileEntry-like objects (with added_at_version/added_at_timestamp)
    // or plain AddActions (for backward compatibility, defaults to 0)
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckpointEntry {
        #[serde(flatten)]
        add: super::actions::AddAction,
        #[serde(default)]
        added_at_version: i64,
        #[serde(default)]
        added_at_timestamp: i64,
    }

    let entries: Vec<super::actions::FileEntry> = match serde_json::from_str::<Vec<CheckpointEntry>>(&entries_str) {
        Ok(checkpoint_entries) => checkpoint_entries.into_iter()
            .map(|ce| super::actions::FileEntry {
                add: ce.add,
                added_at_version: ce.added_at_version,
                added_at_timestamp: ce.added_at_timestamp,
            })
            .collect(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Parse entries: {}", e)); return std::ptr::null_mut(); }
    };

    let metadata: super::actions::MetadataAction = match serde_json::from_str(&meta_str) {
        Ok(m) => m,
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Parse metadata: {}", e)); return std::ptr::null_mut(); }
    };
    let protocol: super::actions::ProtocolAction = match serde_json::from_str(&proto_str) {
        Ok(p) => p,
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Parse protocol: {}", e)); return std::ptr::null_mut(); }
    };

    match block_on_operation(async move {
        distributed::write_checkpoint(&path, &config, entries, metadata, protocol).await
    }) {
        Ok(info) => {
            let buf = serialization::serialize_last_checkpoint(&info);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

// ============================================================================
// STATEFUL READ OPERATIONS (with cache)
// ============================================================================

// Stateful operations use a handle-based pattern.
// For now, the stateless distributed primitives above are sufficient.
// Handle-based operations (nativeCreate, nativeListFiles, nativeClose, etc.)
// will be added when we integrate the TxLogCache into a TransactionLog struct.
