// txlog/jni.rs - JNI entry points for transaction log operations
//
// Java package: io.indextables.jni.txlog
// JNI prefix: io_indextables_jni_txlog_

use arrow::array::Array as _;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;

use crate::common::{buffer_to_jbytearray, build_storage_config, extract_optional_jstring, extract_string, to_java_exception};
use crate::runtime_manager::block_on_operation;

use super::distributed;
use super::serialization;

/// Extract cache and checkpoint config from the Java Map into a Rust HashMap.
fn extract_extended_config(env: &mut JNIEnv, config_map: &JObject) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for key in &["cache.ttl.ms", "cache_ttl_ms", "checkpoint_interval", "checkpoint.interval"] {
        if let Some(val) = extract_string(env, config_map, key) {
            map.insert(key.to_string(), val);
        }
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
    let cache_config = extract_extended_config(&mut env, &config_map);

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
    let metadata_config: std::collections::HashMap<String, String> = match mc_str {
        Some(s) if !s.is_empty() => match serde_json::from_str(&s) {
            Ok(map) => map,
            Err(e) => {
                to_java_exception(&mut env, &anyhow::anyhow!("Invalid metadata_config_json: {}", e));
                return std::ptr::null_mut();
            }
        },
        _ => std::collections::HashMap::new(),
    };

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
    let metadata_config: std::collections::HashMap<String, String> = match mc_str {
        Some(s) if !s.is_empty() => match serde_json::from_str(&s) {
            Ok(map) => map,
            Err(e) => {
                to_java_exception(&mut env, &anyhow::anyhow!("Invalid metadata_config_json: {}", e));
                return std::ptr::null_mut();
            }
        },
        _ => std::collections::HashMap::new(),
    };

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

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();

    match block_on_operation(async move {
        let result = distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await?;
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();

    match block_on_operation(async move {
        let result = distributed::write_version_once(&path, &config, actions).await?;
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();

    match block_on_operation(async move {
        let result = distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await?;
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();

    match block_on_operation(async move {
        let result = distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await?;
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();

    match block_on_operation(async move {
        let result = distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await?;
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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
// PURGE PRIMITIVES
// ============================================================================

/// List retained (non-expired) version numbers. Returns JSON array.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListRetainedVersions(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    retention_ms: jlong,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        super::purge::list_retained_versions(&path, &config, retention_ms).await
    }) {
        Ok(versions) => {
            let json = serde_json::to_string(&versions).unwrap_or_else(|_| "[]".to_string());
            let jstr = match env.new_string(&json) {
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

/// Open a cursor over retained file paths (Arrow FFI streaming).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeOpenRetainedFilesCursor(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    retention_ms: jlong,
) -> jlong {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return -1; }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        super::purge::open_retained_files_cursor(&path, &config, retention_ms).await
    }) {
        Ok(cursor_id) => cursor_id,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            -1
        }
    }
}

/// Read next batch of retained files from cursor. Returns TANT buffer with path/size/version,
/// or null when exhausted.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadNextRetainedFilesBatch(
    mut env: JNIEnv,
    _class: JClass,
    cursor_handle: jlong,
    batch_size: jint,
) -> jbyteArray {
    let bs = if batch_size <= 0 { 10_000 } else { batch_size as usize };

    match super::purge::read_next_retained_files_batch(cursor_handle, bs) {
        Ok(Some(batch)) => {
            // Serialize as TANT buffer: each row is a document with path, size, version fields
            let num_rows = batch.num_rows();
            let path_col = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            let size_col = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            let ver_col = batch.column(2).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();

            let magic: u32 = 0x54414E54;
            let mut buf = Vec::with_capacity(4 + num_rows * 100 + 12);
            buf.extend_from_slice(&magic.to_ne_bytes());

            let mut offsets = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                offsets.push(buf.len() as u32);
                let fc: u16 = 3;
                buf.extend_from_slice(&fc.to_ne_bytes());
                // path
                let name = b"path";
                buf.extend_from_slice(&(name.len() as u16).to_ne_bytes());
                buf.extend_from_slice(name);
                buf.push(0u8); // TEXT
                buf.extend_from_slice(&1u16.to_ne_bytes());
                let pv = path_col.value(i).as_bytes();
                buf.extend_from_slice(&(pv.len() as u32).to_ne_bytes());
                buf.extend_from_slice(pv);
                // size
                let name = b"size";
                buf.extend_from_slice(&(name.len() as u16).to_ne_bytes());
                buf.extend_from_slice(name);
                buf.push(1u8); // INTEGER
                buf.extend_from_slice(&1u16.to_ne_bytes());
                buf.extend_from_slice(&size_col.value(i).to_ne_bytes());
                // version
                let name = b"version";
                buf.extend_from_slice(&(name.len() as u16).to_ne_bytes());
                buf.extend_from_slice(name);
                buf.push(1u8); // INTEGER
                buf.extend_from_slice(&1u16.to_ne_bytes());
                buf.extend_from_slice(&ver_col.value(i).to_ne_bytes());
            }

            // Footer
            let offset_table_start = buf.len() as u32;
            for off in &offsets {
                buf.extend_from_slice(&off.to_ne_bytes());
            }
            buf.extend_from_slice(&offset_table_start.to_ne_bytes());
            buf.extend_from_slice(&(offsets.len() as u32).to_ne_bytes());
            buf.extend_from_slice(&magic.to_ne_bytes());

            buffer_to_jbytearray(&mut env, &buf)
        }
        Ok(None) => std::ptr::null_mut(), // Exhausted
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Close a retained files cursor.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeCloseRetainedFilesCursor(
    _env: JNIEnv,
    _class: JClass,
    cursor_handle: jlong,
) {
    super::purge::close_retained_files_cursor(cursor_handle);
}

/// List skip actions from recent version files. Returns TANT buffer of skip actions.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListSkipActions(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    max_age_ms: jlong,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    match block_on_operation(async move {
        super::purge::list_skip_actions(&path, &config, max_age_ms).await
    }) {
        Ok(skips) => {
            let buf = serialization::serialize_skip_actions(&skips);
            buffer_to_jbytearray(&mut env, &buf)
        }
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            std::ptr::null_mut()
        }
    }
}

/// Delete expired state directories. Returns JSON: { "found": N, "deleted": N }.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeDeleteExpiredStates(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    retention_ms: jlong,
    dry_run: jint,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let is_dry_run = dry_run != 0;

    match block_on_operation(async move {
        super::purge::delete_expired_states(&path, &config, retention_ms, is_dry_run).await
    }) {
        Ok(result) => {
            let json = serde_json::to_string(&result).unwrap_or_else(|_| r#"{"found":0,"deleted":0}"#.to_string());
            let jstr = match env.new_string(&json) {
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

/// Delete expired version files. Returns JSON: { "found": N, "deleted": N }.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeDeleteExpiredVersions(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    retention_ms: jlong,
    dry_run: jint,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let is_dry_run = dry_run != 0;

    match block_on_operation(async move {
        super::purge::delete_expired_versions(&path, &config, retention_ms, is_dry_run).await
    }) {
        Ok(result) => {
            let json = serde_json::to_string(&result).unwrap_or_else(|_| r#"{"found":0,"deleted":0}"#.to_string());
            let jstr = match env.new_string(&json) {
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
// CACHE MANAGEMENT
// ============================================================================

/// Explicitly invalidate all cached data for a table path.
/// The Scala layer should call this after purge/truncate operations to ensure
/// subsequent reads get fresh data. Handles path normalization internally.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeInvalidateCache(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
) {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(_) => return,
    };
    super::cache::invalidate_table_cache(&path);
    // Also clear the global manifest cache for this table
    super::cache::invalidate_manifest_cache_for_table(&path);
}

// ============================================================================
// STATEFUL READ OPERATIONS (with cache)
// ============================================================================

// ============================================================================
// ARROW FFI OPERATIONS (FR1, FR2, FR3)
// ============================================================================

/// FR1: List files with partition pruning, data skipping, and cooldown filtering.
/// Returns result as Arrow FFI columns written to pre-allocated addresses.
/// Returns a TANT buffer with ListFilesResult metadata.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListFilesArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    partition_filter_json: JString,
    data_filter_json: JString,
    exclude_cooldown: jint,
    include_stats: jint,
    array_addrs: jni::sys::jlongArray,
    schema_addrs: jni::sys::jlongArray,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let cache_config = extract_extended_config(&mut env, &config_map);

    let pf_str = extract_optional_jstring(&mut env, &partition_filter_json);
    let df_str = extract_optional_jstring(&mut env, &data_filter_json);
    let exclude_cooldown_files = exclude_cooldown != 0;
    let include_stats_flag = include_stats != 0;

    // Extract array/schema addresses from Java long[]
    if array_addrs.is_null() || schema_addrs.is_null() {
        to_java_exception(&mut env, &anyhow::anyhow!("array_addrs or schema_addrs is null"));
        return std::ptr::null_mut();
    }
    let arr_addrs = match unsafe { env.get_array_elements(&jni::objects::JPrimitiveArray::from_raw(array_addrs), jni::objects::ReleaseMode::NoCopyBack) } {
        Ok(elems) => {
            let slice = unsafe { std::slice::from_raw_parts(elems.as_ptr(), elems.len()) };
            slice.to_vec()
        }
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Failed to read array_addrs: {}", e)); return std::ptr::null_mut(); }
    };
    let sch_addrs = match unsafe { env.get_array_elements(&jni::objects::JPrimitiveArray::from_raw(schema_addrs), jni::objects::ReleaseMode::NoCopyBack) } {
        Ok(elems) => {
            let slice = unsafe { std::slice::from_raw_parts(elems.as_ptr(), elems.len()) };
            slice.to_vec()
        }
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Failed to read schema_addrs: {}", e)); return std::ptr::null_mut(); }
    };

    match block_on_operation(async move {
        unsafe {
            super::list_files::list_files_arrow_ffi(
                &path,
                &config,
                &cache_config,
                pf_str.as_deref(),
                df_str.as_deref(),
                exclude_cooldown_files,
                include_stats_flag,
                &arr_addrs,
                &sch_addrs,
                None, // TODO: pass cache_manager_ptr when JNI signature adds it
            ).await
        }
    }) {
        Ok(result) => {
            // Serialize ListFilesResult as JSON for the Java side
            let json = serde_json::json!({
                "numRows": result.num_rows,
                "numColumns": result.num_columns,
                "schemaJson": result.schema_json,
                "partitionColumns": result.partition_columns,
                "protocolJson": result.protocol_json,
                "metadataConfigJson": result.metadata_config_json,
                "totalFilesBeforeFiltering": result.total_files_before_filtering,
                "filesAfterPartitionPruning": result.files_after_partition_pruning,
                "filesAfterDataSkipping": result.files_after_data_skipping,
                "filesAfterCooldownFiltering": result.files_after_cooldown_filtering,
                "manifestsTotal": result.manifests_total,
                "manifestsPruned": result.manifests_pruned,
            });
            let json_str = json.to_string();
            let jstr = match env.new_string(&json_str) {
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

/// FR2: Write a version file from an Arrow batch via FFI.
/// The Arrow batch contains action rows with an "action_type" column.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeWriteVersionArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    array_addr: jlong,
    schema_addr: jlong,
    retry: jint,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);

    // Import Arrow batch from FFI
    let actions = match unsafe { super::arrow_ffi_import::import_and_convert_actions(array_addr, schema_addr) } {
        Ok(a) => a,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Arrow import failed: {}", e));
            return std::ptr::null_mut();
        }
    };

    let ext_config = extract_extended_config(&mut env, &config_map);
    let path2 = path.clone();
    let config2 = config.clone();
    let ext_config2 = ext_config.clone();
    let do_retry = retry != 0;

    match block_on_operation(async move {
        let result = if do_retry {
            distributed::write_version(&path, &config, actions, distributed::RetryConfig::default()).await?
        } else {
            distributed::write_version_once(&path, &config, actions).await?
        };
        if result.version >= 0 {
            distributed::maybe_auto_checkpoint(&path2, &config2, &ext_config2, result.version).await;
        }
        Ok::<_, super::error::TxLogError>(result)
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

/// FR3: Read next batch of retained files from cursor via Arrow FFI.
/// Writes FileEntry columns directly to pre-allocated FFI addresses.
/// Returns the number of rows written, or 0 when exhausted.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadNextRetainedFilesBatchArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    cursor_handle: jlong,
    batch_size: jint,
    array_addrs: jni::sys::jlongArray,
    schema_addrs: jni::sys::jlongArray,
) -> jint {
    let bs = if batch_size <= 0 { 10_000 } else { batch_size as usize };

    if array_addrs.is_null() || schema_addrs.is_null() {
        to_java_exception(&mut env, &anyhow::anyhow!("array_addrs or schema_addrs is null"));
        return -1;
    }
    let arr_addrs = match unsafe { env.get_array_elements(&jni::objects::JPrimitiveArray::from_raw(array_addrs), jni::objects::ReleaseMode::NoCopyBack) } {
        Ok(elems) => {
            let slice = unsafe { std::slice::from_raw_parts(elems.as_ptr(), elems.len()) };
            slice.to_vec()
        }
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Failed to read array_addrs: {}", e)); return -1; }
    };
    let sch_addrs = match unsafe { env.get_array_elements(&jni::objects::JPrimitiveArray::from_raw(schema_addrs), jni::objects::ReleaseMode::NoCopyBack) } {
        Ok(elems) => {
            let slice = unsafe { std::slice::from_raw_parts(elems.as_ptr(), elems.len()) };
            slice.to_vec()
        }
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("Failed to read schema_addrs: {}", e)); return -1; }
    };

    match unsafe { super::purge::read_next_retained_files_batch_ffi(cursor_handle, bs, &arr_addrs, &sch_addrs) } {
        Ok(num_rows) => num_rows as jint,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("{}", e));
            -1
        }
    }
}

// ============================================================================
// TEST HELPERS (allocate FFI memory on Rust side, return summary as JSON)
// ============================================================================

/// Test helper: list files with filters, allocate FFI internally, read back Arrow columns,
/// return JSON summary with file paths and metrics. No Arrow Java dependency needed.
///
/// Returns JSON: { "numRows": N, "paths": [...], "partitionColumns": [...], "metrics": {...} }
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeTestListFilesRoundtrip(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    config_map: JObject,
    partition_filter_json: JString,
    data_filter_json: JString,
    exclude_cooldown: jint,
) -> jbyteArray {
    let path = match env.get_string(&table_path) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => { to_java_exception(&mut env, &anyhow::anyhow!("{}", e)); return std::ptr::null_mut(); }
    };
    let config = build_storage_config(&mut env, &config_map);
    let cache_config = extract_extended_config(&mut env, &config_map);

    let pf_str = extract_optional_jstring(&mut env, &partition_filter_json);
    let df_str = extract_optional_jstring(&mut env, &data_filter_json);
    let exclude_cooldown_files = exclude_cooldown != 0;

    match block_on_operation(async move {
        // First, get snapshot to determine partition columns for column count
        let snapshot = distributed::get_txlog_snapshot_info_with_cache(
            &path, &config, &cache_config,
        ).await?;
        let partition_columns = snapshot.metadata.partition_columns.clone();
        let num_cols = super::arrow_ffi::column_count(&partition_columns, false);

        // Allocate FFI memory on Rust side using heap-allocated boxes.
        // export_file_entries_ffi writes via write_unaligned to these addresses.
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        let mut ffi_array_boxes: Vec<Box<std::mem::MaybeUninit<FFI_ArrowArray>>> =
            (0..num_cols).map(|_| Box::new(std::mem::MaybeUninit::uninit())).collect();
        let mut ffi_schema_boxes: Vec<Box<std::mem::MaybeUninit<FFI_ArrowSchema>>> =
            (0..num_cols).map(|_| Box::new(std::mem::MaybeUninit::uninit())).collect();

        let array_addrs: Vec<i64> = ffi_array_boxes.iter_mut()
            .map(|b| b.as_mut_ptr() as *mut FFI_ArrowArray as i64)
            .collect();
        let schema_addrs: Vec<i64> = ffi_schema_boxes.iter_mut()
            .map(|b| b.as_mut_ptr() as *mut FFI_ArrowSchema as i64)
            .collect();

        // Call list_files_arrow_ffi
        let result = unsafe {
            super::list_files::list_files_arrow_ffi(
                &path, &config, &cache_config,
                pf_str.as_deref(), df_str.as_deref(),
                exclude_cooldown_files, false,
                &array_addrs, &schema_addrs,
                None,
            ).await?
        };

        // Read back file paths from Arrow column 0 (path)
        // After list_files_arrow_ffi, the MaybeUninit boxes have been written to and are initialized.
        let paths: Vec<String> = if result.num_rows > 0 {
            let ffi_arr = unsafe { ffi_array_boxes[0].assume_init_read() };
            let ffi_sch = unsafe { ffi_schema_boxes[0].assume_init_ref() };
            let data = unsafe { arrow::ffi::from_ffi(ffi_arr, ffi_sch) }
                .map_err(|e| super::error::TxLogError::Storage(anyhow::anyhow!("Arrow reimport col 0: {}", e)))?;
            let path_arr = arrow::array::make_array(data);
            let str_arr = path_arr.as_any().downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| super::error::TxLogError::Storage(anyhow::anyhow!("Column 0 not StringArray")))?;
            (0..str_arr.len()).map(|i| str_arr.value(i).to_string()).collect()
        } else {
            Vec::new()
        };

        // Read back sizes from Arrow column 1
        let sizes: Vec<i64> = if result.num_rows > 0 {
            let ffi_arr = unsafe { ffi_array_boxes[1].assume_init_read() };
            let ffi_sch = unsafe { ffi_schema_boxes[1].assume_init_ref() };
            let data = unsafe { arrow::ffi::from_ffi(ffi_arr, ffi_sch) }
                .map_err(|e| super::error::TxLogError::Storage(anyhow::anyhow!("Arrow reimport col 1: {}", e)))?;
            let size_arr = arrow::array::make_array(data);
            let int_arr = size_arr.as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| super::error::TxLogError::Storage(anyhow::anyhow!("Column 1 not Int64Array")))?;
            (0..int_arr.len()).map(|i| int_arr.value(i)).collect()
        } else {
            Vec::new()
        };

        // Clean up remaining FFI resources to prevent memory leaks.
        // Columns 0 and 1 were consumed above via assume_init_read → from_ffi.
        // Remaining columns need their release callbacks invoked via drop.
        for i in 2..ffi_array_boxes.len() {
            unsafe { drop(ffi_array_boxes[i].assume_init_read()); }
        }
        for i in 0..ffi_schema_boxes.len() {
            // Schema 0 was only borrowed (assume_init_ref), not consumed — also needs cleanup.
            // Schema 1 was borrowed too.
            unsafe { drop(ffi_schema_boxes[i].assume_init_read()); }
        }

        let json = serde_json::json!({
            "numRows": result.num_rows,
            "numColumns": result.num_columns,
            "paths": paths,
            "sizes": sizes,
            "partitionColumns": result.partition_columns,
            "protocolJson": result.protocol_json,
            "metrics": {
                "totalFilesBeforeFiltering": result.total_files_before_filtering,
                "filesAfterPartitionPruning": result.files_after_partition_pruning,
                "filesAfterDataSkipping": result.files_after_data_skipping,
                "filesAfterCooldownFiltering": result.files_after_cooldown_filtering,
                "manifestsTotal": result.manifests_total,
                "manifestsPruned": result.manifests_pruned,
            }
        });

        Ok::<_, super::error::TxLogError>(json.to_string())
    }) {
        Ok(json_str) => {
            let jstr = match env.new_string(&json_str) {
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
