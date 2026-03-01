// parquet_reader/jni.rs - JNI entry points for Hive-style parquet directory reading
//
// Bridges Java ParquetTableReader native methods to the Rust parquet_reader
// module. Uses shared JNI helpers from common.rs.

use jni::objects::{JClass, JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::common::{
    to_java_exception, build_storage_config, buffer_to_jbytearray,
    extract_optional_jstring, parse_optional_predicate, filter_by_predicate,
};
use crate::debug_println;

use super::distributed::{get_parquet_table_info, list_partition_files, parse_partition_values_from_path};
use super::serialization::{serialize_parquet_table_info, serialize_parquet_file_entries};

// ── JNI entry points ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_parquet_ParquetTableReader_nativeGetTableInfo(
    mut env: JNIEnv,
    _class: JClass,
    table_url: JString,
    config_map: JObject,
    predicate_json: JString,
) -> jbyteArray {
    debug_println!("🔧 PARQUET_JNI: nativeGetTableInfo called");

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

    debug_println!(
        "🔧 PARQUET_JNI: getTableInfo url={}, has_aws={}, has_azure={}, has_predicate={}",
        url_str,
        config.aws_access_key.is_some(),
        config.azure_account_name.is_some(),
        predicate.is_some()
    );

    match get_parquet_table_info(&url_str, &config) {
        Ok(mut info) => {
            // Prune partition directories by parsing partition values from path
            // and testing against predicate — biggest win for Hive tables
            if let Some(ref pred) = predicate {
                info.partition_directories.retain(|dir| {
                    let values = parse_partition_values_from_path(dir);
                    pred.evaluate(&values)
                });
                info.root_parquet_files = filter_by_predicate(
                    info.root_parquet_files, &predicate, |e| &e.partition_values,
                );
            }
            debug_println!(
                "🔧 PARQUET_JNI: TableInfo: {} partition dirs, {} root files, partitioned={} (after filtering)",
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
    predicate_json: JString,
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

    let pred_str = extract_optional_jstring(&mut env, &predicate_json);
    let predicate = match parse_optional_predicate(pred_str.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let config = build_storage_config(&mut env, &config_map);

    debug_println!(
        "🔧 PARQUET_JNI: listPartitionFiles url={}, prefix={}, has_predicate={}",
        url_str, prefix_str, predicate.is_some()
    );

    match list_partition_files(&url_str, &config, &prefix_str) {
        Ok(entries) => {
            let entries = filter_by_predicate(entries, &predicate, |e| &e.partition_values);
            debug_println!(
                "🔧 PARQUET_JNI: Listed {} parquet files (after filtering)",
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
