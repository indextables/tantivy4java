// common.rs - Shared JNI helpers used across delta_reader, iceberg_reader, and parquet_reader.
//
// Consolidates extract_string(), buffer_to_jbytearray(), extract_hashmap(),
// extract_string_list(), and build_storage_config() to avoid duplication.

use std::collections::HashMap;

use jni::objects::{JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;
use serde::Deserialize;

use crate::delta_reader::engine::DeltaStorageConfig;

/// Convert Rust error to Java exception.
pub fn to_java_exception(env: &mut JNIEnv, error: &anyhow::Error) {
    let error_message = format!("{}", error);
    let _ = env.throw_new("java/lang/RuntimeException", error_message);
}

/// Extract a String value from a Java HashMap<String,String> by key.
pub fn extract_string(env: &mut JNIEnv, map: &JObject, key: &str) -> Option<String> {
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

/// Copy a byte slice into a new Java byte array (jbyteArray).
///
/// On failure, throws a Java RuntimeException and returns null.
pub fn buffer_to_jbytearray(env: &mut JNIEnv, buffer: &[u8]) -> jbyteArray {
    if buffer.len() > i32::MAX as usize {
        to_java_exception(
            env,
            &anyhow::anyhow!(
                "Buffer too large for Java byte array: {} bytes exceeds i32::MAX",
                buffer.len()
            ),
        );
        return std::ptr::null_mut();
    }
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

/// Extract a full Java HashMap<String,String> into a Rust HashMap.
pub fn extract_hashmap(env: &mut JNIEnv, map: &JObject) -> Result<HashMap<String, String>, String> {
    if map.is_null() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::new();

    let entry_set = env
        .call_method(map, "entrySet", "()Ljava/util/Set;", &[])
        .map_err(|e| format!("Failed to call entrySet(): {}", e))?
        .l()
        .map_err(|e| format!("entrySet() not an object: {}", e))?;

    let iterator = env
        .call_method(&entry_set, "iterator", "()Ljava/util/Iterator;", &[])
        .map_err(|e| format!("Failed to call iterator(): {}", e))?
        .l()
        .map_err(|e| format!("iterator() not an object: {}", e))?;

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

/// Extract a Java List<String> into a Vec<String>.
pub fn extract_string_list(env: &mut JNIEnv, list: &JObject) -> Result<Vec<String>, anyhow::Error> {
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

/// Build a DeltaStorageConfig from a Java HashMap<String,String>.
///
/// Used by delta_reader and parquet_reader JNI entry points.
pub fn build_storage_config(env: &mut JNIEnv, config_map: &JObject) -> DeltaStorageConfig {
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

// ---------------------------------------------------------------------------
// Partition predicate filtering
// ---------------------------------------------------------------------------

/// Type hint for comparison operators (gt, gte, lt, lte).
/// Falls back to string comparison when parsing fails.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CompareType {
    String,
    Long,
    Double,
}

impl Default for CompareType {
    fn default() -> Self {
        CompareType::String
    }
}

/// A partition predicate that can be evaluated against a row's partition values.
///
/// Deserialized from JSON passed across JNI. Example:
/// ```json
/// {"op": "eq", "column": "year", "value": "2024"}
/// {"op": "and", "filters": [{"op": "eq", "column": "year", "value": "2024"}, ...]}
/// ```
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PartitionPredicate {
    Eq {
        column: String,
        value: String,
    },
    Neq {
        column: String,
        value: String,
    },
    Gt {
        column: String,
        value: String,
        #[serde(default)]
        r#type: CompareType,
    },
    Gte {
        column: String,
        value: String,
        #[serde(default)]
        r#type: CompareType,
    },
    Lt {
        column: String,
        value: String,
        #[serde(default)]
        r#type: CompareType,
    },
    Lte {
        column: String,
        value: String,
        #[serde(default)]
        r#type: CompareType,
    },
    In {
        column: String,
        values: Vec<String>,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And {
        filters: Vec<PartitionPredicate>,
    },
    Or {
        filters: Vec<PartitionPredicate>,
    },
    Not {
        filter: Box<PartitionPredicate>,
    },
}

/// Compare two string values with optional numeric interpretation.
///
/// When a numeric type is specified but values fail to parse, falls back to
/// string comparison with a debug warning. NaN values are treated as greater
/// than all other values (consistent with f64::total_cmp behavior).
fn compare(a: &str, b: &str, cmp_type: &CompareType) -> std::cmp::Ordering {
    match cmp_type {
        CompareType::Long => {
            match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(av), Ok(bv)) => av.cmp(&bv),
                _ => {
                    // Numeric parse failed — fall back to string comparison.
                    // This handles cases like partition values that look numeric
                    // but contain unexpected characters.
                    crate::debug_println!(
                        "⚠️ PREDICATE: Long parse failed for '{}' vs '{}', falling back to string",
                        a, b
                    );
                    a.cmp(b)
                }
            }
        }
        CompareType::Double => {
            match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(av), Ok(bv)) => {
                    // Use total_cmp for consistent NaN handling:
                    // NaN is treated as greater than all values, providing
                    // deterministic ordering instead of Ordering::Equal.
                    av.total_cmp(&bv)
                }
                _ => {
                    crate::debug_println!(
                        "⚠️ PREDICATE: Double parse failed for '{}' vs '{}', falling back to string",
                        a, b
                    );
                    a.cmp(b)
                }
            }
        }
        CompareType::String => a.cmp(b),
    }
}

impl PartitionPredicate {
    /// Evaluate this predicate against a row's partition values.
    ///
    /// Semantics for missing columns:
    /// - `eq`/`gt`/`gte`/`lt`/`lte`/`in` → `false` (exclude)
    /// - `is_null` → `true`
    /// - `neq` → `true` (missing ≠ anything)
    /// - `is_not_null` → `false`
    pub fn evaluate(&self, partition_values: &HashMap<String, String>) -> bool {
        match self {
            PartitionPredicate::Eq { column, value } => {
                match partition_values.get(column) {
                    Some(v) => v == value,
                    None => false,
                }
            }
            PartitionPredicate::Neq { column, value } => {
                match partition_values.get(column) {
                    Some(v) => v != value,
                    None => true,
                }
            }
            PartitionPredicate::Gt { column, value, r#type } => {
                match partition_values.get(column) {
                    Some(v) => compare(v, value, r#type) == std::cmp::Ordering::Greater,
                    None => false,
                }
            }
            PartitionPredicate::Gte { column, value, r#type } => {
                match partition_values.get(column) {
                    Some(v) => compare(v, value, r#type) != std::cmp::Ordering::Less,
                    None => false,
                }
            }
            PartitionPredicate::Lt { column, value, r#type } => {
                match partition_values.get(column) {
                    Some(v) => compare(v, value, r#type) == std::cmp::Ordering::Less,
                    None => false,
                }
            }
            PartitionPredicate::Lte { column, value, r#type } => {
                match partition_values.get(column) {
                    Some(v) => compare(v, value, r#type) != std::cmp::Ordering::Greater,
                    None => false,
                }
            }
            PartitionPredicate::In { column, values } => {
                match partition_values.get(column) {
                    Some(v) => values.contains(v),
                    None => false,
                }
            }
            PartitionPredicate::IsNull { column } => {
                !partition_values.contains_key(column)
            }
            PartitionPredicate::IsNotNull { column } => {
                partition_values.contains_key(column)
            }
            PartitionPredicate::And { filters } => {
                filters.iter().all(|f| f.evaluate(partition_values))
            }
            PartitionPredicate::Or { filters } => {
                filters.iter().any(|f| f.evaluate(partition_values))
            }
            PartitionPredicate::Not { filter } => {
                !filter.evaluate(partition_values)
            }
        }
    }
}

/// Parse an optional predicate JSON string into a PartitionPredicate.
///
/// Returns `Ok(None)` if the input is `None` or empty (no filtering).
/// Returns `Err` if the JSON is non-empty but malformed.
pub fn parse_optional_predicate(
    json: Option<&str>,
) -> Result<Option<PartitionPredicate>, anyhow::Error> {
    match json {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => {
            let pred: PartitionPredicate = serde_json::from_str(s)
                .map_err(|e| anyhow::anyhow!("Failed to parse partition predicate: {}", e))?;
            Ok(Some(pred))
        }
    }
}

/// Extract an optional JString parameter from JNI, returning None for null.
pub fn extract_optional_jstring(env: &mut JNIEnv, s: &JString) -> Option<String> {
    if s.is_null() {
        return None;
    }
    env.get_string(s)
        .ok()
        .map(|js| js.to_string_lossy().to_string())
}

/// Filter entries by an optional partition predicate.
///
/// If predicate is None, returns entries unchanged.
/// Otherwise retains only entries whose partition_values satisfy the predicate.
pub fn filter_by_predicate<T, F>(
    entries: Vec<T>,
    predicate: &Option<PartitionPredicate>,
    get_partition_values: F,
) -> Vec<T>
where
    F: Fn(&T) -> &HashMap<String, String>,
{
    match predicate {
        None => entries,
        Some(pred) => entries
            .into_iter()
            .filter(|entry| pred.evaluate(get_partition_values(entry)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // -- Deserialization tests --

    #[test]
    fn test_parse_eq() {
        let json = r#"{"op": "eq", "column": "year", "value": "2024"}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        assert_eq!(
            pred,
            PartitionPredicate::Eq {
                column: "year".into(),
                value: "2024".into()
            }
        );
    }

    #[test]
    fn test_parse_in() {
        let json = r#"{"op": "in", "column": "region", "values": ["us-east-1", "us-west-2"]}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        assert_eq!(
            pred,
            PartitionPredicate::In {
                column: "region".into(),
                values: vec!["us-east-1".into(), "us-west-2".into()]
            }
        );
    }

    #[test]
    fn test_parse_gt_with_type() {
        let json = r#"{"op": "gt", "column": "year", "value": "2022", "type": "long"}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        assert_eq!(
            pred,
            PartitionPredicate::Gt {
                column: "year".into(),
                value: "2022".into(),
                r#type: CompareType::Long,
            }
        );
    }

    #[test]
    fn test_parse_gt_default_type() {
        let json = r#"{"op": "gt", "column": "year", "value": "2022"}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        match pred {
            PartitionPredicate::Gt { r#type, .. } => assert_eq!(r#type, CompareType::String),
            _ => panic!("expected Gt"),
        }
    }

    #[test]
    fn test_parse_and() {
        let json = r#"{"op": "and", "filters": [{"op": "eq", "column": "year", "value": "2024"}, {"op": "eq", "column": "month", "value": "01"}]}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        match pred {
            PartitionPredicate::And { filters } => assert_eq!(filters.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_parse_not() {
        let json = r#"{"op": "not", "filter": {"op": "eq", "column": "year", "value": "2020"}}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        match pred {
            PartitionPredicate::Not { filter } => {
                assert_eq!(
                    *filter,
                    PartitionPredicate::Eq {
                        column: "year".into(),
                        value: "2020".into()
                    }
                );
            }
            _ => panic!("expected Not"),
        }
    }

    #[test]
    fn test_parse_is_null() {
        let json = r#"{"op": "is_null", "column": "category"}"#;
        let pred: PartitionPredicate = serde_json::from_str(json).unwrap();
        assert_eq!(
            pred,
            PartitionPredicate::IsNull {
                column: "category".into()
            }
        );
    }

    // -- Evaluation tests --

    #[test]
    fn test_eval_eq_match() {
        let pred = PartitionPredicate::Eq {
            column: "year".into(),
            value: "2024".into(),
        };
        assert!(pred.evaluate(&pv(&[("year", "2024")])));
    }

    #[test]
    fn test_eval_eq_no_match() {
        let pred = PartitionPredicate::Eq {
            column: "year".into(),
            value: "2024".into(),
        };
        assert!(!pred.evaluate(&pv(&[("year", "2023")])));
    }

    #[test]
    fn test_eval_eq_missing_column() {
        let pred = PartitionPredicate::Eq {
            column: "year".into(),
            value: "2024".into(),
        };
        assert!(!pred.evaluate(&pv(&[("month", "01")])));
    }

    #[test]
    fn test_eval_neq() {
        let pred = PartitionPredicate::Neq {
            column: "year".into(),
            value: "2024".into(),
        };
        assert!(pred.evaluate(&pv(&[("year", "2023")])));
        assert!(!pred.evaluate(&pv(&[("year", "2024")])));
        // Missing column → true
        assert!(pred.evaluate(&pv(&[])));
    }

    #[test]
    fn test_eval_gt_long() {
        let pred = PartitionPredicate::Gt {
            column: "year".into(),
            value: "2022".into(),
            r#type: CompareType::Long,
        };
        assert!(pred.evaluate(&pv(&[("year", "2023")])));
        assert!(!pred.evaluate(&pv(&[("year", "2022")])));
        assert!(!pred.evaluate(&pv(&[("year", "2021")])));
        // Missing → false
        assert!(!pred.evaluate(&pv(&[])));
    }

    #[test]
    fn test_eval_gte_long() {
        let pred = PartitionPredicate::Gte {
            column: "year".into(),
            value: "2022".into(),
            r#type: CompareType::Long,
        };
        assert!(pred.evaluate(&pv(&[("year", "2023")])));
        assert!(pred.evaluate(&pv(&[("year", "2022")])));
        assert!(!pred.evaluate(&pv(&[("year", "2021")])));
    }

    #[test]
    fn test_eval_lt_string() {
        let pred = PartitionPredicate::Lt {
            column: "name".into(),
            value: "B".into(),
            r#type: CompareType::String,
        };
        assert!(pred.evaluate(&pv(&[("name", "A")])));
        assert!(!pred.evaluate(&pv(&[("name", "B")])));
        assert!(!pred.evaluate(&pv(&[("name", "C")])));
    }

    #[test]
    fn test_eval_lte_double() {
        let pred = PartitionPredicate::Lte {
            column: "score".into(),
            value: "3.14".into(),
            r#type: CompareType::Double,
        };
        assert!(pred.evaluate(&pv(&[("score", "3.14")])));
        assert!(pred.evaluate(&pv(&[("score", "2.0")])));
        assert!(!pred.evaluate(&pv(&[("score", "4.0")])));
    }

    #[test]
    fn test_eval_in() {
        let pred = PartitionPredicate::In {
            column: "region".into(),
            values: vec!["us-east-1".into(), "us-west-2".into()],
        };
        assert!(pred.evaluate(&pv(&[("region", "us-east-1")])));
        assert!(pred.evaluate(&pv(&[("region", "us-west-2")])));
        assert!(!pred.evaluate(&pv(&[("region", "eu-west-1")])));
        assert!(!pred.evaluate(&pv(&[])));
    }

    #[test]
    fn test_eval_is_null() {
        let pred = PartitionPredicate::IsNull {
            column: "category".into(),
        };
        assert!(pred.evaluate(&pv(&[])));
        assert!(pred.evaluate(&pv(&[("other", "val")])));
        assert!(!pred.evaluate(&pv(&[("category", "books")])));
    }

    #[test]
    fn test_eval_is_not_null() {
        let pred = PartitionPredicate::IsNotNull {
            column: "category".into(),
        };
        assert!(!pred.evaluate(&pv(&[])));
        assert!(pred.evaluate(&pv(&[("category", "books")])));
    }

    #[test]
    fn test_eval_and() {
        let pred = PartitionPredicate::And {
            filters: vec![
                PartitionPredicate::Eq {
                    column: "year".into(),
                    value: "2024".into(),
                },
                PartitionPredicate::Eq {
                    column: "month".into(),
                    value: "01".into(),
                },
            ],
        };
        assert!(pred.evaluate(&pv(&[("year", "2024"), ("month", "01")])));
        assert!(!pred.evaluate(&pv(&[("year", "2024"), ("month", "02")])));
        assert!(!pred.evaluate(&pv(&[("year", "2023"), ("month", "01")])));
    }

    #[test]
    fn test_eval_or() {
        let pred = PartitionPredicate::Or {
            filters: vec![
                PartitionPredicate::Eq {
                    column: "region".into(),
                    value: "us-east-1".into(),
                },
                PartitionPredicate::Eq {
                    column: "region".into(),
                    value: "us-west-2".into(),
                },
            ],
        };
        assert!(pred.evaluate(&pv(&[("region", "us-east-1")])));
        assert!(pred.evaluate(&pv(&[("region", "us-west-2")])));
        assert!(!pred.evaluate(&pv(&[("region", "eu-west-1")])));
    }

    #[test]
    fn test_eval_not() {
        let pred = PartitionPredicate::Not {
            filter: Box::new(PartitionPredicate::Eq {
                column: "year".into(),
                value: "2020".into(),
            }),
        };
        assert!(pred.evaluate(&pv(&[("year", "2024")])));
        assert!(!pred.evaluate(&pv(&[("year", "2020")])));
        // Missing column → eq returns false → not(false) = true
        assert!(pred.evaluate(&pv(&[])));
    }

    #[test]
    fn test_eval_nested_and_or() {
        // (year == 2024) AND (month IN [01, 02, 03])
        let pred = PartitionPredicate::And {
            filters: vec![
                PartitionPredicate::Eq {
                    column: "year".into(),
                    value: "2024".into(),
                },
                PartitionPredicate::In {
                    column: "month".into(),
                    values: vec!["01".into(), "02".into(), "03".into()],
                },
            ],
        };
        assert!(pred.evaluate(&pv(&[("year", "2024"), ("month", "01")])));
        assert!(pred.evaluate(&pv(&[("year", "2024"), ("month", "03")])));
        assert!(!pred.evaluate(&pv(&[("year", "2024"), ("month", "04")])));
        assert!(!pred.evaluate(&pv(&[("year", "2023"), ("month", "01")])));
    }

    // -- parse_optional_predicate tests --

    #[test]
    fn test_parse_optional_none() {
        assert!(parse_optional_predicate(None).unwrap().is_none());
    }

    #[test]
    fn test_parse_optional_empty() {
        assert!(parse_optional_predicate(Some("")).unwrap().is_none());
    }

    #[test]
    fn test_parse_optional_valid() {
        let pred = parse_optional_predicate(Some(r#"{"op": "eq", "column": "x", "value": "1"}"#))
            .unwrap()
            .unwrap();
        assert_eq!(
            pred,
            PartitionPredicate::Eq {
                column: "x".into(),
                value: "1".into()
            }
        );
    }

    #[test]
    fn test_parse_optional_invalid() {
        assert!(parse_optional_predicate(Some("{invalid json")).is_err());
    }

    // -- filter_by_predicate tests --

    #[test]
    fn test_filter_by_predicate_none() {
        let entries = vec![pv(&[("year", "2024")]), pv(&[("year", "2023")])];
        let result = filter_by_predicate(entries.clone(), &None, |e| e);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_filter_by_predicate_some() {
        let entries = vec![
            pv(&[("year", "2024")]),
            pv(&[("year", "2023")]),
            pv(&[("year", "2024"), ("month", "02")]),
        ];
        let pred = Some(PartitionPredicate::Eq {
            column: "year".into(),
            value: "2024".into(),
        });
        let result = filter_by_predicate(entries, &pred, |e| e);
        assert_eq!(result.len(), 2);
    }

    // -- Long comparison edge cases --

    #[test]
    fn test_long_compare_numeric_not_lexicographic() {
        // "9" > "10" lexicographically, but 9 < 10 numerically
        let pred = PartitionPredicate::Gt {
            column: "id".into(),
            value: "9".into(),
            r#type: CompareType::Long,
        };
        assert!(pred.evaluate(&pv(&[("id", "10")])));

        let pred_str = PartitionPredicate::Gt {
            column: "id".into(),
            value: "9".into(),
            r#type: CompareType::String,
        };
        // Lexicographic: "10" < "9"
        assert!(!pred_str.evaluate(&pv(&[("id", "10")])));
    }

    #[test]
    fn test_double_nan_handling() {
        // NaN should not cause Ordering::Equal — total_cmp treats NaN as greater than all values
        let pred = PartitionPredicate::Gt {
            column: "score".into(),
            value: "NaN".into(),
            r#type: CompareType::Double,
        };
        // "1.0" > "NaN" should be false (NaN is treated as > everything)
        assert!(!pred.evaluate(&pv(&[("score", "1.0")])));

        // NaN > "1.0" should be true
        let pred2 = PartitionPredicate::Gt {
            column: "score".into(),
            value: "1.0".into(),
            r#type: CompareType::Double,
        };
        assert!(pred2.evaluate(&pv(&[("score", "NaN")])));
    }

    #[test]
    fn test_double_parse_failure_fallback() {
        // Non-numeric values with Double type should fall back to string comparison
        let pred = PartitionPredicate::Gt {
            column: "val".into(),
            value: "abc".into(),
            r#type: CompareType::Double,
        };
        // "xyz" > "abc" lexicographically
        assert!(pred.evaluate(&pv(&[("val", "xyz")])));
        // "aaa" < "abc" lexicographically
        assert!(!pred.evaluate(&pv(&[("val", "aaa")])));
    }

    #[test]
    fn test_empty_and_or_evaluation() {
        // Empty And (if constructed manually in Rust) — all() on empty = true
        let pred = PartitionPredicate::And { filters: vec![] };
        assert!(pred.evaluate(&pv(&[("x", "1")])));

        // Empty Or (if constructed manually in Rust) — any() on empty = false
        let pred = PartitionPredicate::Or { filters: vec![] };
        assert!(!pred.evaluate(&pv(&[("x", "1")])));
    }

    // -- Date partition filtering tests --
    // Partition values are always strings. ISO 8601 dates (YYYY-MM-DD) are
    // lexicographically sortable, so string comparison works correctly.
    // Epoch-based date partitions work with CompareType::Long.

    #[test]
    fn test_date_iso8601_string_eq() {
        let pred = PartitionPredicate::Eq {
            column: "date".into(),
            value: "2024-06-15".into(),
        };
        assert!(pred.evaluate(&pv(&[("date", "2024-06-15")])));
        assert!(!pred.evaluate(&pv(&[("date", "2024-06-16")])));
    }

    #[test]
    fn test_date_iso8601_string_range() {
        // String comparison on ISO 8601 dates produces correct ordering
        let pred = PartitionPredicate::Gte {
            column: "date".into(),
            value: "2024-01-01".into(),
            r#type: CompareType::String,
        };
        assert!(pred.evaluate(&pv(&[("date", "2024-01-01")])));  // equal
        assert!(pred.evaluate(&pv(&[("date", "2024-06-15")])));  // after
        assert!(pred.evaluate(&pv(&[("date", "2024-12-31")])));  // end of year
        assert!(!pred.evaluate(&pv(&[("date", "2023-12-31")]))); // previous year
    }

    #[test]
    fn test_date_iso8601_between_range() {
        // Common pattern: filter dates in Q1 2024
        let pred = PartitionPredicate::And {
            filters: vec![
                PartitionPredicate::Gte {
                    column: "date".into(),
                    value: "2024-01-01".into(),
                    r#type: CompareType::String,
                },
                PartitionPredicate::Lt {
                    column: "date".into(),
                    value: "2024-04-01".into(),
                    r#type: CompareType::String,
                },
            ],
        };
        assert!(pred.evaluate(&pv(&[("date", "2024-01-01")])));  // start of Q1
        assert!(pred.evaluate(&pv(&[("date", "2024-02-14")])));  // mid Q1
        assert!(pred.evaluate(&pv(&[("date", "2024-03-31")])));  // end of Q1
        assert!(!pred.evaluate(&pv(&[("date", "2024-04-01")]))); // start of Q2
        assert!(!pred.evaluate(&pv(&[("date", "2023-12-31")]))); // before range
    }

    #[test]
    fn test_date_year_month_day_partition_columns() {
        // Hive-style partitioning: year=2024/month=06/day=15
        let pred = PartitionPredicate::And {
            filters: vec![
                PartitionPredicate::Eq {
                    column: "year".into(),
                    value: "2024".into(),
                },
                PartitionPredicate::Gte {
                    column: "month".into(),
                    value: "6".into(),
                    r#type: CompareType::Long,
                },
                PartitionPredicate::Lte {
                    column: "month".into(),
                    value: "9".into(),
                    r#type: CompareType::Long,
                },
            ],
        };
        // Summer months in 2024
        assert!(pred.evaluate(&pv(&[("year", "2024"), ("month", "6"), ("day", "1")])));
        assert!(pred.evaluate(&pv(&[("year", "2024"), ("month", "9"), ("day", "30")])));
        assert!(!pred.evaluate(&pv(&[("year", "2024"), ("month", "1"), ("day", "15")])));
        assert!(!pred.evaluate(&pv(&[("year", "2024"), ("month", "12"), ("day", "25")])));
        assert!(!pred.evaluate(&pv(&[("year", "2023"), ("month", "7"), ("day", "4")])));
    }

    #[test]
    fn test_date_epoch_millis_long_comparison() {
        // Some tables partition by epoch millis (e.g., 1704067200000 = 2024-01-01T00:00:00Z)
        let pred = PartitionPredicate::Gte {
            column: "timestamp_ms".into(),
            value: "1704067200000".into(),  // 2024-01-01
            r#type: CompareType::Long,
        };
        assert!(pred.evaluate(&pv(&[("timestamp_ms", "1704067200000")])));  // exactly 2024-01-01
        assert!(pred.evaluate(&pv(&[("timestamp_ms", "1719792000000")])));  // 2024-07-01
        assert!(!pred.evaluate(&pv(&[("timestamp_ms", "1672531200000")]))); // 2023-01-01
    }

    #[test]
    fn test_date_epoch_days_long_comparison() {
        // Some tables partition by epoch days (e.g., Delta date type)
        // Day 19723 = 2024-01-01 (days since 1970-01-01)
        let pred = PartitionPredicate::And {
            filters: vec![
                PartitionPredicate::Gte {
                    column: "date_days".into(),
                    value: "19723".into(),  // 2024-01-01
                    r#type: CompareType::Long,
                },
                PartitionPredicate::Lt {
                    column: "date_days".into(),
                    value: "19814".into(),  // 2024-04-01
                    r#type: CompareType::Long,
                },
            ],
        };
        assert!(pred.evaluate(&pv(&[("date_days", "19723")])));   // 2024-01-01
        assert!(pred.evaluate(&pv(&[("date_days", "19750")])));   // mid-range
        assert!(!pred.evaluate(&pv(&[("date_days", "19814")])));  // 2024-04-01 (exclusive)
        assert!(!pred.evaluate(&pv(&[("date_days", "19358")])));  // 2023-01-01
    }

    #[test]
    fn test_date_in_set() {
        // Filter for specific dates
        let pred = PartitionPredicate::In {
            column: "date".into(),
            values: vec![
                "2024-01-01".into(),
                "2024-07-04".into(),
                "2024-12-25".into(),
            ],
        };
        assert!(pred.evaluate(&pv(&[("date", "2024-01-01")])));
        assert!(pred.evaluate(&pv(&[("date", "2024-12-25")])));
        assert!(!pred.evaluate(&pv(&[("date", "2024-06-15")])));
    }

    #[test]
    fn test_date_iso8601_with_time() {
        // ISO 8601 with time component — still lexicographically sortable
        let pred = PartitionPredicate::Gte {
            column: "timestamp".into(),
            value: "2024-01-01T00:00:00Z".into(),
            r#type: CompareType::String,
        };
        assert!(pred.evaluate(&pv(&[("timestamp", "2024-01-01T00:00:00Z")])));
        assert!(pred.evaluate(&pv(&[("timestamp", "2024-06-15T12:30:00Z")])));
        assert!(!pred.evaluate(&pv(&[("timestamp", "2023-12-31T23:59:59Z")])));
    }
}
