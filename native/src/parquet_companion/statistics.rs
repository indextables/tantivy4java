// statistics.rs - Column statistics computation (Phase 3)
//
// Observes values during the indexing loop and computes min/max/null_count
// per column for split-level pruning.

use serde::{Serialize, Deserialize};

/// Accumulated statistics for a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatisticsResult {
    pub field_name: String,
    pub field_type: String,
    pub min_long: Option<i64>,
    pub max_long: Option<i64>,
    pub min_double: Option<f64>,
    pub max_double: Option<f64>,
    pub min_string: Option<String>,
    pub max_string: Option<String>,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    pub min_bool: Option<bool>,
    pub max_bool: Option<bool>,
    pub null_count: u64,
}

/// Accumulator for computing column statistics during indexing
pub struct StatisticsAccumulator {
    field_name: String,
    field_type: String,
    min_long: Option<i64>,
    max_long: Option<i64>,
    min_double: Option<f64>,
    max_double: Option<f64>,
    min_string: Option<String>,
    max_string: Option<String>,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    min_bool: Option<bool>,
    max_bool: Option<bool>,
    null_count: u64,
    truncate_length: usize,
}

impl StatisticsAccumulator {
    pub fn new(field_name: &str, field_type: &str, truncate_length: usize) -> Self {
        Self {
            field_name: field_name.to_string(),
            field_type: field_type.to_string(),
            min_long: None,
            max_long: None,
            min_double: None,
            max_double: None,
            min_string: None,
            max_string: None,
            min_timestamp: None,
            max_timestamp: None,
            min_bool: None,
            max_bool: None,
            null_count: 0,
            truncate_length,
        }
    }

    pub fn observe_null(&mut self) {
        self.null_count += 1;
    }

    pub fn observe_i64(&mut self, value: i64) {
        self.min_long = Some(self.min_long.map_or(value, |m| m.min(value)));
        self.max_long = Some(self.max_long.map_or(value, |m| m.max(value)));
    }

    pub fn observe_f64(&mut self, value: f64) {
        // Exclude NaN from statistics
        if value.is_nan() {
            return;
        }
        self.min_double = Some(self.min_double.map_or(value, |m| m.min(value)));
        self.max_double = Some(self.max_double.map_or(value, |m| m.max(value)));
    }

    pub fn observe_string(&mut self, value: &str) {
        let truncated_min = if value.len() > self.truncate_length {
            value[..self.truncate_length].to_string()
        } else {
            value.to_string()
        };

        let truncated_max = if value.len() > self.truncate_length {
            truncate_ceiling(&value[..self.truncate_length])
        } else {
            value.to_string()
        };

        self.min_string = Some(
            self.min_string
                .as_ref()
                .map_or(truncated_min.clone(), |m| {
                    if truncated_min < *m { truncated_min.clone() } else { m.clone() }
                }),
        );
        self.max_string = Some(
            self.max_string
                .as_ref()
                .map_or(truncated_max.clone(), |m| {
                    if truncated_max > *m { truncated_max.clone() } else { m.clone() }
                }),
        );
    }

    pub fn observe_timestamp_micros(&mut self, micros: i64) {
        self.min_timestamp = Some(self.min_timestamp.map_or(micros, |m| m.min(micros)));
        self.max_timestamp = Some(self.max_timestamp.map_or(micros, |m| m.max(micros)));
    }

    pub fn observe_bool(&mut self, value: bool) {
        self.min_bool = Some(self.min_bool.map_or(value, |m| m && value));
        self.max_bool = Some(self.max_bool.map_or(value, |m| m || value));
    }

    pub fn finalize(self) -> ColumnStatisticsResult {
        ColumnStatisticsResult {
            field_name: self.field_name,
            field_type: self.field_type,
            min_long: self.min_long,
            max_long: self.max_long,
            min_double: self.min_double,
            max_double: self.max_double,
            min_string: self.min_string,
            max_string: self.max_string,
            min_timestamp_micros: self.min_timestamp,
            max_timestamp_micros: self.max_timestamp,
            min_bool: self.min_bool,
            max_bool: self.max_bool,
            null_count: self.null_count,
        }
    }
}

/// Truncate a string and adjust the last character up by 1 to create a ceiling value.
/// This ensures pruning correctness: any string that starts with the original prefix
/// is guaranteed to be <= the ceiling.
fn truncate_ceiling(s: &str) -> String {
    let mut chars: Vec<char> = s.chars().collect();
    // Walk backwards to find a character we can increment
    while let Some(last) = chars.last_mut() {
        if *last < char::MAX {
            *last = char::from_u32(*last as u32 + 1).unwrap_or(char::MAX);
            return chars.into_iter().collect();
        }
        chars.pop();
    }
    // All chars were MAX - return the original
    s.to_string()
}

/// Validate that statistics fields don't include unsupported types (Json, Bytes)
pub fn validate_statistics_fields(
    fields: &[String],
    field_types: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    for field in fields {
        if let Some(field_type) = field_types.get(field) {
            if field_type == "Json" || field_type == "Bytes" {
                return Err(format!(
                    "Field '{}' has type '{}' which does not support statistics",
                    field, field_type
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i64_statistics() {
        let mut acc = StatisticsAccumulator::new("count", "I64", 256);
        acc.observe_i64(10);
        acc.observe_i64(-5);
        acc.observe_i64(100);
        let result = acc.finalize();
        assert_eq!(result.min_long, Some(-5));
        assert_eq!(result.max_long, Some(100));
        assert_eq!(result.null_count, 0);
    }

    #[test]
    fn test_f64_nan_excluded() {
        let mut acc = StatisticsAccumulator::new("score", "F64", 256);
        acc.observe_f64(1.0);
        acc.observe_f64(f64::NAN);
        acc.observe_f64(3.0);
        let result = acc.finalize();
        assert_eq!(result.min_double, Some(1.0));
        assert_eq!(result.max_double, Some(3.0));
    }

    #[test]
    fn test_null_count() {
        let mut acc = StatisticsAccumulator::new("x", "I64", 256);
        acc.observe_null();
        acc.observe_i64(42);
        acc.observe_null();
        let result = acc.finalize();
        assert_eq!(result.null_count, 2);
        assert_eq!(result.min_long, Some(42));
    }

    #[test]
    fn test_string_truncation() {
        let mut acc = StatisticsAccumulator::new("name", "Str", 5);
        acc.observe_string("abcdefgh");
        let result = acc.finalize();
        assert_eq!(result.min_string, Some("abcde".to_string()));
        // max should be ceiling-truncated
        assert_eq!(result.max_string, Some("abcdf".to_string()));
    }

    #[test]
    fn test_truncate_ceiling() {
        assert_eq!(truncate_ceiling("abc"), "abd");
        assert_eq!(truncate_ceiling("a"), "b");
    }

    #[test]
    fn test_bool_statistics() {
        let mut acc = StatisticsAccumulator::new("flag", "Bool", 256);
        acc.observe_bool(true);
        acc.observe_bool(false);
        let result = acc.finalize();
        assert_eq!(result.min_bool, Some(false));
        assert_eq!(result.max_bool, Some(true));
    }

    #[test]
    fn test_validate_statistics_fields_ok() {
        let mut types = std::collections::HashMap::new();
        types.insert("id".to_string(), "I64".to_string());
        types.insert("score".to_string(), "F64".to_string());
        types.insert("name".to_string(), "Str".to_string());
        types.insert("ts".to_string(), "Date".to_string());
        types.insert("flag".to_string(), "Bool".to_string());
        let result = validate_statistics_fields(
            &["id".to_string(), "score".to_string(), "name".to_string(), "ts".to_string(), "flag".to_string()],
            &types,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_statistics_fields_rejects_json() {
        let mut types = std::collections::HashMap::new();
        types.insert("data".to_string(), "Json".to_string());
        let result = validate_statistics_fields(&["data".to_string()], &types);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Json"));
    }

    #[test]
    fn test_validate_statistics_fields_rejects_bytes() {
        let mut types = std::collections::HashMap::new();
        types.insert("blob".to_string(), "Bytes".to_string());
        let result = validate_statistics_fields(&["blob".to_string()], &types);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Bytes"));
    }

    #[test]
    fn test_validate_statistics_fields_unknown_field_passes() {
        // Unknown fields (not in types map) should pass — validation only rejects known bad types
        let types = std::collections::HashMap::new();
        let result = validate_statistics_fields(&["mystery".to_string()], &types);
        assert!(result.is_ok());
    }

    #[test]
    fn test_timestamp_statistics() {
        let mut acc = StatisticsAccumulator::new("ts", "Date", 256);
        acc.observe_timestamp_micros(1_704_067_200_000_000); // 2024-01-01
        acc.observe_timestamp_micros(1_704_153_600_000_000); // 2024-01-02
        acc.observe_timestamp_micros(1_703_980_800_000_000); // 2023-12-31
        let result = acc.finalize();
        assert_eq!(result.min_timestamp_micros, Some(1_703_980_800_000_000));
        assert_eq!(result.max_timestamp_micros, Some(1_704_153_600_000_000));
    }

    #[test]
    fn test_all_nulls_statistics() {
        let mut acc = StatisticsAccumulator::new("empty", "I64", 256);
        acc.observe_null();
        acc.observe_null();
        acc.observe_null();
        let result = acc.finalize();
        assert_eq!(result.null_count, 3);
        assert_eq!(result.min_long, None);
        assert_eq!(result.max_long, None);
    }

    #[test]
    fn test_mixed_sign_i64_statistics() {
        let mut acc = StatisticsAccumulator::new("val", "I64", 256);
        acc.observe_i64(i64::MIN);
        acc.observe_i64(0);
        acc.observe_i64(i64::MAX);
        let result = acc.finalize();
        assert_eq!(result.min_long, Some(i64::MIN));
        assert_eq!(result.max_long, Some(i64::MAX));
    }

    #[test]
    fn test_empty_string_statistics() {
        let mut acc = StatisticsAccumulator::new("s", "Str", 256);
        acc.observe_string("");
        acc.observe_string("zzz");
        let result = acc.finalize();
        assert_eq!(result.min_string, Some("".to_string()));
        assert_eq!(result.max_string, Some("zzz".to_string()));
    }

    #[test]
    fn test_f64_infinity_statistics() {
        let mut acc = StatisticsAccumulator::new("v", "F64", 256);
        acc.observe_f64(f64::NEG_INFINITY);
        acc.observe_f64(0.0);
        acc.observe_f64(f64::INFINITY);
        let result = acc.finalize();
        assert_eq!(result.min_double, Some(f64::NEG_INFINITY));
        assert_eq!(result.max_double, Some(f64::INFINITY));
    }

    #[test]
    fn test_f64_all_nan_statistics() {
        let mut acc = StatisticsAccumulator::new("v", "F64", 256);
        acc.observe_f64(f64::NAN);
        acc.observe_f64(f64::NAN);
        let result = acc.finalize();
        // All NaN → no min/max
        assert_eq!(result.min_double, None);
        assert_eq!(result.max_double, None);
    }
}
