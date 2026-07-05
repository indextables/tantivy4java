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
    /// Count of NaN float values, tracked separately from null_count so an
    /// all-NaN column (min/max None, null_count 0, nan_count > 0) is
    /// distinguishable from an empty/all-null column and a pruning consumer
    /// does not treat it as "no data". Defaulted for manifest back-compat.
    #[serde(default)]
    pub nan_count: u64,
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
    nan_count: u64,
    /// Set when a truncated string value has no representable ceiling (all
    /// prefix chars are char::MAX). Once set, max_string is reported as None so
    /// pruning never uses a too-low upper bound (L6).
    max_string_unbounded: bool,
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
            nan_count: 0,
            max_string_unbounded: false,
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
        // NaN has no ordering and can't participate in min/max; count it
        // separately so an all-NaN column is distinguishable from empty (M10).
        if value.is_nan() {
            self.nan_count += 1;
            return;
        }
        self.min_double = Some(self.min_double.map_or(value, |m| m.min(value)));
        self.max_double = Some(self.max_double.map_or(value, |m| m.max(value)));
    }

    pub fn observe_string(&mut self, value: &str) {
        // Borrow the truncated prefix; allocate only when it becomes a new
        // extreme, and never clone the retained value on the no-change path (E4).
        let truncated_min: &str =
            if self.truncate_length > 0 && value.len() > self.truncate_length {
                safe_truncate(value, self.truncate_length)
            } else {
                value
            };

        match self.min_string {
            Some(ref m) if truncated_min >= m.as_str() => {} // unchanged, no alloc
            _ => self.min_string = Some(truncated_min.to_string()),
        }

        // Ceiling for the max bound. truncate_ceiling returns None when the
        // truncated prefix has no representable ceiling (all chars char::MAX);
        // in that case the column's max is unbounded and must report None so
        // pruning never uses a too-low upper bound (L6).
        if !self.max_string_unbounded {
            let truncated_max: Option<String> =
                if self.truncate_length > 0 && value.len() > self.truncate_length {
                    match truncate_ceiling(safe_truncate(value, self.truncate_length)) {
                        Some(c) => Some(c),
                        None => {
                            self.max_string_unbounded = true;
                            self.max_string = None;
                            None
                        }
                    }
                } else {
                    Some(value.to_string())
                };

            if let Some(tmax) = truncated_max {
                match self.max_string {
                    Some(ref m) if tmax <= *m => {} // unchanged, no alloc
                    _ => self.max_string = Some(tmax),
                }
            }
        }
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
            nan_count: self.nan_count,
        }
    }
}

/// Safely truncate a string to at most `max_bytes` bytes without splitting a UTF-8
/// multi-byte character. Returns the longest prefix that fits.
fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let end = s.char_indices()
        .take_while(|(i, c)| i + c.len_utf8() <= max_bytes)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    &s[..end]
}

/// Truncate a string and adjust the last character up by 1 to create a ceiling value.
/// This ensures pruning correctness: any string that starts with the original prefix
/// is guaranteed to be <= the ceiling.
///
/// Returns `None` when no valid ceiling exists (every character is `char::MAX`),
/// so callers can mark the upper bound as unbounded rather than emit a floor that
/// would wrongly prune values above the prefix (L6).
fn truncate_ceiling(s: &str) -> Option<String> {
    let mut chars: Vec<char> = s.chars().collect();
    // Walk backwards to find a character we can increment.
    while let Some(&last) = chars.last() {
        if last < char::MAX {
            let mut next_u = last as u32 + 1;
            // Skip the UTF-16 surrogate gap (0xD800..=0xDFFF), which is not a
            // valid Unicode scalar value; the next scalar after 0xD7FF is 0xE000.
            if (0xD800..=0xDFFF).contains(&next_u) {
                next_u = 0xE000;
            }
            if let Some(next) = char::from_u32(next_u) {
                *chars.last_mut().unwrap() = next;
                return Some(chars.into_iter().collect());
            }
        }
        chars.pop();
    }
    // All chars were char::MAX — no representable ceiling.
    None
}

/// Validate that requested statistics fields exist and have a supported type.
///
/// `field_types` maps every indexed (non-skipped, mapped) tantivy field to its
/// type. A requested field absent from this map was skipped or does not exist,
/// which would otherwise silently produce all-`None` statistics (L5) — reject it
/// so the misconfiguration surfaces instead of yielding empty stats.
pub fn validate_statistics_fields(
    fields: &[String],
    field_types: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    for field in fields {
        match field_types.get(field) {
            None => {
                return Err(format!(
                    "Statistics requested for field '{}', but it is not an indexed \
                     column (it was skipped or does not exist in the parquet schema)",
                    field
                ));
            }
            Some(field_type) if field_type == "Json" || field_type == "Bytes" => {
                return Err(format!(
                    "Field '{}' has type '{}' which does not support statistics",
                    field, field_type
                ));
            }
            Some(_) => {}
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
        assert_eq!(truncate_ceiling("abc").as_deref(), Some("abd"));
        assert_eq!(truncate_ceiling("a").as_deref(), Some("b"));
        // All-char::MAX prefix has no representable ceiling → None (L6).
        let all_max: String = std::iter::repeat(char::MAX).take(3).collect();
        assert_eq!(truncate_ceiling(&all_max), None);
    }

    #[test]
    fn test_truncate_ceiling_surrogate_boundary() {
        // A char just below the UTF-16 surrogate gap must increment to 0xE000,
        // not loop forever or produce an invalid scalar.
        let c = char::from_u32(0xD7FF).unwrap();
        let ceiling = truncate_ceiling(&c.to_string()).unwrap();
        assert_eq!(ceiling.chars().next().unwrap(), char::from_u32(0xE000).unwrap());
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
    fn test_string_truncation_multibyte_utf8() {
        // Emoji (4 bytes each): truncate_length=5 should not panic
        let mut acc = StatisticsAccumulator::new("emoji", "Str", 5);
        acc.observe_string("😀😁😂😃");
        let result = acc.finalize();
        // "😀" is 4 bytes — fits in 5. "😁" would be byte 4..8 — doesn't fit.
        assert_eq!(result.min_string, Some("😀".to_string()));

        // CJK characters (3 bytes each): truncate_length=5
        let mut acc2 = StatisticsAccumulator::new("cjk", "Str", 5);
        acc2.observe_string("你好世界");
        let result2 = acc2.finalize();
        // "你" is 3 bytes, "好" is 3 bytes (total 6 > 5), so only "你" fits
        assert_eq!(result2.min_string, Some("你".to_string()));
    }

    #[test]
    fn test_safe_truncate() {
        assert_eq!(safe_truncate("hello", 3), "hel");
        assert_eq!(safe_truncate("hello", 10), "hello");
        assert_eq!(safe_truncate("😀😁", 4), "😀");
        assert_eq!(safe_truncate("😀😁", 5), "😀");
        assert_eq!(safe_truncate("😀😁", 8), "😀😁");
        assert_eq!(safe_truncate("", 5), "");
        // Mixed ASCII + multi-byte
        assert_eq!(safe_truncate("ab😀cd", 4), "ab");
        assert_eq!(safe_truncate("ab😀cd", 6), "ab😀");
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
    fn test_validate_statistics_fields_unknown_field_rejected() {
        // Unknown fields (skipped or nonexistent) must be rejected rather than
        // silently yielding all-None statistics (L5).
        let types = std::collections::HashMap::new();
        let result = validate_statistics_fields(&["mystery".to_string()], &types);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not an indexed column"));
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
    fn test_string_no_truncation_when_zero() {
        // truncate_length=0 means disabled — strings should not be truncated
        let mut acc = StatisticsAccumulator::new("s", "Str", 0);
        acc.observe_string("data.csv");
        acc.observe_string("report.json");
        acc.observe_string("config.json");
        acc.observe_string("readme.txt");
        acc.observe_string("schema.json");
        let result = acc.finalize();
        assert_eq!(result.min_string, Some("config.json".to_string()));
        assert_eq!(result.max_string, Some("schema.json".to_string()));
    }

    #[test]
    fn test_string_no_truncation_long_values_when_zero() {
        // truncate_length=0 means disabled — even long strings pass through
        let mut acc = StatisticsAccumulator::new("s", "Str", 0);
        let long_val = "a".repeat(500);
        acc.observe_string(&long_val);
        let result = acc.finalize();
        assert_eq!(result.min_string.as_ref().unwrap().len(), 500);
        assert_eq!(result.max_string.as_ref().unwrap().len(), 500);
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
        // All NaN → no min/max, but nan_count distinguishes it from empty (M10).
        assert_eq!(result.min_double, None);
        assert_eq!(result.max_double, None);
        assert_eq!(result.nan_count, 2);
        assert_eq!(result.null_count, 0);
    }

    #[test]
    fn test_string_max_ceiling_unbounded_reports_none() {
        // A truncated value whose prefix is all char::MAX has no ceiling, so the
        // max bound must report None rather than a too-low floor (L6).
        let all_max: String = std::iter::repeat(char::MAX).take(8).collect();
        let mut acc = StatisticsAccumulator::new("s", "Str", 4);
        acc.observe_string(&all_max);
        let result = acc.finalize();
        assert_eq!(result.max_string, None);
        // min still records the truncated prefix.
        assert!(result.min_string.is_some());
    }
}
