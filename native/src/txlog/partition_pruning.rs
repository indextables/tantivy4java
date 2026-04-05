// txlog/partition_pruning.rs - Manifest-level partition pruning
//
// Evaluates partition filters against manifest-level bounds to skip
// entire manifests that cannot contain matching data.

use std::collections::HashMap;

use super::actions::{ManifestInfo, PartitionBounds};

/// Partition filter for manifest-level pruning.
///
/// Simpler than the file-level `PartitionPredicate` in common.rs — operates
/// on string values with numeric-aware comparison and supports bounds-based
/// evaluation for manifest pruning.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PartitionFilter {
    Eq { column: String, value: String },
    Neq { column: String, value: String },
    Gt { column: String, value: String },
    Gte { column: String, value: String },
    Lt { column: String, value: String },
    Lte { column: String, value: String },
    In { column: String, values: Vec<String> },
    IsNull { column: String },
    IsNotNull { column: String },
    StringStartsWith { column: String, prefix: String },
    StringEndsWith { column: String, suffix: String },
    StringContains { column: String, value: String },
    And { filters: Vec<PartitionFilter> },
    Or { filters: Vec<PartitionFilter> },
    Not { filter: Box<PartitionFilter> },
}

impl PartitionFilter {
    /// Evaluate against concrete partition values (for file-level filtering).
    pub fn evaluate(&self, partition_values: &HashMap<String, String>) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => {
                partition_values.get(column).map_or(false, |v| v == value)
            }
            PartitionFilter::Gt { column, value } => {
                partition_values.get(column).map_or(false, |v| {
                    compare_values(v, value) == std::cmp::Ordering::Greater
                })
            }
            PartitionFilter::Gte { column, value } => {
                partition_values.get(column).map_or(false, |v| {
                    compare_values(v, value) != std::cmp::Ordering::Less
                })
            }
            PartitionFilter::Lt { column, value } => {
                partition_values.get(column).map_or(false, |v| {
                    compare_values(v, value) == std::cmp::Ordering::Less
                })
            }
            PartitionFilter::Lte { column, value } => {
                partition_values.get(column).map_or(false, |v| {
                    compare_values(v, value) != std::cmp::Ordering::Greater
                })
            }
            PartitionFilter::Neq { column, value } => {
                partition_values.get(column).map_or(true, |v| v != value)
            }
            PartitionFilter::In { column, values } => {
                partition_values.get(column).map_or(false, |v| values.contains(v))
            }
            PartitionFilter::IsNull { column } => {
                partition_values.get(column).map_or(true, |v| v.is_empty())
            }
            PartitionFilter::IsNotNull { column } => {
                partition_values.get(column).map_or(false, |v| !v.is_empty())
            }
            PartitionFilter::StringStartsWith { column, prefix } => {
                partition_values.get(column).map_or(false, |v| v.starts_with(prefix.as_str()))
            }
            PartitionFilter::StringEndsWith { column, suffix } => {
                partition_values.get(column).map_or(false, |v| v.ends_with(suffix.as_str()))
            }
            PartitionFilter::StringContains { column, value } => {
                partition_values.get(column).map_or(false, |v| v.contains(value.as_str()))
            }
            PartitionFilter::And { filters } => {
                filters.iter().all(|f| f.evaluate(partition_values))
            }
            PartitionFilter::Or { filters } => {
                filters.iter().any(|f| f.evaluate(partition_values))
            }
            PartitionFilter::Not { filter } => {
                !filter.evaluate(partition_values)
            }
        }
    }

    /// Evaluate against partition bounds (for manifest-level pruning).
    /// Returns true if the bounds MAY contain matching data (conservative).
    pub fn may_match_bounds(&self, bounds: &PartitionBounds) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => {
                // value must be >= min AND <= max
                let min = bounds.min_values.get(column);
                let max = bounds.max_values.get(column);
                match (min, max) {
                    (Some(min_val), Some(max_val)) => {
                        compare_values(value, min_val) != std::cmp::Ordering::Less
                            && compare_values(value, max_val) != std::cmp::Ordering::Greater
                    }
                    _ => true, // No bounds for this column, can't prune
                }
            }
            PartitionFilter::Gt { column, value } => {
                // max must be > value
                match bounds.max_values.get(column) {
                    Some(max_val) => compare_values(max_val, value) == std::cmp::Ordering::Greater,
                    None => true,
                }
            }
            PartitionFilter::Gte { column, value } => {
                // max must be >= value
                match bounds.max_values.get(column) {
                    Some(max_val) => compare_values(max_val, value) != std::cmp::Ordering::Less,
                    None => true,
                }
            }
            PartitionFilter::Lt { column, value } => {
                // min must be < value
                match bounds.min_values.get(column) {
                    Some(min_val) => compare_values(min_val, value) == std::cmp::Ordering::Less,
                    None => true,
                }
            }
            PartitionFilter::Lte { column, value } => {
                // min must be <= value
                match bounds.min_values.get(column) {
                    Some(min_val) => compare_values(min_val, value) != std::cmp::Ordering::Greater,
                    None => true,
                }
            }
            PartitionFilter::Neq { column, value } => {
                // Can only prune if the entire range is exactly one value equal to the excluded value
                let min = bounds.min_values.get(column);
                let max = bounds.max_values.get(column);
                match (min, max) {
                    (Some(min_val), Some(max_val)) if min_val == max_val && min_val == value => false,
                    _ => true,
                }
            }
            PartitionFilter::In { column, values } => {
                // At least one value must be in [min, max]
                let min = bounds.min_values.get(column);
                let max = bounds.max_values.get(column);
                match (min, max) {
                    (Some(min_val), Some(max_val)) => {
                        values.iter().any(|v| {
                            compare_values(v, min_val) != std::cmp::Ordering::Less
                                && compare_values(v, max_val) != std::cmp::Ordering::Greater
                        })
                    }
                    _ => true, // No bounds, can't prune
                }
            }
            // IsNull/IsNotNull: can't determine nullability from bounds alone
            PartitionFilter::IsNull { .. } => true,
            PartitionFilter::IsNotNull { .. } => true,
            // String operations: conservative — can't prune from bounds
            PartitionFilter::StringStartsWith { .. } => true,
            PartitionFilter::StringEndsWith { .. } => true,
            PartitionFilter::StringContains { .. } => true,
            PartitionFilter::And { filters } => {
                // All sub-filters must match bounds
                filters.iter().all(|f| f.may_match_bounds(bounds))
            }
            PartitionFilter::Or { filters } => {
                // At least one sub-filter must match bounds
                filters.iter().any(|f| f.may_match_bounds(bounds))
            }
            PartitionFilter::Not { filter } => {
                // Negation of bounds pruning is tricky.
                // NOT(Eq value) can still match if range has more than one value.
                // Conservative: only prune if inner filter covers the ENTIRE range.
                // For simplicity, we are conservative and always return true
                // except for the case where the inner would match every possible value.
                //
                // Actually, for correctness: NOT(X).may_match_bounds should return true
                // unless X is guaranteed to match ALL rows in the manifest, which we
                // can't determine from bounds alone. So always return true (can't prune).
                //
                // However, a slightly smarter approach: if the inner filter says the
                // manifest CANNOT match (may_match_bounds returns false), then NOT of
                // that means ALL rows match, so may_match_bounds should return true.
                // If inner says MAY match, NOT may or may not match, so return true.
                // Either way, we return true. This is correct and conservative.
                let _ = filter;
                true
            }
        }
    }

    /// Evaluate against file-level min/max statistics for data skipping.
    /// Returns true if the file can be SKIPPED (i.e., definitely contains no matching data).
    /// Conservative: returns false (don't skip) when uncertain.
    ///
    /// This operates on per-column min/max values from AddAction stats, NOT partition bounds.
    /// Used for data skipping (FR1) after partition pruning.
    pub fn can_skip_by_stats(
        &self,
        min_values: &HashMap<String, String>,
        max_values: &HashMap<String, String>,
    ) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => {
                // Skip if value < min OR value > max
                if let Some(min_val) = min_values.get(column) {
                    if compare_values(value, min_val) == std::cmp::Ordering::Less {
                        return true;
                    }
                }
                if let Some(max_val) = max_values.get(column) {
                    if compare_values(value, max_val) == std::cmp::Ordering::Greater {
                        return true;
                    }
                }
                false
            }
            PartitionFilter::Neq { column, value } => {
                // Can only skip if entire file has exactly one value matching the excluded value
                let min = min_values.get(column);
                let max = max_values.get(column);
                match (min, max) {
                    (Some(min_val), Some(max_val)) if min_val == max_val && min_val == value => true,
                    _ => false,
                }
            }
            PartitionFilter::Gt { column, value } => {
                // Skip if max <= value (no value in file is > value)
                if let Some(max_val) = max_values.get(column) {
                    return compare_values(max_val, value) != std::cmp::Ordering::Greater;
                }
                false
            }
            PartitionFilter::Gte { column, value } => {
                // Skip if max < value, BUT not if value is a proper extension of max
                // (truncation safety: max="aaa" could mean actual max is "aaaz")
                if let Some(max_val) = max_values.get(column) {
                    if value.len() > max_val.len() && value.starts_with(max_val.as_str()) {
                        return false; // max may be truncated
                    }
                    return compare_values(max_val, value) == std::cmp::Ordering::Less;
                }
                false
            }
            PartitionFilter::Lt { column, value } => {
                // Skip if min >= value, BUT not if value is a proper extension of min
                // (truncation safety: min="abc" could mean actual min is "abc..." which is < "abcd")
                if let Some(min_val) = min_values.get(column) {
                    if value.len() > min_val.len() && value.starts_with(min_val.as_str()) {
                        return false; // min may be truncated
                    }
                    return compare_values(min_val, value) != std::cmp::Ordering::Less;
                }
                false
            }
            PartitionFilter::Lte { column, value } => {
                // Skip if min > value, BUT not if value is a proper extension of min
                if let Some(min_val) = min_values.get(column) {
                    if value.len() > min_val.len() && value.starts_with(min_val.as_str()) {
                        return false; // min may be truncated
                    }
                    return compare_values(min_val, value) == std::cmp::Ordering::Greater;
                }
                false
            }
            PartitionFilter::In { column, values } => {
                // Skip if ALL values are outside [min, max]. Empty list → don't skip.
                if values.is_empty() {
                    return false;
                }
                let min = min_values.get(column);
                let max = max_values.get(column);
                match (min, max) {
                    (Some(min_val), Some(max_val)) => {
                        values.iter().all(|v| {
                            compare_values(v, min_val) == std::cmp::Ordering::Less
                                || compare_values(v, max_val) == std::cmp::Ordering::Greater
                        })
                    }
                    _ => false,
                }
            }
            // IsNull/IsNotNull: can't determine from min/max stats
            PartitionFilter::IsNull { .. } => false,
            PartitionFilter::IsNotNull { .. } => false,
            // String operations: can't reliably skip from min/max
            PartitionFilter::StringStartsWith { .. } => false,
            PartitionFilter::StringEndsWith { .. } => false,
            PartitionFilter::StringContains { .. } => false,
            PartitionFilter::And { filters } => {
                // Skip if ANY sub-filter says skip (intersection of predicates)
                filters.iter().any(|f| f.can_skip_by_stats(min_values, max_values))
            }
            PartitionFilter::Or { filters } => {
                // Skip only if ALL sub-filters say skip (union of predicates)
                !filters.is_empty()
                    && filters.iter().all(|f| f.can_skip_by_stats(min_values, max_values))
            }
            PartitionFilter::Not { .. } => {
                // Conservative: can't skip
                false
            }
        }
    }

    /// Type-aware variant of `can_skip_by_stats` that handles date/timestamp columns.
    ///
    /// `field_types` maps column names to Spark type strings ("date", "timestamp", etc.).
    /// For columns with known types, uses type-specific parsing before comparison.
    pub fn can_skip_by_stats_typed(
        &self,
        min_values: &HashMap<String, String>,
        max_values: &HashMap<String, String>,
        field_types: &HashMap<String, String>,
        tz_offset_secs: Option<i32>,
    ) -> bool {
        if field_types.is_empty() {
            return self.can_skip_by_stats(min_values, max_values);
        }
        self.can_skip_by_stats_typed_inner(min_values, max_values, field_types, tz_offset_secs)
    }

    fn can_skip_by_stats_typed_inner(
        &self,
        min_values: &HashMap<String, String>,
        max_values: &HashMap<String, String>,
        ft: &HashMap<String, String>,
        tz_offset_secs: Option<i32>,
    ) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => {
                let ftype = ft.get(column).map(|s| s.as_str());
                if let Some(min_val) = min_values.get(column) {
                    if compare_values_typed(value, min_val, ftype, tz_offset_secs) == std::cmp::Ordering::Less {
                        return true;
                    }
                }
                if let Some(max_val) = max_values.get(column) {
                    if compare_values_typed(value, max_val, ftype, tz_offset_secs) == std::cmp::Ordering::Greater {
                        return true;
                    }
                }
                false
            }
            PartitionFilter::Gt { column, value } => {
                let ftype = ft.get(column).map(|s| s.as_str());
                if let Some(max_val) = max_values.get(column) {
                    return compare_values_typed(max_val, value, ftype, tz_offset_secs) != std::cmp::Ordering::Greater;
                }
                false
            }
            PartitionFilter::Gte { column, value } => {
                let ftype = ft.get(column).map(|s| s.as_str());
                if let Some(max_val) = max_values.get(column) {
                    if value.len() > max_val.len() && value.starts_with(max_val.as_str()) {
                        return false;
                    }
                    return compare_values_typed(max_val, value, ftype, tz_offset_secs) == std::cmp::Ordering::Less;
                }
                false
            }
            PartitionFilter::Lt { column, value } => {
                let ftype = ft.get(column).map(|s| s.as_str());
                if let Some(min_val) = min_values.get(column) {
                    if value.len() > min_val.len() && value.starts_with(min_val.as_str()) {
                        return false;
                    }
                    return compare_values_typed(min_val, value, ftype, tz_offset_secs) != std::cmp::Ordering::Less;
                }
                false
            }
            PartitionFilter::Lte { column, value } => {
                let ftype = ft.get(column).map(|s| s.as_str());
                if let Some(min_val) = min_values.get(column) {
                    if value.len() > min_val.len() && value.starts_with(min_val.as_str()) {
                        return false;
                    }
                    return compare_values_typed(min_val, value, ftype, tz_offset_secs) == std::cmp::Ordering::Greater;
                }
                false
            }
            PartitionFilter::And { filters } => {
                filters.iter().any(|f| f.can_skip_by_stats_typed_inner(min_values, max_values, ft, tz_offset_secs))
            }
            PartitionFilter::Or { filters } => {
                !filters.is_empty()
                    && filters.iter().all(|f| f.can_skip_by_stats_typed_inner(min_values, max_values, ft, tz_offset_secs))
            }
            // All other variants delegate to untyped
            _ => self.can_skip_by_stats(min_values, max_values),
        }
    }
}

/// Prune manifest list to only those whose bounds may match the filters.
///
/// If a manifest has no partition_bounds, it cannot be pruned (included by default).
/// If filters is empty, all manifests are returned.
pub fn prune_manifests<'a>(
    manifests: &'a [ManifestInfo],
    filters: &[PartitionFilter],
) -> Vec<&'a ManifestInfo> {
    if filters.is_empty() {
        return manifests.iter().collect();
    }

    manifests.iter().filter(|m| {
        match &m.partition_bounds {
            None => true, // No bounds, can't prune
            Some(bounds) => {
                filters.iter().all(|f| f.may_match_bounds(bounds))
            }
        }
    }).collect()
}

/// Type-aware comparison for data skipping.
///
/// When `field_type` is provided, uses type-specific conversion before comparison:
/// - "date": converts date strings ("2024-01-15") to epoch days for comparison with
///   stat values stored as epoch day integers or epoch microseconds
/// - "timestamp": converts timestamp strings to epoch microseconds using the session
///   timezone offset. If no timezone offset is provided, falls back to conservative
///   comparison (Ordering::Equal = never skip).
/// - For all other types (or when field_type is None): falls through to `compare_values`
///
/// `tz_offset_seconds`: session timezone as seconds east of UTC (e.g., -18000 for EST).
/// Passed from JVM via config map key "session.timezone.offset.seconds".
pub fn compare_values_typed(a: &str, b: &str, field_type: Option<&str>, tz_offset_secs: Option<i32>) -> std::cmp::Ordering {
    match field_type {
        Some("date") => {
            // Try to normalize both values to epoch days for comparison.
            // Stats are stored as epoch day ints ("19738"), filter values as "2024-01-15".
            let a_days = parse_as_epoch_days(a);
            let b_days = parse_as_epoch_days(b);
            if let (Some(ad), Some(bd)) = (a_days, b_days) {
                return ad.cmp(&bd);
            }
            // Fallback to default comparison if either fails to parse
            compare_values(a, b)
        }
        Some("timestamp") | Some("timestamp_ntz") => {
            // Stats are stored as epoch micros. Filter values may be bare datetime
            // strings ("2025-11-07 05:00:00") that require a timezone to convert
            // correctly to epoch micros.
            //
            // When both sides are numeric, compare directly as epoch micros.
            // When a filter is a datetime string, use tz_offset_secs to convert.
            // Without a timezone offset, be conservative (don't skip).
            let a_is_numeric = a.parse::<i64>().is_ok();
            let b_is_numeric = b.parse::<i64>().is_ok();

            if a_is_numeric && b_is_numeric {
                let a_micros = parse_as_epoch_micros(a);
                let b_micros = parse_as_epoch_micros(b);
                if let (Some(am), Some(bm)) = (a_micros, b_micros) {
                    return am.cmp(&bm);
                }
            }

            // If either side has an explicit timezone (Z, +HH:MM), we can parse
            // unambiguously without needing the session timezone
            let has_explicit_tz = a.contains('Z') || b.contains('Z')
                || a.contains('+') || b.contains('+')
                || (a.len() > 19 && a[19..].contains('-'))
                || (b.len() > 19 && b[19..].contains('-'));
            if has_explicit_tz {
                let a_micros = parse_as_epoch_micros(a);
                let b_micros = parse_as_epoch_micros(b);
                if let (Some(am), Some(bm)) = (a_micros, b_micros) {
                    return am.cmp(&bm);
                }
            }

            // Bare datetime string — need session timezone
            if let Some(tz_offset) = tz_offset_secs {
                let a_micros = parse_as_epoch_micros_with_tz(a, tz_offset);
                let b_micros = parse_as_epoch_micros_with_tz(b, tz_offset);
                if let (Some(am), Some(bm)) = (a_micros, b_micros) {
                    return am.cmp(&bm);
                }
            }

            // No timezone available — conservative (never skip)
            std::cmp::Ordering::Equal
        }
        _ => compare_values(a, b),
    }
}

/// Parse a value as epoch days. Handles:
/// - Integer string ("19738") — epoch days directly (Spark stores DateType stats this way)
/// - Date string ("2024-01-15") — parsed via chrono (Spark filter values)
/// - ISO-8601 date with time ("2024-01-15T00:00:00Z") — date portion extracted
///
/// Spark convention: stats are stored as epoch days (Int), filter values as "YYYY-MM-DD".
fn parse_as_epoch_days(s: &str) -> Option<i64> {
    // Try integer first. Stats may be stored as:
    //   - epoch days (from Scala StatisticsCalculator): small integers like 19403
    //   - epoch microseconds (from Rust Arrow FFI import): large integers like 1676419200000000
    //   - epoch milliseconds: medium integers like 1676419200000
    if let Ok(v) = s.parse::<i64>() {
        if v > 1_000_000_000_000_000 {
            // Epoch microseconds → convert to days
            return Some(v / (86_400 * 1_000_000));
        } else if v > 1_000_000_000_000 {
            // Epoch milliseconds → convert to days
            return Some(v / (86_400 * 1_000));
        } else if v > 1_000_000_000 {
            // Epoch seconds → convert to days
            return Some(v / 86_400);
        }
        // Small integer — already epoch days
        return Some(v);
    }
    // Try YYYY-MM-DD (exactly 10 chars)
    if s.len() >= 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-') {
        // Take only the date portion (handles "2024-01-15" and "2024-01-15T00:00:00Z")
        let date_part = &s[..10];
        if let Ok(date) = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            return Some((date - epoch).num_days());
        }
    }
    None
}

/// Parse a value as epoch microseconds. Handles:
/// - Integer string ("1699336800000000") — epoch microseconds directly
/// - Integer string ("1699336800000") — epoch milliseconds → converted to micros
/// - Integer string ("1699336800") — epoch seconds → converted to micros
/// - Timestamp string ("2023-11-07 05:00:00") — parsed as UTC
/// - ISO-8601 ("2023-11-07T05:00:00Z", "2023-11-07T05:00:00+05:00")
///
/// Epoch unit heuristic (for integer values):
///   > 1e15 → microseconds (year 2001+ in micros)
///   > 1e12 → milliseconds (year 2001+ in millis)
///   > 0    → seconds (all positive epoch seconds, including < year 2001)
///
/// This matches Spark's convention: stats are stored as epoch microseconds,
/// filter values may be epoch seconds or millis depending on the Spark version.
fn parse_as_epoch_micros(s: &str) -> Option<i64> {
    // Try integer first — determine unit by magnitude
    if let Ok(v) = s.parse::<i64>() {
        if v > 1_000_000_000_000_000 {
            return Some(v); // Microseconds
        } else if v > 1_000_000_000_000 {
            return Some(v * 1_000); // Milliseconds → microseconds
        } else if v > 0 {
            return Some(v * 1_000_000); // Seconds → microseconds
        }
        return Some(0); // Zero or negative — epoch start
    }
    // Try full ISO-8601 with timezone (RFC 3339):
    //   "2023-11-07T05:00:00Z"
    //   "2023-11-07T05:00:00+05:00"
    //   "2023-11-07T05:00:00.123456Z"
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }

    // Try JDBC / Spark format (no timezone — treated as UTC):
    //   "2023-11-07 05:00:00"
    //   "2023-11-07T05:00:00"
    let normalized = s.replace('T', " ");
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_micros());
    }
    // With fractional seconds
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_micros());
    }
    None
}

/// Parse a value as epoch microseconds, using a timezone offset for bare datetime strings.
/// The offset is seconds east of UTC (e.g., -18000 for EST/UTC-5, 0 for UTC).
fn parse_as_epoch_micros_with_tz(s: &str, tz_offset_secs: i32) -> Option<i64> {
    // If it's already a number, use the standard parser
    if s.parse::<i64>().is_ok() {
        return parse_as_epoch_micros(s);
    }

    // Try RFC 3339 first (has explicit timezone — ignore tz_offset_secs)
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }

    // Bare datetime string — apply the session timezone offset
    let normalized = s.replace('T', " ");
    let naive = chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f"))
        .ok()?;

    let offset = chrono::FixedOffset::east_opt(tz_offset_secs)?;
    let dt = naive.and_local_timezone(offset).single()?;
    Some(dt.timestamp_micros())
}

/// Numeric-aware comparison: try i64 first for exact integer precision,
/// then f64 for decimals, then fall back to string comparison.
/// This matches Scala behavior where partition values like "9" and "10"
/// are compared numerically. Using i64-first avoids precision loss for
/// large integers that cannot be exactly represented as f64.
pub fn compare_values(a: &str, b: &str) -> std::cmp::Ordering {
    // Try i64 first (exact for integers)
    if let (Ok(na), Ok(nb)) = (a.parse::<i64>(), b.parse::<i64>()) {
        return na.cmp(&nb);
    }
    // Then try f64 (handles decimals)
    if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return na.total_cmp(&nb);
    }
    // Fall back to string comparison
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(min: &[(&str, &str)], max: &[(&str, &str)]) -> PartitionBounds {
        PartitionBounds {
            min_values: min.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            max_values: max.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    fn pv(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    // ========================================================================
    // evaluate() tests (file-level)
    // ========================================================================

    #[test]
    fn test_evaluate_eq() {
        let f = PartitionFilter::Eq { column: "year".into(), value: "2024".into() };
        assert!(f.evaluate(&pv(&[("year", "2024")])));
        assert!(!f.evaluate(&pv(&[("year", "2023")])));
        assert!(!f.evaluate(&pv(&[])));
    }

    #[test]
    fn test_evaluate_gt() {
        let f = PartitionFilter::Gt { column: "year".into(), value: "2022".into() };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(!f.evaluate(&pv(&[("year", "2022")])));
        assert!(!f.evaluate(&pv(&[("year", "2021")])));
    }

    #[test]
    fn test_evaluate_gte() {
        let f = PartitionFilter::Gte { column: "year".into(), value: "2022".into() };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(f.evaluate(&pv(&[("year", "2022")])));
        assert!(!f.evaluate(&pv(&[("year", "2021")])));
    }

    #[test]
    fn test_evaluate_lt() {
        let f = PartitionFilter::Lt { column: "year".into(), value: "2024".into() };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(!f.evaluate(&pv(&[("year", "2024")])));
        assert!(!f.evaluate(&pv(&[("year", "2025")])));
    }

    #[test]
    fn test_evaluate_lte() {
        let f = PartitionFilter::Lte { column: "year".into(), value: "2024".into() };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(f.evaluate(&pv(&[("year", "2024")])));
        assert!(!f.evaluate(&pv(&[("year", "2025")])));
    }

    #[test]
    fn test_evaluate_in() {
        let f = PartitionFilter::In {
            column: "region".into(),
            values: vec!["us-east-1".into(), "us-west-2".into()],
        };
        assert!(f.evaluate(&pv(&[("region", "us-east-1")])));
        assert!(f.evaluate(&pv(&[("region", "us-west-2")])));
        assert!(!f.evaluate(&pv(&[("region", "eu-west-1")])));
        assert!(!f.evaluate(&pv(&[])));
    }

    #[test]
    fn test_evaluate_and() {
        let f = PartitionFilter::And {
            filters: vec![
                PartitionFilter::Eq { column: "year".into(), value: "2024".into() },
                PartitionFilter::Eq { column: "month".into(), value: "01".into() },
            ],
        };
        assert!(f.evaluate(&pv(&[("year", "2024"), ("month", "01")])));
        assert!(!f.evaluate(&pv(&[("year", "2024"), ("month", "02")])));
    }

    #[test]
    fn test_evaluate_or() {
        let f = PartitionFilter::Or {
            filters: vec![
                PartitionFilter::Eq { column: "year".into(), value: "2023".into() },
                PartitionFilter::Eq { column: "year".into(), value: "2024".into() },
            ],
        };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(f.evaluate(&pv(&[("year", "2024")])));
        assert!(!f.evaluate(&pv(&[("year", "2025")])));
    }

    #[test]
    fn test_evaluate_not() {
        let f = PartitionFilter::Not {
            filter: Box::new(PartitionFilter::Eq { column: "year".into(), value: "2020".into() }),
        };
        assert!(f.evaluate(&pv(&[("year", "2024")])));
        assert!(!f.evaluate(&pv(&[("year", "2020")])));
    }

    // ========================================================================
    // may_match_bounds() tests (manifest-level)
    // ========================================================================

    #[test]
    fn test_bounds_eq_in_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Eq { column: "year".into(), value: "2023".into() };
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_eq_below_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Eq { column: "year".into(), value: "2021".into() };
        assert!(!f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_eq_above_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Eq { column: "year".into(), value: "2026".into() };
        assert!(!f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_eq_at_min() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Eq { column: "year".into(), value: "2022".into() };
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_eq_at_max() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Eq { column: "year".into(), value: "2025".into() };
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_gt() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        // max (2025) > value (2023) → true
        assert!(PartitionFilter::Gt { column: "year".into(), value: "2023".into() }
            .may_match_bounds(&b));
        // max (2025) > value (2025) → false
        assert!(!PartitionFilter::Gt { column: "year".into(), value: "2025".into() }
            .may_match_bounds(&b));
        // max (2025) > value (2026) → false
        assert!(!PartitionFilter::Gt { column: "year".into(), value: "2026".into() }
            .may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_gte() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        assert!(PartitionFilter::Gte { column: "year".into(), value: "2025".into() }
            .may_match_bounds(&b));
        assert!(!PartitionFilter::Gte { column: "year".into(), value: "2026".into() }
            .may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_lt() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        // min (2022) < value (2023) → true
        assert!(PartitionFilter::Lt { column: "year".into(), value: "2023".into() }
            .may_match_bounds(&b));
        // min (2022) < value (2022) → false
        assert!(!PartitionFilter::Lt { column: "year".into(), value: "2022".into() }
            .may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_lte() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        assert!(PartitionFilter::Lte { column: "year".into(), value: "2022".into() }
            .may_match_bounds(&b));
        assert!(!PartitionFilter::Lte { column: "year".into(), value: "2021".into() }
            .may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_in_some_in_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::In {
            column: "year".into(),
            values: vec!["2020".into(), "2023".into(), "2030".into()],
        };
        assert!(f.may_match_bounds(&b)); // 2023 is in range
    }

    #[test]
    fn test_bounds_in_none_in_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::In {
            column: "year".into(),
            values: vec!["2020".into(), "2021".into(), "2026".into()],
        };
        assert!(!f.may_match_bounds(&b)); // none in [2022, 2025]
    }

    #[test]
    fn test_bounds_and() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::And {
            filters: vec![
                PartitionFilter::Gte { column: "year".into(), value: "2020".into() },
                PartitionFilter::Lte { column: "year".into(), value: "2023".into() },
            ],
        };
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_or() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Or {
            filters: vec![
                PartitionFilter::Eq { column: "year".into(), value: "2020".into() }, // out of range
                PartitionFilter::Eq { column: "year".into(), value: "2023".into() }, // in range
            ],
        };
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_or_all_out_of_range() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Or {
            filters: vec![
                PartitionFilter::Eq { column: "year".into(), value: "2020".into() },
                PartitionFilter::Eq { column: "year".into(), value: "2021".into() },
            ],
        };
        assert!(!f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_not_always_conservative() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        let f = PartitionFilter::Not {
            filter: Box::new(PartitionFilter::Eq { column: "year".into(), value: "2023".into() }),
        };
        // NOT is always conservative → true
        assert!(f.may_match_bounds(&b));
    }

    #[test]
    fn test_bounds_missing_column_is_conservative() {
        let b = bounds(&[("year", "2022")], &[("year", "2025")]);
        // Filter on a column not in the bounds → can't prune → true
        let f = PartitionFilter::Eq { column: "month".into(), value: "01".into() };
        assert!(f.may_match_bounds(&b));
    }

    // ========================================================================
    // compare_values() tests
    // ========================================================================

    #[test]
    fn test_compare_values_numeric() {
        // "9" < "10" numerically (but "9" > "10" lexicographically)
        assert_eq!(compare_values("9", "10"), std::cmp::Ordering::Less);
        assert_eq!(compare_values("10", "9"), std::cmp::Ordering::Greater);
        assert_eq!(compare_values("10", "10"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_values_string_fallback() {
        // Non-numeric strings fall back to string comparison
        assert_eq!(compare_values("abc", "def"), std::cmp::Ordering::Less);
        assert_eq!(compare_values("def", "abc"), std::cmp::Ordering::Greater);
        assert_eq!(compare_values("abc", "abc"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_values_mixed() {
        // One numeric, one not → string comparison fallback
        assert_eq!(compare_values("abc", "123"), std::cmp::Ordering::Greater); // 'a' > '1'
    }

    #[test]
    fn test_compare_values_float() {
        assert_eq!(compare_values("3.14", "2.71"), std::cmp::Ordering::Greater);
        assert_eq!(compare_values("2.71", "3.14"), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_compare_values_negative() {
        assert_eq!(compare_values("-5", "3"), std::cmp::Ordering::Less);
        assert_eq!(compare_values("3", "-5"), std::cmp::Ordering::Greater);
    }

    // ========================================================================
    // prune_manifests() tests
    // ========================================================================

    #[test]
    fn test_prune_manifests_empty_filters() {
        let manifests = vec![
            ManifestInfo { path: "m1.avro".into(), file_count: 10, partition_bounds: None, ..Default::default() },
            ManifestInfo { path: "m2.avro".into(), file_count: 20, partition_bounds: None, ..Default::default() },
        ];
        let result = prune_manifests(&manifests, &[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_prune_manifests_no_bounds_kept() {
        let manifests = vec![
            ManifestInfo { path: "m1.avro".into(), file_count: 10, partition_bounds: None, ..Default::default() },
        ];
        let filters = vec![
            PartitionFilter::Eq { column: "year".into(), value: "2024".into() },
        ];
        let result = prune_manifests(&manifests, &filters);
        // No bounds → can't prune → kept
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_prune_manifests_mixed() {
        let manifests = vec![
            ManifestInfo {
                path: "m-2023.avro".into(),
                file_count: 10,
                partition_bounds: Some(bounds(&[("year", "2023")], &[("year", "2023")])),
                ..Default::default()
            },
            ManifestInfo {
                path: "m-2024.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("year", "2024")], &[("year", "2024")])),
                ..Default::default()
            },
            ManifestInfo {
                path: "m-2025.avro".into(),
                file_count: 30,
                partition_bounds: Some(bounds(&[("year", "2025")], &[("year", "2025")])),
                ..Default::default()
            },
            ManifestInfo {
                path: "m-no-bounds.avro".into(),
                file_count: 5,
                partition_bounds: None,
                ..Default::default()
            },
        ];
        let filters = vec![
            PartitionFilter::Eq { column: "year".into(), value: "2024".into() },
        ];
        let result = prune_manifests(&manifests, &filters);
        // Should keep m-2024 (matches) and m-no-bounds (no bounds, can't prune)
        assert_eq!(result.len(), 2);
        let paths: Vec<&str> = result.iter().map(|m| m.path.as_str()).collect();
        assert!(paths.contains(&"m-2024.avro"));
        assert!(paths.contains(&"m-no-bounds.avro"));
    }

    #[test]
    fn test_prune_manifests_range_filter() {
        let manifests = vec![
            ManifestInfo {
                path: "m-old.avro".into(),
                file_count: 10,
                partition_bounds: Some(bounds(&[("year", "2020")], &[("year", "2021")])),
                ..Default::default()
            },
            ManifestInfo {
                path: "m-recent.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("year", "2023")], &[("year", "2025")])),
                ..Default::default()
            },
        ];
        let filters = vec![
            PartitionFilter::Gte { column: "year".into(), value: "2023".into() },
        ];
        let result = prune_manifests(&manifests, &filters);
        // m-old: max=2021, 2021 >= 2023 → false → pruned
        // m-recent: max=2025, 2025 >= 2023 → true → kept
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "m-recent.avro");
    }

    #[test]
    fn test_prune_manifests_numeric_comparison() {
        // Verify numeric-aware comparison works through prune_manifests
        let manifests = vec![
            ManifestInfo {
                path: "m-low.avro".into(),
                file_count: 10,
                partition_bounds: Some(bounds(&[("id", "1")], &[("id", "9")])),
                ..Default::default()
            },
            ManifestInfo {
                path: "m-high.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("id", "10")], &[("id", "100")])),
                ..Default::default()
            },
        ];
        // With numeric comparison, Gt id > 9 should match m-high (max=100 > 9)
        // and m-low (max=9 > 9 is false → pruned)
        let filters = vec![
            PartitionFilter::Gt { column: "id".into(), value: "9".into() },
        ];
        let result = prune_manifests(&manifests, &filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "m-high.avro");
    }

    // ========================================================================
    // Deserialization tests
    // ========================================================================

    // ========================================================================
    // evaluate() tests for new variants
    // ========================================================================

    #[test]
    fn test_evaluate_neq() {
        let f = PartitionFilter::Neq { column: "year".into(), value: "2024".into() };
        assert!(f.evaluate(&pv(&[("year", "2023")])));
        assert!(!f.evaluate(&pv(&[("year", "2024")])));
        // Missing column → true (null != value)
        assert!(f.evaluate(&pv(&[])));
    }

    #[test]
    fn test_evaluate_is_null() {
        let f = PartitionFilter::IsNull { column: "region".into() };
        assert!(f.evaluate(&pv(&[])));
        assert!(f.evaluate(&pv(&[("region", "")])));
        assert!(!f.evaluate(&pv(&[("region", "us-east-1")])));
    }

    #[test]
    fn test_evaluate_is_not_null() {
        let f = PartitionFilter::IsNotNull { column: "region".into() };
        assert!(!f.evaluate(&pv(&[])));
        assert!(!f.evaluate(&pv(&[("region", "")])));
        assert!(f.evaluate(&pv(&[("region", "us-east-1")])));
    }

    #[test]
    fn test_evaluate_string_starts_with() {
        let f = PartitionFilter::StringStartsWith { column: "path".into(), prefix: "/data/".into() };
        assert!(f.evaluate(&pv(&[("path", "/data/foo.parquet")])));
        assert!(!f.evaluate(&pv(&[("path", "/logs/foo.parquet")])));
        assert!(!f.evaluate(&pv(&[])));
    }

    #[test]
    fn test_evaluate_string_ends_with() {
        let f = PartitionFilter::StringEndsWith { column: "path".into(), suffix: ".parquet".into() };
        assert!(f.evaluate(&pv(&[("path", "/data/foo.parquet")])));
        assert!(!f.evaluate(&pv(&[("path", "/data/foo.csv")])));
    }

    #[test]
    fn test_evaluate_string_contains() {
        let f = PartitionFilter::StringContains { column: "path".into(), value: "foo".into() };
        assert!(f.evaluate(&pv(&[("path", "/data/foo/bar")])));
        assert!(!f.evaluate(&pv(&[("path", "/data/baz/bar")])));
    }

    // ========================================================================
    // can_skip_by_stats() tests (data skipping)
    // ========================================================================

    fn stats(min: &[(&str, &str)], max: &[(&str, &str)]) -> (HashMap<String, String>, HashMap<String, String>) {
        (
            min.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            max.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        )
    }

    #[test]
    fn test_skip_eq_below_min() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Eq { column: "age".into(), value: "10".into() };
        assert!(f.can_skip_by_stats(&min, &max)); // 10 < min(20) → skip
    }

    #[test]
    fn test_skip_eq_above_max() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Eq { column: "age".into(), value: "50".into() };
        assert!(f.can_skip_by_stats(&min, &max)); // 50 > max(40) → skip
    }

    #[test]
    fn test_no_skip_eq_in_range() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Eq { column: "age".into(), value: "30".into() };
        assert!(!f.can_skip_by_stats(&min, &max)); // 30 in [20,40] → don't skip
    }

    #[test]
    fn test_skip_gt_max_not_greater() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Gt { column: "age".into(), value: "40".into() };
        assert!(f.can_skip_by_stats(&min, &max)); // max(40) not > 40 → skip
    }

    #[test]
    fn test_no_skip_gt_max_greater() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Gt { column: "age".into(), value: "30".into() };
        assert!(!f.can_skip_by_stats(&min, &max)); // max(40) > 30 → don't skip
    }

    #[test]
    fn test_skip_lt_min_not_less() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Lt { column: "age".into(), value: "20".into() };
        assert!(f.can_skip_by_stats(&min, &max)); // min(20) not < 20 → skip
    }

    #[test]
    fn test_skip_and_any_skips() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::And {
            filters: vec![
                PartitionFilter::Gt { column: "age".into(), value: "50".into() }, // skip
                PartitionFilter::Lt { column: "age".into(), value: "100".into() }, // don't skip
            ],
        };
        assert!(f.can_skip_by_stats(&min, &max)); // AND: any skip → skip
    }

    #[test]
    fn test_skip_or_all_skip() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Or {
            filters: vec![
                PartitionFilter::Gt { column: "age".into(), value: "50".into() }, // skip
                PartitionFilter::Lt { column: "age".into(), value: "10".into() }, // skip
            ],
        };
        assert!(f.can_skip_by_stats(&min, &max)); // OR: all skip → skip
    }

    #[test]
    fn test_no_skip_or_one_matches() {
        let (min, max) = stats(&[("age", "20")], &[("age", "40")]);
        let f = PartitionFilter::Or {
            filters: vec![
                PartitionFilter::Gt { column: "age".into(), value: "50".into() }, // skip
                PartitionFilter::Lt { column: "age".into(), value: "30".into() }, // don't skip
            ],
        };
        assert!(!f.can_skip_by_stats(&min, &max)); // OR: not all skip → don't skip
    }

    #[test]
    fn test_no_skip_missing_stats() {
        let (min, max) = stats(&[], &[]);
        let f = PartitionFilter::Eq { column: "age".into(), value: "30".into() };
        assert!(!f.can_skip_by_stats(&min, &max)); // no stats → conservative
    }

    #[test]
    fn test_skip_in_all_outside() {
        let (min, max) = stats(&[("id", "10")], &[("id", "20")]);
        let f = PartitionFilter::In {
            column: "id".into(),
            values: vec!["1".into(), "5".into(), "25".into()],
        };
        assert!(f.can_skip_by_stats(&min, &max)); // all outside [10,20]
    }

    #[test]
    fn test_no_skip_in_some_inside() {
        let (min, max) = stats(&[("id", "10")], &[("id", "20")]);
        let f = PartitionFilter::In {
            column: "id".into(),
            values: vec!["1".into(), "15".into(), "25".into()],
        };
        assert!(!f.can_skip_by_stats(&min, &max)); // 15 is in [10,20]
    }

    // ========================================================================
    // Truncation awareness tests (can_skip_by_stats)
    // ========================================================================

    #[test]
    fn test_no_skip_gte_truncated_max() {
        // max="aaa" but actual max might be "aaaz" (truncated)
        // filter: value >= "aaab" — should NOT skip because actual max could be "aaaz"
        let (min, max) = stats(&[("name", "aaa")], &[("name", "aaa")]);
        let f = PartitionFilter::Gte { column: "name".into(), value: "aaab".into() };
        // "aaab".starts_with("aaa") → true → truncation safety → don't skip
        assert!(!f.can_skip_by_stats(&min, &max));
    }

    #[test]
    fn test_skip_gte_not_truncated() {
        // max="aaa", filter: value >= "bbb" — safe to skip (no truncation concern)
        let (min, max) = stats(&[("name", "aaa")], &[("name", "aaa")]);
        let f = PartitionFilter::Gte { column: "name".into(), value: "bbb".into() };
        assert!(f.can_skip_by_stats(&min, &max));
    }

    #[test]
    fn test_no_skip_lte_truncated_min() {
        // min="bbb", filter: value <= "bbba" — should NOT skip because actual min could be "bbb"
        let (min, max) = stats(&[("name", "bbb")], &[("name", "zzz")]);
        let f = PartitionFilter::Lte { column: "name".into(), value: "bbba".into() };
        // "bbba".starts_with("bbb") → true → truncation safety → don't skip
        assert!(!f.can_skip_by_stats(&min, &max));
    }

    #[test]
    fn test_skip_lt_not_truncated() {
        // min="ccc", filter: value < "bbb" — safe to skip
        let (min, max) = stats(&[("name", "ccc")], &[("name", "zzz")]);
        let f = PartitionFilter::Lt { column: "name".into(), value: "bbb".into() };
        assert!(f.can_skip_by_stats(&min, &max));
    }

    #[test]
    fn test_no_skip_lt_truncated_min() {
        // min="abc", filter: value < "abcd" — should NOT skip
        let (min, max) = stats(&[("name", "abc")], &[("name", "zzz")]);
        let f = PartitionFilter::Lt { column: "name".into(), value: "abcd".into() };
        assert!(!f.can_skip_by_stats(&min, &max));
    }

    #[test]
    fn test_truncation_irrelevant_for_numeric() {
        // Numeric: "100" >= "99" max — no truncation concern for numbers
        let (min, max) = stats(&[("id", "50")], &[("id", "99")]);
        let f = PartitionFilter::Gte { column: "id".into(), value: "100".into() };
        // "100".starts_with("99") → false → normal comparison → max(99) < 100 → skip
        assert!(f.can_skip_by_stats(&min, &max));
    }

    // ========================================================================
    // Type-aware data skipping tests
    // ========================================================================

    #[test]
    fn test_date_eq_typed_match_epoch_micros() {
        // Rust Arrow FFI stores date stats as epoch MICROSECONDS (tantivy DateTime)
        // 2023-02-15 = 19403 epoch days = 1676419200000000 epoch micros
        let (min, max) = stats(&[("d", "1676419200000000")], &[("d", "1676419200000000")]);
        let ft: HashMap<String, String> = [("d".into(), "date".into())].into();
        let f = PartitionFilter::Eq { column: "d".into(), value: "2023-02-15".into() };
        assert!(!f.can_skip_by_stats_typed(&min, &max, &ft, None), "Should NOT skip: date matches");
    }

    #[test]
    fn test_date_eq_typed_match_epoch_days() {
        // Scala StatisticsCalculator stores date stats as epoch days
        let (min, max) = stats(&[("d", "19403")], &[("d", "19403")]);
        let ft: HashMap<String, String> = [("d".into(), "date".into())].into();
        let f = PartitionFilter::Eq { column: "d".into(), value: "2023-02-15".into() };
        assert!(!f.can_skip_by_stats_typed(&min, &max, &ft, None), "Should NOT skip: date matches");
    }

    #[test]
    fn test_date_eq_typed_no_match() {
        let (min, max) = stats(&[("d", "1676419200000000")], &[("d", "1676419200000000")]);
        let ft: HashMap<String, String> = [("d".into(), "date".into())].into();
        let f = PartitionFilter::Eq { column: "d".into(), value: "2024-01-01".into() };
        assert!(f.can_skip_by_stats_typed(&min, &max, &ft, None), "Should skip: dates don't match");
    }

    #[test]
    fn test_timestamp_eq_typed_match() {
        // Stat as epoch micros, filter as ISO-8601
        let (min, max) = stats(&[("ts", "1699336800000000")], &[("ts", "1699336800000000")]);
        let ft: HashMap<String, String> = [("ts".into(), "timestamp".into())].into();
        let f = PartitionFilter::Eq { column: "ts".into(), value: "2023-11-07T06:00:00Z".into() };
        assert!(!f.can_skip_by_stats_typed(&min, &max, &ft, None));
    }

    #[test]
    fn test_timestamp_range_typed() {
        // Range: ts > "2023-11-06T00:00:00Z", stats [2023-11-07, 2023-11-08] in micros
        let (min, max) = stats(&[("ts", "1699315200000000")], &[("ts", "1699401600000000")]);
        let ft: HashMap<String, String> = [("ts".into(), "timestamp".into())].into();
        let f = PartitionFilter::Gt { column: "ts".into(), value: "2023-11-06T00:00:00Z".into() };
        // max (2023-11-08) > filter (2023-11-06) → should NOT skip
        assert!(!f.can_skip_by_stats_typed(&min, &max, &ft, None));
    }

    #[test]
    fn test_no_field_types_falls_back() {
        // Without field_types, uses numeric comparison (both are parseable as i64)
        let (min, max) = stats(&[("d", "19403")], &[("d", "19403")]);
        let ft: HashMap<String, String> = HashMap::new();
        let f = PartitionFilter::Eq { column: "d".into(), value: "19403".into() };
        assert!(!f.can_skip_by_stats_typed(&min, &max, &ft, None));
    }

    // ========================================================================
    // Deserialization tests
    // ========================================================================

    #[test]
    fn test_deserialize_new_variants() {
        let json = r#"{"op": "neq", "column": "x", "value": "1"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::Neq { .. }));

        let json = r#"{"op": "is_null", "column": "x"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::IsNull { .. }));

        let json = r#"{"op": "is_not_null", "column": "x"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::IsNotNull { .. }));

        let json = r#"{"op": "string_starts_with", "column": "p", "prefix": "/data/"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::StringStartsWith { .. }));

        let json = r#"{"op": "string_ends_with", "column": "p", "suffix": ".parquet"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::StringEndsWith { .. }));

        let json = r#"{"op": "string_contains", "column": "p", "value": "foo"}"#;
        assert!(matches!(serde_json::from_str::<PartitionFilter>(json).unwrap(), PartitionFilter::StringContains { .. }));
    }

    #[test]
    fn test_deserialize_eq() {
        let json = r#"{"op": "eq", "column": "year", "value": "2024"}"#;
        let f: PartitionFilter = serde_json::from_str(json).unwrap();
        match f {
            PartitionFilter::Eq { column, value } => {
                assert_eq!(column, "year");
                assert_eq!(value, "2024");
            }
            other => panic!("expected Eq, got {:?}", other),
        }
    }

    #[test]
    fn test_deserialize_and() {
        let json = r#"{"op": "and", "filters": [{"op": "eq", "column": "a", "value": "1"}, {"op": "gt", "column": "b", "value": "5"}]}"#;
        let f: PartitionFilter = serde_json::from_str(json).unwrap();
        match f {
            PartitionFilter::And { filters } => assert_eq!(filters.len(), 2),
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn test_deserialize_not() {
        let json = r#"{"op": "not", "filter": {"op": "eq", "column": "x", "value": "1"}}"#;
        let f: PartitionFilter = serde_json::from_str(json).unwrap();
        match f {
            PartitionFilter::Not { .. } => {}
            other => panic!("expected Not, got {:?}", other),
        }
    }
}

