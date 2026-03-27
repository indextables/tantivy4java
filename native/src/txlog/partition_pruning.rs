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
    Gt { column: String, value: String },
    Gte { column: String, value: String },
    Lt { column: String, value: String },
    Lte { column: String, value: String },
    In { column: String, values: Vec<String> },
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
            PartitionFilter::In { column, values } => {
                partition_values.get(column).map_or(false, |v| values.contains(v))
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
            ManifestInfo { path: "m1.avro".into(), file_count: 10, partition_bounds: None },
            ManifestInfo { path: "m2.avro".into(), file_count: 20, partition_bounds: None },
        ];
        let result = prune_manifests(&manifests, &[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_prune_manifests_no_bounds_kept() {
        let manifests = vec![
            ManifestInfo { path: "m1.avro".into(), file_count: 10, partition_bounds: None },
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
            },
            ManifestInfo {
                path: "m-2024.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("year", "2024")], &[("year", "2024")])),
            },
            ManifestInfo {
                path: "m-2025.avro".into(),
                file_count: 30,
                partition_bounds: Some(bounds(&[("year", "2025")], &[("year", "2025")])),
            },
            ManifestInfo {
                path: "m-no-bounds.avro".into(),
                file_count: 5,
                partition_bounds: None,
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
            },
            ManifestInfo {
                path: "m-recent.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("year", "2023")], &[("year", "2025")])),
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
            },
            ManifestInfo {
                path: "m-high.avro".into(),
                file_count: 20,
                partition_bounds: Some(bounds(&[("id", "10")], &[("id", "100")])),
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
