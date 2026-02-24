// string_indexing.rs - Compact string indexing modes for companion splits
//
// Provides alternative indexing strategies for string fields that reduce index size:
// - exact_only: Replace full Str field with U64 xxHash64 (term + fast)
// - text_uuid_exactonly: Strip UUIDs from text, store UUID hashes in companion U64 field
// - text_uuid_strip: Strip UUIDs from text, discard them
// - text_custom_exactonly:<regex>: Like uuid_exactonly but with custom regex
// - text_custom_strip:<regex>: Like uuid_strip but with custom regex

use std::collections::HashMap;
use anyhow::Result;

/// Standard UUID regex pattern (dashed form only).
pub const UUID_REGEX: &str = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

/// Suffix appended to the original field name to create the companion hash field.
pub const COMPANION_SUFFIX: &str = "__uuids";

/// Compact string indexing mode for a field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum StringIndexingMode {
    /// Index xxHash64 as U64 (TERM + fast). No Str field created.
    /// Term queries are automatically hashed.
    ExactOnly,
    /// Strip UUIDs from text, index remaining text with "default" tokenizer.
    /// UUIDs stored as xxHash64 in `<field>__uuids` companion U64 field.
    TextUuidExactonly,
    /// Strip UUIDs from text, index remaining text with "default" tokenizer.
    /// UUIDs are discarded.
    TextUuidStrip,
    /// Strip custom regex matches from text, index remaining text with "default" tokenizer.
    /// Matches stored as xxHash64 in `<field>__uuids` companion U64 field.
    TextCustomExactonly { regex: String },
    /// Strip custom regex matches from text, index remaining text with "default" tokenizer.
    /// Matches are discarded.
    TextCustomStrip { regex: String },
}

/// Metadata about a companion hash field created for a string indexing mode.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompanionFieldInfo {
    /// The original field name that this companion field was created for.
    pub original_field_name: String,
    /// The regex pattern used to extract values (UUID pattern or custom).
    pub regex_pattern: String,
}

/// Parse a tokenizer override value into a StringIndexingMode.
///
/// Returns `None` for standard tokenizer names ("raw", "default", "en_stem", etc.).
/// Returns `Some(mode)` for recognized compact indexing mode strings.
pub fn parse_tokenizer_override(value: &str) -> Option<StringIndexingMode> {
    match value {
        "exact_only" => Some(StringIndexingMode::ExactOnly),
        "text_uuid_exactonly" => Some(StringIndexingMode::TextUuidExactonly),
        "text_uuid_strip" => Some(StringIndexingMode::TextUuidStrip),
        _ => {
            if let Some(regex) = value.strip_prefix("text_custom_exactonly:") {
                if !regex.is_empty() {
                    Some(StringIndexingMode::TextCustomExactonly { regex: regex.to_string() })
                } else {
                    None
                }
            } else if let Some(regex) = value.strip_prefix("text_custom_strip:") {
                if !regex.is_empty() {
                    Some(StringIndexingMode::TextCustomStrip { regex: regex.to_string() })
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

/// Build the companion field name for a given original field name.
pub fn companion_field_name(original: &str) -> String {
    format!("{}{}", original, COMPANION_SUFFIX)
}

/// Get the regex pattern for a string indexing mode.
///
/// Returns the UUID regex for uuid variants, the custom regex for custom variants,
/// and None for ExactOnly (which has no regex pattern).
pub fn regex_pattern(mode: &StringIndexingMode) -> Option<&str> {
    match mode {
        StringIndexingMode::ExactOnly => None,
        StringIndexingMode::TextUuidExactonly | StringIndexingMode::TextUuidStrip => Some(UUID_REGEX),
        StringIndexingMode::TextCustomExactonly { regex } | StringIndexingMode::TextCustomStrip { regex } => {
            Some(regex.as_str())
        }
    }
}

/// Check whether a mode needs a companion hash field.
///
/// True for `*_exactonly` variants which store extracted matches as hashes.
pub fn needs_companion_field(mode: &StringIndexingMode) -> bool {
    matches!(mode,
        StringIndexingMode::TextUuidExactonly | StringIndexingMode::TextCustomExactonly { .. }
    )
}

/// Strip all regex matches from text, collapsing multiple whitespace to single spaces.
pub fn strip_pattern(text: &str, compiled_regex: &regex::Regex) -> String {
    let stripped = compiled_regex.replace_all(text, " ");
    // Collapse multiple whitespace and trim
    let mut collapsed = String::new();
    for (i, word) in stripped.split_whitespace().enumerate() {
        if i > 0 { collapsed.push(' '); }
        collapsed.push_str(word);
    }
    collapsed
}

/// Extract all regex matches from text, returning (stripped_text, matches).
pub fn extract_pattern(text: &str, compiled_regex: &regex::Regex) -> (String, Vec<String>) {
    let matches: Vec<String> = compiled_regex
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .collect();

    let stripped = strip_pattern(text, compiled_regex);
    (stripped, matches)
}

/// Compile regex patterns for all string indexing modes.
///
/// Returns a map from field name to compiled Regex.
pub fn compile_regexes(
    modes: &HashMap<String, StringIndexingMode>,
) -> Result<HashMap<String, regex::Regex>> {
    let mut compiled = HashMap::new();

    for (field_name, mode) in modes {
        if let Some(pattern) = regex_pattern(mode) {
            let regex = regex::Regex::new(pattern)
                .map_err(|e| anyhow::anyhow!(
                    "Invalid regex pattern for field '{}': {} (pattern: '{}')",
                    field_name, e, pattern
                ))?;
            compiled.insert(field_name.clone(), regex);
        }
    }

    Ok(compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exact_only() {
        assert_eq!(
            parse_tokenizer_override("exact_only"),
            Some(StringIndexingMode::ExactOnly)
        );
    }

    #[test]
    fn test_parse_text_uuid_exactonly() {
        assert_eq!(
            parse_tokenizer_override("text_uuid_exactonly"),
            Some(StringIndexingMode::TextUuidExactonly)
        );
    }

    #[test]
    fn test_parse_text_uuid_strip() {
        assert_eq!(
            parse_tokenizer_override("text_uuid_strip"),
            Some(StringIndexingMode::TextUuidStrip)
        );
    }

    #[test]
    fn test_parse_text_custom_exactonly() {
        let mode = parse_tokenizer_override("text_custom_exactonly:\\d{3}-\\d{4}");
        assert_eq!(
            mode,
            Some(StringIndexingMode::TextCustomExactonly { regex: "\\d{3}-\\d{4}".to_string() })
        );
    }

    #[test]
    fn test_parse_text_custom_strip() {
        let mode = parse_tokenizer_override("text_custom_strip:SSN-\\d+");
        assert_eq!(
            mode,
            Some(StringIndexingMode::TextCustomStrip { regex: "SSN-\\d+".to_string() })
        );
    }

    #[test]
    fn test_parse_standard_tokenizers_return_none() {
        assert_eq!(parse_tokenizer_override("raw"), None);
        assert_eq!(parse_tokenizer_override("default"), None);
        assert_eq!(parse_tokenizer_override("en_stem"), None);
        assert_eq!(parse_tokenizer_override("whitespace"), None);
    }

    #[test]
    fn test_parse_empty_custom_regex_returns_none() {
        assert_eq!(parse_tokenizer_override("text_custom_exactonly:"), None);
        assert_eq!(parse_tokenizer_override("text_custom_strip:"), None);
    }

    #[test]
    fn test_companion_field_name() {
        assert_eq!(companion_field_name("trace_id"), "trace_id__uuids");
        assert_eq!(companion_field_name("message"), "message__uuids");
    }

    #[test]
    fn test_regex_pattern_exact_only() {
        assert_eq!(regex_pattern(&StringIndexingMode::ExactOnly), None);
    }

    #[test]
    fn test_regex_pattern_uuid_variants() {
        assert_eq!(
            regex_pattern(&StringIndexingMode::TextUuidExactonly),
            Some(UUID_REGEX)
        );
        assert_eq!(
            regex_pattern(&StringIndexingMode::TextUuidStrip),
            Some(UUID_REGEX)
        );
    }

    #[test]
    fn test_regex_pattern_custom_variants() {
        assert_eq!(
            regex_pattern(&StringIndexingMode::TextCustomExactonly { regex: "abc".to_string() }),
            Some("abc")
        );
        assert_eq!(
            regex_pattern(&StringIndexingMode::TextCustomStrip { regex: "xyz".to_string() }),
            Some("xyz")
        );
    }

    #[test]
    fn test_needs_companion_field() {
        assert!(!needs_companion_field(&StringIndexingMode::ExactOnly));
        assert!(needs_companion_field(&StringIndexingMode::TextUuidExactonly));
        assert!(!needs_companion_field(&StringIndexingMode::TextUuidStrip));
        assert!(needs_companion_field(&StringIndexingMode::TextCustomExactonly { regex: "x".to_string() }));
        assert!(!needs_companion_field(&StringIndexingMode::TextCustomStrip { regex: "x".to_string() }));
    }

    #[test]
    fn test_strip_pattern_uuid() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "Error in request 550e8400-e29b-41d4-a716-446655440000 from user";
        let result = strip_pattern(text, &re);
        assert_eq!(result, "Error in request from user");
    }

    #[test]
    fn test_strip_pattern_multiple_uuids() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "a]550e8400-e29b-41d4-a716-446655440000 b 123e4567-e89b-12d3-a456-426614174000 c";
        let result = strip_pattern(text, &re);
        assert_eq!(result, "a] b c");
    }

    #[test]
    fn test_strip_pattern_no_match() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "no uuids here";
        let result = strip_pattern(text, &re);
        assert_eq!(result, "no uuids here");
    }

    #[test]
    fn test_extract_pattern_uuid() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "trace=550e8400-e29b-41d4-a716-446655440000 status=ok";
        let (stripped, matches) = extract_pattern(text, &re);
        assert_eq!(stripped, "trace= status=ok");
        assert_eq!(matches, vec!["550e8400-e29b-41d4-a716-446655440000"]);
    }

    #[test]
    fn test_extract_pattern_multiple() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "a 550e8400-e29b-41d4-a716-446655440000 b 123e4567-e89b-12d3-a456-426614174000";
        let (stripped, matches) = extract_pattern(text, &re);
        assert_eq!(stripped, "a b");
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(matches[1], "123e4567-e89b-12d3-a456-426614174000");
    }

    #[test]
    fn test_compile_regexes_ok() {
        let mut modes = HashMap::new();
        modes.insert("trace".to_string(), StringIndexingMode::TextUuidExactonly);
        modes.insert("msg".to_string(), StringIndexingMode::TextCustomStrip { regex: "\\d+".to_string() });
        modes.insert("id".to_string(), StringIndexingMode::ExactOnly);

        let compiled = compile_regexes(&modes).unwrap();
        assert_eq!(compiled.len(), 2); // ExactOnly has no regex
        assert!(compiled.contains_key("trace"));
        assert!(compiled.contains_key("msg"));
    }

    #[test]
    fn test_compile_regexes_invalid_pattern() {
        let mut modes = HashMap::new();
        modes.insert("bad".to_string(), StringIndexingMode::TextCustomExactonly {
            regex: "[invalid".to_string()
        });

        let result = compile_regexes(&modes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_serde_roundtrip_exact_only() {
        let mode = StringIndexingMode::ExactOnly;
        let json = serde_json::to_string(&mode).unwrap();
        let parsed: StringIndexingMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, mode);
    }

    #[test]
    fn test_serde_roundtrip_text_uuid_exactonly() {
        let mode = StringIndexingMode::TextUuidExactonly;
        let json = serde_json::to_string(&mode).unwrap();
        let parsed: StringIndexingMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, mode);
    }

    #[test]
    fn test_serde_roundtrip_text_custom_exactonly() {
        let mode = StringIndexingMode::TextCustomExactonly { regex: "\\d{3}-\\d{4}".to_string() };
        let json = serde_json::to_string(&mode).unwrap();
        let parsed: StringIndexingMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, mode);
    }

    #[test]
    fn test_serde_roundtrip_companion_field_info() {
        let info = CompanionFieldInfo {
            original_field_name: "trace_id".to_string(),
            regex_pattern: UUID_REGEX.to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: CompanionFieldInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, info);
    }

    #[test]
    fn test_strip_pattern_custom_regex() {
        let re = regex::Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap(); // SSN pattern
        let text = "User SSN 123-45-6789 processed";
        let result = strip_pattern(text, &re);
        assert_eq!(result, "User SSN processed");
    }

    #[test]
    fn test_extract_pattern_no_match() {
        let re = regex::Regex::new(UUID_REGEX).unwrap();
        let text = "no matches here";
        let (stripped, matches) = extract_pattern(text, &re);
        assert_eq!(stripped, "no matches here");
        assert!(matches.is_empty());
    }
}
