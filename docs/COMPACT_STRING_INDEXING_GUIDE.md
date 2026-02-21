# Compact String Indexing Guide

## Overview

Companion splits index text fields as full Str types with TERM dictionaries, postings, and (in HYBRID mode) fast fields. For fields where only exact-match or hash-based lookup is needed, this wastes significant index space. Additionally, fields containing UUIDs (common in log/event data) inflate the term dictionary with high-cardinality unique strings that rarely benefit from tokenized search.

Compact string indexing modes reduce index size by:
1. Replacing full string indexing with compact U64 hash indexing (`exact_only`)
2. Stripping UUIDs (or custom patterns) before text indexing, with optional hash-based exact lookup for the extracted patterns

## The Problem

Raw string TERM dictionaries + postings dominate index size for UUID-heavy or high-cardinality string fields. A field like `trace_id` with 1M unique UUIDs creates a massive FST dictionary and posting list that rarely benefits from tokenized search — users only ever do exact-match lookups.

Similarly, log messages like `"Error processing request 550e8400-e29b-41d4-a716-446655440000 from user"` waste index space storing UUIDs in the text index that are better handled by hash-based lookup.

## Available Modes

| Mode | Tokenizer Override Value | Behavior | Use Case |
|------|--------------------------|----------|----------|
| **Exact Only** | `exact_only` | Index xxHash64 as U64 (TERM + fast). No Str field. | High-cardinality ID fields (trace_id, request_id) |
| **Text UUID Exactonly** | `text_uuid_exactonly` | Strip UUIDs → "default" tokenizer text. UUIDs → companion U64 hash. | Log messages with embedded UUIDs |
| **Text UUID Strip** | `text_uuid_strip` | Strip UUIDs → "default" tokenizer text. UUIDs discarded. | Log messages where UUIDs aren't queryable |
| **Text Custom Exactonly** | `text_custom_exactonly:<regex>` | Like UUID exactonly but with custom regex. | Custom patterns (SSNs, order IDs) |
| **Text Custom Strip** | `text_custom_strip:<regex>` | Like UUID strip but with custom regex. | Strip custom patterns from text |

UUID pattern used: `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}` (dashed only)

## Usage Examples

```java
import io.indextables.tantivy4java.split.ParquetCompanionConfig;
import io.indextables.tantivy4java.split.ParquetCompanionConfig.StringIndexingMode;

// Create config with compact string indexing
Map<String, String> tokenizers = new HashMap<>();

// High-cardinality ID field: exact_only (80% size reduction)
tokenizers.put("trace_id", StringIndexingMode.EXACT_ONLY);
tokenizers.put("request_id", StringIndexingMode.EXACT_ONLY);

// Log message with UUIDs: strip UUIDs, keep them queryable via hash
tokenizers.put("message", StringIndexingMode.TEXT_UUID_EXACTONLY);

// Log message with UUIDs: strip UUIDs, discard them
tokenizers.put("raw_log", StringIndexingMode.TEXT_UUID_STRIP);

// Custom pattern: strip SSN-like patterns, keep them queryable
tokenizers.put("audit_log", StringIndexingMode.textCustomExactonly("\\d{3}-\\d{2}-\\d{4}"));

// Custom pattern: strip order IDs, discard them
tokenizers.put("notes", StringIndexingMode.textCustomStrip("ORD-\\d{8}"));

ParquetCompanionConfig config = new ParquetCompanionConfig()
    .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
    .withTokenizerOverrides(tokenizers);
```

## How It Works

### Schema Derivation

When a field has a compact indexing mode, the schema derivation creates different field types:

- **`exact_only`**: Creates a single U64 field (indexed + fast) instead of a Str field
- **`text_*_exactonly`**: Creates a Str field (with "default" tokenizer) + a U64 companion field `<name>__uuids`
- **`text_*_strip`**: Creates a Str field (with "default" tokenizer) only

### Indexing Pipeline

During document indexing:

- **`exact_only`**: Computes xxHash64 of the string value and stores it as U64
- **`text_*_exactonly`**: Extracts regex matches, stores stripped text in the Str field, and stores xxHash64 of each match in the companion U64 field
- **`text_*_strip`**: Strips regex matches and stores the cleaned text in the Str field

### Query Rewriting

At query time, term queries are automatically rewritten:

- **`exact_only`**: The search term is hashed and the query targets the U64 field
- **`text_*_exactonly`**: If the search term matches the field's regex pattern, the query is redirected to the companion `__uuids` hash field with the hashed value. Non-matching terms query the text field directly.
- **`text_*_strip`**: No rewriting — queries hit the text field directly

### Aggregation Support

The existing hash-based aggregation pipeline handles compact indexing modes automatically:

- `exact_only` fields are registered in `string_hash_fields` (mapped to themselves)
- Companion `__uuids` fields are registered in `string_hash_fields` (mapped to themselves)
- The existing `rewrite_aggs_for_hash_fields()` + `build_hash_resolution_map()` pipeline resolves hash bucket keys back to original strings via parquet reads

## Querying Behavior

| Query Type | `exact_only` | `text_*_exactonly` | `text_*_strip` |
|-----------|-------------|-------------------|---------------|
| **Term (exact)** | Hash match on U64 | Regex match → companion hash; else text search | Text search (stripped) |
| **Phrase** | Not supported | On stripped text | On stripped text |
| **Wildcard** | Not supported | On stripped text | On stripped text |
| **Exists** | U64 field presence | Text field presence | Text field presence |
| **Range** | Not meaningful | On text field | On text field |
| **Terms agg** | Via hash touchup | Text or companion depending on field | On text field |

## Size Reduction Estimates

- **`exact_only`**: ~80% reduction for high-cardinality string fields (e.g., UUIDs, trace IDs). A 1M-row UUID column drops from ~50MB (FST + postings) to ~8MB (U64 fast field).
- **`text_uuid_strip`**: 20-50% reduction depending on UUID density in text
- **`text_uuid_exactonly`**: 15-40% reduction (some space used by companion hash field)

## Limitations

- **Hash collisions**: xxHash64 has ~1 in 2^64 collision probability per pair. In practice, this is negligible for any realistic dataset size.
- **No phrase/wildcard on `exact_only`**: Since only hashes are stored, phrase and wildcard queries are not supported on `exact_only` fields.
- **UUID pattern**: Only dashed UUID format is recognized. Non-dashed UUIDs require a custom regex via `text_custom_*` modes.
- **Irreversible hashing**: Original string values cannot be recovered from hashes. For `exact_only` fields, document retrieval still works via parquet (the original string is in the parquet file).

## Configuration Reference

| Parameter | Type | Description |
|-----------|------|-------------|
| `exact_only` | Tokenizer override | Replace Str with U64 hash |
| `text_uuid_exactonly` | Tokenizer override | Strip UUIDs, hash to companion |
| `text_uuid_strip` | Tokenizer override | Strip UUIDs, discard |
| `text_custom_exactonly:<regex>` | Tokenizer override | Strip custom regex, hash to companion |
| `text_custom_strip:<regex>` | Tokenizer override | Strip custom regex, discard |

All modes are configured via `withTokenizerOverrides()` on `ParquetCompanionConfig`. The `StringIndexingMode` class provides convenience constants.
