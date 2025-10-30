# JSON Field Support Design for Tantivy4Java

## Executive Summary

This document provides a comprehensive design for adding full JSON data type support to tantivy4java, matching the capabilities of both Tantivy core and Quickwit. The design covers schema definition, document indexing, query support, and complete lifecycle management for both standard Tantivy indices and Quickwit splits.

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Tantivy JSON Field Implementation](#2-tantivy-json-field-implementation)
3. [Quickwit JSON Integration](#3-quickwit-json-integration)
4. [Java API Design](#4-java-api-design)
5. [Native/JNI Layer](#5-nativejni-layer)
6. [Usage Examples](#6-usage-examples)
7. [Complete Unit Test Suite](#7-complete-unit-test-suite)
8. [Performance Considerations](#8-performance-considerations)
9. [Implementation Phases](#9-implementation-phases)
10. [Migration Guide](#10-migration-guide)

---

## 1. Architecture Overview

### 1.1 JSON Field Capabilities

JSON fields in Tantivy provide:

- **Flexible Schema**: Store arbitrary nested JSON objects
- **Automatic Indexing**: All nested values (strings, numbers, booleans, dates) indexed automatically
- **JSONPath Queries**: Query nested fields using dot notation (`user.name`, `cart.items.price`)
- **Type Inference**: Automatic type detection for query values
- **Fast Fields**: Columnar storage for efficient filtering and aggregations
- **Array Support**: Multi-valued fields handled natively
- **Dot Expansion**: Configurable handling of dots in JSON keys

### 1.2 System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Java API Layer                           │
│  SchemaBuilder, JsonObjectOptions, Document, Query          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    JNI Binding Layer                        │
│  Schema creation, Document JSON, Query construction         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   Tantivy Core (Rust)                       │
│  JsonObjectOptions, Term::from_field_json_path, indexing    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   Storage Formats                           │
│  Inverted Index, Doc Store, Fast Fields, Quickwit Splits    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Tantivy JSON Field Implementation

### 2.1 Core Type System

**Tantivy Rust Type Definitions** (`tantivy/src/schema/field_type.rs`):

```rust
pub enum Type {
    Str = b's',
    U64 = b'u',
    I64 = b'i',
    F64 = b'f',
    Bool = b'o',
    Date = b'd',
    Facet = b'h',
    Bytes = b'b',
    Json = b'j',      // ← JSON type marker
    IpAddr = b'p',
}

pub enum FieldType {
    JsonObject(JsonObjectOptions),  // ← JSON field configuration
    // ... other types
}
```

### 2.2 JSON Field Configuration

**JsonObjectOptions Structure** (`tantivy/src/schema/json_object_options.rs`):

```rust
pub struct JsonObjectOptions {
    stored: bool,                           // Store original JSON
    indexing: Option<TextFieldIndexing>,    // Index nested values
    fast: FastFieldTextOptions,             // Columnar storage
    expand_dots_enabled: bool,              // Handle dots in keys
}
```

**Configuration Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `stored` | Store entire JSON object for retrieval | `false` |
| `indexing` | Index all nested values with text analyzer | `None` |
| `fast` | Enable columnar storage with optional tokenizer | `None` |
| `expand_dots_enabled` | Treat dots as path separators | `false` |

**Expand Dots Behavior:**

- **`expand_dots_enabled = false`**:
  - Input: `{"k8s.node.id": 5}`
  - Query: `k8s\.node\.id:5` (dots must be escaped)

- **`expand_dots_enabled = true`**:
  - Input: `{"k8s.node.id": 5}`
  - Treated as: `{"k8s": {"node": {"id": 5}}}`
  - Query: `k8s.node.id:5` (natural path syntax)

### 2.3 JSON Path Encoding

**Internal Path Representation:**

JSON objects are flattened into paths with values:

```
Example JSON: {"user": {"name": "John", "age": 30}}

Flattened representation:
  user.name → "John" (Type::Str)
  user.age → 30 (Type::I64)

Internal term structure:
  [Field ID (4 bytes)][Type Code 'j'][Path: user\x01name\x00][Type: s][Value: "John"]
  [Field ID (4 bytes)][Type Code 'j'][Path: user\x01age\x00][Type: i][Value: 30]
```

**Path Separator:** `\x01` (JSON_PATH_SEGMENT_SEP) used internally

### 2.4 Supported Nested Types

| Type | Example | Indexing Behavior |
|------|---------|-------------------|
| **String** | `"name": "John"` | Tokenized with text analyzer |
| **Integer** | `"age": 30` | Indexed as i64 fast field |
| **Unsigned** | `"count": 42` | Indexed as u64 fast field |
| **Float** | `"price": 29.99` | Indexed as f64 fast field |
| **Boolean** | `"active": true` | Indexed as bool fast field |
| **Date** | `"created": "2025-01-29T10:30:00Z"` | Indexed as DateTime with precision |
| **Array** | `"tags": ["a", "b"]` | Each element indexed at same path (multi-valued) |
| **Object** | `"user": {"name": "John"}` | Recursively flattened |

**Not Supported in JSON:**
- PreTokenizedString
- Bytes
- Facets
- IP Addresses (use top-level field instead)

### 2.5 Type Inference for Queries

**Query Value Type Inference Priority** (`tantivy/src/core/json_utils.rs`):

When querying with string values, Tantivy attempts type inference:

1. **RFC3339 Date** → `DateTime` (e.g., `"2025-01-29T10:30:00Z"`)
2. **Integer** → `i64` (e.g., `"30"` → 30)
3. **Unsigned** → `u64` (e.g., `"42"` → 42u64)
4. **Float** → `f64` (e.g., `"29.99"` → 29.99)
5. **Boolean** → `bool` (e.g., `"true"` → true)
6. **Fallback** → String (if none match)

This allows natural query syntax:
```
user.age:30          // Inferred as integer
price:29.99          // Inferred as float
active:true          // Inferred as boolean
created:"2025-01-29" // Inferred as date
```

---

## 3. Quickwit JSON Integration

### 3.1 Quickwit Field Mapping

**QuickwitJsonOptions** (`quickwit/quickwit-doc-mapper/src/doc_mapper/field_mapping_entry.rs`):

```rust
pub struct QuickwitJsonOptions {
    description: Option<String>,
    indexing_options: Option<TextIndexingOptions>,  // Text indexing config
    stored: bool,                                   // Store entire JSON
    expand_dots: bool,                              // Handle dots in keys
    fast: FastFieldOptions,                         // Columnar storage
}
```

**Default Behavior:**

```rust
impl Default for QuickwitJsonOptions {
    fn default() -> Self {
        QuickwitJsonOptions {
            description: None,
            indexing_options: Some(TextIndexingOptions::default_json()),
            stored: true,           // ← Stored by default
            expand_dots: true,      // ← Dots expanded by default
            fast: FastFieldOptions::default(),
        }
    }
}
```

**Dynamic Mapping Default:**

```rust
pub fn default_dynamic() -> Self {
    QuickwitJsonOptions {
        fast: FastFieldOptions::default_enabled(),  // ← Fast field enabled
        ..Default::default()
    }
}
```

### 3.2 Conversion to Tantivy

Quickwit converts `QuickwitJsonOptions` → `JsonObjectOptions`:

```rust
impl From<QuickwitJsonOptions> for JsonObjectOptions {
    fn from(quickwit_json_options: QuickwitJsonOptions) -> Self {
        let mut json_options = JsonObjectOptions::default();

        if quickwit_json_options.stored {
            json_options = json_options.set_stored();
        }

        if let Some(indexing_options) = quickwit_json_options.indexing_options {
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer(indexing_options.tokenizer.name())
                .set_index_option(indexing_options.record);
            json_options = json_options.set_indexing_options(text_field_indexing);
        }

        if quickwit_json_options.expand_dots {
            json_options = json_options.set_expand_dots_enabled();
        }

        match &quickwit_json_options.fast {
            FastFieldOptions::EnabledWithNormalizer { normalizer } => {
                json_options = json_options.set_fast(Some(normalizer.get_name()));
            }
            FastFieldOptions::Disabled => {}
        }

        json_options
    }
}
```

### 3.3 Quickwit Split Support

JSON fields in Quickwit splits:
- ✅ Stored using Tantivy's JSON field format
- ✅ Support all JSONPath query patterns
- ✅ Benefit from columnar compression when `fast=true`
- ✅ Merge operations preserve JSON structure
- ✅ Compatible with distributed search architecture

---

## 4. Java API Design

### 4.1 JsonObjectOptions Class

**File: `src/main/java/io/indextables/tantivy4java/core/JsonObjectOptions.java`**

```java
package io.indextables.tantivy4java.core;

/**
 * Configuration options for JSON object fields.
 *
 * JSON fields allow storing and indexing arbitrary nested JSON objects.
 * All nested values (strings, numbers, booleans, dates, arrays) are
 * automatically indexed and can be queried using JSONPath syntax.
 *
 * Example:
 * <pre>
 * JsonObjectOptions options = JsonObjectOptions.create()
 *     .setStored(true)
 *     .setIndexing(TextFieldIndexing.withPositions())
 *     .setFast(true)
 *     .setExpandDots(true);
 *
 * Field jsonField = schemaBuilder.addJsonField("attributes", options);
 * </pre>
 */
public class JsonObjectOptions {
    private boolean stored;
    private TextFieldIndexing indexing;  // null if not indexed
    private boolean fast;
    private String fastTokenizer;  // null or tokenizer name
    private boolean expandDots;

    private JsonObjectOptions() {
        this.stored = false;
        this.indexing = null;
        this.fast = false;
        this.fastTokenizer = null;
        this.expandDots = false;
    }

    /**
     * Create default JSON field options (not stored, not indexed).
     */
    public static JsonObjectOptions create() {
        return new JsonObjectOptions();
    }

    /**
     * Store the entire JSON object for retrieval.
     */
    public JsonObjectOptions setStored(boolean stored) {
        this.stored = stored;
        return this;
    }

    /**
     * Enable indexing of nested values with text field configuration.
     *
     * @param indexing Text field indexing settings (tokenizer, positions, etc.)
     */
    public JsonObjectOptions setIndexing(TextFieldIndexing indexing) {
        this.indexing = indexing;
        return this;
    }

    /**
     * Enable columnar storage (fast fields) for efficient filtering.
     */
    public JsonObjectOptions setFast(boolean fast) {
        this.fast = fast;
        return this;
    }

    /**
     * Set tokenizer for fast field text values.
     *
     * @param tokenizer Tokenizer name (e.g., "raw", "default")
     */
    public JsonObjectOptions setFastTokenizer(String tokenizer) {
        this.fastTokenizer = tokenizer;
        return this;
    }

    /**
     * Enable dot expansion in JSON keys.
     *
     * When true:  {"k8s.node.id": 5} → {"k8s": {"node": {"id": 5}}}
     * When false: {"k8s.node.id": 5} → query as k8s\.node\.id:5
     */
    public JsonObjectOptions setExpandDots(boolean expandDots) {
        this.expandDots = expandDots;
        return this;
    }

    // Getters
    public boolean isStored() { return stored; }
    public TextFieldIndexing getIndexing() { return indexing; }
    public boolean isFast() { return fast; }
    public String getFastTokenizer() { return fastTokenizer; }
    public boolean isExpandDots() { return expandDots; }

    // Static factory methods

    /**
     * Create JSON field that is only stored (not searchable).
     */
    public static JsonObjectOptions stored() {
        return create().setStored(true);
    }

    /**
     * Create JSON field that is indexed (searchable) but not stored.
     */
    public static JsonObjectOptions indexed() {
        return create().setIndexing(TextFieldIndexing.defaultIndexing());
    }

    /**
     * Create JSON field that is both stored and indexed.
     * This is the most common configuration.
     */
    public static JsonObjectOptions storedAndIndexed() {
        return create()
            .setStored(true)
            .setIndexing(TextFieldIndexing.defaultIndexing());
    }

    /**
     * Create JSON field with all features enabled.
     * Stored + indexed + fast fields + dot expansion.
     */
    public static JsonObjectOptions full() {
        return create()
            .setStored(true)
            .setIndexing(TextFieldIndexing.withPositions())
            .setFast(true)
            .setExpandDots(true);
    }
}
```

### 4.2 SchemaBuilder Enhancement

**File: `src/main/java/io/indextables/tantivy4java/core/SchemaBuilder.java`**

```java
/**
 * Add a JSON object field to the schema.
 *
 * JSON fields allow storing and indexing arbitrary nested JSON objects.
 * All nested values are automatically indexed according to their type.
 *
 * Example:
 * <pre>
 * Field attrs = schemaBuilder.addJsonField("attributes",
 *     JsonObjectOptions.storedAndIndexed()
 *         .setExpandDots(true));
 * </pre>
 *
 * @param name Field name
 * @param options JSON field configuration
 * @return Field handle for this JSON field
 */
public Field addJsonField(String name, JsonObjectOptions options) {
    // Validate field name
    if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be null or empty");
    }

    // Call native method to add JSON field to schema
    int fieldId = nativeAddJsonField(
        schemaPtr,
        name,
        options.isStored(),
        options.getIndexing() != null,
        options.isFast(),
        options.getFastTokenizer(),
        options.isExpandDots(),
        options.getIndexing() != null ? options.getIndexing().getTokenizerName() : null,
        options.getIndexing() != null ? options.getIndexing().getIndexOption() : 0
    );

    return new Field(name, fieldId);
}

private native int nativeAddJsonField(
    long schemaPtr,
    String name,
    boolean stored,
    boolean indexed,
    boolean fast,
    String fastTokenizer,
    boolean expandDots,
    String tokenizerName,
    int recordOption
);
```

### 4.3 Document API Enhancement

**File: `src/main/java/io/indextables/tantivy4java/core/Document.java`**

```java
/**
 * Add JSON value from string.
 *
 * The JSON string must be valid according to RFC 8259.
 * All nested values will be indexed according to field configuration.
 *
 * Example:
 * <pre>
 * doc.addJson(field, "{\"user\": {\"name\": \"John\", \"age\": 30}}");
 * </pre>
 *
 * @param field JSON field handle
 * @param jsonString Valid JSON string
 * @throws IllegalArgumentException if JSON is invalid
 */
public void addJson(Field field, String jsonString) {
    if (field == null) {
        throw new IllegalArgumentException("Field cannot be null");
    }
    if (jsonString == null) {
        throw new IllegalArgumentException("JSON string cannot be null");
    }

    nativeAddJsonFromString(docPtr, field.getFieldId(), jsonString);
}

/**
 * Add JSON value from Java object.
 *
 * Automatically serializes the object to JSON. Supported types:
 * - Map<String, Object> → JSON object
 * - List<Object> → JSON array
 * - String, Integer, Long, Float, Double, Boolean → JSON primitives
 * - java.time.Instant, java.util.Date → ISO 8601 date string
 *
 * Example:
 * <pre>
 * Map&lt;String, Object&gt; data = new HashMap&lt;&gt;();
 * data.put("name", "John");
 * data.put("age", 30);
 * doc.addJson(field, data);
 * </pre>
 *
 * @param field JSON field handle
 * @param value Java object to serialize
 * @throws IllegalArgumentException if object cannot be serialized
 */
public void addJson(Field field, Object value) {
    if (field == null) {
        throw new IllegalArgumentException("Field cannot be null");
    }
    if (value == null) {
        throw new IllegalArgumentException("Value cannot be null");
    }

    // Serialize to JSON string using Gson or Jackson
    String jsonString = serializeToJson(value);
    addJson(field, jsonString);
}

private native void nativeAddJsonFromString(long docPtr, int fieldId, String jsonString);

// Helper method for serialization
private String serializeToJson(Object value) {
    // Use Gson for lightweight JSON serialization
    com.google.gson.Gson gson = new com.google.gson.GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        .create();
    return gson.toJson(value);
}
```

### 4.4 Query API Enhancement

**File: `src/main/java/io/indextables/tantivy4java/query/Query.java`**

```java
/**
 * Create term query on JSON field path.
 *
 * Queries a specific path within a JSON object using dot notation.
 * The value type is automatically inferred (string, number, boolean, date).
 *
 * Examples:
 * <pre>
 * // Query nested string
 * Query q1 = Query.jsonTermQuery(schema, field, "user.name", "John");
 *
 * // Query nested number
 * Query q2 = Query.jsonTermQuery(schema, field, "user.age", 30);
 *
 * // Query nested boolean
 * Query q3 = Query.jsonTermQuery(schema, field, "user.active", true);
 *
 * // Query with escaped dots (when expand_dots=false)
 * Query q4 = Query.jsonTermQuery(schema, field, "k8s\\.pod\\.name", "api-123");
 * </pre>
 *
 * @param schema Schema containing the field
 * @param field JSON field to query
 * @param jsonPath Path using dot notation (e.g., "user.name", "cart.items.price")
 * @param value Query value (String, Number, Boolean, or Date)
 * @return Query for matching documents
 */
public static Query jsonTermQuery(Schema schema, Field field, String jsonPath, Object value) {
    if (schema == null || field == null || jsonPath == null || value == null) {
        throw new IllegalArgumentException("Arguments cannot be null");
    }

    // Convert value to string for type inference
    String valueString = convertValueToString(value);

    long queryPtr = nativeCreateJsonTermQuery(
        schema.getSchemaPtr(),
        field.getFieldId(),
        jsonPath,
        valueString
    );

    return new Query(queryPtr);
}

/**
 * Create range query on JSON numeric or date field.
 *
 * Queries a numeric or date path within a JSON object.
 *
 * Examples:
 * <pre>
 * // Age between 18 and 65
 * Query q1 = Query.jsonRangeQuery(schema, field, "user.age",
 *     18, true, 65, true);
 *
 * // Price greater than 100
 * Query q2 = Query.jsonRangeQuery(schema, field, "product.price",
 *     100.0, false, null, false);
 *
 * // Created after 2025-01-01
 * Query q3 = Query.jsonRangeQuery(schema, field, "event.created",
 *     Instant.parse("2025-01-01T00:00:00Z"), true,
 *     null, false);
 * </pre>
 *
 * @param schema Schema containing the field
 * @param field JSON field to query
 * @param jsonPath Path to numeric/date field
 * @param lowerBound Lower bound (null for unbounded)
 * @param lowerInclusive Include lower bound
 * @param upperBound Upper bound (null for unbounded)
 * @param upperInclusive Include upper bound
 * @return Query for matching documents
 */
public static Query jsonRangeQuery(Schema schema, Field field, String jsonPath,
                                    Object lowerBound, boolean lowerInclusive,
                                    Object upperBound, boolean upperInclusive) {
    if (schema == null || field == null || jsonPath == null) {
        throw new IllegalArgumentException("Schema, field, and jsonPath cannot be null");
    }

    String lowerStr = lowerBound != null ? convertValueToString(lowerBound) : null;
    String upperStr = upperBound != null ? convertValueToString(upperBound) : null;

    long queryPtr = nativeCreateJsonRangeQuery(
        schema.getSchemaPtr(),
        field.getFieldId(),
        jsonPath,
        lowerStr, lowerInclusive,
        upperStr, upperInclusive
    );

    return new Query(queryPtr);
}

/**
 * Create exists query for JSON field path.
 *
 * Checks if a specific path exists in the JSON object.
 *
 * Examples:
 * <pre>
 * // Check if user.email exists (exact path)
 * Query q1 = Query.jsonExistsQuery(schema, field, "user.email", false);
 *
 * // Check if any subpath of user exists
 * Query q2 = Query.jsonExistsQuery(schema, field, "user", true);
 * </pre>
 *
 * @param schema Schema containing the field
 * @param field JSON field to query
 * @param jsonPath Path to check
 * @param checkSubpaths If true, matches any subpath; if false, exact path only
 * @return Query for matching documents
 */
public static Query jsonExistsQuery(Schema schema, Field field, String jsonPath,
                                     boolean checkSubpaths) {
    if (schema == null || field == null || jsonPath == null) {
        throw new IllegalArgumentException("Arguments cannot be null");
    }

    long queryPtr = nativeCreateJsonExistsQuery(
        schema.getSchemaPtr(),
        field.getFieldId(),
        jsonPath,
        checkSubpaths
    );

    return new Query(queryPtr);
}

// Native methods
private static native long nativeCreateJsonTermQuery(
    long schemaPtr, int fieldId, String jsonPath, String valueString);

private static native long nativeCreateJsonRangeQuery(
    long schemaPtr, int fieldId, String jsonPath,
    String lowerBound, boolean lowerInclusive,
    String upperBound, boolean upperInclusive);

private static native long nativeCreateJsonExistsQuery(
    long schemaPtr, int fieldId, String jsonPath, boolean checkSubpaths);

// Helper method
private static String convertValueToString(Object value) {
    if (value instanceof String) {
        return (String) value;
    } else if (value instanceof Number) {
        return value.toString();
    } else if (value instanceof Boolean) {
        return value.toString();
    } else if (value instanceof java.time.Instant) {
        return ((java.time.Instant) value).toString();
    } else if (value instanceof java.util.Date) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .format((java.util.Date) value);
    } else {
        return value.toString();
    }
}
```

---

## 5. Native/JNI Layer

### 5.1 Schema Creation

**File: `native/src/schema.rs`**

```rust
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong};
use tantivy::schema::{JsonObjectOptions, TextFieldIndexing, IndexRecordOption};

#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_core_SchemaBuilder_nativeAddJsonField(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    name: JString,
    stored: jboolean,
    indexed: jboolean,
    fast: jboolean,
    fast_tokenizer: JString,
    expand_dots: jboolean,
    tokenizer_name: JString,
    record_option: jint,
) -> jint {
    // Get schema builder from pointer
    let schema_builder = unsafe {
        &mut *(schema_ptr as *mut tantivy::schema::SchemaBuilder)
    };

    // Extract field name
    let field_name: String = env.get_string(&name)
        .expect("Failed to get field name")
        .into();

    // Build JsonObjectOptions
    let mut json_options = JsonObjectOptions::default();

    if stored == jni::sys::JNI_TRUE {
        json_options = json_options.set_stored();
    }

    if indexed == jni::sys::JNI_TRUE {
        // Extract tokenizer name
        let tokenizer: String = if !tokenizer_name.is_null() {
            env.get_string(&tokenizer_name)
                .expect("Failed to get tokenizer name")
                .into()
        } else {
            "default".to_string()
        };

        // Convert record option
        let index_option = match record_option {
            0 => IndexRecordOption::Basic,
            1 => IndexRecordOption::WithFreqs,
            2 => IndexRecordOption::WithFreqsAndPositions,
            _ => IndexRecordOption::WithFreqsAndPositions,
        };

        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer(&tokenizer)
            .set_index_option(index_option);

        json_options = json_options.set_indexing_options(text_indexing);
    }

    if fast == jni::sys::JNI_TRUE {
        // Extract fast tokenizer if provided
        let fast_tok: Option<String> = if !fast_tokenizer.is_null() {
            Some(env.get_string(&fast_tokenizer)
                .expect("Failed to get fast tokenizer")
                .into())
        } else {
            None
        };

        json_options = json_options.set_fast(fast_tok);
    }

    if expand_dots == jni::sys::JNI_TRUE {
        json_options = json_options.set_expand_dots_enabled();
    }

    // Add field to schema
    let field = schema_builder.add_json_field(&field_name, json_options);

    field.field_id() as jint
}
```

### 5.2 Document JSON Indexing

**File: `native/src/document.rs`**

```rust
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use tantivy::schema::{Field, OwnedValue};
use serde_json;

#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_core_Document_nativeAddJsonFromString(
    mut env: JNIEnv,
    _class: JClass,
    doc_ptr: jlong,
    field_id: jint,
    json_string: JString,
) {
    // Get document from pointer
    let doc = unsafe {
        &mut *(doc_ptr as *mut tantivy::Document)
    };

    // Extract JSON string
    let json_str: String = env.get_string(&json_string)
        .expect("Failed to get JSON string")
        .into();

    // Parse JSON to serde_json::Value
    let json_value: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid JSON: {}", e)
            ).expect("Failed to throw exception");
            return;
        }
    };

    // Convert to OwnedValue
    let owned_value: OwnedValue = json_value.into();

    // Create field
    let field = Field::from_field_id(field_id as u32);

    // Add to document
    doc.add_object(field, owned_value);
}
```

### 5.3 Query Construction

**File: `native/src/query.rs`**

```rust
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong};
use tantivy::schema::{Field, Schema, Term};
use tantivy::query::{TermQuery, RangeQuery, ExistsQuery, Query};
use tantivy::core::json_utils::convert_to_fast_value_and_get_term;

#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_query_Query_nativeCreateJsonTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_id: jint,
    json_path: JString,
    value_string: JString,
) -> jlong {
    // Get schema
    let schema = unsafe {
        &*(schema_ptr as *const Schema)
    };

    // Extract parameters
    let path: String = env.get_string(&json_path)
        .expect("Failed to get JSON path")
        .into();

    let value: String = env.get_string(&value_string)
        .expect("Failed to get value string")
        .into();

    // Create field
    let field = Field::from_field_id(field_id as u32);

    // Check if expand_dots is enabled
    let field_entry = schema.get_field_entry(field);
    let expand_dots = if let FieldType::JsonObject(json_opts) = field_entry.field_type() {
        json_opts.is_expand_dots_enabled()
    } else {
        false
    };

    // Create term with JSON path
    let term = Term::from_field_json_path(field, &path, expand_dots);

    // Try to infer type and append value
    let term = match convert_to_fast_value_and_get_term(term, &value, false) {
        Some(t) => t,
        None => {
            // Fallback to string
            let mut t = term;
            t.append_type_and_str(&value);
            t
        }
    };

    // Create term query
    let query = TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);

    // Box and return pointer
    Box::into_raw(Box::new(query)) as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_query_Query_nativeCreateJsonRangeQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_id: jint,
    json_path: JString,
    lower_bound: JString,
    lower_inclusive: jboolean,
    upper_bound: JString,
    upper_inclusive: jboolean,
) -> jlong {
    // Get schema
    let schema = unsafe {
        &*(schema_ptr as *const Schema)
    };

    // Extract JSON path
    let path: String = env.get_string(&json_path)
        .expect("Failed to get JSON path")
        .into();

    // Create field
    let field = Field::from_field_id(field_id as u32);

    // Extract bounds
    let lower: Option<String> = if !lower_bound.is_null() {
        Some(env.get_string(&lower_bound)
            .expect("Failed to get lower bound")
            .into())
    } else {
        None
    };

    let upper: Option<String> = if !upper_bound.is_null() {
        Some(env.get_string(&upper_bound)
            .expect("Failed to get upper bound")
            .into())
    } else {
        None
    };

    // Get expand_dots setting
    let field_entry = schema.get_field_entry(field);
    let expand_dots = if let FieldType::JsonObject(json_opts) = field_entry.field_type() {
        json_opts.is_expand_dots_enabled()
    } else {
        false
    };

    // Create base term for path
    let base_term = Term::from_field_json_path(field, &path, expand_dots);

    // Create lower bound term
    let lower_term = if let Some(ref lower_val) = lower {
        convert_to_fast_value_and_get_term(base_term.clone(), lower_val, false)
    } else {
        None
    };

    // Create upper bound term
    let upper_term = if let Some(ref upper_val) = upper {
        convert_to_fast_value_and_get_term(base_term.clone(), upper_val, false)
    } else {
        None
    };

    // Create range query
    let query = RangeQuery::new_term_bounds(
        field,
        &Type::Json,
        &lower_term,
        &upper_term,
        lower_inclusive == jni::sys::JNI_TRUE,
        upper_inclusive == jni::sys::JNI_TRUE,
    );

    // Box and return pointer
    Box::into_raw(Box::new(query)) as jlong
}

#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_query_Query_nativeCreateJsonExistsQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_id: jint,
    json_path: JString,
    check_subpaths: jboolean,
) -> jlong {
    // Get schema
    let _schema = unsafe {
        &*(schema_ptr as *const Schema)
    };

    // Extract JSON path
    let path: String = env.get_string(&json_path)
        .expect("Failed to get JSON path")
        .into();

    // Create field
    let field = Field::from_field_id(field_id as u32);

    // Create exists query
    let query = ExistsQuery::new_json_exists_query(
        field,
        path,
        check_subpaths == jni::sys::JNI_TRUE,
    );

    // Box and return pointer
    Box::into_raw(Box::new(query)) as jlong
}
```

---

## 6. Usage Examples

### 6.1 Basic JSON Field

```java
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;

public class BasicJsonExample {
    public static void main(String[] args) {
        // Create schema with JSON field
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        // Create index
        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2);

        // Add document with nested JSON
        Document doc = new Document();
        doc.addJson(jsonField, "{\"user\": {\"name\": \"John\", \"age\": 30}}");
        writer.addDocument(doc);
        writer.commit();

        // Query nested field
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "user.name", "John");
        SearchResult results = searcher.search(query, 10);

        System.out.println("Found " + results.getHits().size() + " documents");

        // Retrieve and display
        for (var hit : results.getHits()) {
            try (Document resultDoc = searcher.doc(hit.getDocAddress())) {
                String json = (String) resultDoc.getFirst(jsonField);
                System.out.println("Document: " + json);
            }
        }
    }
}
```

### 6.2 E-commerce Product Attributes

```java
import java.util.*;

public class EcommerceExample {
    public static void main(String[] args) {
        // Schema with flexible product attributes
        SchemaBuilder builder = new SchemaBuilder();
        Field idField = builder.addIntegerField("id", true, true, false);
        Field attributesField = builder.addJsonField("attributes",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setFast(true)
                .setExpandDots(true));
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2);

        // Add products with different attributes
        Document product1 = new Document();
        product1.addInt(idField, 1);

        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("brand", "Apple");
        attrs1.put("category", "Electronics");
        attrs1.put("price", 999.99);
        attrs1.put("specs", Map.of(
            "ram", "16GB",
            "storage", "512GB",
            "screen", "14 inch"
        ));
        product1.addJson(attributesField, attrs1);
        writer.addDocument(product1);

        Document product2 = new Document();
        product2.addInt(idField, 2);

        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("brand", "Dell");
        attrs2.put("category", "Electronics");
        attrs2.put("price", 799.99);
        attrs2.put("specs", Map.of(
            "ram", "8GB",
            "storage", "256GB",
            "screen", "13 inch"
        ));
        product2.addJson(attributesField, attrs2);
        writer.addDocument(product2);

        writer.commit();

        // Query by brand
        Searcher searcher = index.reader().searcher();
        Query brandQuery = Query.jsonTermQuery(schema, attributesField, "brand", "Apple");
        SearchResult brandResults = searcher.search(brandQuery, 10);
        System.out.println("Apple products: " + brandResults.getHits().size());

        // Query by nested spec
        Query storageQuery = Query.jsonTermQuery(schema, attributesField,
                                                  "specs.storage", "512GB");
        SearchResult storageResults = searcher.search(storageQuery, 10);
        System.out.println("512GB storage: " + storageResults.getHits().size());

        // Range query on price
        Query priceQuery = Query.jsonRangeQuery(schema, attributesField, "price",
            800.0, true, 1000.0, true);
        SearchResult priceResults = searcher.search(priceQuery, 10);
        System.out.println("Price 800-1000: " + priceResults.getHits().size());
    }
}
```

### 6.3 Log Analytics with Kubernetes Labels

```java
public class LogAnalyticsExample {
    public static void main(String[] args) {
        // Schema for semi-structured logs
        SchemaBuilder builder = new SchemaBuilder();
        Field timestampField = builder.addDateField("timestamp", true, true, false);
        Field eventField = builder.addJsonField("event",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.defaultIndexing())
                .setFast(true)
                .setExpandDots(false));  // Don't expand dots for k8s labels
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2);

        // Add log entry with k8s labels
        Document log = new Document();
        log.addDate(timestampField, java.time.Instant.now());

        String logJson = """
        {
          "level": "ERROR",
          "k8s.pod.name": "api-server-123",
          "k8s.namespace": "production",
          "k8s.node.id": "node-5",
          "message": "Connection timeout to database",
          "duration_ms": 5000
        }
        """;
        log.addJson(eventField, logJson);
        writer.addDocument(log);
        writer.commit();

        // Query with escaped dots (expand_dots=false)
        Searcher searcher = index.reader().searcher();
        Query podQuery = Query.jsonTermQuery(schema, eventField,
                                              "k8s\\.pod\\.name", "api-server-123");
        SearchResult results = searcher.search(podQuery, 10);

        System.out.println("Found " + results.getHits().size() + " log entries");

        // Range query on duration
        Query slowQuery = Query.jsonRangeQuery(schema, eventField, "duration_ms",
            1000, true, null, false);
        SearchResult slowResults = searcher.search(slowQuery, 10);
        System.out.println("Slow requests (>1s): " + slowResults.getHits().size());

        // Exists query - find logs with k8s.namespace
        Query hasNamespace = Query.jsonExistsQuery(schema, eventField,
                                                    "k8s\\.namespace", false);
        SearchResult nsResults = searcher.search(hasNamespace, 10);
        System.out.println("Logs with namespace: " + nsResults.getHits().size());
    }
}
```

### 6.4 Quickwit Split with JSON Fields

```java
import io.indextables.tantivy4java.split.*;

public class QuickwitJsonExample {
    public static void main(String[] args) throws Exception {
        // Create index with JSON field
        SchemaBuilder builder = new SchemaBuilder();
        Field dataField = builder.addJsonField("data",
            JsonObjectOptions.full());  // All features enabled
        Schema schema = builder.build();

        Index index = Index.createFromTempDir(schema);
        IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4);

        // Add documents
        for (int i = 0; i < 1000; i++) {
            Document doc = new Document();
            Map<String, Object> data = Map.of(
                "id", i,
                "category", "cat-" + (i % 10),
                "metrics", Map.of(
                    "views", i * 100,
                    "clicks", i * 10
                )
            );
            doc.addJson(dataField, data);
            writer.addDocument(doc);
        }
        writer.commit();

        // Convert to Quickwit split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "my-index", "my-source", "node-1");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            index.getIndexPath(), "/tmp/data.split", config);

        System.out.println("Split ID: " + metadata.getSplitId());
        System.out.println("Documents: " + metadata.getNumDocs());

        // Query split with JSON path
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("test-cache");

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher =
                    cacheManager.createSplitSearcher("file:///tmp/data.split")) {

                // Query nested field in split
                SplitQuery query = new SplitTermQuery("data.metrics.views", "5000");
                SearchResult results = searcher.search(query, 10);

                System.out.println("Found " + results.getHits().size() + " in split");
            }
        }
    }
}
```

---

## 7. Complete Unit Test Suite

### 7.1 Schema and Indexing Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonFieldSchemaTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSON field schema creation and configuration.
 */
public class JsonFieldSchemaTest {

    @Test
    public void testBasicJsonFieldCreation() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data", JsonObjectOptions.stored());
        Schema schema = builder.build();

        assertNotNull(jsonField);
        assertTrue(schema.hasField("data"));
    }

    @Test
    public void testJsonFieldWithIndexing() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.defaultIndexing()));
        Schema schema = builder.build();

        assertNotNull(jsonField);
    }

    @Test
    public void testJsonFieldWithFastFields() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setFast(true));
        Schema schema = builder.build();

        assertNotNull(jsonField);
    }

    @Test
    public void testJsonFieldWithExpandDots() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.defaultIndexing())
                .setExpandDots(true));
        Schema schema = builder.build();

        assertNotNull(jsonField);
    }

    @Test
    public void testJsonFieldFullConfiguration() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        assertNotNull(jsonField);
        assertTrue(schema.hasField("data"));
    }

    @Test
    public void testMultipleJsonFields() {
        SchemaBuilder builder = new SchemaBuilder();
        Field field1 = builder.addJsonField("attributes", JsonObjectOptions.stored());
        Field field2 = builder.addJsonField("metadata", JsonObjectOptions.indexed());
        Field field3 = builder.addJsonField("config", JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        assertNotNull(field1);
        assertNotNull(field2);
        assertNotNull(field3);
        assertTrue(schema.hasField("attributes"));
        assertTrue(schema.hasField("metadata"));
        assertTrue(schema.hasField("config"));
    }

    @Test
    public void testJsonFieldWithCustomTokenizer() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.create()
                    .withTokenizer("raw")
                    .withPositions())
                .setFastTokenizer("raw"));
        Schema schema = builder.build();

        assertNotNull(jsonField);
    }
}
```

### 7.2 Document Indexing Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonFieldIndexingTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSON document indexing functionality.
 */
public class JsonFieldIndexingTest {

    @Test
    public void testAddJsonFromString() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        doc.addJson(jsonField, "{\"name\": \"John\", \"age\": 30}");
        writer.addDocument(doc);
        writer.commit();

        assertEquals(1, index.reader().searcher().numDocs());
    }

    @Test
    public void testAddJsonFromMap() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        Map<String, Object> data = new HashMap<>();
        data.put("name", "Jane");
        data.put("age", 25);
        doc.addJson(jsonField, data);
        writer.addDocument(doc);
        writer.commit();

        assertEquals(1, index.reader().searcher().numDocs());
    }

    @Test
    public void testNestedJsonObject() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        Map<String, Object> data = new HashMap<>();
        data.put("user", Map.of(
            "name", "Alice",
            "profile", Map.of(
                "age", 28,
                "city", "NYC"
            )
        ));
        doc.addJson(jsonField, data);
        writer.addDocument(doc);
        writer.commit();

        assertEquals(1, index.reader().searcher().numDocs());
    }

    @Test
    public void testJsonArrayValues() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        Map<String, Object> data = new HashMap<>();
        data.put("tags", List.of("java", "search", "tantivy"));
        data.put("scores", List.of(10, 20, 30));
        doc.addJson(jsonField, data);
        writer.addDocument(doc);
        writer.commit();

        assertEquals(1, index.reader().searcher().numDocs());
    }

    @Test
    public void testMixedTypeValues() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        Map<String, Object> data = new HashMap<>();
        data.put("string_field", "text");
        data.put("int_field", 42);
        data.put("float_field", 3.14);
        data.put("bool_field", true);
        data.put("date_field", java.time.Instant.parse("2025-01-29T10:30:00Z"));
        doc.addJson(jsonField, data);
        writer.addDocument(doc);
        writer.commit();

        assertEquals(1, index.reader().searcher().numDocs());
    }

    @Test
    public void testInvalidJsonString() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        assertThrows(IllegalArgumentException.class, () -> {
            doc.addJson(jsonField, "{invalid json}");
        });
    }

    @Test
    public void testMultipleDocumentsWithJson() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            Map<String, Object> data = Map.of(
                "id", i,
                "value", "item-" + i
            );
            doc.addJson(jsonField, data);
            writer.addDocument(doc);
        }
        writer.commit();

        assertEquals(100, index.reader().searcher().numDocs());
    }
}
```

### 7.3 JSONPath Query Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonPathQueryTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSONPath query functionality.
 */
public class JsonPathQueryTest {

    private Schema schema;
    private Field jsonField;
    private Index index;

    @BeforeEach
    public void setup() {
        SchemaBuilder builder = new SchemaBuilder();
        jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setFast(true)
                .setExpandDots(true));
        schema = builder.build();

        index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add test documents
        Document doc1 = new Document();
        doc1.addJson(jsonField, Map.of(
            "user", Map.of("name", "Alice", "age", 30),
            "category", "admin"
        ));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.addJson(jsonField, Map.of(
            "user", Map.of("name", "Bob", "age", 25),
            "category", "user"
        ));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.addJson(jsonField, Map.of(
            "user", Map.of("name", "Charlie", "age", 35),
            "category", "admin"
        ));
        writer.addDocument(doc3);

        writer.commit();
    }

    @Test
    public void testSimplePathQuery() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "category", "admin");
        SearchResult results = searcher.search(query, 10);

        assertEquals(2, results.getHits().size(),
            "Should find 2 admin users");
    }

    @Test
    public void testNestedPathQuery() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "user.name", "Alice");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find Alice");
    }

    @Test
    public void testDeepNestedPathQuery() {
        // Add document with deep nesting
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addJson(jsonField, Map.of(
            "level1", Map.of(
                "level2", Map.of(
                    "level3", Map.of(
                        "value", "deep"
                    )
                )
            )
        ));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "level1.level2.level3.value", "deep");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find deeply nested value");
    }

    @Test
    public void testNumericPathQuery() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "user.age", 30);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find user with age 30");
    }

    @Test
    public void testBooleanPathQuery() {
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addJson(jsonField, Map.of("active", true, "verified", false));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "active", true);
        SearchResult results = searcher.search(query, 10);

        assertTrue(results.getHits().size() >= 1,
            "Should find active documents");
    }

    @Test
    public void testArrayElementQuery() {
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addJson(jsonField, Map.of(
            "tags", List.of("java", "search", "tantivy")
        ));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField, "tags", "java");
        SearchResult results = searcher.search(query, 10);

        assertTrue(results.getHits().size() >= 1,
            "Should find documents with 'java' tag");
    }

    @Test
    public void testNonExistentPath() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "nonexistent.path", "value");
        SearchResult results = searcher.search(query, 10);

        assertEquals(0, results.getHits().size(),
            "Should not find any documents");
    }
}
```

### 7.4 JSON Range Query Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonRangeQueryTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSON range query functionality.
 */
public class JsonRangeQueryTest {

    private Schema schema;
    private Field jsonField;
    private Index index;

    @BeforeEach
    public void setup() {
        SchemaBuilder builder = new SchemaBuilder();
        jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setFast(true)
                .setExpandDots(true));
        schema = builder.build();

        index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add test documents with numeric values
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.addJson(jsonField, Map.of(
                "id", i,
                "score", i * 10.5,
                "metrics", Map.of(
                    "views", i * 100,
                    "clicks", i * 10
                )
            ));
            writer.addDocument(doc);
        }

        writer.commit();
    }

    @Test
    public void testIntegerRangeInclusive() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "id",
            3, true, 7, true);
        SearchResult results = searcher.search(query, 20);

        assertEquals(5, results.getHits().size(),
            "Should find 5 documents (3, 4, 5, 6, 7)");
    }

    @Test
    public void testIntegerRangeExclusive() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "id",
            3, false, 7, false);
        SearchResult results = searcher.search(query, 20);

        assertEquals(3, results.getHits().size(),
            "Should find 3 documents (4, 5, 6)");
    }

    @Test
    public void testFloatRange() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "score",
            30.0, true, 60.0, true);
        SearchResult results = searcher.search(query, 20);

        assertTrue(results.getHits().size() >= 3,
            "Should find multiple documents in score range");
    }

    @Test
    public void testNestedFieldRange() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "metrics.views",
            200, true, 600, true);
        SearchResult results = searcher.search(query, 20);

        assertTrue(results.getHits().size() >= 4,
            "Should find documents with views in range");
    }

    @Test
    public void testUnboundedLowerRange() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "id",
            null, false, 5, true);
        SearchResult results = searcher.search(query, 20);

        assertEquals(6, results.getHits().size(),
            "Should find documents with id <= 5");
    }

    @Test
    public void testUnboundedUpperRange() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "id",
            5, true, null, false);
        SearchResult results = searcher.search(query, 20);

        assertEquals(5, results.getHits().size(),
            "Should find documents with id >= 5");
    }

    @Test
    public void testDateRange() {
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add documents with dates
        for (int i = 0; i < 5; i++) {
            Document doc = new Document();
            doc.addJson(jsonField, Map.of(
                "created", java.time.Instant.parse("2025-01-" +
                    String.format("%02d", i + 10) + "T10:00:00Z")
            ));
            writer.addDocument(doc);
        }
        writer.commit();

        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonRangeQuery(schema, jsonField, "created",
            java.time.Instant.parse("2025-01-11T00:00:00Z"), true,
            java.time.Instant.parse("2025-01-13T23:59:59Z"), true);
        SearchResult results = searcher.search(query, 20);

        assertTrue(results.getHits().size() >= 3,
            "Should find documents in date range");
    }
}
```

### 7.5 JSON Exists Query Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonExistsQueryTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSON exists query functionality.
 */
public class JsonExistsQueryTest {

    private Schema schema;
    private Field jsonField;
    private Index index;

    @BeforeEach
    public void setup() {
        SchemaBuilder builder = new SchemaBuilder();
        jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setExpandDots(true));
        schema = builder.build();

        index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add documents with varying structure
        Document doc1 = new Document();
        doc1.addJson(jsonField, Map.of(
            "user", Map.of("name", "Alice", "email", "alice@example.com"),
            "verified", true
        ));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.addJson(jsonField, Map.of(
            "user", Map.of("name", "Bob"),
            "verified", false
        ));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.addJson(jsonField, Map.of(
            "guest", Map.of("id", "guest-123")
        ));
        writer.addDocument(doc3);

        writer.commit();
    }

    @Test
    public void testExistsExactPath() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonExistsQuery(schema, jsonField, "user.email", false);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find 1 document with user.email");
    }

    @Test
    public void testExistsWithSubpaths() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonExistsQuery(schema, jsonField, "user", true);
        SearchResult results = searcher.search(query, 10);

        assertEquals(2, results.getHits().size(),
            "Should find 2 documents with any user subpath");
    }

    @Test
    public void testExistsTopLevelField() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonExistsQuery(schema, jsonField, "verified", false);
        SearchResult results = searcher.search(query, 10);

        assertEquals(2, results.getHits().size(),
            "Should find 2 documents with verified field");
    }

    @Test
    public void testNotExistsPath() {
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonExistsQuery(schema, jsonField,
            "user.phone", false);
        SearchResult results = searcher.search(query, 10);

        assertEquals(0, results.getHits().size(),
            "Should not find any documents with user.phone");
    }

    @Test
    public void testExistsDeepPath() {
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addJson(jsonField, Map.of(
            "a", Map.of(
                "b", Map.of(
                    "c", Map.of(
                        "d", "value"
                    )
                )
            )
        ));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonExistsQuery(schema, jsonField,
            "a.b.c.d", false);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find document with deep nested path");
    }
}
```

### 7.6 Expand Dots Configuration Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonExpandDotsTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test expand_dots configuration behavior.
 */
public class JsonExpandDotsTest {

    @Test
    public void testExpandDotsEnabled() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setExpandDots(true));  // Enabled
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add document with dots in key
        Document doc = new Document();
        doc.addJson(jsonField, Map.of("k8s.pod.name", "api-server-123"));
        writer.addDocument(doc);
        writer.commit();

        // Query with natural path syntax (dots NOT escaped)
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "k8s.pod.name", "api-server-123");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find document with expand_dots=true using natural syntax");
    }

    @Test
    public void testExpandDotsDisabled() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setExpandDots(false));  // Disabled
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add document with dots in key
        Document doc = new Document();
        doc.addJson(jsonField, Map.of("k8s.pod.name", "api-server-123"));
        writer.addDocument(doc);
        writer.commit();

        // Query with escaped dots
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "k8s\\.pod\\.name", "api-server-123");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find document with expand_dots=false using escaped syntax");
    }

    @Test
    public void testExpandDotsWithNestedObjects() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions())
                .setExpandDots(true));
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add document with nested structure
        Document doc = new Document();
        doc.addJson(jsonField, Map.of(
            "kubernetes", Map.of(
                "pod", Map.of(
                    "name", "api-server"
                )
            )
        ));
        writer.addDocument(doc);
        writer.commit();

        // Query with dot notation
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "kubernetes.pod.name", "api-server");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should find document with nested object structure");
    }
}
```

### 7.7 Type Inference Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonTypeInferenceTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test automatic type inference for JSON query values.
 */
public class JsonTypeInferenceTest {

    private Schema schema;
    private Field jsonField;
    private Index index;

    @BeforeEach
    public void setup() {
        SchemaBuilder builder = new SchemaBuilder();
        jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        schema = builder.build();

        index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        doc.addJson(jsonField, Map.of(
            "string_val", "hello",
            "int_val", 42,
            "float_val", 3.14,
            "bool_val", true,
            "date_val", java.time.Instant.parse("2025-01-29T10:00:00Z")
        ));
        writer.addDocument(doc);
        writer.commit();
    }

    @Test
    public void testStringTypeInference() {
        Searcher searcher = index.reader().searcher();

        // Query with string value
        Query query = Query.jsonTermQuery(schema, jsonField,
            "string_val", "hello");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should infer string type correctly");
    }

    @Test
    public void testIntegerTypeInference() {
        Searcher searcher = index.reader().searcher();

        // Query with integer value
        Query query = Query.jsonTermQuery(schema, jsonField,
            "int_val", 42);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should infer integer type correctly");
    }

    @Test
    public void testFloatTypeInference() {
        Searcher searcher = index.reader().searcher();

        // Query with float value
        Query query = Query.jsonTermQuery(schema, jsonField,
            "float_val", 3.14);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should infer float type correctly");
    }

    @Test
    public void testBooleanTypeInference() {
        Searcher searcher = index.reader().searcher();

        // Query with boolean value
        Query query = Query.jsonTermQuery(schema, jsonField,
            "bool_val", true);
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should infer boolean type correctly");
    }

    @Test
    public void testDateTypeInference() {
        Searcher searcher = index.reader().searcher();

        // Query with date value
        Query query = Query.jsonTermQuery(schema, jsonField,
            "date_val", java.time.Instant.parse("2025-01-29T10:00:00Z"));
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should infer date type correctly");
    }
}
```

### 7.8 Quickwit Split Integration Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonQuickwitSplitTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test JSON fields in Quickwit splits.
 */
public class JsonQuickwitSplitTest {

    @TempDir
    Path tempDir;

    @Test
    public void testJsonFieldInSplit() throws Exception {
        // Create index with JSON field
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        String indexPath = tempDir.resolve("index").toString();
        Index index = Index.createFromTempDir(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add documents
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.addJson(jsonField, Map.of(
                "id", i,
                "category", "cat-" + (i % 5),
                "data", Map.of(
                    "value", i * 10,
                    "label", "item-" + i
                )
            ));
            writer.addDocument(doc);
        }
        writer.commit();

        // Convert to split
        String splitPath = tempDir.resolve("test.split").toString();
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "json-index", "json-source", "node-1");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            index.getIndexPath(), splitPath, config);

        assertEquals(100, metadata.getNumDocs(),
            "Split should contain 100 documents");

        // Query split with JSON path
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("json-test-cache");

        try (SplitCacheManager cacheManager =
                SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher =
                    cacheManager.createSplitSearcher("file://" + splitPath)) {

                // Query simple path
                SplitQuery query1 = new SplitTermQuery("data.category", "cat-2");
                SearchResult results1 = searcher.search(query1, 50);
                assertEquals(20, results1.getHits().size(),
                    "Should find 20 documents in category cat-2");

                // Query nested path
                SplitQuery query2 = new SplitTermQuery("data.data.label", "item-42");
                SearchResult results2 = searcher.search(query2, 10);
                assertEquals(1, results2.getHits().size(),
                    "Should find 1 document with label item-42");
            }
        }
    }

    @Test
    public void testMergeSplitsWithJsonFields() throws Exception {
        // Create two indices with JSON fields
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        // First index
        Index index1 = Index.createFromTempDir(schema);
        IndexWriter writer1 = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        for (int i = 0; i < 50; i++) {
            Document doc = new Document();
            doc.addJson(jsonField, Map.of("id", i, "source", "index1"));
            writer1.addDocument(doc);
        }
        writer1.commit();

        String split1Path = tempDir.resolve("split1.split").toString();
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "merge-test", "source-1", "node-1");
        QuickwitSplit.convertIndexFromPath(index1.getIndexPath(), split1Path, config);

        // Second index
        Index index2 = Index.createFromTempDir(schema);
        IndexWriter writer2 = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        for (int i = 50; i < 100; i++) {
            Document doc = new Document();
            doc.addJson(jsonField, Map.of("id", i, "source", "index2"));
            writer2.addDocument(doc);
        }
        writer2.commit();

        String split2Path = tempDir.resolve("split2.split").toString();
        QuickwitSplit.convertIndexFromPath(index2.getIndexPath(), split2Path, config);

        // Merge splits
        String mergedPath = tempDir.resolve("merged.split").toString();
        List<String> splits = Arrays.asList(split1Path, split2Path);

        QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
            splits, mergedPath, config);

        assertEquals(100, merged.getNumDocs(),
            "Merged split should contain 100 documents");

        // Query merged split
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("merge-cache");

        try (SplitCacheManager cacheManager =
                SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher =
                    cacheManager.createSplitSearcher("file://" + mergedPath)) {

                // Verify all documents present
                SplitQuery query1 = new SplitTermQuery("data.source", "index1");
                SearchResult results1 = searcher.search(query1, 100);
                assertEquals(50, results1.getHits().size(),
                    "Should find 50 documents from index1");

                SplitQuery query2 = new SplitTermQuery("data.source", "index2");
                SearchResult results2 = searcher.search(query2, 100);
                assertEquals(50, results2.getHits().size(),
                    "Should find 50 documents from index2");
            }
        }
    }
}
```

### 7.9 Python Parity Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonPythonParityTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Python tantivy-py JSON functionality parity.
 *
 * Based on tantivy-py tests:
 * - tests/tantivy_test.py::test_query_from_json_field
 * - tests/test_json_bug.py
 */
public class JsonPythonParityTest {

    @Test
    public void testQueryFromJsonField() {
        // Port of tantivy-py test_query_from_json_field
        SchemaBuilder builder = new SchemaBuilder();
        Field attributesField = builder.addJsonField("attributes",
            JsonObjectOptions.create()
                .setStored(true)
                .setIndexing(TextFieldIndexing.withPositions()));
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add documents matching Python test
        Document doc1 = new Document();
        doc1.addJson(attributesField, Map.of(
            "order", 1.1,
            "target", "submit-button",
            "cart", Map.of("product_id", 133),
            "description", "das keyboard"
        ));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.addJson(attributesField, Map.of(
            "order", 1.2,
            "target", "submit-button",
            "cart", Map.of("product_id", 134)
        ));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.addJson(attributesField, Map.of(
            "order", 1.3,
            "target", "submit-query",
            "cart", Map.of("product_id", 135)
        ));
        writer.addDocument(doc3);

        writer.commit();

        Searcher searcher = index.reader().searcher();

        // Test: target:submit-button
        Query query1 = Query.jsonTermQuery(schema, attributesField,
            "target", "submit-button");
        SearchResult results1 = searcher.search(query1, 10);
        assertEquals(2, results1.getHits().size(),
            "Python test expects 2 results for target:submit-button");

        // Test: cart.product_id:134
        Query query2 = Query.jsonTermQuery(schema, attributesField,
            "cart.product_id", 134);
        SearchResult results2 = searcher.search(query2, 10);
        assertEquals(1, results2.getHits().size(),
            "Python test expects 1 result for cart.product_id:134");

        // Test: order:1.1
        Query query3 = Query.jsonTermQuery(schema, attributesField,
            "order", 1.1);
        SearchResult results3 = searcher.search(query3, 10);
        assertEquals(1, results3.getHits().size(),
            "Python test expects 1 result for order:1.1");

        // Test: description query
        Query query4 = Query.jsonTermQuery(schema, attributesField,
            "description", "keyboard");
        SearchResult results4 = searcher.search(query4, 10);
        assertEquals(1, results4.getHits().size(),
            "Python test expects 1 result for description:keyboard");
    }

    @Test
    public void testJsonFieldArrayHandling() {
        // Test array handling to match Python behavior
        SchemaBuilder builder = new SchemaBuilder();
        Field dataField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        doc.addJson(dataField, Map.of(
            "tags", List.of("rust", "search", "tantivy"),
            "scores", List.of(10, 20, 30)
        ));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();

        // Query array element
        Query query1 = Query.jsonTermQuery(schema, dataField, "tags", "search");
        SearchResult results1 = searcher.search(query1, 10);
        assertEquals(1, results1.getHits().size(),
            "Should find document with 'search' in tags array");

        Query query2 = Query.jsonTermQuery(schema, dataField, "scores", 20);
        SearchResult results2 = searcher.search(query2, 10);
        assertEquals(1, results2.getHits().size(),
            "Should find document with 20 in scores array");
    }

    @Test
    public void testJsonFieldMixedTypes() {
        // Verify all JSON types work like Python
        SchemaBuilder builder = new SchemaBuilder();
        Field dataField = builder.addJsonField("data",
            JsonObjectOptions.storedAndIndexed());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        Document doc = new Document();
        doc.addJson(dataField, Map.of(
            "str_field", "text",
            "int_field", 42,
            "float_field", 3.14,
            "bool_field", true,
            "null_field", (Object) null,
            "nested", Map.of("key", "value")
        ));
        writer.addDocument(doc);
        writer.commit();

        Searcher searcher = index.reader().searcher();

        // Verify each type is queryable
        assertEquals(1, searcher.search(
            Query.jsonTermQuery(schema, dataField, "str_field", "text"),
            10).getHits().size());

        assertEquals(1, searcher.search(
            Query.jsonTermQuery(schema, dataField, "int_field", 42),
            10).getHits().size());

        assertEquals(1, searcher.search(
            Query.jsonTermQuery(schema, dataField, "float_field", 3.14),
            10).getHits().size());

        assertEquals(1, searcher.search(
            Query.jsonTermQuery(schema, dataField, "bool_field", true),
            10).getHits().size());

        assertEquals(1, searcher.search(
            Query.jsonTermQuery(schema, dataField, "nested.key", "value"),
            10).getHits().size());
    }
}
```

### 7.10 Performance and Stress Tests

**File: `src/test/java/io/indextables/tantivy4java/JsonPerformanceTest.java`**

```java
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance and stress tests for JSON fields.
 */
public class JsonPerformanceTest {

    @Test
    public void testLargeJsonDocuments() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4);

        // Index 10,000 documents with nested JSON
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            Document doc = new Document();
            Map<String, Object> data = new HashMap<>();
            data.put("id", i);
            data.put("category", "cat-" + (i % 100));
            data.put("nested", Map.of(
                "level1", Map.of(
                    "level2", Map.of(
                        "value", i * 1.5
                    )
                )
            ));
            doc.addJson(jsonField, data);
            writer.addDocument(doc);
        }
        writer.commit();

        long indexTime = System.currentTimeMillis() - startTime;
        System.out.println("Indexed 10,000 JSON documents in " + indexTime + "ms");

        // Query performance
        Searcher searcher = index.reader().searcher();
        startTime = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            Query query = Query.jsonTermQuery(schema, jsonField,
                "category", "cat-" + i);
            SearchResult results = searcher.search(query, 200);
            assertEquals(100, results.getHits().size());
        }

        long queryTime = System.currentTimeMillis() - startTime;
        System.out.println("Executed 100 queries in " + queryTime + "ms");

        assertTrue(indexTime < 30000,
            "Indexing should complete within 30 seconds");
        assertTrue(queryTime < 5000,
            "Queries should complete within 5 seconds");
    }

    @Test
    public void testDeepNestingLevels() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Create deeply nested structure (10 levels)
        Map<String, Object> current = new HashMap<>();
        current.put("value", "deep");

        for (int i = 0; i < 10; i++) {
            Map<String, Object> wrapper = new HashMap<>();
            wrapper.put("level" + i, current);
            current = wrapper;
        }

        Document doc = new Document();
        doc.addJson(jsonField, current);
        writer.addDocument(doc);
        writer.commit();

        // Query deep path
        Searcher searcher = index.reader().searcher();
        Query query = Query.jsonTermQuery(schema, jsonField,
            "level0.level1.level2.level3.level4.level5.level6.level7.level8.level9.value",
            "deep");
        SearchResult results = searcher.search(query, 10);

        assertEquals(1, results.getHits().size(),
            "Should handle deeply nested structures");
    }

    @Test
    public void testManyFieldsInJson() {
        SchemaBuilder builder = new SchemaBuilder();
        Field jsonField = builder.addJsonField("data",
            JsonObjectOptions.full());
        Schema schema = builder.build();

        Index index = Index.createInRam(schema);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Create JSON with 1000 fields
        Map<String, Object> data = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            data.put("field_" + i, "value_" + i);
        }

        Document doc = new Document();
        doc.addJson(jsonField, data);
        writer.addDocument(doc);
        writer.commit();

        // Query various fields
        Searcher searcher = index.reader().searcher();
        for (int i = 0; i < 100; i += 10) {
            Query query = Query.jsonTermQuery(schema, jsonField,
                "field_" + i, "value_" + i);
            SearchResult results = searcher.search(query, 10);
            assertEquals(1, results.getHits().size());
        }
    }
}
```

---

## 8. Performance Considerations

### 8.1 Indexing Performance

**Factors Affecting Indexing Speed:**

1. **JSON Parsing Overhead**
   - Minimal (uses serde_json, native Rust)
   - Parsing time: ~1-5µs per document for typical JSON

2. **Path Explosion**
   - Each nested field creates separate inverted index entries
   - 100 fields × 10k docs = 1M index entries
   - **Recommendation**: Limit nesting depth to 5 levels

3. **Text Tokenization**
   - String values tokenized according to analyzer
   - Default tokenizer: ~100-500 tokens/sec per field
   - **Optimization**: Use "raw" tokenizer for exact-match-only fields

4. **Fast Fields**
   - Enables columnar storage for all nested values
   - Trade-off: 20-30% more disk space, 3-5x faster filtering
   - **Recommendation**: Enable for frequently filtered fields

**Best Practices:**

```java
// Good: Reasonable nesting, selective fast fields
JsonObjectOptions good = JsonObjectOptions.create()
    .setStored(true)
    .setIndexing(TextFieldIndexing.defaultIndexing())
    .setFast(true)      // Enable for filtering
    .setExpandDots(true);

// Avoid: Too many features for rarely-used fields
JsonObjectOptions overkill = JsonObjectOptions.full()
    .setFastTokenizer("default");  // Unnecessary for most cases
```

### 8.2 Query Performance

**Query Type Performance:**

| Query Type | Performance | Use Case |
|------------|-------------|----------|
| **Term Query** | Fast (1-5ms) | Exact value matching |
| **Range Query** | Fast with fast fields (2-10ms) | Numeric/date filtering |
| **Exists Query** | Fast (1-3ms) | Field presence checks |
| **Text Search** | Medium (5-50ms) | Full-text within JSON strings |

**Optimization Tips:**

1. **Use Fast Fields for Filtering**
   ```java
   // Fast: Uses columnar storage
   JsonObjectOptions.create().setFast(true);
   ```

2. **Avoid Wildcard JSON Paths**
   ```java
   // Slow: Scans all paths
   Query.jsonTermQuery(schema, field, "*.name", "value");

   // Fast: Specific path
   Query.jsonTermQuery(schema, field, "user.name", "value");
   ```

3. **Type Inference Caching**
   - Type inference happens once per unique query
   - Reusing query objects improves performance

### 8.3 Storage Efficiency

**Storage Breakdown:**

```
Total Storage = Stored JSON + Inverted Index + Fast Fields

Example (1M documents, 10 fields avg):
- Stored JSON:      500MB (compressed)
- Inverted Index:   200MB (postings + dictionary)
- Fast Fields:      150MB (columnar)
Total:              850MB
```

**Storage Trade-offs:**

| Configuration | Disk Usage | Query Speed | Retrieval Speed |
|---------------|------------|-------------|-----------------|
| `stored=true, indexed=false, fast=false` | 1x | N/A | Fast |
| `stored=true, indexed=true, fast=false` | 1.5x | Medium | Fast |
| `stored=true, indexed=true, fast=true` | 2x | Fast | Fast |
| `stored=false, indexed=true, fast=true` | 1.5x | Fast | N/A |

**Compression Benefits:**

- JSON stored fields: 3-5x compression ratio
- Fast fields: 2-10x compression (depending on cardinality)
- Inverted index: 10-50x compression (high term repetition)

---

## 9. Implementation Phases

### Phase 1: Core Infrastructure (Week 1)

**Deliverables:**
- ✅ `JsonObjectOptions` Java class
- ✅ `SchemaBuilder.addJsonField()` implementation
- ✅ Native JNI binding for schema creation
- ✅ Unit tests: `JsonFieldSchemaTest`

**Acceptance Criteria:**
- Can create schema with JSON field
- All configuration options work correctly
- Tests pass with 100% coverage

### Phase 2: Document Support (Week 2)

**Deliverables:**
- ✅ `Document.addJson(Field, String)` implementation
- ✅ `Document.addJson(Field, Object)` with Gson serialization
- ✅ Native JNI binding for document JSON fields
- ✅ Unit tests: `JsonFieldIndexingTest`

**Acceptance Criteria:**
- Can index JSON from string and objects
- Nested objects and arrays work correctly
- Invalid JSON throws proper exceptions
- Tests pass with nested structures

### Phase 3: Query Support (Week 3)

**Deliverables:**
- ✅ `Query.jsonTermQuery()` implementation
- ✅ JSON path parsing utilities
- ✅ Type inference logic
- ✅ Native JNI bindings for JSON queries
- ✅ Unit tests: `JsonPathQueryTest`, `JsonTypeInferenceTest`

**Acceptance Criteria:**
- Can query simple and nested paths
- Type inference works for all types
- Queries return correct results
- Tests pass for all query patterns

### Phase 4: Advanced Features (Week 4)

**Deliverables:**
- ✅ `Query.jsonRangeQuery()` implementation
- ✅ `Query.jsonExistsQuery()` implementation
- ✅ Expand dots configuration support
- ✅ Unit tests: `JsonRangeQueryTest`, `JsonExistsQueryTest`, `JsonExpandDotsTest`

**Acceptance Criteria:**
- Range queries work on numeric/date fields
- Exists queries check field presence
- Expand dots configuration behaves correctly
- Tests pass for all advanced features

### Phase 5: Integration & Testing (Week 5)

**Deliverables:**
- ✅ Quickwit split integration tests
- ✅ Python parity tests
- ✅ Performance tests
- ✅ Complete documentation
- ✅ Example applications

**Acceptance Criteria:**
- JSON fields work in Quickwit splits
- Behavior matches tantivy-py exactly
- Performance meets benchmarks
- Documentation is complete and accurate

---

## 10. Migration Guide

### 10.1 From Separate Fields to JSON

**Before: Multiple Typed Fields**

```java
SchemaBuilder builder = new SchemaBuilder();
Field titleField = builder.addTextField("product_title", TEXT);
Field priceField = builder.addFloatField("product_price", INDEXED);
Field categoryField = builder.addTextField("product_category", TEXT);
Field inStockField = builder.addBooleanField("product_in_stock", INDEXED);
Schema schema = builder.build();

// Indexing
Document doc = new Document();
doc.addText(titleField, "Laptop");
doc.addFloat(priceField, 999.99f);
doc.addText(categoryField, "Electronics");
doc.addBoolean(inStockField, true);
writer.addDocument(doc);

// Querying
Query titleQuery = Query.termQuery(schema, titleField, "Laptop");
Query priceQuery = Query.rangeQuery(schema, priceField,
    Range.Type.FLOAT, 500.0f, true, 1500.0f, true);
```

**After: JSON Field**

```java
SchemaBuilder builder = new SchemaBuilder();
Field productField = builder.addJsonField("product",
    JsonObjectOptions.full());
Schema schema = builder.build();

// Indexing
Document doc = new Document();
doc.addJson(productField, Map.of(
    "title", "Laptop",
    "price", 999.99,
    "category", "Electronics",
    "in_stock", true
));
writer.addDocument(doc);

// Querying
Query titleQuery = Query.jsonTermQuery(schema, productField, "title", "Laptop");
Query priceQuery = Query.jsonRangeQuery(schema, productField, "price",
    500.0, true, 1500.0, true);
```

**Benefits:**
- ✅ Flexible schema - add fields without schema changes
- ✅ Nested data support - complex hierarchies
- ✅ Simpler indexing code - single field vs multiple
- ✅ Easier maintenance - fewer field definitions

**Trade-offs:**
- ⚠️ Slightly slower queries (path resolution overhead)
- ⚠️ More disk space if fast fields enabled
- ⚠️ Less type safety at compile time

### 10.2 When to Use JSON Fields

**Use JSON Fields When:**
- ✅ Schema evolves frequently
- ✅ Data has variable structure (e.g., product attributes)
- ✅ Nested objects are natural (e.g., user profiles)
- ✅ Working with external JSON APIs

**Use Typed Fields When:**
- ✅ Schema is stable and well-defined
- ✅ Maximum query performance required
- ✅ Strong compile-time type safety needed
- ✅ Simple flat data structure

### 10.3 Hybrid Approach

**Best Practice: Combine Both**

```java
SchemaBuilder builder = new SchemaBuilder();

// Fixed schema fields for common queries
Field idField = builder.addIntegerField("id", true, true, false);
Field timestampField = builder.addDateField("timestamp", true, true, false);
Field typeField = builder.addTextField("type", TEXT | FAST);

// JSON field for flexible attributes
Field attributesField = builder.addJsonField("attributes",
    JsonObjectOptions.full());

Schema schema = builder.build();

// Indexing
Document doc = new Document();
doc.addInt(idField, 12345);
doc.addDate(timestampField, Instant.now());
doc.addText(typeField, "product");
doc.addJson(attributesField, Map.of(
    "brand", "Apple",
    "specs", Map.of("ram", "16GB", "storage", "512GB"),
    "custom_field_123", "dynamic value"
));
writer.addDocument(doc);

// Fast queries on fixed fields, flexibility on JSON
Query fastQuery = Query.rangeQuery(schema, timestampField,
    Range.Type.DATE, yesterday, true, now, true);
Query flexQuery = Query.jsonTermQuery(schema, attributesField,
    "specs.ram", "16GB");
Query combined = Query.booleanQuery(
    Query.BooleanClause.must(fastQuery),
    Query.BooleanClause.must(flexQuery)
);
```

---

## 11. Summary and Recommendations

### 11.1 Key Achievements

This design provides:

✅ **Complete JSON Field Support** - Matching Tantivy and Quickwit capabilities
✅ **Python API Parity** - Exact compatibility with tantivy-py
✅ **Comprehensive Testing** - 70+ unit tests covering all functionality
✅ **Production-Ready Performance** - Optimized for real-world workloads
✅ **Clear Documentation** - Complete API docs and examples
✅ **Quickwit Integration** - Full split file support

### 11.2 Implementation Recommendations

1. **Start with Phase 1** - Core infrastructure is foundation
2. **Test Incrementally** - Each phase has dedicated test suite
3. **Follow Python Patterns** - Maintain parity with tantivy-py
4. **Optimize Early** - Performance tests in Phase 5 catch issues
5. **Document Thoroughly** - Keep examples updated with code

### 11.3 Future Enhancements

**Potential Additions (Post-MVP):**

- **JSON Schema Validation** - Validate documents against JSON Schema
- **Aggregations on JSON** - Bucket/metric aggregations on JSON paths
- **JSON Query DSL** - Elasticsearch-style JSON query syntax
- **JSONPath Wildcards** - Support for `*.field` and `[*]` patterns
- **Performance Profiling** - Detailed instrumentation for optimization

### 11.4 Success Criteria

The implementation will be considered successful when:

1. ✅ All 70+ unit tests pass
2. ✅ Python parity tests match tantivy-py behavior exactly
3. ✅ Performance benchmarks meet targets (10k docs/sec indexing, <10ms queries)
4. ✅ Quickwit splits with JSON fields work in production
5. ✅ Documentation enables developers to use JSON fields effectively

---

## Appendix A: Reference Files

### Tantivy Core
- `tantivy/src/schema/field_type.rs` - Type system
- `tantivy/src/schema/json_object_options.rs` - JSON configuration
- `tantivy/src/core/json_utils.rs` - JSON indexing logic
- `tantivy/src/schema/term.rs` - Term API for JSON paths
- `tantivy/src/query/exist_query.rs` - Exists query implementation
- `tantivy/examples/json_field.rs` - Usage examples

### Quickwit
- `quickwit-doc-mapper/src/doc_mapper/field_mapping_entry.rs` - Field mappings
- `quickwit-doc-mapper/src/doc_mapper/field_mapping_type.rs` - Type system

### Python Bindings
- `tantivy-py/src/schemabuilder.rs` - Schema builder (line 338)
- `tantivy-py/src/document.rs` - Document API (line 765)
- `tantivy-py/tests/tantivy_test.py` - Integration tests (line 730)
- `tantivy-py/tests/test_json_bug.py` - Bug regression tests

---

## Appendix B: API Quick Reference

### Schema Creation
```java
Field jsonField = schemaBuilder.addJsonField("data",
    JsonObjectOptions.storedAndIndexed());
```

### Document Indexing
```java
doc.addJson(field, "{\"key\": \"value\"}");
doc.addJson(field, Map.of("key", "value"));
```

### Querying
```java
Query.jsonTermQuery(schema, field, "path.to.field", value);
Query.jsonRangeQuery(schema, field, "path", lower, lowerInc, upper, upperInc);
Query.jsonExistsQuery(schema, field, "path", checkSubpaths);
```

### Configuration Options
```java
JsonObjectOptions.create()
    .setStored(true)        // Store original JSON
    .setIndexing(indexing)  // Enable searching
    .setFast(true)          // Enable fast filtering
    .setExpandDots(true);   // Treat dots as paths
```

---

**End of Design Document**

*Version 1.0 - January 2025*
