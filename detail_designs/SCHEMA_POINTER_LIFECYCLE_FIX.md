# Schema Pointer Lifecycle Fix for SplitSearcher.parseQuery()

## Problem Summary

The `splitSearcher.parseQuery("engine")` method is incorrectly returning `SplitMatchAllQuery` instead of creating proper multi-field text queries when no field prefix is specified. This causes queries like "engine" to match all documents instead of only documents containing "engine" in text fields.

## Root Cause Analysis

### The Issue
**Schema Pointer Lifecycle Mismatch** - Two different schema pointers are created during the process:

1. **Schema Creation** (`getSchemaFromNative`):
   - Creates schema from doc mapping JSON: `Schema extracted and registered with pointer: 7`
   - Registers Arc with pointer `7` in native registry

2. **Query Parsing** (`parseQuery`):
   - Receives different schema pointer: `🚀 Parsing query string: 'engine' with schema_ptr: 6135982496`
   - Registry lookup fails: `❌ Failed to retrieve schema from registry with pointer: 6135982496`

### Technical Flow
```
SplitSearcher Context:
├── doc_mapping_json: Available ✅
├── getSchemaFromNative() → Creates Schema(ptr=7) → Registers in Arc registry
└── Java SplitSearcher.getSchema() → Returns new Schema(ptr=6135982496) ❌

parseQuery() receives ptr=6135982496 → Registry lookup fails → No field extraction → Match-all fallback
```

### Debug Evidence
```rust
RUST DEBUG: Schema extracted and registered with pointer: 7
RUST DEBUG: 🚀 Parsing query string: 'engine' with schema_ptr: 6135982496  
RUST DEBUG: ❌ Failed to retrieve schema from registry with pointer: 6135982496
RUST DEBUG: Warning: No indexed text fields found in schema, query will use empty default fields
RUST DEBUG: ❌ Parsing failed: query requires a default search field and none was supplied
```

## Proposed Solution

### Maintain Schema Clone in SplitSearcher Context

Instead of relying on Java-managed schema pointers, store a schema clone directly in the SplitSearcher native context alongside the existing doc mapping JSON.

#### Current Context Structure:
```rust
Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, HashMap<String, String>, u64, u64, Option<String>)>
//   searcher,            runtime,                split_uri, aws_config,                footer_start, footer_end, doc_mapping_json
```

#### Proposed Extended Context:
```rust
Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, HashMap<String, String>, u64, u64, Option<String>, Option<tantivy::schema::Schema>)>
//   searcher,            runtime,                split_uri, aws_config,                footer_start, footer_end, doc_mapping_json, schema_clone
```

### Implementation Steps

#### 1. Extend SplitSearcher Context Creation
```rust
// In split_searcher_replacement.rs - Java_com_tantivy4java_SplitSearcher_createNative
let schema_clone = if let Some(doc_mapping_str) = &doc_mapping_json {
    create_schema_from_doc_mapping(doc_mapping_str).ok()
} else {
    None
};

let searcher_context = std::sync::Arc::new((
    searcher, 
    runtime, 
    split_uri.clone(), 
    aws_config, 
    split_footer_start, 
    split_footer_end, 
    doc_mapping_json,
    schema_clone  // Add schema clone
));
```

#### 2. Update All Context Access Points
Update all `with_arc_safe` calls to handle the 8-tuple:
```rust
// Before: 7-tuple
|searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, HashMap<String, String>, u64, u64, Option<String>)>|

// After: 8-tuple  
|searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, HashMap<String, String>, u64, u64, Option<String>, Option<tantivy::schema::Schema>)>|
```

#### 3. Modify parseQuery Field Extraction
```rust
// In split_query.rs - extract_text_fields_from_schema
fn extract_text_fields_from_schema(_env: &mut JNIEnv, schema_ptr: jlong) -> Result<Vec<String>> {
    // First try registry lookup (fallback for compatibility)
    if let Some(schema) = crate::utils::jlong_to_arc::<tantivy::schema::Schema>(schema_ptr) {
        return extract_fields_from_schema(&schema);
    }
    
    // NEW: Try to get schema from SplitSearcher context
    // This requires passing searcher context or implementing global schema cache
    // ... Implementation details below
}
```

#### 4. Alternative: Global Schema Cache by Split URI
Since parseQuery doesn't have direct access to SplitSearcher context, implement a global schema cache:

```rust
// Global cache mapping split URI to schema
static SPLIT_SCHEMA_CACHE: Lazy<Arc<Mutex<HashMap<String, tantivy::schema::Schema>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Store schema when SplitSearcher is created
fn store_split_schema(split_uri: &str, schema: tantivy::schema::Schema) {
    let mut cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    cache.insert(split_uri.to_string(), schema);
}

// Retrieve schema in parseQuery
fn get_split_schema(split_uri: &str) -> Option<tantivy::schema::Schema> {
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    cache.get(split_uri).cloned()
}
```

## Recommended Implementation Path

### Phase 1: Minimal Fix (Immediate)
1. Implement global schema cache approach
2. Store schema clone when SplitSearcher is created  
3. Modify `extract_text_fields_from_schema` to use cached schema
4. Test with `SimpleTermBugTest` to verify fix

### Phase 2: Clean Architecture (Future)
1. Extend SplitSearcher context to 8-tuple
2. Update all context access points
3. Implement proper schema lifecycle management
4. Remove global cache in favor of context-based approach

## Expected Outcome

After implementation:
```rust
RUST DEBUG: Schema extracted and registered with pointer: 7
RUST DEBUG: Schema clone stored in cache for split: file:///tmp/term_bug_test.split  
RUST DEBUG: 🚀 Parsing query string: 'engine' with schema_ptr: 6135982496
RUST DEBUG: Registry lookup failed, trying schema cache...
RUST DEBUG: ✅ Retrieved schema from cache for split: file:///tmp/term_bug_test.split
RUST DEBUG: Found indexed text field: 'review_text'  
RUST DEBUG: Found indexed text field: 'title'
RUST DEBUG: Extracted 2 text fields from schema: ["review_text", "title"]
RUST DEBUG: Created proper multi-field query for: engine
```

## Benefits

1. **Fixes Core Issue**: Resolves schema pointer lifecycle mismatch
2. **No Java Changes**: Solution entirely in native layer
3. **Maintains Compatibility**: Existing code continues to work
4. **Proper Multi-Field Search**: Enables correct "search all fields" behavior
5. **Clear Error Handling**: Maintains diagnostic capabilities

## Testing

Validate with existing test case:
```bash
TANTIVY4JAVA_DEBUG=1 java -cp ".:target/classes" SimpleTermBugTest
```

Expected: Query "engine" should match only Document 2 (containing "analytics engine"), not all 4 documents.