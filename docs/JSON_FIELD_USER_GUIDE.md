# JSON Field Support Guide for Tantivy4Java

**A practical guide for developers adding JSON field support to existing Tantivy4Java applications**

---

## ⚠️ Version Requirement

**JSON field support requires Tantivy4Java version 0.25.2 or later.**

If you're using an earlier version, upgrade to 0.25.2:

```xml
<dependency>
    <groupId>io.indextables</groupId>
    <artifactId>tantivy4java</artifactId>
    <version>0.25.2</version>
    <classifier>linux-x86_64</classifier>
</dependency>
```

**What's New in 0.25.2:**
- ✅ Complete JSON field support for regular indexes
- ✅ JSON field integration with Quickwit splits
- ✅ All JSON query types (term, range, exists, nested, boolean)
- ✅ Full test coverage (15/15 tests passing)

---

## Overview

JSON fields in Tantivy4Java enable you to index and search dynamic, schema-less data structures without defining every field upfront. This is ideal for:

- **Flexible data models** - Product metadata, user profiles, event logs
- **Evolving schemas** - Add new fields without schema migrations
- **Nested structures** - Complex hierarchical data with arbitrary depth
- **Mixed-type data** - Documents with varying field sets

**Key Benefits:**
- ✅ No schema migrations needed when data structure changes
- ✅ Full text search on all text values in JSON
- ✅ Range queries on numeric fields
- ✅ Nested path queries with dot notation
- ✅ Works identically in both indexes and Quickwit splits
- ✅ Zero performance overhead vs. strongly-typed fields

---

## Quick Migration Path

If you already have a Tantivy4Java application, adding JSON field support requires just **3 steps**:

### Step 1: Add JSON Field to Schema

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Your existing fields
    builder.addTextField("title", true, false, "default", "position");
    builder.addIntegerField("id", true, true, false);

    // NEW: Add JSON field
    Field jsonField = builder.addJsonField("metadata", JsonObjectOptions.storedAndIndexed());

    try (Schema schema = builder.build()) {
        // Continue with index creation...
    }
}
```

### Step 2: Index JSON Documents

```java
try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
    Document doc = new Document();

    // Your existing fields
    doc.addText("title", "Product Name");
    doc.addInteger("id", 12345);

    // NEW: Add JSON data
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("category", "electronics");
    metadata.put("price", 299.99);
    metadata.put("in_stock", true);
    metadata.put("tags", Arrays.asList("laptop", "portable", "work"));

    doc.addJson(jsonField, metadata);
    writer.addDocument(doc);
    writer.commit();
}
```

### Step 3: Query JSON Fields

```java
try (Searcher searcher = index.searcher()) {
    // Query JSON field
    try (Query query = Query.jsonTermQuery(schema, "metadata", "category", "electronics")) {
        SearchResult results = searcher.search(query, 10);

        for (SearchResult.Hit hit : results.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                // Retrieve JSON data
                Map<String, Object> metadata = doc.getJsonMap("metadata");
                System.out.println("Category: " + metadata.get("category"));
                System.out.println("Price: " + metadata.get("price"));
            }
        }
    }
}
```

That's it! Your application now supports flexible JSON documents.

---

## Configuration Options

### Basic Presets

Choose the configuration that matches your use case:

```java
// Searchable + Retrievable (most common)
Field json = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

// Searchable only (save storage space)
Field json = builder.addJsonField("data", JsonObjectOptions.indexed());

// Retrievable only (no search, just storage)
Field json = builder.addJsonField("data", JsonObjectOptions.stored());

// Everything enabled (search + retrieve + fast fields)
Field json = builder.addJsonField("data", JsonObjectOptions.full());
```

### Advanced Configuration

For fine-grained control, build custom options:

```java
// Configure text indexing
TextFieldIndexing textIndexing = TextFieldIndexing.default()
    .setTokenizer("default")
    .setIndexOption(IndexRecordOption.WithFreqsAndPositions);

// Build custom JSON options
JsonObjectOptions options = JsonObjectOptions.default()
    .setStored()                          // Enable retrieval
    .setIndexingOptions(textIndexing)     // Enable text search
    .setFast("raw")                       // Enable fast fields for sorting
    .setExpandDotsEnabled();              // Flatten nested objects

Field json = builder.addJsonField("data", options);
```

**Option Details:**

| Option | Purpose | When to Use |
|--------|---------|-------------|
| `setStored()` | Retrieve original JSON | Always, unless you only need search |
| `setIndexingOptions()` | Enable text search | For text fields in JSON |
| `setFast(tokenizer)` | Enable range queries & sorting | For numeric/date fields in JSON |
| `setExpandDotsEnabled()` | Flatten nested objects | For simpler querying of deep structures |

---

## Complete Query Reference

### 1. Term Queries

Search for exact text matches in JSON fields:

```java
// Simple term query
Query query = Query.jsonTermQuery(schema, "metadata", "status", "active");

// Nested path with dot notation
Query query = Query.jsonTermQuery(schema, "metadata", "user.role", "admin");

// Array values (matches if any element matches)
Query query = Query.jsonTermQuery(schema, "metadata", "tags", "urgent");
```

**Use Cases:**
- Status flags (`"active"`, `"pending"`, `"completed"`)
- Categories and classifications
- User roles and permissions
- Tag matching

### 2. Range Queries

Query numeric or date ranges (requires fast fields):

```java
// Numeric range (inclusive bounds)
Query query = Query.jsonRangeQuery(
    schema, "metadata", "price",
    100L,    // lower bound
    500L,    // upper bound
    true,    // include lower
    true     // include upper
);

// Exclusive bounds
Query query = Query.jsonRangeQuery(
    schema, "metadata", "score",
    0L, 100L, false, false  // 0 < score < 100
);

// Unbounded range (null = no limit)
Query query = Query.jsonRangeQuery(
    schema, "metadata", "age",
    18L,     // minimum age
    null,    // no maximum
    true, true
);

// Nested numeric field
Query query = Query.jsonRangeQuery(
    schema, "metadata", "stats.views",
    1000L, 10000L, true, false
);
```

**Use Cases:**
- Price filtering
- Date ranges
- Scoring thresholds
- Quantity ranges
- Performance metrics

**Important:** Range queries require fast fields enabled:
```java
JsonObjectOptions.full()  // or
JsonObjectOptions.default().setFast("raw")
```

### 3. Exists Queries

Check if a field exists in the JSON:

```java
// Check if field is present
Query query = Query.jsonExistsQuery(schema, "metadata", "email");

// Nested field existence
Query query = Query.jsonExistsQuery(schema, "metadata", "contact.phone");
```

**Use Cases:**
- Find documents with optional fields
- Filter complete vs. incomplete records
- Identify data quality issues
- Conditional logic based on field presence

### 4. Boolean Combinations

Combine multiple JSON queries with AND/OR/NOT logic:

```java
// AND: Both conditions must match
Query q1 = Query.jsonTermQuery(schema, "metadata", "category", "electronics");
Query q2 = Query.jsonRangeQuery(schema, "metadata", "price", 100L, 500L, true, true);
Query combined = Query.booleanQuery(
    Arrays.asList(q1, q2),
    Arrays.asList(Occur.MUST, Occur.MUST)
);

// OR: Either condition can match
Query q1 = Query.jsonTermQuery(schema, "metadata", "priority", "high");
Query q2 = Query.jsonTermQuery(schema, "metadata", "urgent", "true");
Query combined = Query.booleanQuery(
    Arrays.asList(q1, q2),
    Arrays.asList(Occur.SHOULD, Occur.SHOULD)
);

// NOT: Exclude documents
Query q1 = Query.jsonTermQuery(schema, "metadata", "status", "active");
Query q2 = Query.jsonTermQuery(schema, "metadata", "archived", "true");
Query combined = Query.booleanQuery(
    Arrays.asList(q1, q2),
    Arrays.asList(Occur.MUST, Occur.MUST_NOT)  // active AND NOT archived
);

// Complex: Mix JSON and regular fields
Query jsonQuery = Query.jsonTermQuery(schema, "metadata", "verified", "true");
Query textQuery = Query.termQuery(schema, "title", "premium");
Query combined = Query.booleanQuery(
    Arrays.asList(jsonQuery, textQuery),
    Arrays.asList(Occur.MUST, Occur.MUST)
);
```

**Occur Types:**
- `Occur.MUST` - Document must match this query (AND)
- `Occur.SHOULD` - Document should match this query (OR)
- `Occur.MUST_NOT` - Document must not match this query (NOT)

---

## Working with Nested Data

### Indexing Nested Objects

```java
Document doc = new Document();

Map<String, Object> product = new HashMap<>();
product.put("name", "Laptop Pro");
product.put("price", 1299.99);

// Nested object
Map<String, Object> specs = new HashMap<>();
specs.put("cpu", "Intel i7");
specs.put("ram", "16GB");
specs.put("storage", "512GB SSD");
product.put("specs", specs);

// Nested array of objects
List<Map<String, Object>> reviews = Arrays.asList(
    Map.of("rating", 5, "comment", "Excellent"),
    Map.of("rating", 4, "comment", "Good value")
);
product.put("reviews", reviews);

doc.addJson(jsonField, product);
```

### Querying Nested Paths

Use dot notation to query nested fields:

```java
// Query nested field
Query query = Query.jsonTermQuery(schema, "product", "specs.cpu", "Intel i7");

// Query deeper nesting
Query query = Query.jsonTermQuery(schema, "data", "user.address.city", "New York");

// Range query on nested numeric
Query query = Query.jsonRangeQuery(schema, "product", "specs.ram_gb", 8L, 32L, true, true);
```

### Retrieving Nested Data

```java
try (Document doc = searcher.doc(hit.getDocAddress())) {
    Map<String, Object> product = doc.getJsonMap("product");

    // Access nested object
    Map<String, Object> specs = (Map<String, Object>) product.get("specs");
    System.out.println("CPU: " + specs.get("cpu"));

    // Access nested array
    List<Map<String, Object>> reviews =
        (List<Map<String, Object>>) product.get("reviews");
    for (Map<String, Object> review : reviews) {
        System.out.println("Rating: " + review.get("rating"));
    }
}
```

---

## Working with Arrays

JSON fields support arrays of both primitives and objects:

### Indexing Arrays

```java
Map<String, Object> data = new HashMap<>();

// Array of strings
data.put("tags", Arrays.asList("electronics", "laptop", "portable"));

// Array of numbers
data.put("scores", Arrays.asList(85, 92, 78, 95));

// Array of objects
List<Map<String, Object>> items = Arrays.asList(
    Map.of("id", "A1", "qty", 5),
    Map.of("id", "B2", "qty", 3)
);
data.put("items", items);

doc.addJson(jsonField, data);
```

### Querying Arrays

```java
// Match if any array element matches
Query query = Query.jsonTermQuery(schema, "data", "tags", "laptop");
// Matches documents where "laptop" is in the tags array

// Range query on array values
Query query = Query.jsonRangeQuery(schema, "data", "scores", 90L, 100L, true, true);
// Matches documents where any score is between 90-100
```

### Retrieving Array Data

```java
Map<String, Object> data = doc.getJsonMap("data");

// String array
List<String> tags = (List<String>) data.get("tags");
for (String tag : tags) {
    System.out.println("Tag: " + tag);
}

// Numeric array
List<Number> scores = (List<Number>) data.get("scores");
double average = scores.stream()
    .mapToDouble(Number::doubleValue)
    .average()
    .orElse(0.0);
```

---

## Quickwit Split Integration

JSON fields work seamlessly with Quickwit splits - no code changes needed!

### Creating Splits with JSON Fields

```java
// 1. Create index with JSON field (same as regular index)
try (SchemaBuilder builder = new SchemaBuilder()) {
    Field jsonField = builder.addJsonField("data", JsonObjectOptions.full());

    try (Schema schema = builder.build();
         Index index = new Index(schema, "/tmp/json_index", false)) {

        // Add documents...
        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            Document doc = new Document();
            Map<String, Object> data = Map.of(
                "product", "laptop",
                "price", 999.99,
                "available", true
            );
            doc.addJson(jsonField, data);
            writer.addDocument(doc);
            writer.commit();
        }

        // 2. Convert to Quickwit split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "products-index", "catalog-source", "indexer-1");
        QuickwitSplit.convertIndexFromPath(
            "/tmp/json_index",
            "/tmp/products.split",
            config
        );
    }
}
```

### Searching JSON Fields in Splits

```java
// Search split (identical API to regular index)
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("cache")
        .withMaxCacheSize(100_000_000);

try (SplitCacheManager manager = SplitCacheManager.getInstance(cacheConfig);
     SplitSearcher searcher = manager.createSplitSearcher("file:///tmp/products.split")) {

    Schema schema = searcher.getSchema();

    // All JSON query types work in splits
    SplitQuery termQuery = new SplitTermQuery("data", "product", "laptop");
    SearchResult results = searcher.search(termQuery, 10);

    // Retrieve JSON data from split
    for (SearchResult.Hit hit : results.getHits()) {
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            Map<String, Object> data = doc.getJsonMap("data");
            System.out.println("Product: " + data.get("product"));
            System.out.println("Price: " + data.get("price"));
        }
    }
}
```

**Important:** All JSON query types (term, range, exists, nested, boolean) work identically in both regular indexes and splits. No code changes needed!

---

## Common Patterns

### Pattern 1: E-Commerce Product Catalog

```java
// Schema
Field productField = builder.addJsonField("product", JsonObjectOptions.full());

// Index product
Map<String, Object> product = new HashMap<>();
product.put("sku", "LAP-001");
product.put("name", "Professional Laptop");
product.put("category", "electronics");
product.put("price", 1299.99);
product.put("in_stock", true);
product.put("tags", Arrays.asList("laptop", "business", "portable"));

Map<String, Object> specs = Map.of(
    "screen", "15.6 inch",
    "weight_kg", 1.8,
    "battery_hours", 10
);
product.put("specifications", specs);

doc.addJson(productField, product);

// Query: Find laptops under $1500 in stock
Query categoryQ = Query.jsonTermQuery(schema, "product", "category", "electronics");
Query priceQ = Query.jsonRangeQuery(schema, "product", "price", null, 1500L, true, true);
Query stockQ = Query.jsonTermQuery(schema, "product", "in_stock", "true");

Query combined = Query.booleanQuery(
    Arrays.asList(categoryQ, priceQ, stockQ),
    Arrays.asList(Occur.MUST, Occur.MUST, Occur.MUST)
);
```

### Pattern 2: User Event Logging

```java
// Schema
Field eventField = builder.addJsonField("event", JsonObjectOptions.storedAndIndexed());

// Index event
Map<String, Object> event = new HashMap<>();
event.put("event_type", "page_view");
event.put("user_id", "user-12345");
event.put("timestamp", System.currentTimeMillis());
event.put("page", "/products/laptop-pro");
event.put("session_duration_seconds", 145);

Map<String, Object> metadata = Map.of(
    "referrer", "google.com",
    "device", "mobile",
    "browser", "chrome"
);
event.put("metadata", metadata);

doc.addJson(eventField, event);

// Query: Find mobile page views from Google in last hour
long oneHourAgo = System.currentTimeMillis() - 3600000;
Query typeQ = Query.jsonTermQuery(schema, "event", "event_type", "page_view");
Query deviceQ = Query.jsonTermQuery(schema, "event", "metadata.device", "mobile");
Query referrerQ = Query.jsonTermQuery(schema, "event", "metadata.referrer", "google.com");
Query timeQ = Query.jsonRangeQuery(schema, "event", "timestamp",
    oneHourAgo, null, true, true);

Query combined = Query.booleanQuery(
    Arrays.asList(typeQ, deviceQ, referrerQ, timeQ),
    Arrays.asList(Occur.MUST, Occur.MUST, Occur.MUST, Occur.MUST)
);
```

### Pattern 3: API Response Caching

```java
// Schema
Field responseField = builder.addJsonField("api_response", JsonObjectOptions.stored());
Field metaField = builder.addJsonField("meta", JsonObjectOptions.full());

// Index API response with metadata
Map<String, Object> meta = Map.of(
    "endpoint", "/api/users/123",
    "status_code", 200,
    "cache_ttl_seconds", 3600,
    "cached_at", System.currentTimeMillis()
);
doc.addJson(metaField, meta);

// Store full response (not searchable, just retrievable)
Map<String, Object> response = fetchFromAPI();
doc.addJson(responseField, response);

// Query: Find cached 200 responses for endpoint
Query endpointQ = Query.jsonTermQuery(schema, "meta", "endpoint", "/api/users/123");
Query statusQ = Query.jsonTermQuery(schema, "meta", "status_code", "200");
Query combined = Query.booleanQuery(
    Arrays.asList(endpointQ, statusQ),
    Arrays.asList(Occur.MUST, Occur.MUST)
);
```

### Pattern 4: Configuration Storage

```java
// Schema
Field configField = builder.addJsonField("config", JsonObjectOptions.storedAndIndexed());

// Index configuration
Map<String, Object> config = new HashMap<>();
config.put("service_name", "payment-processor");
config.put("environment", "production");
config.put("version", "2.1.0");
config.put("enabled", true);

Map<String, Object> settings = Map.of(
    "timeout_ms", 5000,
    "retry_attempts", 3,
    "max_batch_size", 100
);
config.put("settings", settings);

List<String> features = Arrays.asList("webhooks", "batching", "compression");
config.put("enabled_features", features);

doc.addJson(configField, config);

// Query: Find all production configs with webhooks enabled
Query envQ = Query.jsonTermQuery(schema, "config", "environment", "production");
Query featureQ = Query.jsonTermQuery(schema, "config", "enabled_features", "webhooks");
Query combined = Query.booleanQuery(
    Arrays.asList(envQ, featureQ),
    Arrays.asList(Occur.MUST, Occur.MUST)
);
```

---

## Performance Tips

### 1. Choose the Right Options

```java
// For searchable text: indexed
JsonObjectOptions.indexed()  // ~50% less storage than storedAndIndexed

// For numeric ranges: fast fields required
JsonObjectOptions.full()     // Enables range queries and sorting

// For retrieval only: stored
JsonObjectOptions.stored()   // Fastest writes, no search overhead
```

### 2. Index Size Considerations

JSON fields have similar storage overhead to equivalent strongly-typed fields:

```java
// This JSON field:
Map<String, Object> data = Map.of("price", 99.99, "name", "Product");

// Has similar size to:
doc.addFloat("price", 99.99f);
doc.addText("name", "Product");

// No significant performance difference!
```

### 3. Query Performance

- **Term queries**: Same speed as regular term queries
- **Range queries**: Requires fast fields, similar to numeric range queries
- **Nested paths**: No performance penalty, resolved at query time
- **Boolean combinations**: Standard boolean query performance

### 4. Caching with Splits

When using JSON fields with Quickwit splits, the cache works identically:

```java
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("json-cache")
        .withMaxCacheSize(500_000_000);  // 500MB shared cache

// Cache benefits:
// - Schema cached once per split
// - JSON field data uses standard component caching
// - No additional memory overhead for JSON
```

---

## Migration from Strongly-Typed Fields

### Scenario: Adding Flexibility to Existing Schema

You don't need to migrate existing fields - just add JSON alongside them:

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Keep existing strongly-typed fields
    builder.addTextField("title", true, false, "default", "position");
    builder.addIntegerField("id", true, true, false);
    builder.addFloatField("price", true, true, true);

    // Add JSON field for flexible metadata
    Field metaField = builder.addJsonField("metadata", JsonObjectOptions.storedAndIndexed());

    try (Schema schema = builder.build()) {
        // Now you have best of both worlds:
        // - Strongly-typed fields for known structure
        // - JSON field for flexible metadata
    }
}
```

**When to use each:**

| Use Strongly-Typed When | Use JSON When |
|-------------------------|---------------|
| Schema is stable | Schema evolves frequently |
| Performance critical | Flexibility more important |
| All documents have field | Field is optional/varies |
| Need compile-time safety | Need runtime flexibility |

### Hybrid Approach (Recommended)

```java
// Strongly-typed for core fields
builder.addTextField("title", true, false, "default", "position");
builder.addIntegerField("product_id", true, true, false);
builder.addFloatField("base_price", true, true, true);

// JSON for variable metadata
Field extrasField = builder.addJsonField("extras", JsonObjectOptions.storedAndIndexed());

// Add document
Document doc = new Document();
doc.addText("title", "Laptop Pro");
doc.addInteger("product_id", 12345);
doc.addFloat("base_price", 999.99f);

// Variable metadata in JSON
Map<String, Object> extras = new HashMap<>();
extras.put("color", "silver");
extras.put("warranty_years", 2);
extras.put("bundle_items", Arrays.asList("mouse", "bag"));
doc.addJson(extrasField, extras);

// Query both types
Query titleQ = Query.termQuery(schema, "title", "laptop");
Query colorQ = Query.jsonTermQuery(schema, "extras", "color", "silver");
Query combined = Query.booleanQuery(
    Arrays.asList(titleQ, colorQ),
    Arrays.asList(Occur.MUST, Occur.MUST)
);
```

---

## Troubleshooting

### Issue: Range Queries Not Working

**Problem:**
```java
Query query = Query.jsonRangeQuery(schema, "data", "price", 100L, 500L, true, true);
// Returns no results even though documents exist
```

**Solution:** Enable fast fields:
```java
// Change from:
JsonObjectOptions.storedAndIndexed()

// To:
JsonObjectOptions.full()  // or
JsonObjectOptions.default().setFast("raw")
```

### Issue: Cannot Retrieve JSON Data

**Problem:**
```java
Map<String, Object> data = doc.getJsonMap("metadata");
// Returns null
```

**Solution:** Enable stored option:
```java
// Change from:
JsonObjectOptions.indexed()

// To:
JsonObjectOptions.storedAndIndexed()  // or
JsonObjectOptions.full()
```

### Issue: Nested Queries Not Working

**Problem:**
```java
Query query = Query.jsonTermQuery(schema, "data", "user.name", "Alice");
// No results
```

**Solution:** Check dot notation - ensure JSON structure matches:
```java
// Your JSON must have this structure:
{
  "user": {
    "name": "Alice"  // Not "user.name": "Alice"
  }
}

// Or enable expand_dots:
JsonObjectOptions.default()
    .setStored()
    .setIndexingOptions(textIndexing)
    .setExpandDotsEnabled()  // Allows flattened structures
```

### Issue: Text Search Case Sensitivity

**Problem:**
```java
Query query = Query.jsonTermQuery(schema, "data", "status", "Active");
// Doesn't match documents with "active"
```

**Solution:** Terms are case-sensitive. Normalize your data:
```java
// At index time:
data.put("status", status.toLowerCase());

// At query time:
Query query = Query.jsonTermQuery(schema, "data", "status", searchTerm.toLowerCase());
```

---

## Next Steps

Now that you understand JSON fields in Tantivy4Java:

1. **Start Small** - Add one JSON field to an existing schema
2. **Experiment** - Try different query types with sample data
3. **Test in Splits** - Verify your queries work in Quickwit splits
4. **Optimize** - Choose the right `JsonObjectOptions` for your use case
5. **Scale** - Apply to production workloads

### Additional Resources

- **API Documentation**: See JavaDoc for `JsonObjectOptions` class
- **Test Examples**: Review `JsonFieldQueryTest.java` and `JsonFieldSplitIntegrationTest.java`
- **Main README**: [README.md](README.md) for general Tantivy4Java usage
- **Project Guide**: [CLAUDE.md](CLAUDE.md) for technical implementation details

---

## Summary

**JSON fields in Tantivy4Java provide:**

✅ **Flexibility** - No schema migrations, evolving data structures
✅ **Full Query Support** - Term, range, exists, nested, boolean
✅ **Split Compatibility** - Works identically in indexes and Quickwit splits
✅ **Performance** - No overhead vs. strongly-typed fields
✅ **Ease of Use** - 3-line change to add to existing applications

**You're ready to add JSON support to your application!**
