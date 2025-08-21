# Reference

## API Overview

This reference provides complete API documentation for Tantivy4Java, organized by functionality. All examples assume proper resource management with try-with-resources blocks.

## Setup

We'll use a test index for the examples that follow:

```java
import com.tantivy4java.*;
import java.util.*;

// Create schema
try (SchemaBuilder builder = new SchemaBuilder()) {
    builder.addIntegerField("doc_id", true, true, true);
    builder.addTextField("title", true, false, "default", "position");
    builder.addTextField("body", false, true, "default", "position");
    
    try (Schema schema = builder.build();
         Index index = new Index(schema, "", true);
         IndexWriter writer = index.writer(15_000_000, 1)) {
        
        // Add test documents
        writer.addJson("{ \"doc_id\": 1, \"title\": \"The Old Man and the Sea\", " +
                      "\"body\": \"He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.\" }");
        
        writer.addJson("{ \"doc_id\": 2, \"title\": \"The Old Man and the Sea II\", " +
                      "\"body\": \"He was an old man who sailed alone.\" }");
        
        writer.commit();
        index.reload();
        
        // Index is ready for examples
    }
}
```

## Core Classes

### Schema and SchemaBuilder

#### SchemaBuilder

The `SchemaBuilder` class is used to define the structure of your documents.

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Add various field types
    Schema schema = builder
        .addTextField("title", stored, indexed, tokenizer, indexOption)
        .addIntegerField("doc_id", stored, indexed, fast)
        .addFloatField("rating", stored, indexed, fast)
        .addBooleanField("is_published", stored, indexed, fast)
        .addIpAddressField("ip", stored, indexed, fast)
        .build();
}
```

**Parameters:**
- `stored`: Whether field values should be stored for retrieval
- `indexed`: Whether the field should be searchable
- `fast`: Whether to create fast fields for aggregations/sorting (numeric/boolean fields)
- `tokenizer`: Text analysis strategy (`"default"`, `"raw"`, `"en_stem"`)
- `indexOption`: Indexing detail level (`"position"`, `"freq"`, `"basic"`)

#### Schema

Represents the document structure. Use with queries and document operations:

```java
try (Schema schema = builder.build()) {
    // Schema methods
    String fieldName = "title";
    // Schema is used in query construction and document access
}
```

### Index Operations

#### Creating Indices

```java
// In-memory index
try (Index index = new Index(schema, "", true)) {
    // Index exists only in memory
}

// Persistent index
try (Index index = new Index(schema, "/path/to/index", false)) {
    // Index persisted to disk
}

// Open existing index
try (Index index = Index.openOrCreate("/path/to/index")) {
    // Opens existing or creates new
}
```

#### Index Management

```java
// Check if index exists
boolean exists = Index.exists("/path/to/index");

// Reload index to see latest changes
index.reload();

// Get schema from existing index
try (Schema schema = index.getSchema()) {
    // Work with schema
}
```

### IndexWriter

Handles document addition, updates, and commits.

#### Creating Writers

```java
try (IndexWriter writer = index.writer(heapSize, numThreads)) {
    // heapSize: Memory limit in bytes (e.g., 50_000_000 for 50MB)
    // numThreads: Number of indexing threads (typically 1)
}
```

#### Adding Documents

```java
// JSON documents (recommended)
writer.addJson("{ \"doc_id\": 1, \"title\": \"Hello World\" }");

// Commit changes
writer.commit();
```

#### Document Updates and Deletes

```java
// Delete documents by term
writer.deleteDocuments(schema, "doc_id", 1L);

// Update is delete + add
writer.deleteDocuments(schema, "doc_id", 1L);
writer.addJson("{ \"doc_id\": 1, \"title\": \"Updated Title\" }");
writer.commit();
```

### Searcher and Search Operations

#### Creating Searchers

```java
try (Searcher searcher = index.searcher()) {
    // Ready to search
}
```

#### Performing Searches

```java
try (Query query = Query.termQuery(schema, "title", "old");
     SearchResult result = searcher.search(query, limit)) {
    
    for (Hit hit : result.getHits()) {
        double score = hit.getScore();
        DocAddress address = hit.getDocAddress();
        
        try (Document doc = searcher.doc(address)) {
            // Access document fields
        }
    }
}
```

## Query Types

### Term Queries

Search for exact terms in fields:

```java
// Text field
try (Query query = Query.termQuery(schema, "title", "sea")) {
    // Matches documents with "sea" in title
}

// Integer field
try (Query query = Query.termQuery(schema, "doc_id", 1L)) {
    // Matches documents with doc_id = 1
}

// Boolean field
try (Query query = Query.termQuery(schema, "is_published", true)) {
    // Matches published documents
}
```

### Boolean Queries

Combine multiple queries with boolean logic:

```java
try (Query q1 = Query.termQuery(schema, "title", "old");
     Query q2 = Query.termQuery(schema, "title", "man");
     Query boolQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, q1),      // Required
         new Query.OccurQuery(Occur.SHOULD, q2),    // Optional (boosts score)
         new Query.OccurQuery(Occur.MUST_NOT, q3)   // Excluded
     ))) {
    // Complex boolean logic
}
```

**Occur Types:**
- `Occur.MUST`: Document must match this query
- `Occur.SHOULD`: Document should match (optional, boosts score)  
- `Occur.MUST_NOT`: Document must not match this query

### Phrase Queries

Search for sequences of terms:

```java
List<Object> terms = Arrays.asList("old", "man");

// Exact phrase
try (Query exactPhrase = Query.phraseQuery(schema, "title", terms, 0)) {
    // Matches "old man" exactly
}

// With slop (allows gaps)
try (Query sloppyPhrase = Query.phraseQuery(schema, "body", terms, 2)) {
    // Matches "old ... man" with up to 2 words between
}
```

### Fuzzy Queries

Handle spelling variations:

```java
try (Query fuzzyQuery = Query.fuzzyTermQuery(schema, "title", "sea", 
         distance,      // Edit distance (1-2 recommended)
         transposition, // Allow letter swapping
         prefix        // Require prefix match
     )) {
    // Matches "sea", "see", "seal", etc.
}
```

### Range Queries

Search numeric and date ranges:

```java
// Integer range
try (Query intRange = Query.rangeQuery(schema, "doc_id", FieldType.INTEGER,
         1, 10,        // Lower and upper bounds
         true, false   // Include lower, exclude upper
     )) {
    // Matches doc_id >= 1 AND doc_id < 10
}

// Float range
try (Query floatRange = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
         3.0, 5.0, true, true)) {
    // Matches rating >= 3.0 AND rating <= 5.0
}

// Date range (using integer timestamps)
long startDate = System.currentTimeMillis() - 86400000; // 24 hours ago
long endDate = System.currentTimeMillis();
try (Query dateRange = Query.rangeQuery(schema, "timestamp", FieldType.INTEGER,
         startDate, endDate, true, true)) {
    // Recent documents
}
```

### Boost Queries

Modify relevance scores:

```java
try (Query baseQuery = Query.termQuery(schema, "title", "important");
     Query boostedQuery = Query.boostQuery(baseQuery, 2.0)) {
    // Doubles the relevance score
}
```

### Const Score Queries

Assign uniform scores:

```java
try (Query baseQuery = Query.termQuery(schema, "category", "news");
     Query constQuery = Query.constScoreQuery(baseQuery, 1.0)) {
    // All matching documents get score of 1.0
}
```

## Document Access

### Document Fields

```java
try (Document doc = searcher.doc(hit.getDocAddress())) {
    // Get field values (returns List<Object>)
    List<Object> titles = doc.get("title");
    List<Object> docIds = doc.get("doc_id");
    
    // Extract specific values with type casting
    String title = (String) titles.get(0);
    Long docId = (Long) docIds.get(0);
    
    // Handle multi-value fields
    List<Object> tags = doc.get("tags");
    for (Object tag : tags) {
        System.out.println("Tag: " + tag);
    }
}
```

### Field Type Handling

```java
// Text fields -> String
String textValue = (String) doc.get("title").get(0);

// Integer fields -> Long
Long intValue = (Long) doc.get("doc_id").get(0);

// Float fields -> Double
Double floatValue = (Double) doc.get("rating").get(0);

// Boolean fields -> Boolean
Boolean boolValue = (Boolean) doc.get("is_published").get(0);

// Date fields (stored as Long timestamps)
Long timestamp = (Long) doc.get("created_date").get(0);
Date date = new Date(timestamp);
```

## Field Types and Configuration

### Text Fields

```java
builder.addTextField(fieldName, stored, indexed, tokenizer, indexOption);
```

**Tokenizer Options:**
- `"default"`: Whitespace/punctuation splitting, lowercasing, 40-char limit
- `"raw"`: No tokenization (exact matching)
- `"en_stem"`: Default processing + English stemming

**Index Options:**
- `"basic"`: Term presence only
- `"freq"`: Term frequency information
- `"position"`: Term positions (required for phrase queries)

### Numeric Fields

```java
// Integer fields
builder.addIntegerField(fieldName, stored, indexed, fast);

// Float fields
builder.addFloatField(fieldName, stored, indexed, fast);
```

**Fast Fields:** Enable for:
- Range queries
- Sorting (future feature)
- Aggregations (future feature)

### Boolean Fields

```java
builder.addBooleanField(fieldName, stored, indexed, fast);

// Query boolean fields
try (Query boolQuery = Query.termQuery(schema, "is_active", true)) {
    // Matches active documents
}
```

### IP Address Fields

```java
builder.addIpAddressField(fieldName, stored, indexed, fast);

// Query with IP addresses (as strings)
try (Query ipQuery = Query.termQuery(schema, "client_ip", "192.168.1.1")) {
    // Exact IP match
}
```

## Error Handling and Best Practices

### Resource Management

Always use try-with-resources for automatic cleanup:

```java
try (SchemaBuilder builder = new SchemaBuilder();
     Schema schema = builder.build();
     Index index = new Index(schema);
     IndexWriter writer = index.writer();
     Searcher searcher = index.searcher()) {
    
    // All resources automatically closed
}
```

### Common Patterns

#### Safe Field Access

```java
List<Object> values = doc.get(fieldName);
if (!values.isEmpty()) {
    Object value = values.get(0);
    if (value instanceof String) {
        String stringValue = (String) value;
        // Process string value
    }
}
```

#### Index Lifecycle

```java
// Standard index workflow
try (Schema schema = buildSchema()) {
    try (Index index = new Index(schema, indexPath, false)) {
        
        // Indexing phase
        try (IndexWriter writer = index.writer()) {
            // Add documents
            writer.commit();
        }
        
        // Searching phase  
        index.reload();
        try (Searcher searcher = index.searcher()) {
            // Perform searches
        }
    }
}
```

#### Query Composition

```java
// Build complex queries step by step
List<Query.OccurQuery> clauses = new ArrayList<>();

if (titleTerm != null) {
    try (Query titleQuery = Query.termQuery(schema, "title", titleTerm)) {
        clauses.add(new Query.OccurQuery(Occur.SHOULD, titleQuery));
    }
}

if (bodyTerm != null) {
    try (Query bodyQuery = Query.termQuery(schema, "body", bodyTerm)) {
        clauses.add(new Query.OccurQuery(Occur.SHOULD, bodyQuery));
    }
}

if (!clauses.isEmpty()) {
    try (Query finalQuery = Query.booleanQuery(clauses)) {
        // Execute search
    }
}
```

## Performance Guidelines

### Indexing Performance

- Use appropriate heap size for IndexWriter (50-200MB typical)
- Batch document additions before commit
- Use single indexing thread unless you have high-volume requirements
- Consider fast fields for numeric data you'll query frequently

### Search Performance

- Reload index only when necessary (after commits)
- Reuse searcher instances when possible
- Use appropriate query types (term queries are fastest)
- Limit search results to what you actually need

### Memory Management

- Always use try-with-resources
- Close resources promptly in long-running applications
- Monitor memory usage with multiple concurrent searches
- Consider index size vs. memory trade-offs

This reference covers the complete Tantivy4Java API. For practical examples, see the [Tutorials](tutorials.md) and [Examples](../examples/).