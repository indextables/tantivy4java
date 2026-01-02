# Tantivy4Java

**High-performance full-text search for Java applications powered by Tantivy**

Tantivy4Java is a production-ready Java library that provides direct access to the Tantivy search engine through JNI bindings. Built for Java developers who need enterprise-grade full-text search capabilities with Rust-level performance.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Installation](#installation)
4. [Basic Usage](#basic-usage)
5. [Advanced Features](#advanced-features)
6. [Quickwit Integration](#quickwit-integration)
   - [Aggregations](#aggregations)
7. [L2 Tiered Disk Cache](#l2-tiered-disk-cache)
8. [Performance Guide](#performance-guide)
9. [Developer Guide](#developer-guide)
10. [API Reference](#api-reference)

---

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>io.indextables</groupId>
    <artifactId>tantivy4javaexperimental</artifactId>
    <version>0.25.14</version>
    <classifier>linux-x86_64</classifier>
</dependency>
```

> **Note:** This experimental version includes the new L2 tiered disk cache feature. See [L2 Disk Cache](#l2-tiered-disk-cache) for details.

### 30-Second Example

```java
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;

// Build schema
try (SchemaBuilder builder = new SchemaBuilder()) {
    builder.addTextField("title", true, false, "default", "position");
    builder.addTextField("body", true, false, "default", "position");

    try (Schema schema = builder.build()) {
        // Create in-memory index
        try (Index index = Index.createInRam(schema)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                // Add document
                Document doc = new Document();
                doc.addText("title", "Getting Started with Search");
                doc.addText("body", "Full-text search in Java made easy");
                writer.addDocument(doc);
                writer.commit();
            }

            // Search
            index.reload();
            try (Searcher searcher = index.searcher()) {
                try (Query query = Query.termQuery(schema, "body", "search")) {
                    SearchResult results = searcher.search(query, 10);
                    System.out.println("Found: " + results.getHits().size() + " documents");
                }
            }
        }
    }
}
```

---

## Core Concepts

### Schema

The schema defines the structure of your index - what fields exist and how they're processed.

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Text fields (always indexed, tokenized)
    builder.addTextField("title", true, false, "default", "position");

    // Numeric fields (explicit index control)
    builder.addIntegerField("price", true, true, true);  // stored, indexed, fast
    builder.addFloatField("rating", true, true, false);

    // Date fields
    builder.addDateField("published_date", true, true, true);

    // Boolean fields
    builder.addBooleanField("featured", true, true, false);

    // JSON fields (dynamic schema-less documents)
    Field jsonField = builder.addJsonField("metadata", JsonObjectOptions.storedAndIndexed());

    Schema schema = builder.build();
}
```

**Field Options:**
- **stored** - Can retrieve original value in search results
- **indexed** - Can search/filter on this field (text fields always indexed)
- **fast** - Enables fast access for sorting, aggregations, filtering

### JSON Fields

JSON fields enable schema-less, flexible document structures without predefined fields.

```java
// Add JSON field to schema
Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

// Add nested JSON documents
Document doc = new Document();
Map<String, Object> jsonData = new HashMap<>();
jsonData.put("name", "Alice");
jsonData.put("age", 30);
jsonData.put("email", "alice@example.com");

// Nested objects
Map<String, Object> address = new HashMap<>();
address.put("city", "New York");
address.put("zip", "10001");
jsonData.put("address", address);

// Arrays
jsonData.put("tags", Arrays.asList("developer", "engineer"));

doc.addJson(jsonField, jsonData);
writer.addDocument(doc);
```

**JSON Query Types:**

```java
// Term query on JSON field
Query query = Query.jsonTermQuery(schema, "data", "name", "Alice");

// Range query on numeric JSON field
Query query = Query.jsonRangeQuery(schema, "data", "age", 25L, 35L, true, true);

// Exists query - check if field is present
Query query = Query.jsonExistsQuery(schema, "data", "email");

// Nested path query
Query query = Query.jsonTermQuery(schema, "data", "address.city", "New York");

// Boolean combinations
Query q1 = Query.jsonTermQuery(schema, "data", "name", "Alice");
Query q2 = Query.jsonRangeQuery(schema, "data", "age", 25L, 35L, true, true);
Query combined = Query.booleanQuery(
    Arrays.asList(q1, q2),
    Arrays.asList(Occur.MUST, Occur.MUST));
```

**JSON Field Options:**

```java
// Basic presets
JsonObjectOptions.storedAndIndexed()  // Searchable + retrievable
JsonObjectOptions.indexed()           // Searchable only
JsonObjectOptions.stored()            // Retrievable only
JsonObjectOptions.full()              // Everything enabled

// Advanced configuration
JsonObjectOptions.default()
    .setStored()                      // Enable storage
    .setIndexingOptions(textIndexing) // Configure text indexing
    .setFast(tokenizer)               // Enable fast fields
    .setExpandDotsEnabled()           // Enable dot notation
```

**JSON fields work in both regular indexes and Quickwit split files with full query support.**

### Documents

Documents are collections of field values that match your schema.

```java
// Manual field addition
Document doc = new Document();
doc.addText("title", "Product Title");
doc.addInteger("price", 2999);
doc.addFloat("rating", 4.5f);
doc.addBoolean("featured", true);
doc.addDate("published_date", LocalDateTime.now());

// From map (for JSON-like data)
Map<String, Object> data = Map.of(
    "title", "Product Title",
    "price", 2999,
    "rating", 4.5
);
Document doc = Document.fromMap(data, schema);
```

### Indexing

Add documents to the index for searching.

```java
try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
    // Add single document
    writer.addDocument(doc);

    // Commit to make searchable
    writer.commit();
}

// Reload index to see changes
index.reload();
```

### Searching

Execute queries and retrieve results.

```java
try (Searcher searcher = index.searcher()) {
    Query query = Query.termQuery(schema, "title", "product");
    SearchResult results = searcher.search(query, 10);

    for (SearchResult.Hit hit : results.getHits()) {
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            String title = (String) doc.getFirst("title");
            Double score = hit.getScore();
            System.out.println(title + " (score: " + score + ")");
        }
    }
}
```

---

## Installation

### Prerequisites

- **Java 11 or higher**
- **Maven 3.6+** (for building from source)
- **Rust toolchain** (for building from source)

### Binary Distribution

Download pre-built JARs from releases (includes native libraries for your platform).

### Build from Source

```bash
# Clone repository
git clone https://github.com/indextables/tantivy4java.git
cd tantivy4java

# Build for current platform
mvn clean package

# Run tests
mvn test
```

### Platform-Specific Builds

**Linux (Docker):**
```bash
docker run --rm -v $(pwd):/workspace -w /workspace \
  ubuntu:22.04 sh -c '
    apt-get update && apt-get install -y build-essential curl openjdk-11-jdk maven &&
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&
    export PATH="/root/.cargo/bin:${PATH}" &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    mvn clean package
  '
```

---

## Basic Usage

### 1. Creating Your First Index

```java
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;

public class SearchExample {
    public static void main(String[] args) throws Exception {
        // Define schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("description", true, false, "default", "position")
                   .addIntegerField("year", true, true, true)
                   .addFloatField("rating", true, true, true);

            try (Schema schema = builder.build()) {
                // Create persistent index
                try (Index index = new Index(schema, "/tmp/my_index", false)) {
                    // Index documents
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocument(writer, "The Shawshank Redemption",
                            "Two imprisoned men bond...", 1994, 9.3f);
                        addDocument(writer, "The Godfather",
                            "The aging patriarch...", 1972, 9.2f);
                        addDocument(writer, "The Dark Knight",
                            "When the menace...", 2008, 9.0f);

                        writer.commit();
                    }

                    // Search
                    index.reload();
                    searchMovies(index, schema);
                }
            }
        }
    }

    private static void addDocument(IndexWriter writer, String title,
            String desc, int year, float rating) throws Exception {
        Document doc = new Document();
        doc.addText("title", title);
        doc.addText("description", desc);
        doc.addInteger("year", year);
        doc.addFloat("rating", rating);
        writer.addDocument(doc);
    }

    private static void searchMovies(Index index, Schema schema) throws Exception {
        try (Searcher searcher = index.searcher()) {
            // Simple term search
            try (Query query = Query.termQuery(schema, "title", "godfather")) {
                SearchResult results = searcher.search(query, 10);
                printResults(searcher, results, "Term Search");
            }
        }
    }

    private static void printResults(Searcher searcher, SearchResult results,
            String queryType) throws Exception {
        System.out.println("\n=== " + queryType + " ===");
        for (SearchResult.Hit hit : results.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                String title = (String) doc.getFirst("title");
                Long year = (Long) doc.getFirst("year");
                Double rating = (Double) doc.getFirst("rating");
                System.out.printf("%s (%d) - Rating: %.1f [Score: %.3f]\n",
                    title, year, rating, hit.getScore());
            }
        }
    }
}
```

### 2. Query Types

#### Term Query
Exact term matching (case-sensitive, lowercase for "default" tokenizer):

```java
Query query = Query.termQuery(schema, "title", "godfather");
```

#### Phrase Query
Multi-word sequences with optional slop:

```java
// Exact phrase
Query exactPhrase = Query.phraseQuery(schema, "description",
    List.of("imprisoned", "men"), 0);

// With slop (allows 2 words between)
Query flexiblePhrase = Query.phraseQuery(schema, "description",
    List.of("imprisoned", "bond"), 2);
```

#### Boolean Query
Combine queries with MUST/SHOULD/MUST_NOT:

```java
Query mustHaveGodfather = Query.termQuery(schema, "title", "godfather");
Query shouldBeHighRated = Query.rangeQuery(schema, "rating", "f64",
    new RangeBound("9.0", true), new RangeBound("10.0", true));

Query combined = Query.booleanQuery(
    List.of(mustHaveGodfather),  // MUST clauses
    List.of(shouldBeHighRated),  // SHOULD clauses
    List.of()                     // MUST_NOT clauses
);
```

#### Range Query
Numeric and date ranges:

```java
// Movies from 2000-2010
Query yearRange = Query.rangeQuery(schema, "year", "i64",
    new RangeBound("2000", true),   // inclusive
    new RangeBound("2010", false)); // exclusive

// Highly rated movies
Query ratingRange = Query.rangeQuery(schema, "rating", "f64",
    new RangeBound("9.0", true),
    new RangeBound("10.0", true));
```

#### Fuzzy Query
Edit distance matching:

```java
// Find "godfather" with 1 character difference
Query fuzzy = Query.fuzzyTermQuery(schema, "title", "godfater", 1, true);
```

#### Wildcard Query
Pattern matching with * and ?:

```java
// Case-sensitive wildcard
Query wildcard = Query.wildcardQuery(schema, "title", "*dark*");

// Case-insensitive wildcard
Query caseInsensitive = Query.wildcardQueryCaseInsensitive(schema, "title", "*DARK*");
```

### 3. High-Performance Batch Indexing

For large-scale indexing, use batch operations for 3x better performance:

```java
try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {
    // Automatic batch processing
    try (BatchDocumentBuilder builder = new BatchDocumentBuilder(writer)) {
        for (int i = 0; i < 1_000_000; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Movie " + i);
            doc.addText("description", "Description for movie number " + i);
            doc.addInteger("year", 2000 + (i % 24));
            doc.addFloat("rating", 5.0f + (i % 50) / 10.0f);

            builder.addDocument(doc);

            // Optional: Manual flush every 10K docs
            if (builder.getDocumentCount() >= 10_000) {
                builder.flush();
            }
        }
        // Auto-flush on close
    }

    writer.commit();
}
```

**Performance Tips:**
- Use `Index.Memory.LARGE_HEAP_SIZE` or `XL_HEAP_SIZE` for bulk operations
- Increase thread count (4-8 threads) for parallel indexing
- Flush every 5K-10K documents for memory management
- Use try-with-resources for automatic cleanup

---

## Advanced Features

### Schema Introspection

Discover schema structure at runtime:

```java
// Get all fields
List<String> allFields = schema.getFieldNames();
int fieldCount = schema.getFieldCount();

// Check field existence
boolean hasTitle = schema.hasField("title");

// Filter by capabilities
List<String> storedFields = schema.getStoredFieldNames();
List<String> indexedFields = schema.getIndexedFieldNames();
List<String> fastFields = schema.getFastFieldNames();

// Filter by type
List<String> textFields = schema.getFieldNamesByType("text");
List<String> integerFields = schema.getFieldNamesByType("i64");
List<String> floatFields = schema.getFieldNamesByType("f64");

// Advanced filtering (stored=true, indexed=true, fast=false)
List<String> searchableStored = schema.getFieldNamesByCapabilities(true, true, false);

// Get comprehensive schema info
String summary = schema.getSchemaSummary();
System.out.println(summary);
```

### Text Analysis and Tokenization

Understand how text is processed:

```java
import io.indextables.tantivy4java.util.TextAnalyzer;

// Static tokenization
String text = "Hello World! This is a TEST.";

List<String> defaultTokens = TextAnalyzer.tokenize(text);
// Output: [hello, world, this, is, a, test]

List<String> simpleTokens = TextAnalyzer.tokenize(text, "simple");
// Output: [hello, world, this, is, a, test]

List<String> whitespaceTokens = TextAnalyzer.tokenize(text, "whitespace");
// Output: [Hello, World!, This, is, a, TEST.]

List<String> keywordTokens = TextAnalyzer.tokenize(text, "keyword");
// Output: [Hello World! This is a TEST.]

// Instance-based analysis (reusable)
try (TextAnalyzer analyzer = TextAnalyzer.create("default")) {
    List<String> tokens1 = analyzer.analyze("First document");
    List<String> tokens2 = analyzer.analyze("Second document");
}
```

**Tokenizer Types:**
- **default** - Lowercase + stopword removal + alphanumeric splitting
- **simple** - Lowercase + alphanumeric splitting
- **whitespace** - Lowercase + whitespace splitting only
- **keyword** - Entire input as single token (no splitting)
- **raw** - Minimal processing

**Important:** When searching with term queries on fields using "default" tokenizer, lowercase your search terms to match indexed tokens.

### Index Optimization

Merge segments for better search performance:

```java
try (Searcher searcher = index.searcher()) {
    // Get current segments
    List<String> segmentIds = searcher.getSegmentIds();
    System.out.println("Current segments: " + segmentIds.size());

    if (segmentIds.size() > 1) {
        // Merge first two segments
        List<String> toMerge = segmentIds.subList(0, 2);

        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            SegmentMeta result = writer.merge(toMerge);

            System.out.println("New segment ID: " + result.getSegmentId());
            System.out.println("Documents: " + result.getMaxDoc());
            System.out.println("Deleted docs: " + result.getNumDeletedDocs());

            writer.commit();
        }

        index.reload();
    }
}
```

**When to merge:**
- After large batch operations
- When segment count exceeds 10-20
- During off-peak hours for production systems

---

## Quickwit Integration

Tantivy4Java includes first-class support for Quickwit splits, enabling distributed search across large datasets.

### What are Splits?

Splits are self-contained, immutable index segments optimized for distributed search. Key benefits:

- **Distributed** - Store and search across multiple nodes
- **Cloud-Native** - Store in S3/MinIO for scalable architectures
- **Efficient** - Optimized footer metadata for fast opening
- **Mergeable** - Combine splits without re-indexing

### Creating Splits

Convert a Tantivy index to a Quickwit split:

```java
import io.indextables.tantivy4java.split.merge.*;

// Configure split metadata
QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
    "movies-index-v1",    // index UID
    "imdb-source",        // source ID
    "indexer-node-1"      // node ID
);

// Convert index to split
QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
    "/tmp/my_index",           // input index path
    "/tmp/movies.split",       // output split path
    config
);

System.out.println("Created split: " + metadata.getSplitId());
System.out.println("Documents: " + metadata.getNumDocs());
System.out.println("Size: " + metadata.getUncompressedSizeBytes() + " bytes");

// Validate split integrity
boolean valid = QuickwitSplit.validateSplit("/tmp/movies.split");
System.out.println("Split valid: " + valid);
```

### Searching Splits

Use SplitSearcher for efficient split file searching:

```java
import io.indextables.tantivy4java.split.*;

// Create cache manager (shared across searches)
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("main-cache")
        .withMaxCacheSize(500_000_000);  // 500MB cache

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
    // Open split searcher
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
            "file:///tmp/movies.split")) {

        // Get schema from split
        Schema schema = searcher.getSchema();

        // Search split
        SplitQuery query = new SplitTermQuery("title", "godfather");
        SearchResult results = searcher.search(query, 10);

        // Process results
        for (SearchResult.Hit hit : results.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                String title = (String) doc.getFirst("title");
                System.out.println("Found: " + title);
            }
        }
    }
}
```

### JSON Fields in Splits

JSON fields work seamlessly in Quickwit splits, enabling flexible schema-less documents:

```java
// 1. Create index with JSON fields
try (SchemaBuilder builder = new SchemaBuilder()) {
    Field jsonField = builder.addJsonField("metadata", JsonObjectOptions.full());

    try (Schema schema = builder.build();
         Index index = new Index(schema, "/tmp/json_index", false)) {

        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add JSON documents
            Document doc = new Document();
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("product", "laptop");
            metadata.put("price", 999.99);
            metadata.put("category", "electronics");
            doc.addJson(jsonField, metadata);
            writer.addDocument(doc);
            writer.commit();
        }

        // Convert to split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "products", "catalog", "node-1");
        QuickwitSplit.convertIndexFromPath(
            "/tmp/json_index", "/tmp/products.split", config);
    }
}

// 2. Search JSON fields in split
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("products-cache")
        .withMaxCacheSize(100_000_000);

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
     SplitSearcher searcher = cacheManager.createSplitSearcher(
         "file:///tmp/products.split")) {

    Schema schema = searcher.getSchema();

    // Query JSON fields (all query types supported)
    SplitQuery query = new SplitTermQuery("metadata", "category", "electronics");
    SearchResult results = searcher.search(query, 10);

    // Retrieve JSON data
    for (SearchResult.Hit hit : results.getHits()) {
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            Map<String, Object> metadata = doc.getJsonMap("metadata");
            System.out.println("Product: " + metadata.get("product"));
            System.out.println("Price: " + metadata.get("price"));
        }
    }
}
```

**All JSON query types work in splits:** term queries, range queries, exists queries, nested paths, and boolean combinations.

### Merging Splits

#### Local Split Merging

Combine multiple splits efficiently:

```java
List<String> splitPaths = List.of(
    "/tmp/split-2024-01.split",
    "/tmp/split-2024-02.split",
    "/tmp/split-2024-03.split"
);

QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
    "movies-merged",
    "imdb-source",
    "merge-node"
);

QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
    splitPaths,
    "/tmp/merged-2024-q1.split",
    mergeConfig
);

System.out.println("Merged " + splitPaths.size() + " splits");
System.out.println("Total docs: " + merged.getNumDocs());
```

#### S3 Remote Split Merging

Merge splits stored in S3 without downloading:

```java
// Configure AWS credentials
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "AKIA...",           // access key
    "secret-key",        // secret key
    "us-east-1"          // region
);

QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
    "movies-distributed", "s3-source", "worker-1", awsConfig);

// S3 split URLs
List<String> s3Splits = List.of(
    "s3://my-bucket/splits/movies-01.split",
    "s3://my-bucket/splits/movies-02.split",
    "s3://my-bucket/splits/movies-03.split"
);

// Merge remote splits
QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
    s3Splits,
    "/tmp/merged-movies.split",  // local output
    config
);

System.out.println("Merged " + s3Splits.size() + " S3 splits");
System.out.println("Total documents: " + merged.getNumDocs());
```

#### Advanced S3 Configuration

```java
// Temporary credentials with session token
QuickwitSplit.AwsConfig sessionConfig = new QuickwitSplit.AwsConfig(
    "ASIA...",              // temp access key
    "temp-secret",          // temp secret key
    "session-token",        // STS token
    "us-west-2",            // region
    "https://minio.example.com",  // custom endpoint
    true                    // force path style (for MinIO)
);

// Mix local and remote splits
List<String> mixedSplits = List.of(
    "/local/split-01.split",                    // local file
    "file:///shared/storage/split-02.split",    // file URL
    "s3://bucket-a/split-03.split",             // S3 URL
    "s3://bucket-b/split-04.split"              // different bucket
);

QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
    "mixed-index", "multi-source", "worker-2", sessionConfig);

QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
    mixedSplits, "/tmp/final-merged.split", config);
```

### Searching S3 Splits

Search splits directly from S3:

```java
// Configure cache with AWS credentials
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("s3-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials("AKIA...", "secret-key")
        .withAwsRegion("us-east-1");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(config)) {
    // Open S3 split
    String s3Url = "s3://my-bucket/splits/movies.split";
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(s3Url)) {
        // Validate before searching
        if (!searcher.validateSplit()) {
            System.err.println("Split validation failed");
            return;
        }

        // Get metadata
        QuickwitSplit.SplitMetadata metadata = searcher.getSplitMetadata();
        System.out.println("Searching " + metadata.getNumDocs() + " documents");

        // Search
        Schema schema = searcher.getSchema();
        SplitQuery query = new SplitTermQuery("title", "matrix");
        SearchResult results = searcher.search(query, 20);

        System.out.println("Found " + results.getHits().size() + " results");
    }
}
```

### Azure Blob Storage Support

Tantivy4Java provides complete Azure Blob Storage support with feature parity to AWS S3.

#### Creating Azure Configurations

```java
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

// Basic Azure configuration with account name and key
QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
    "mystorageaccount",  // Azure storage account name
    "account-key"        // Azure storage account access key
);

// Azure with custom endpoint (for Azurite testing or custom domains)
QuickwitSplit.AzureConfig azuriteConfig = new QuickwitSplit.AzureConfig(
    "devstoreaccount1",  // account name
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",  // well-known key
    "http://127.0.0.1:10000/devstoreaccount1"  // Azurite endpoint
);

// Azure from connection string
QuickwitSplit.AzureConfig connStringConfig = QuickwitSplit.AzureConfig.fromConnectionString(
    "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key;EndpointSuffix=core.windows.net"
);

// Azure with OAuth Bearer Token (for Azure Active Directory / Managed Identity)
QuickwitSplit.AzureConfig oauthConfig = QuickwitSplit.AzureConfig.withBearerToken(
    "mystorageaccount",  // Azure storage account name
    "eyJ0eXAiOiJKV1QiLCJhbGc..."  // OAuth 2.0 bearer token from Azure AD
);
```

#### Searching Azure Blob Storage Splits

```java
// Configure cache with Azure account key credentials
SplitCacheManager.CacheConfig azureCache =
    new SplitCacheManager.CacheConfig("azure-cache")
        .withMaxCacheSize(500_000_000)
        .withAzureCredentials("myaccount", "account-key");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(azureCache)) {
    // Open Azure split
    String azureUrl = "azure://my-container/splits/movies.split";
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(azureUrl)) {
        // Validate and search just like S3
        Schema schema = searcher.getSchema();
        SplitQuery query = new SplitTermQuery("title", "interstellar");
        SearchResult results = searcher.search(query, 10);

        System.out.println("Found " + results.getHits().size() + " results in Azure");
    }
}

// Alternative: Configure cache with OAuth Bearer Token (Azure AD / Managed Identity)
SplitCacheManager.CacheConfig oauthCache =
    new SplitCacheManager.CacheConfig("azure-oauth-cache")
        .withMaxCacheSize(500_000_000)
        .withAzureBearerToken("myaccount", "eyJ0eXAiOiJKV1QiLCJhbGc...");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(oauthCache)) {
    // OAuth-authenticated access to Azure splits
    try (SplitSearcher searcher = cacheManager.createSplitSearcher("azure://container/split.split")) {
        // Same search operations as account key authentication
    }
}
```

#### Merging Azure Blob Storage Splits

```java
// Configure Azure for merge operations with account key
QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
    "mystorageaccount",
    "account-key"
);

QuickwitSplit.MergeConfig mergeConfig = QuickwitSplit.MergeConfig.builder()
    .indexUid("movies-azure")
    .sourceId("azure-source")
    .nodeId("merge-node-1")
    .azureConfig(azureConfig)
    .build();

// Azure split URLs
List<String> azureSplits = List.of(
    "azure://my-container/splits/movies-01.split",
    "azure://my-container/splits/movies-02.split",
    "azure://my-container/splits/movies-03.split"
);

// Merge Azure splits
QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
    azureSplits,
    "/tmp/merged-azure.split",  // local output
    mergeConfig
);

System.out.println("Merged " + azureSplits.size() + " Azure splits");
System.out.println("Total documents: " + merged.getNumDocs());

// Alternative: Merge with OAuth Bearer Token authentication
QuickwitSplit.AzureConfig oauthConfig = QuickwitSplit.AzureConfig.withBearerToken(
    "mystorageaccount",
    "eyJ0eXAiOiJKV1QiLCJhbGc..."  // OAuth token from Azure AD
);

QuickwitSplit.MergeConfig oauthMergeConfig = QuickwitSplit.MergeConfig.builder()
    .indexUid("movies-azure-oauth")
    .sourceId("azure-oauth-source")
    .nodeId("merge-node-1")
    .azureConfig(oauthConfig)
    .build();

// Merge using OAuth authentication
QuickwitSplit.SplitMetadata oauthMerged = QuickwitSplit.mergeSplits(
    azureSplits,
    "/tmp/merged-azure-oauth.split",
    oauthMergeConfig
);
```

#### Multi-Cloud Configuration (AWS + Azure)

```java
// Configure both AWS and Azure credentials for hybrid deployments
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "AKIA...",
    "aws-secret",
    "us-east-1"
);

QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
    "azureaccount",
    "azure-key"
);

// Use Builder pattern for clean multi-cloud configuration
QuickwitSplit.MergeConfig hybridConfig = QuickwitSplit.MergeConfig.builder()
    .indexUid("hybrid-index")
    .sourceId("multi-cloud")
    .nodeId("worker-1")
    .awsConfig(awsConfig)      // AWS configuration
    .azureConfig(azureConfig)  // Azure configuration
    .build();

// Mix AWS S3 and Azure Blob Storage splits in same operation
List<String> multiCloudSplits = List.of(
    "s3://aws-bucket/split-01.split",
    "azure://azure-container/split-02.split",
    "/local/split-03.split"
);

QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
    multiCloudSplits,
    "/tmp/multi-cloud-merged.split",
    hybridConfig
);
```

#### Testing with Azurite

For local development and testing, use Azurite (Azure Storage Emulator):

```java
import io.indextables.tantivy4java.AzuriteContainer;

// Start Azurite container
AzuriteContainer azurite = new AzuriteContainer();
azurite.start();

// Configure with Azurite endpoint
QuickwitSplit.AzureConfig azuriteConfig = new QuickwitSplit.AzureConfig(
    AzuriteContainer.ACCOUNT,  // "devstoreaccount1"
    AzuriteContainer.KEY,       // well-known emulator key
    azurite.endpoint()          // http://localhost:xxxxx/devstoreaccount1
);

// Use for testing
SplitCacheManager.CacheConfig testCache =
    new SplitCacheManager.CacheConfig("test-cache")
        .withAzureCredentials(AzuriteContainer.ACCOUNT, AzuriteContainer.KEY)
        .withAzureEndpoint(azurite.endpoint());

// Test your Azure integration without real cloud costs
try (SplitCacheManager manager = SplitCacheManager.getInstance(testCache)) {
    // Your test code here
}

// Cleanup
azurite.stop();
```

### Aggregations

Tantivy4Java provides comprehensive aggregation support for analytics and faceted search on split files.

#### Terms Aggregation

Group documents by field values (categorical data):

```java
TermsAggregation termsAgg = new TermsAggregation("categories", "category");

SearchResult result = searcher.search(query, 10, "categories", termsAgg);
TermsResult termsResult = (TermsResult) result.getAggregation("categories");

for (TermsResult.TermBucket bucket : termsResult.getBuckets()) {
    System.out.println(bucket.getKey() + ": " + bucket.getDocCount() + " docs");
}
```

#### Histogram Aggregation

Create numeric buckets with fixed intervals:

```java
HistogramAggregation histAgg = new HistogramAggregation("price_dist", "price", 100.0)
    .setOffset(25.0)           // Shift bucket boundaries
    .setMinDocCount(1)         // Only buckets with docs
    .setHardBounds(0.0, 1000.0)    // Limit bucket range
    .setExtendedBounds(0.0, 2000.0); // Force bucket creation

SearchResult result = searcher.search(query, 10, "price_dist", histAgg);
HistogramResult histResult = (HistogramResult) result.getAggregation("price_dist");

for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
    System.out.println("Bucket " + bucket.getKey() + ": " + bucket.getDocCount() + " docs");
}
```

#### Date Histogram Aggregation

Time-series bucketing with fixed intervals:

```java
// Supported intervals: 1ms, 10ms, 100ms, 1s, 30s, 1m, 30m, 1h, 6h, 1d, 7d
DateHistogramAggregation dateAgg = new DateHistogramAggregation("daily", "timestamp", "1d")
    .setMinDocCount(1)
    .setOffset("6h");  // Shift day boundaries by 6 hours

SearchResult result = searcher.search(query, 10, "daily", dateAgg);
DateHistogramResult dateResult = (DateHistogramResult) result.getAggregation("daily");

for (DateHistogramResult.DateHistogramBucket bucket : dateResult.getBuckets()) {
    System.out.println(bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
}
```

#### Range Aggregation

Custom range buckets for numeric fields:

```java
RangeAggregation rangeAgg = new RangeAggregation("price_ranges", "price")
    .addRange("cheap", null, 100.0)      // < 100
    .addRange("medium", 100.0, 500.0)    // 100-500
    .addRange("expensive", 500.0, null); // >= 500

SearchResult result = searcher.search(query, 10, "price_ranges", rangeAgg);
RangeResult rangeResult = (RangeResult) result.getAggregation("price_ranges");

for (RangeResult.RangeBucket bucket : rangeResult.getBuckets()) {
    System.out.println(bucket.getKey() + ": " + bucket.getDocCount() + " docs");
}
```

#### Multiple Aggregations

Run multiple aggregations in a single query:

```java
Map<String, SplitAggregation> aggs = new HashMap<>();
aggs.put("categories", new TermsAggregation("categories", "category"));
aggs.put("price_hist", new HistogramAggregation("price_hist", "price", 100.0));
aggs.put("price_ranges", new RangeAggregation("price_ranges", "price")
    .addRange("low", null, 100.0)
    .addRange("high", 100.0, null));

SearchResult result = searcher.search(query, 10, aggs);

TermsResult categories = (TermsResult) result.getAggregation("categories");
HistogramResult priceHist = (HistogramResult) result.getAggregation("price_hist");
RangeResult priceRanges = (RangeResult) result.getAggregation("price_ranges");
```

#### Sub-Aggregations

Nest aggregations within bucket aggregations:

```java
HistogramAggregation histAgg = new HistogramAggregation("price_hist", "price", 500.0)
    .addSubAggregation(new StatsAggregation("qty_stats", "quantity"))
    .addSubAggregation(new AverageAggregation("avg_rating", "rating"));

SearchResult result = searcher.search(query, 10, "price_hist", histAgg);
HistogramResult histResult = (HistogramResult) result.getAggregation("price_hist");

for (HistogramResult.HistogramBucket bucket : histResult.getBuckets()) {
    StatsResult stats = bucket.getSubAggregation("qty_stats", StatsResult.class);
    if (stats != null) {
        System.out.println("Bucket " + bucket.getKey() +
            " - Avg quantity: " + stats.getAverage());
    }
}
```

For comprehensive documentation, see [Bucket Aggregation Developer Guide](docs/BUCKET_AGGREGATION_GUIDE.md).

### Cache Management

Monitor and control split cache performance:

```java
// Get cache statistics
SplitSearcher.CacheStats stats = searcher.getCacheStats();
System.out.println("Cache hits: " + stats.getHits());
System.out.println("Cache misses: " + stats.getMisses());
System.out.println("Evictions: " + stats.getEvictions());
System.out.println("Size: " + stats.getSizeBytes() + " bytes");

// Check what's cached
Map<String, Boolean> status = searcher.getComponentCacheStatus();
status.forEach((component, cached) ->
    System.out.println(component + ": " + (cached ? "cached" : "not cached")));

// Evict components to free memory
searcher.evictComponents(List.of("postings"));
```

### Component Prewarming

Prewarming loads index components into the cache before queries execute, eliminating cache misses and reducing latency:

```java
try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
    // Prewarm all search-critical components
    searcher.preloadComponents(
        SplitSearcher.IndexComponent.TERM,      // Term dictionaries (FST)
        SplitSearcher.IndexComponent.POSTINGS,  // Posting lists
        SplitSearcher.IndexComponent.FIELDNORM, // Field norms for scoring
        SplitSearcher.IndexComponent.FASTFIELD, // Fast fields for sorting/filtering
        SplitSearcher.IndexComponent.STORE      // Document storage for retrieval
    ).join();

    // Now all queries have zero cache misses for prewarmed components
    SearchResult result = searcher.search(query, 10);
}
```

**Available Components:**

| Component | Description | When to Prewarm |
|-----------|-------------|-----------------|
| `TERM` | Term dictionaries (FST) | Diverse term queries, autocomplete, aggregations |
| `POSTINGS` | Term posting lists | High-volume term queries |
| `FIELDNORM` | Field norm data for scoring | Relevance scoring optimization |
| `FASTFIELD` | Fast field data for sorting/filtering | Range queries, sorting, faceting |
| `STORE` | Document storage (stored field data) | Document retrieval heavy workloads |

**Recommended Prewarm Strategies:**

```java
// Search workloads (without document retrieval)
searcher.preloadComponents(
    SplitSearcher.IndexComponent.TERM,
    SplitSearcher.IndexComponent.POSTINGS,
    SplitSearcher.IndexComponent.FIELDNORM,
    SplitSearcher.IndexComponent.FASTFIELD
).join();

// Complete prewarm (including document retrieval)
searcher.preloadComponents(
    SplitSearcher.IndexComponent.TERM,
    SplitSearcher.IndexComponent.POSTINGS,
    SplitSearcher.IndexComponent.FIELDNORM,
    SplitSearcher.IndexComponent.FASTFIELD,
    SplitSearcher.IndexComponent.STORE
).join();

// Async prewarm while doing other initialization
CompletableFuture<Void> prewarmFuture = searcher.preloadComponents(
    SplitSearcher.IndexComponent.TERM
);
initializeOtherComponents();  // Do other work
prewarmFuture.join();         // Wait for prewarm
```

**Validating Prewarm Effectiveness:**

```java
// Before prewarm
var statsBeforePrewarm = searcher.getCacheStats();
long missesBeforePrewarm = statsBeforePrewarm.getMissCount();

// Prewarm
searcher.preloadComponents(SplitSearcher.IndexComponent.TERM).join();

// Query
searcher.search(query, 10);

// Verify zero new cache misses
var statsAfterQuery = searcher.getCacheStats();
long newMisses = statsAfterQuery.getMissCount() - missesBeforePrewarm;
assert newMisses == 0 : "Prewarm should eliminate cache misses";
```

For detailed documentation, see [Term Prewarm Developer Guide](docs/TERM_PREWARM_DEVELOPER_GUIDE.md).

#### Field-Specific Prewarm

For finer control, you can prewarm specific fields instead of entire components:

```java
// Prewarm only specific fields for fast field access
searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "score").join();
searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "timestamp").join();

// Prewarm fieldnorms for specific text fields
searcher.preloadFields(SplitSearcher.IndexComponent.FIELDNORM, "title").join();
searcher.preloadFields(SplitSearcher.IndexComponent.FIELDNORM, "content").join();
```

This is useful when you only need certain fields for your query workload and want to minimize cache usage.

### Per-Field Component Sizes

Get precise size information at the field+component level for capacity planning and prewarm validation:

```java
// Get per-field component sizes
Map<String, Long> fieldSizes = searcher.getPerFieldComponentSizes();

// Example output:
// score.fastfield: 107 bytes
// id.fastfield: 30 bytes
// title.fieldnorm: 100 bytes
// content.fieldnorm: 100 bytes

for (Map.Entry<String, Long> entry : fieldSizes.entrySet()) {
    System.out.println(entry.getKey() + ": " + entry.getValue() + " bytes");
}

// Validate prewarm operations
long expectedSize = fieldSizes.getOrDefault("score.fastfield", 0L);
long bytesBefore = cacheManager.getComprehensiveCacheStats().getByteRangeCache().getSizeBytes();
searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "score").join();
long bytesAfter = cacheManager.getComprehensiveCacheStats().getByteRangeCache().getSizeBytes();
long downloaded = bytesAfter - bytesBefore;

assert downloaded > 0 : "Prewarm should download data";
assert downloaded <= expectedSize * 2 : "Downloaded bytes should be bounded";
```

For detailed component sizes documentation, see [Component Sizes Developer Guide](docs/COMPONENT_SIZES_DEVELOPER_GUIDE.md).

### SearcherCache Monitoring

The internal SearcherCache is an LRU cache (default: 1000 entries) that stores Tantivy searcher objects for efficient document retrieval. Monitor these statistics to detect memory issues and optimize cache performance:

```java
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitCacheManager.SearcherCacheStats;

// Get searcher cache statistics
SearcherCacheStats stats = SplitCacheManager.getSearcherCacheStats();

System.out.println("📊 SearcherCache Performance:");
System.out.println("  Cache Hit Rate: " + stats.getHitRate() + "%");
System.out.println("  Total Hits: " + stats.getHits());
System.out.println("  Total Misses: " + stats.getMisses());
System.out.println("  Total Evictions: " + stats.getEvictions());

// Detect potential issues
if (stats.getHitRate() < 50.0) {
    System.out.println("⚠️ Low cache hit rate - workload may have poor locality");
}

if (stats.getEvictions() > 1000) {
    System.out.println("⚠️ High eviction count - cache thrashing detected");
}

// Reset statistics for next monitoring period
SplitCacheManager.resetSearcherCacheStats();
```

**Memory Leak Prevention:**

The SearcherCache uses LRU eviction to prevent unbounded memory growth that was present in earlier versions:

```java
// OLD: Unbounded HashMap (memory leak risk ❌)
// Cache would grow forever, never evict entries
// Could cause OutOfMemoryError in production

// NEW: LRU Cache (memory safe ✅)
// - Default: 1000 entries maximum
// - Automatic eviction when full
// - Statistics track evictions for monitoring
// - Safe for long-running production deployments
```

**Key Metrics:**

- **Cache Hit Rate** - Target >90% for workloads with repeated document access
- **Evictions** - Should be minimal for stable workloads; high evictions indicate cache size too small
- **Hits vs Misses** - Ratio indicates cache effectiveness and access pattern locality

---

## L2 Tiered Disk Cache

The disk cache provides persistent caching between the application and remote storage (S3/Azure), dramatically reducing latency and cloud egress costs.

### Quick Configuration

```java
import io.indextables.tantivy4java.split.*;

// Configure disk cache
SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
    .withDiskCachePath("/mnt/nvme/tantivy_cache")  // Fast SSD recommended
    .withMaxDiskSize(100_000_000_000L)              // 100GB limit
    .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

// Create cache manager with disk cache
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("production-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials(accessKey, secretKey)
        .withAwsRegion("us-east-1")
        .withTieredCache(tieredConfig);

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
        // First search: cache miss → S3 download → populate disk cache
        SearchResult result1 = searcher.search(query, 10);

        // Subsequent searches: cache HIT (fast)
        SearchResult result2 = searcher.search(query, 10);
    }
}

// After JVM restart: disk cache HIT → no S3 download needed
```

### Cache Configuration Options

| Method | Default | Description |
|--------|---------|-------------|
| `withDiskCachePath(path)` | *required* | Directory for cache files |
| `withMaxDiskSize(bytes)` | 0 | Max cache size; 0 = auto (2/3 available disk) |
| `withCompression(algo)` | LZ4 | Compression: `LZ4`, `ZSTD`, or `NONE` |
| `withMinCompressSize(bytes)` | 4096 | Skip compression below this threshold |
| `withManifestSyncInterval(secs)` | 30 | How often to persist manifest to disk |

### Compression Algorithms

| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| `LZ4` | ~400 MB/s | 50-70% | Default, best balance |
| `ZSTD` | ~150 MB/s | 60-80% | Cold data, max compression |
| `NONE` | N/A | 0% | Pre-compressed data, CPU-limited |

### Cache Statistics

```java
SplitCacheManager.DiskCacheStats stats = cacheManager.getDiskCacheStats();
if (stats != null) {
    System.out.println("Total bytes: " + stats.getTotalBytes());
    System.out.println("Max bytes: " + stats.getMaxBytes());
    System.out.println("Usage: " + stats.getUsagePercent() + "%");
    System.out.println("Splits cached: " + stats.getSplitCount());
    System.out.println("Components cached: " + stats.getComponentCount());
}
```

### Performance Characteristics

| Storage Layer | Typical Latency | Notes |
|---------------|-----------------|-------|
| Disk Cache (NVMe) | 1-5ms | Local I/O, decompression |
| S3/Azure | 50-200ms | Network RTT + transfer |

**Key features:**
- **Non-blocking writes** - Searches complete immediately; cache population happens in background
- **LRU eviction** - Automatic eviction at 95% capacity (to 90%)
- **Smart compression** - Automatically skips small data and numeric fields
- **Crash recovery** - Manifest persistence with backup for recovery
- **Cross-process persistence** - Cache survives JVM restarts with zero re-downloads

### Cross-Process Disk Cache Persistence

The L2 disk cache properly persists data across JVM restarts. Data prewarmed in one process is immediately available to subsequent processes without re-downloading from S3/Azure:

```java
// Process 1: Prewarm and populate disk cache
SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
    .withDiskCachePath("/shared/disk/cache")
    .withMaxDiskSize(100_000_000_000L);

SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("cache")
    .withMaxCacheSize(500_000_000)
    .withAwsCredentials(accessKey, secretKey)
    .withAwsRegion("us-east-1")
    .withTieredCache(tieredConfig);

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {
        // Prewarm downloads from S3/Azure and writes to disk cache
        searcher.preloadComponents(
            SplitSearcher.IndexComponent.TERM,
            SplitSearcher.IndexComponent.POSTINGS,
            SplitSearcher.IndexComponent.FASTFIELD
        ).join();  // join() ensures manifest is synced to disk
    }
}
// Process exits - disk cache persists on disk

// Process 2 (after JVM restart): Queries served from disk cache
try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {
        // ZERO S3/Azure downloads - all data served from disk cache!
        SearchResult result = searcher.search(query, 10);
    }
}
```

**Important notes:**
- Always call `.join()` after `preloadComponents()` to ensure manifest is synced to disk
- Use the same `diskCachePath` across processes to share cached data
- Prewarm writes only to L2 disk cache (not L1 memory) to prevent OOM on large tables

For detailed documentation, see [L2 Disk Cache Developer Guide](docs/L2_DISK_CACHE_GUIDE.md) and [Cross-Process Fix](docs/DISK_CACHE_CROSS_PROCESS_FIX.md).

---

## Performance Guide

### Memory Configuration

Use appropriate heap sizes for your workload:

```java
// Minimum (15MB) - small indices, testing
Index.Memory.MIN_HEAP_SIZE

// Default (50MB) - typical workloads
Index.Memory.DEFAULT_HEAP_SIZE

// Large (128MB) - bulk operations
Index.Memory.LARGE_HEAP_SIZE

// Extra Large (256MB) - very large indices
Index.Memory.XL_HEAP_SIZE

// Custom size
long customHeapSize = 200_000_000L;  // 200MB
```

### Index Design

**Field Configuration:**
```java
// Text fields: stored=true for retrieval, fast=false (not needed)
builder.addTextField("title", true, false, "default", "position");

// Numeric fields for filtering: indexed=true, fast=true
builder.addIntegerField("price", true, true, true);

// Numeric fields for sorting: fast=true, indexed can be false
builder.addFloatField("rating", true, false, true);

// Fields only for display: stored=true, indexed=false, fast=false
builder.addTextField("description", true, false, "keyword", "");
```

### Batch Indexing

**Optimal batch configuration:**
```java
try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {
    try (BatchDocumentBuilder builder = new BatchDocumentBuilder(writer)) {
        for (Document doc : documents) {
            builder.addDocument(doc);

            // Flush every 5K-10K documents
            if (builder.getDocumentCount() >= 5_000) {
                builder.flush();
            }
        }
    }
    writer.commit();
}
```

**Key points:**
- Use 4-8 threads for parallel indexing
- Flush every 5K-10K documents
- Use `LARGE_HEAP_SIZE` or `XL_HEAP_SIZE`
- Commit once at the end

### Search Performance

**Fast searches:**
- Use fast fields for filtering and sorting
- Merge segments regularly (keep count < 20)
- Use appropriate cache sizes for splits
- Preload frequently accessed components

**Query optimization:**
- Use term queries for exact matches
- Avoid wildcards on large fields
- Use fast fields for range filters
- Combine with boolean queries for complex logic

### Split Architecture

**Best practices:**
- Split size: 1-10GB per split (optimal for distributed search)
- Merge periodically to reduce split count
- Use S3 for storage, local cache for performance
- Configure appropriate cache sizes (500MB-2GB per searcher)

---

## Developer Guide

### Error Handling

```java
try (Index index = new Index(schema, path)) {
    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
        writer.addDocument(doc);
        writer.commit();
    } catch (RuntimeException e) {
        System.err.println("Indexing failed: " + e.getMessage());
        e.printStackTrace();
    }
} catch (Exception e) {
    System.err.println("Index creation failed: " + e.getMessage());
}
```

### Resource Management

**Always use try-with-resources:**
```java
// Automatic cleanup
try (Schema schema = builder.build();
     Index index = new Index(schema, path);
     IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
     Document doc = new Document()) {
    // Use resources
}
// Automatically closed in reverse order
```

### Thread Safety

- **Index**: Thread-safe for reading
- **IndexWriter**: Not thread-safe (use single writer per index)
- **Searcher**: Thread-safe for searching
- **SplitCacheManager**: Thread-safe (designed for shared use)

### Debugging

Enable debug logging:
```bash
export TANTIVY4JAVA_DEBUG=1
mvn test -Dtest=YourTest
```

Query debugging:
```java
Query query = Query.booleanQuery(...);
System.out.println("Query: " + query.toString());
```

### Testing

```java
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

class SearchTest {
    @Test
    void testSearch(@TempDir Path tempDir) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                String indexPath = tempDir.resolve("test_index").toString();
                try (Index index = new Index(schema, indexPath, false)) {
                    // Test indexing
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        Document doc = new Document();
                        doc.addText("title", "test document");
                        writer.addDocument(doc);
                        writer.commit();
                    }

                    // Test searching
                    index.reload();
                    try (Searcher searcher = index.searcher()) {
                        Query query = Query.termQuery(schema, "title", "test");
                        SearchResult results = searcher.search(query, 10);
                        assertEquals(1, results.getHits().size());
                    }
                }
            }
        }
    }
}
```

---

## API Reference

### Core Classes

**io.indextables.tantivy4java.core**
- `Index` - Main index interface
- `IndexWriter` - Document indexing with batch support
- `Searcher` - Search execution
- `Schema` - Index schema with introspection
- `SchemaBuilder` - Schema construction
- `Document` - Document representation
- `DocAddress` - Document location reference
- `FieldType` - Field type enumeration
- `SegmentMeta` - Segment metadata

**io.indextables.tantivy4java.query**
- `Query` - Query construction utilities
- `Range` - Range bounds for queries
- `RangeBound` - Range boundary specification
- `Occur` - Boolean query occurrence (MUST/SHOULD/MUST_NOT)
- `Order` - Sort order enumeration
- `Snippet` - Text snippet for highlighting
- `SnippetGenerator` - Snippet generation
- `Explanation` - Query score explanation

**io.indextables.tantivy4java.result**
- `SearchResult` - Search result container
- `SearchResult.Hit` - Individual search hit

**io.indextables.tantivy4java.batch**
- `BatchDocument` - High-performance document for batch operations
- `BatchDocumentBuilder` - AutoCloseable batch builder

**io.indextables.tantivy4java.split**
- `SplitSearcher` - Split file search operations
- `SplitCacheManager` - Shared cache management
- `SplitQuery` - Base for split queries
- `SplitTermQuery` - Term query for splits
- `SplitBooleanQuery` - Boolean query for splits
- `SplitRangeQuery` - Range query for splits
- `SplitPhraseQuery` - Phrase query for splits
- `SplitMatchAllQuery` - Match all query for splits
- `SplitParsedQuery` - Parsed query for splits

**io.indextables.tantivy4java.split.merge**
- `QuickwitSplit` - Split conversion and merging
- `QuickwitSplit.SplitConfig` - Split configuration
- `QuickwitSplit.SplitMetadata` - Split metadata
- `QuickwitSplit.MergeConfig` - Merge configuration (with Builder pattern)
- `QuickwitSplit.AwsConfig` - AWS S3 credential configuration
- `QuickwitSplit.AzureConfig` - Azure Blob Storage credential configuration
- `QuickwitSplitInspector` - Split inspection utilities

**io.indextables.tantivy4java.aggregation**
- `TermsAggregation` - Terms/categorical aggregation
- `HistogramAggregation` - Numeric histogram with configurable intervals
- `DateHistogramAggregation` - Date/time histogram with fixed intervals
- `RangeAggregation` - Custom range buckets
- `StatsAggregation` - Statistical metrics (count, sum, avg, min, max)
- `AverageAggregation` - Average value aggregation
- `SumAggregation` - Sum aggregation
- `MinAggregation` - Minimum value aggregation
- `MaxAggregation` - Maximum value aggregation
- `CountAggregation` - Document count aggregation
- Result classes: `TermsResult`, `HistogramResult`, `DateHistogramResult`, `RangeResult`, `StatsResult`

**io.indextables.tantivy4java.config**
- `FileSystemConfig` - File system configuration
- `GlobalCacheConfig` - Global cache settings
- `RuntimeManager` - Runtime management

**io.indextables.tantivy4java.util**
- `TextAnalyzer` - Text tokenization and analysis
- `Facet` - Faceted search support

### Memory Constants

```java
Index.Memory.MIN_HEAP_SIZE      // 15MB - minimum
Index.Memory.DEFAULT_HEAP_SIZE  // 50MB - typical
Index.Memory.LARGE_HEAP_SIZE    // 128MB - bulk operations
Index.Memory.XL_HEAP_SIZE       // 256MB - very large indices
```

---

## Platform Support

- **macOS** - Intel (x86_64) and Apple Silicon (ARM64)
- **Linux** - x86_64 and ARM64 (glibc 2.17+)
- **Windows** - x86_64

## Requirements

- **Java** - 11 or higher
- **Memory** - Minimum 100MB heap recommended
- **Disk** - Depends on index size

## Known Limitations

### SplitSearcher Aggregation Limitations

The following aggregation features have Quickwit/Tantivy limitations:
- **Overlapping ranges** - RangeAggregation does not support overlapping range definitions
- **Calendar intervals** - DateHistogramAggregation only supports fixed intervals (1d, 1h, etc.), not calendar intervals (1M, 1y)

All core aggregation types (Terms, Histogram, DateHistogram, Range) are fully supported for both `Searcher` and `SplitSearcher` APIs.

### Tokenization

When using the "default" tokenizer, remember to lowercase search terms:
```java
// ✅ Correct
Query query = Query.termQuery(schema, "title", "search");

// ❌ Wrong (won't match)
Query query = Query.termQuery(schema, "title", "Search");
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/indextables/tantivy4java/issues)
- **Documentation**: [Full API Docs](https://indextables.github.io/tantivy4java)
- **Examples**: See [examples/](examples/) directory

## Acknowledgments

Built on top of:
- [Tantivy](https://github.com/quickwit-oss/tantivy) - The Rust search engine
- [Quickwit](https://github.com/quickwit-oss/quickwit) - Distributed search platform

**Development Tools:**
- Much of this codebase was generated and maintained using [Anthropic Claude Code](https://claude.ai/claude-code)

---

**Made with ❤️ for Java developers who need fast, reliable full-text search without giving up platform interoperability**
