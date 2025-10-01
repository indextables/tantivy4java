# Tantivy4Java

**High-performance full-text search for Java applications powered by Tantivy**

Tantivy4Java is a production-ready Java library that provides direct access to the Tantivy search engine through JNI bindings. Built for Java developers who need enterprise-grade full-text search capabilities with Rust-level performance.

## Why Tantivy4Java?

- **🚀 Blazing Fast** - Zero-copy operations and direct memory sharing for maximum performance
- **🎯 Type-Safe API** - Idiomatic Java interfaces with full generic type support
- **📦 Distributed Search** - Built-in Quickwit split support for large-scale deployments
- **☁️ Cloud Native** - First-class S3/MinIO support for remote index storage
- **🔧 Production Ready** - Comprehensive memory management and error handling
- **🌍 Cross-Platform** - Supports macOS (Intel/ARM), Linux (x86_64/ARM64), and Windows

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Installation](#installation)
4. [Basic Usage](#basic-usage)
5. [Advanced Features](#advanced-features)
6. [Quickwit Integration](#quickwit-integration)
7. [Performance Guide](#performance-guide)
8. [Developer Guide](#developer-guide)
9. [API Reference](#api-reference)

---

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>io.indextables</groupId>
    <artifactId>tantivy4java</artifactId>
    <version>0.24.1</version>
</dependency>
```

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

    Schema schema = builder.build();
}
```

**Field Options:**
- **stored** - Can retrieve original value in search results
- **indexed** - Can search/filter on this field (text fields always indexed)
- **fast** - Enables fast access for sorting, aggregations, filtering

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

### Cache Management

Monitor and control split cache performance:

```java
// Get cache statistics
SplitSearcher.CacheStats stats = searcher.getCacheStats();
System.out.println("Cache hits: " + stats.getHits());
System.out.println("Cache misses: " + stats.getMisses());
System.out.println("Evictions: " + stats.getEvictions());
System.out.println("Size: " + stats.getSizeBytes() + " bytes");

// Preload components for faster queries
List<String> components = List.of("postings", "fast_fields", "fieldnorms");
searcher.preloadComponents(components);

// Check what's cached
Map<String, Boolean> status = searcher.getComponentCacheStatus();
status.forEach((component, cached) ->
    System.out.println(component + ": " + (cached ? "cached" : "not cached")));

// Evict components to free memory
searcher.evictComponents(List.of("postings"));
```

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
- `QuickwitSplit.MergeConfig` - Merge configuration
- `QuickwitSplit.AwsConfig` - AWS credential configuration
- `QuickwitSplitInspector` - Split inspection utilities

**io.indextables.tantivy4java.aggregation**
- `TermsAggregation` - Terms aggregation (supported)
- `RangeAggregation` - Range aggregation (not yet supported for splits)
- `HistogramAggregation` - Histogram aggregation (not yet supported for splits)
- `DateHistogramAggregation` - Date histogram (not yet supported for splits)

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

### SplitSearcher Aggregations

Only **TermsAggregation** is currently supported for SplitSearcher. The following aggregations are not yet implemented:
- DateHistogramAggregation
- HistogramAggregation
- RangeAggregation

All aggregation types work correctly with the standard `Searcher` API.

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
