# Tantivy4Java

A high-performance Java library providing complete access to Tantivy search engine capabilities with advanced Quickwit integration for distributed search.

## Overview

Tantivy4Java is a production-ready JNI wrapper for the Rust-based Tantivy search engine, offering Java developers full-text search capabilities with zero-copy operations and direct memory sharing for maximum performance. The library includes comprehensive Quickwit integration for distributed search scenarios and split-based indexing.

## Key Features

### Core Search Engine
- **Schema Building** - Complete support for all field types (text, integer, float, boolean, date, IP address)
- **Document Management** - Creation, indexing, JSON support, multi-value fields
- **Index Operations** - Create, reload, commit, open, exists, schema access
- **Query System** - All query types with complex boolean logic
- **Search Pipeline** - Complete search with scoring and result handling
- **Document Retrieval** - Field extraction with proper type conversion

### Advanced Query Types
- **Term Queries** - Exact term matching
- **Phrase Queries** - Position-aware matching with configurable slop
- **Fuzzy Queries** - Edit distance, transposition costs, prefix matching
- **Boolean Logic** - MUST/SHOULD/MUST_NOT with nested combinations
- **Range Queries** - All field types with inclusive/exclusive bounds
- **Wildcard Queries** - Advanced pattern matching with multi-segment expansion
- **Boost Queries** - Score multiplication and relevance tuning
- **Const Score Queries** - Uniform scoring

### Field Type Support
- **Text Fields** - Full tokenization, always indexed, position tracking
- **Numeric Fields** - Integer, Float with explicit indexed control and range queries
- **Boolean Fields** - True/false queries and filtering
- **Date Fields** - Temporal queries with proper date handling
- **Multi-value Fields** - Array support in documents and queries
- **Schema Introspection** - Runtime field discovery, type checking, and metadata access

### Index Optimization
- **Segment Merging** - Programmatic index optimization with metadata access
- **Index Persistence** - Complete lifecycle management
- **Performance Tuning** - Fast fields, stored fields optimization
- **Memory Management** - Zero-copy operations where possible

### Quickwit Integration

#### SplitSearcher Engine
- **Split File Search** - Direct search within Quickwit split files (file:// or s3://)
- **Advanced Caching System** - Multi-level caching with statistics and control
- **S3 Storage Backend** - Full AWS S3/MinIO support with error handling
- **Memory Optimization** - Component-level cache control and preloading
- **Connection Validation** - Robust error handling for network issues

#### Split Operations
- **Split Creation** - Convert Tantivy indices to Quickwit split format
- **Split Merging** - Efficient Quickwit-style split merging for large-scale indices
- **Split Validation** - Verify split file integrity and accessibility
- **Metadata Access** - Complete split information and statistics

#### Cloud Storage Support
- **AWS S3 Integration** - Full compatibility with Amazon S3
- **MinIO Support** - Private cloud storage backend
- **Custom Endpoints** - Support for mock servers and development environments
- **Credential Management** - AWS access keys, secret keys, session tokens, and IAM roles

### Cross-Platform Support
- **macOS ARM64** (aarch64-apple-darwin)
- **Linux x86_64** (x86_64-unknown-linux-gnu)
- **Linux ARM64** (aarch64-unknown-linux-gnu)
- **Maven Integration** - Complete build system with platform-specific profiles

## Building the Project

### Prerequisites
- Java 11 or higher
- Maven 3.6+
- Rust toolchain (for native compilation)

### Quick Build (Current Platform)
```bash
mvn clean package
```

### Cross-Platform Build
```bash
# Install cross-compilation toolchain
./install-cross-compile.sh

# Build for all supported platforms
mvn clean package -Pcross-compile

# Build for specific platforms
mvn clean package -Pdarwin-aarch64
mvn clean package -Plinux-x86_64
mvn clean package -Plinux-aarch64

# Build for multiple platforms
mvn clean package -Pdarwin-aarch64,linux-x86_64
```

### Running Tests
```bash
mvn test
```

## Basic Usage Examples

### Creating an Index and Adding Documents

```java
import com.tantivy4java.*;

// Create schema
SchemaBuilder schemaBuilder = new SchemaBuilder();
schemaBuilder.addTextField("title", true, false, "default", "position");
schemaBuilder.addTextField("content", true, false, "default", "position");
schemaBuilder.addIntegerField("rating", true, true, false);
Schema schema = schemaBuilder.build();

// Create index
Index index = Index.createInRam(schema);
IndexWriter writer = index.writer(50_000_000);

// Add documents
writer.addDocument(Map.of(
    "title", "Great Article",
    "content", "This is the content of a great article about search engines.",
    "rating", 5
));

writer.addDocument(Map.of(
    "title", "Another Article", 
    "content", "This article discusses advanced search techniques.",
    "rating", 4
));

writer.commit();
```

### Searching with Complex Queries

```java
// Create searcher
IndexReader reader = index.reader();
Searcher searcher = reader.searcher();

// Term query
Query termQuery = Query.termQuery(schema, "title", "article");

// Boolean query combining multiple conditions
Query booleanQuery = Query.booleanQuery(
    Map.of(
        BooleanQueryType.MUST, List.of(
            Query.termQuery(schema, "content", "search")
        ),
        BooleanQueryType.SHOULD, List.of(
            Query.rangeQuery(schema, "rating", RangeQueryType.INTEGER, 4, 5, true, true)
        )
    )
);

// Phrase query with slop
Query phraseQuery = Query.phraseQuery(schema, "content", 
    List.of("search", "engines"), 2);

// Wildcard query with advanced pattern matching
Query wildcardQuery = Query.wildcardQuery(schema, "content", "*search*engine*");

// Execute search
SearchResult result = searcher.search(booleanQuery, 10);
for (Hit hit : result.getHits()) {
    Document doc = searcher.doc(hit.getDocAddress());
    System.out.println("Title: " + doc.getFirst("title"));
    System.out.println("Rating: " + doc.getFirst("rating"));
    System.out.println("Score: " + hit.getScore());
}
```

### Index Optimization with Segment Merging

```java
// Get current segment information
List<String> segmentIds = searcher.getSegmentIds();
System.out.println("Current segments: " + segmentIds.size());

// Merge segments for optimization
if (segmentIds.size() > 1) {
    List<String> toMerge = segmentIds.subList(0, 2);
    SegmentMeta result = writer.merge(toMerge);
    
    System.out.println("Merged segment ID: " + result.getSegmentId());
    System.out.println("Documents in merged segment: " + result.getMaxDoc());
    System.out.println("Deleted documents: " + result.getNumDeletedDocs());
}
```

### Schema Introspection

```java
// Runtime schema discovery
List<String> fieldNames = schema.getFieldNames();
int fieldCount = schema.getFieldCount();
boolean hasTitle = schema.hasField("title");

System.out.println("Schema contains " + fieldCount + " fields: " + fieldNames);

// Filter fields by capabilities
List<String> storedFields = schema.getStoredFieldNames();
List<String> indexedFields = schema.getIndexedFieldNames();
List<String> fastFields = schema.getFastFieldNames();

// Get fields by type
List<String> textFields = schema.getFieldNamesByType("text");
List<String> integerFields = schema.getFieldNamesByType("i64");

// Advanced field filtering
List<String> searchableStoredFields = schema.getFieldNamesByCapabilities(true, true, false);
```

## Working with Quickwit Splits

### Converting Index to Split

```java
// Create split configuration
QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
    "my-index-uid", 
    "my-source", 
    "my-node"
);

// Convert Tantivy index to Quickwit split
QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
    "/path/to/tantivy/index", 
    "/tmp/my_index.split", 
    config
);

System.out.println("Split ID: " + metadata.getSplitId());
System.out.println("Documents: " + metadata.getNumDocs());
System.out.println("Size: " + metadata.getUncompressedSizeBytes());

// Validate split integrity
boolean isValid = QuickwitSplit.validateSplit("/tmp/my_index.split");
System.out.println("Split is valid: " + isValid);
```

### Merging Multiple Splits

```java
// Merge multiple splits using Quickwit's efficient approach
List<String> splitPaths = List.of(
    "/path/to/split1.split",
    "/path/to/split2.split", 
    "/path/to/split3.split"
);

QuickwitSplit.SplitConfig mergeConfig = new QuickwitSplit.SplitConfig(
    "merged-index-uid",
    "merged-source",
    "merge-node"
);

QuickwitSplit.SplitMetadata mergedSplit = QuickwitSplit.mergeSplits(
    splitPaths,
    "/tmp/merged.split",
    mergeConfig
);

System.out.println("Merged " + splitPaths.size() + " splits");
System.out.println("Total documents: " + mergedSplit.getNumDocs());
System.out.println("Merged split size: " + mergedSplit.getUncompressedSizeBytes());
```

### Searching Split Files

```java
// Create shared cache manager for efficient split searching
SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("main-cache")
    .withMaxCacheSize(200_000_000)  // 200MB shared cache
    .withAwsCredentials("access-key", "secret-key")  // For S3 splits
    .withAwsRegion("us-east-1");

SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

// Search local split file
try (SplitSearcher searcher = cacheManager.createSplitSearcher("file:///tmp/my_index.split")) {
    Schema schema = searcher.getSchema();
    
    Query query = Query.termQuery(schema, "content", "search");
    SearchResult result = searcher.search(query, 10);
    
    for (Hit hit : result.getHits()) {
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            System.out.println("Found: " + doc.getFirst("title"));
            System.out.println("Score: " + hit.getScore());
        }
    }
}

// Search S3-hosted split
try (SplitSearcher s3Searcher = cacheManager.createSplitSearcher("s3://my-bucket/splits/index.split")) {
    // Validate split accessibility
    boolean isValid = s3Searcher.validateSplit();
    if (!isValid) {
        System.err.println("Split validation failed");
        return;
    }
    
    // Get split metadata
    QuickwitSplit.SplitMetadata metadata = s3Searcher.getSplitMetadata();
    System.out.println("Searching split with " + metadata.getNumDocs() + " documents");
    
    // Perform search
    Query query = Query.booleanQuery(Map.of(
        BooleanQueryType.MUST, List.of(
            Query.termQuery(s3Searcher.getSchema(), "status", "published")
        )
    ));
    
    SearchResult result = s3Searcher.search(query, 20);
    processSearchResults(result, s3Searcher);
}
```

### Advanced Cache Management

```java
// Monitor cache performance
SplitSearcher.CacheStats stats = searcher.getCacheStats();
System.out.println("Cache hits: " + stats.getHits());
System.out.println("Cache misses: " + stats.getMisses());
System.out.println("Cache size: " + stats.getSizeBytes());

// Component-level cache control
List<String> components = List.of("postings", "fast_fields", "fieldnorms");

// Preload specific components
searcher.preloadComponents(components);

// Check component cache status
Map<String, Boolean> status = searcher.getComponentCacheStatus();
for (Map.Entry<String, Boolean> entry : status.entrySet()) {
    System.out.println(entry.getKey() + " cached: " + entry.getValue());
}

// Manual cache eviction
searcher.evictComponents(List.of("postings"));
```

## Configuration

### Debug Logging
Enable detailed logging for troubleshooting:
```bash
export TANTIVY4JAVA_DEBUG=1
```

### AWS Credentials Configuration
```java
// Basic credentials (long-term access)
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("cache")
    .withAwsCredentials("access-key", "secret-key")
    .withAwsRegion("us-east-1");

// Temporary credentials (with session token)
SplitCacheManager.CacheConfig sessionConfig = new SplitCacheManager.CacheConfig("session-cache")
    .withAwsCredentials("access-key", "secret-key", "session-token")
    .withAwsRegion("us-east-1");

// Custom S3 endpoint (for MinIO or testing)
SplitCacheManager.CacheConfig customConfig = new SplitCacheManager.CacheConfig("custom")
    .withAwsCredentials("access-key", "secret-key")
    .withAwsRegion("us-east-1")
    .withS3Endpoint("http://localhost:9000");  // MinIO endpoint
```

## Performance Tuning

### Memory Management
- Use appropriate cache sizes based on available memory
- Enable fast fields for frequently accessed numeric/date fields
- Consider field storage requirements (stored vs indexed vs fast)

### Index Optimization
- Regularly merge segments to improve search performance
- Use appropriate merge policies for your use case
- Monitor segment count and optimize when necessary

### Split-based Architecture
- Use splits for distributed search scenarios
- Implement appropriate caching strategies for remote splits
- Consider split size for optimal performance (typically 1-10GB per split)

## API Reference

### Core Classes
- `Index` - Main index interface
- `IndexWriter` - Document indexing operations
- `IndexReader` - Index reading operations  
- `Searcher` - Search execution
- `Schema` - Index schema definition
- `SchemaBuilder` - Schema construction
- `Query` - Query construction utilities
- `Document` - Document representation

### Quickwit Classes
- `SplitSearcher` - Split file search operations
- `SplitCacheManager` - Shared cache management
- `QuickwitSplit` - Split conversion and merging utilities
- `SplitMetadata` - Split information access

## Requirements
- Java 11 or higher
- Compatible with all major operating systems (macOS, Linux)
- Supports both x86_64 and ARM64 architectures

## License
[Include your license information here]

## Contributing
[Include contribution guidelines here]

## Support
[Include support information here]