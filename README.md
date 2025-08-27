# Tantivy4Java

![Tantivy4Java](https://img.shields.io/badge/Java-11+-blue)
![Maven](https://img.shields.io/badge/Maven-3.6+-blue)
![Rust](https://img.shields.io/badge/Rust-1.70+-orange)
![Tantivy](https://img.shields.io/badge/Tantivy-0.24+-green)

**High-performance, full-featured search engine library for Java built on Tantivy and Quickwit**

Tantivy4Java provides Java bindings for the blazingly fast Tantivy search engine, offering zero-copy operations and direct memory sharing through JNI for maximum performance. Built for production workloads, it delivers enterprise-grade search capabilities with comprehensive query support and advanced index management.  Also includes support for Quickwit splits, caches, and cloud storage providers.

## 🚀 Key Features

### **Core Search Engine**
- **Full-Text Search** - Advanced tokenization, stemming, and linguistic analysis
- **Multi-Field Queries** - Search across multiple document fields simultaneously  
- **Boolean Logic** - Complex query combinations with MUST/SHOULD/MUST_NOT operators
- **Phrase Queries** - Exact phrase matching with configurable slop tolerance
- **Fuzzy Search** - Typo-tolerant search with edit distance control
- **Range Queries** - Numeric, date, and lexicographic range filtering
- **Wildcard & Regex** - Pattern-based text matching capabilities

### **Document Management**
- **JSON Document Support** - Native JSON document indexing and querying
- **Multi-Value Fields** - Array support for document fields
- **CRUD Operations** - Complete document lifecycle management
- **Batch Operations** - High-throughput document processing
- **Field Types** - Text, Integer, Float, Boolean, Date, IP Address, Binary

### **Advanced Query Features**
- **Query Boosting** - Fine-tune relevance scoring
- **More-Like-This** - Content-based recommendation queries
- **Constant Score** - Override relevance scoring
- **Disjunction Max** - Advanced multi-field ranking
- **Term Sets** - Efficient OR queries across multiple terms

### **Index Operations**
- **Memory & Disk Indices** - Flexible storage options
- **Index Persistence** - Reliable index lifecycle management
- **Segment Merging** - Advanced index optimization with metadata access
- **Index Reloading** - Live index updates without downtime  
- **Schema Management** - Dynamic schema definition and validation

### **Performance & Scalability**
- **Zero-Copy Operations** - Direct memory sharing between Rust and Java
- **JNI Optimization** - Minimal marshalling overhead
- **Concurrent Access** - Thread-safe operations
- **Memory Efficiency** - Optimized resource utilization
- **Fast Fields** - Columnar storage for rapid filtering and sorting

## 🏗️ Architecture

```
┌─────────────────────────────────────┐
│          Java Application           │
├─────────────────────────────────────┤
│         Tantivy4Java API            │
│   (Schema, Index, Query, Search)    │
├─────────────────────────────────────┤
│          JNI Bridge Layer           │
│        (Zero-copy operations)       │
├─────────────────────────────────────┤
│         Tantivy Core (Rust)         │
│    (Indexing, Search, Storage)      │
├─────────────────────────────────────┤
│      Quickwit Integration           │
│  (Distributed Search & Storage)     │
└─────────────────────────────────────┘
```

## 📦 Installation

### Prerequisites
- **Java 11+** - Required for compilation and runtime
- **Maven 3.6+** - Build system
- **Rust 1.70+** - Required for native library compilation
- **Cargo** - Rust package manager

### Build from Source

```bash
# Clone the repository
git clone https://github.com/tantivy-search/tantivy4java
cd tantivy4java

# Build native library and Java components
mvn clean compile

# Run tests
mvn test

# Create JAR with native libraries
mvn package
```

The build process automatically:
1. Compiles Rust native library using Cargo
2. Compiles Java source code
3. Packages native libraries into JAR
4. Creates platform-specific binaries

### Cross-Platform Builds

```bash
# Install cross-compilation targets
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu

# Build for specific platforms
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target aarch64-unknown-linux-gnu
```

## 🚦 Quick Start

### Basic Search Example

```java
import com.tantivy4java.*;
import java.util.*;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        
        // 1. Create Schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position")
                   .addTextField("content", true, true, "default", "position")
                   .addIntegerField("doc_id", true, true, true);
            
            try (Schema schema = builder.build()) {
                
                // 2. Create Index
                try (Index index = new Index(schema, "", true)) { // In-memory
                    
                    // 3. Index Documents
                    try (IndexWriter writer = index.writer(50_000_000, 1)) {
                        
                        // Add documents via JSON
                        writer.addJson("{ \"doc_id\": 1, \"title\": \"Search Basics\", " +
                                      "\"content\": \"Full-text search fundamentals\" }");
                        writer.addJson("{ \"doc_id\": 2, \"title\": \"Advanced Queries\", " +
                                      "\"content\": \"Complex boolean search patterns\" }");
                        
                        writer.commit();
                    }
                    
                    // 4. Search Documents
                    index.reload(); // Make changes visible
                    try (Searcher searcher = index.searcher()) {
                        
                        // Simple term query
                        try (Query query = Query.termQuery(schema, "content", "search");
                             SearchResult results = searcher.search(query, 10)) {
                            
                            System.out.println("Found " + results.getHits().size() + " documents");
                            
                            for (Hit hit : results.getHits()) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    String title = (String) doc.get("title").get(0);
                                    System.out.println("Title: " + title + " (Score: " + hit.getScore() + ")");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
```

## 📋 Complete Feature Reference

### Schema Definition

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Text fields with tokenization
    builder.addTextField("title", stored, fast, "default", "position")
           .addTextField("content", stored, fast, "en_stem", "position")
           .addTextField("tags", stored, fast, "raw", "");
    
    // Numeric fields
    builder.addIntegerField("doc_id", stored, indexed, fast)
           .addFloatField("rating", stored, indexed, fast)
           .addUnsignedField("timestamp", stored, indexed, fast);
    
    // Other field types
    builder.addBooleanField("is_published", stored, indexed, fast)
           .addDateField("created_date", stored, indexed, fast)
           .addIpAddrField("client_ip", stored, indexed, fast)
           .addBytesField("binary_data", stored, indexed, fast, "position")
           .addJsonField("metadata", stored, "default", "position")
           .addFacetField("category");
    
    Schema schema = builder.build();
}
```

### Query Types

```java
// Term queries
Query termQuery = Query.termQuery(schema, "title", "search");
Query termSetQuery = Query.termSetQuery(schema, "category", Arrays.asList("tech", "science"));

// Phrase queries
Query phraseQuery = Query.phraseQuery(schema, "content", 
    Arrays.asList("machine", "learning"), 2); // slop = 2

// Fuzzy search
Query fuzzyQuery = Query.fuzzyTermQuery(schema, "title", "machien", 
    2, true, false); // edit distance, transposition, prefix

// Range queries  
Query rangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
    4.0, 5.0, true, false); // inclusive lower, exclusive upper

// Boolean combinations
Query booleanQuery = Query.booleanQuery(Arrays.asList(
    new Query.OccurQuery(Occur.MUST, contentQuery),
    new Query.OccurQuery(Occur.SHOULD, titleQuery),
    new Query.OccurQuery(Occur.MUST_NOT, excludeQuery)
));

// Advanced queries
Query boostQuery = Query.boostQuery(titleQuery, 2.5);
Query constScoreQuery = Query.constScoreQuery(categoryQuery, 1.0);
Query regexQuery = Query.regexQuery(schema, "title", ".*search.*");
Query moreLikeThisQuery = Query.moreLikeThisQuery(docAddress);
```

### Document Operations

```java
try (IndexWriter writer = index.writer(50_000_000, 1)) {
    
    // Add via JSON (recommended)
    writer.addJson("{ \"title\": \"Document Title\", \"content\": \"Document content\" }");
    
    // Add via Document API
    try (Document doc = new Document()) {
        doc.addText("title", "Document Title");
        doc.addText("content", "Document content");
        doc.addInteger("doc_id", 123);
        doc.addFloat("rating", 4.5f);
        doc.addBoolean("published", true);
        
        // Multi-value fields
        doc.addText("tags", "java");
        doc.addText("tags", "search");
        
        writer.addDocument(doc);
    }
    
    // Delete documents
    writer.deleteDocuments(Query.termQuery(schema, "doc_id", 123));
    
    writer.commit();
}
```

### Advanced Index Operations

```java
// Index segment merging for optimization
try (Searcher searcher = index.searcher()) {
    List<String> segmentIds = searcher.getSegmentIds();
    System.out.println("Current segments: " + segmentIds.size());
    
    if (segmentIds.size() > 1) {
        try (IndexWriter writer = index.writer(50_000_000, 1)) {
            // Merge first two segments
            List<String> toMerge = segmentIds.subList(0, 2);
            SegmentMeta result = writer.merge(toMerge);
            
            System.out.println("Merged segment ID: " + result.getSegmentId());
            System.out.println("Document count: " + result.getMaxDoc());
            System.out.println("Deleted docs: " + result.getNumDeletedDocs());
        }
    }
}
```

### Working with Splits (Quickwit Integration)

Tantivy4Java provides comprehensive support for Quickwit splits, enabling distributed search capabilities:

```java
import com.tantivy4java.quickwit.*;

public class SplitExample {
    public static void main(String[] args) throws Exception {
        
        // Create split-aware searcher
        try (SplitSearcher splitSearcher = new SplitSearcher()) {
            
            // Configure storage backends
            splitSearcher.addS3Storage("s3://my-bucket/splits/", 
                "us-west-2", "access-key", "secret-key");
            splitSearcher.addGcsStorage("gs://my-bucket/splits/", 
                "/path/to/service-account.json");
            splitSearcher.addAzureStorage("https://account.blob.core.windows.net/splits/",
                "account-key");
            
            // Load splits from metadata
            List<String> splitIds = Arrays.asList(
                "split-2024-01-01-001",
                "split-2024-01-01-002", 
                "split-2024-01-02-001"
            );
            
            for (String splitId : splitIds) {
                splitSearcher.addSplit(splitId, storageUri + "/" + splitId);
            }
            
            // Create schema for split searches
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("content", true, true, "default", "position")
                       .addIntegerField("timestamp", true, true, true)
                       .addTextField("source", true, true, "raw", "");
                
                try (Schema schema = builder.build()) {
                    
                    // Search across all splits
                    try (Query query = Query.termQuery(schema, "content", "analytics");
                         SplitSearchResult results = splitSearcher.search(query, 100)) {
                        
                        System.out.println("Found " + results.getTotalHits() + " documents across " + 
                                          results.getSplitCount() + " splits");
                        
                        // Process results from multiple splits
                        for (SplitHit hit : results.getHits()) {
                            try (Document doc = splitSearcher.getDocument(hit.getSplitId(), 
                                                                        hit.getDocAddress())) {
                                String content = (String) doc.get("content").get(0);
                                String source = (String) doc.get("source").get(0);
                                
                                System.out.printf("Split: %s, Source: %s, Score: %.3f%n", 
                                                 hit.getSplitId(), source, hit.getScore());
                                System.out.println("Content: " + content.substring(0, 
                                                  Math.min(100, content.length())) + "...");
                            }
                        }
                    }
                    
                    // Multi-split aggregation
                    SplitAggregationResult timeAgg = splitSearcher.aggregateByTime(
                        query, "timestamp", "day", 30);
                    
                    System.out.println("Time-based aggregation:");
                    for (TimeWindow window : timeAgg.getWindows()) {
                        System.out.printf("Date: %s, Count: %d, Splits: %d%n",
                                         window.getDate(), window.getCount(), 
                                         window.getSplitCount());
                    }
                    
                    // Split-level statistics
                    SplitStats stats = splitSearcher.getSplitStatistics();
                    System.out.printf("Total splits: %d, Total docs: %d, Cache hit ratio: %.2f%n",
                                     stats.getSplitCount(), stats.getTotalDocuments(), 
                                     stats.getCacheHitRatio());
                }
            }
        }
    }
}
```

### Storage Configuration for Splits

```java
// S3 Configuration
SplitStorageConfig s3Config = SplitStorageConfig.builder()
    .type("s3")
    .region("us-west-2")
    .bucket("my-search-splits")
    .accessKey("AKIA...")
    .secretKey("secret...")
    .pathPrefix("quickwit/splits/")
    .build();

// Google Cloud Storage
SplitStorageConfig gcsConfig = SplitStorageConfig.builder()
    .type("gcs")
    .bucket("my-search-splits")
    .serviceAccountPath("/path/to/service-account.json")
    .pathPrefix("quickwit/splits/")
    .build();

// Azure Blob Storage
SplitStorageConfig azureConfig = SplitStorageConfig.builder()
    .type("azure")
    .accountName("mystorageaccount")
    .accountKey("key...")
    .containerName("search-splits")
    .pathPrefix("quickwit/splits/")
    .build();

// Apply configuration
splitSearcher.configureStorage(s3Config);
```

## 🔧 Configuration

### JVM Options
```bash
# Increase heap for large indices
-Xmx8g -Xms2g

# Optimize GC for search workloads  
-XX:+UseG1GC -XX:MaxGCPauseMillis=200

# JNI optimization
-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC
```

### Index Writer Configuration
```java
// Configure for high-throughput indexing
try (IndexWriter writer = index.writer(
    100_000_000,  // Memory budget (bytes)
    4             // Number of threads
)) {
    // Batch operations for better performance
    List<String> jsonDocs = loadDocuments();
    for (String doc : jsonDocs) {
        writer.addJson(doc);
    }
    writer.commit();
}
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Run all tests
mvn test

# Run specific test categories
mvn test -Dtest="*FunctionalityTest"
mvn test -Dtest="*IntegrationTest"
mvn test -Dtest="*PerformanceTest"

# Run with verbose output
mvn test -Dtest.verbose=true
```

## 📊 Performance

### Benchmarks
- **Indexing**: 50,000+ documents/second
- **Search Latency**: <5ms for typical queries  
- **Memory Usage**: Minimal overhead through zero-copy operations
- **Concurrent Access**: Scales linearly with CPU cores

### Optimization Tips
1. **Use JSON indexing** for best performance
2. **Configure appropriate memory budgets** for IndexWriter
3. **Enable fast fields** for filtering and sorting
4. **Use segment merging** to optimize read performance
5. **Batch operations** when indexing multiple documents

## 🛠️ Troubleshooting

### Common Issues

**Native Library Loading**
```java
// Ensure native library is in classpath
System.setProperty("java.library.path", "/path/to/native/libs");
```

**Memory Issues**
```bash
# Increase JVM heap
export MAVEN_OPTS="-Xmx4g"
```

**Build Issues**
```bash
# Clean and rebuild
mvn clean compile -X
```

## 📚 Documentation

- **[API Reference](docs/reference.md)** - Complete API documentation
- **[How-To Guides](docs/howto.md)** - Common usage patterns  
- **[Tutorials](docs/tutorials.md)** - Step-by-step learning guides
- **[Examples](examples/)** - Working code examples

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
```bash
git clone https://github.com/tantivy-search/tantivy4java
cd tantivy4java

# Install dependencies
rustup install stable
mvn dependency:resolve

# Run development build
mvn clean compile test
```

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## 🔗 Related Projects

- **[Tantivy](https://github.com/quickwit-oss/tantivy)** - Core Rust search engine
- **[Quickwit](https://github.com/quickwit-oss/quickwit)** - Distributed search platform
- **[Tantivy-py](https://github.com/quickwit-oss/tantivy-py)** - Python bindings for Tantivy

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/tantivy-search/tantivy4java/issues)
- **Discussions**: [GitHub Discussions](https://github.com/tantivy-search/tantivy4java/discussions)
- **Community**: [Quickwit Community](https://quickwit.io/community)

---

**Built with ❤️ by the Tantivy4Java community**
