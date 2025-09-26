# Tantivy4Java Cache Types Explained

## Overview

Tantivy4Java implements a multi-level caching system with five distinct cache types, each optimized for different aspects of search performance. Understanding what each cache does and when to tune it is crucial for optimal performance.

## Cache Types and Their Purposes

### 1. Fast Fields Cache (Default: 1GB memory)

**Purpose**: Stores columnar data for fields marked as "fast" in the schema, enabling efficient batch operations and aggregations.

**What it caches**:
- Numeric fields (integers, floats, dates) in columnar format
- Stored field values for quick retrieval without document parsing
- Pre-computed data structures for range queries and sorting
- Docvalue data for aggregations and faceting

**When it's used**:
- **Sorting operations**: Sort by price, date, score, or any numeric field
- **Aggregations**: Computing sum, average, min, max, percentiles, histograms
- **Range queries**: Finding documents within numeric ranges
- **Field retrieval**: Getting stored values without full document deserialization
- **Faceting**: Computing term frequencies and distributions

**Real-world example**:
```java
// These operations benefit from fast fields cache:
Query rangeQuery = Query.rangeQuery(schema, "price", 10.0f, 100.0f);
searcher.search(query, 10, "price");  // Sort by price
```

**Performance impact**:
- **Cache hit**: 10-100x faster aggregations, instant sorting
- **Cache miss**: Must read from disk and decode columnar data (10-50ms penalty)

---

### 2. Split Footer Cache (Default: 500MB memory)

**Purpose**: Caches the metadata footer of split files, which contains critical information about the structure and content of each split.

**What it caches**:
- Field information and field norms
- Posting list offsets and sizes
- Term dictionary locations
- Store and fast field offsets
- Compression codec information
- Delete bitset information
- Schema embedded in the split

**When it's used**:
- **Every split open operation**: Before any search can begin
- **Query planning**: Determining which parts of split to read
- **Field existence checks**: Verifying fields exist in a split
- **Statistics gathering**: Getting document counts and field statistics

**Real-world example**:
```java
// Opening a split to search it:
SplitSearcher searcher = new SplitSearcher("s3://bucket/index.split");
// The footer (last ~10KB) is fetched once and cached
// Subsequent operations use the cached footer
```

**Performance impact**:
- **Cache hit**: Saves network round-trip (10-100ms for S3)
- **Cache miss**: Must fetch footer from storage, especially costly for remote storage

---

### 3. Leaf Search Cache (Default: 64MB memory)

**Purpose**: Caches partial search results at the leaf (individual split) level before final result merging.

**What it caches**:
- Query results for specific (query + split) combinations
- Document IDs matching queries
- Scored and ranked results per split
- Partial aggregation results
- Top-K results before merging

**When it's used**:
- **Repeated identical queries**: Same search on same data
- **Pagination**: Moving through result pages
- **Query refinement**: Adding filters to existing queries
- **Time-range queries**: Overlapping time windows

**Real-world example**:
```java
// First search - executes and caches
SearchResult page1 = searcher.search(query, 10, 0);    // Offset 0
// Second search - hits cache for split results
SearchResult page2 = searcher.search(query, 10, 10);   // Offset 10
```

**Cache key structure**:
```
CacheKey = Hash(QueryAST + SplitID + SearchParameters)
```

**Performance impact**:
- **Cache hit**: Instant results (< 1ms)
- **Cache miss**: Full search execution (10-1000ms depending on complexity)

---

### 4. List Fields Cache (Default: 64MB memory)

**Purpose**: Caches schema and field metadata information for quick introspection and validation.

**What it caches**:
- Complete field listings per index
- Field types and configurations
- Field capabilities (indexed, stored, fast, searchable)
- Dynamic field mappings
- Schema version information
- Field statistics and cardinality

**When it's used**:
- **Schema introspection**: `schema.getFieldNames()`
- **Query validation**: Checking field existence before searching
- **Dynamic query building**: Constructing queries based on available fields
- **Auto-completion**: Suggesting field names in query builders
- **Index statistics**: Getting field counts and types

**Real-world example**:
```java
// These operations use list fields cache:
List<String> fields = schema.getFieldNames();
boolean hasPrice = schema.hasField("price");
List<String> fastFields = schema.getFastFieldNames();
```

**Performance impact**:
- **Cache hit**: Instant response (< 1ms)
- **Cache miss**: Must parse schema from splits (10-50ms)

---

### 5. Split Cache (Default: 10GB disk)

**Purpose**: Caches entire split files on local disk to eliminate network I/O for remote storage.

**What it caches**:
- Complete split files downloaded from remote storage
- Maintains LRU eviction based on access patterns
- File descriptors for efficient repeated access
- Preserves original file structure for memory-mapped access

**When it's used**:
- **Remote storage access**: S3, Azure Blob, GCS splits
- **Repeated searches**: Multiple queries against same splits
- **High-latency networks**: WAN or cross-region access
- **Bandwidth-limited environments**: Reducing transfer costs

**Real-world example**:
```java
// First search - downloads 500MB split from S3 to local cache
searcher.search("s3://bucket/split1.split", query1);
// Second search - reads from local disk cache (100x faster)
searcher.search("s3://bucket/split1.split", query2);
```

**Cache location**:
- Default: System temp directory
- Configurable: SSD recommended for performance

**Performance impact**:
- **Cache hit**: Local disk read (0.1-1ms)
- **Cache miss**: Network download (100-10000ms depending on size and bandwidth)

---

## Cache Interaction Diagram

```
Query Execution Flow with Cache Interactions:

User Query
    ↓
[List Fields Cache] 
    → Validate fields exist
    → Get field configurations
    ↓
[Split Cache]
    → Check if split file is cached locally
    → Download from S3 if not cached
    ↓
[Split Footer Cache]
    → Read split metadata
    → Locate data structures
    ↓
[Fast Fields Cache]
    → Load columnar data for sorts/aggregations
    → Prepare docvalues
    ↓
[Leaf Search Cache]
    → Check for cached query results
    → Return cached or execute search
    ↓
Final Results
```

## Performance Characteristics

| Cache Type | Typical Hit Rate | Hit Latency | Miss Latency | Memory vs Disk |
|------------|-----------------|-------------|--------------|----------------|
| **Fast Fields** | 70-90% | < 0.1ms | 10-50ms | Memory |
| **Split Footer** | 95-99% | < 0.1ms | 10-100ms | Memory |
| **Leaf Search** | 20-60% | < 1ms | 10-1000ms | Memory |
| **List Fields** | 99%+ | < 0.1ms | 10-50ms | Memory |
| **Split Cache** | 60-90% | 0.1-1ms | 100-10000ms | Disk |

## Configuration Guidelines

### High Query Volume (Same Queries)
```java
new GlobalCacheConfig()
    .withLeafSearchCacheMB(256)    // Increase for repeated queries
    .withSplitCacheGB(50)           // Cache frequently accessed splits
```

### Analytics and Aggregations Workload
```java
new GlobalCacheConfig()
    .withFastFieldCacheMB(4096)     // 4GB for heavy aggregations
    .withSplitFooterCacheMB(1024)   // More metadata for statistics
```

### Remote Storage (S3/Cloud)
```java
new GlobalCacheConfig()
    .withSplitCacheGB(100)           // Large local cache
    .withSplitCachePath("/mnt/ssd")  // Fast SSD storage
    .withSplitFooterCacheMB(2048)    // Cache more footers
```

### Memory-Constrained Environment
```java
new GlobalCacheConfig()
    .withFastFieldCacheMB(256)       // Minimum memory usage
    .withSplitFooterCacheMB(128)
    .withLeafSearchCacheMB(32)
    .withSplitCacheGB(20)             // Rely more on disk cache
```

### Many Indexes/Dynamic Schemas
```java
new GlobalCacheConfig()
    .withListFieldsCacheMB(256)      // More schema caching
    .withSplitFooterCacheMB(2048)    // Many different footers
```

## Cache Warming Strategies

### Pre-warm Critical Data
```java
// Pre-load frequently accessed splits
for (String splitPath : criticalSplits) {
    cacheManager.preloadSplit(splitPath);
}

// Pre-populate fast fields for sort fields
searcher.warmupFastFields(Arrays.asList("timestamp", "price"));
```

### Gradual Warming
```java
// Start with smaller caches and grow based on usage
GlobalCacheConfig config = new GlobalCacheConfig()
    .withFastFieldCacheMB(512)      // Start conservative
    .withSplitCacheGB(10);           // Grow as needed
```

## Monitoring and Metrics

### Key Metrics to Track

1. **Cache Hit Rates**
   - Target: > 80% for footer cache
   - Target: > 60% for split cache
   - Target: > 30% for leaf search cache

2. **Cache Memory Usage**
   ```java
   CacheStats stats = cacheManager.getCacheStats();
   System.out.println("Fast fields: " + stats.fastFieldBytes);
   System.out.println("Hit rate: " + stats.hitRate);
   ```

3. **Eviction Rates**
   - High eviction = cache too small
   - No eviction = cache possibly oversized

4. **Network I/O Reduction**
   - Bytes saved = (Cache hits × Average split size)
   - Cost saved = (Bytes saved × Cloud egress cost)

## Common Issues and Solutions

### Issue: High Memory Usage
**Solution**: Reduce memory caches, increase disk cache
```java
.withFastFieldCacheMB(512)
.withSplitCacheGB(50)  // Use disk instead of memory
```

### Issue: Slow S3 Access
**Solution**: Increase split cache and use SSD
```java
.withSplitCacheGB(200)
.withSplitCachePath("/mnt/nvme/cache")
```

### Issue: Repeated Query Latency
**Solution**: Increase leaf search cache
```java
.withLeafSearchCacheMB(512)
```

### Issue: Aggregation Performance
**Solution**: Increase fast fields cache and ensure fields are marked as "fast"
```java
.withFastFieldCacheMB(8192)  // 8GB for heavy analytics
```

## Best Practices

1. **Start with defaults** - The default configuration works well for most use cases

2. **Monitor and adjust** - Use metrics to guide configuration changes

3. **Use SSD for split cache** - Dramatic performance improvement over HDD

4. **Cache sizing ratio** - Maintain roughly 2:1:0.5:0.5 ratio for fast:footer:leaf:list

5. **Regular cache cleanup** - Implement cache cleanup policies for long-running applications

6. **Warm critical data** - Pre-load frequently accessed data during off-peak hours

7. **Network locality** - Place split cache on same network as compute for best performance

## Conclusion

Understanding and properly configuring these five cache types is essential for achieving optimal performance in Tantivy4Java. Each cache serves a specific purpose in the search pipeline, and proper tuning based on your workload characteristics can result in 10-100x performance improvements, especially when dealing with remote storage systems like S3.