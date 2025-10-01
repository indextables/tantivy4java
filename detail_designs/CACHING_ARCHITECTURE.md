# Tantivy4Java Global Caching Architecture

## Overview

Tantivy4Java implements a global caching architecture that follows Quickwit's design patterns. This ensures efficient resource utilization by sharing caches across all split searcher instances rather than creating isolated caches for each searcher.

## Architecture Principles

### 1. Global Singleton Pattern
Following Quickwit's approach, all caches are implemented as global singletons that are initialized once and shared across the entire application lifecycle.

### 2. Multi-Level Cache Hierarchy
The caching system implements multiple cache levels, each optimized for different access patterns:

```
Application Layer
    ↓
Global Cache Components
    ├── Fast Fields Cache (Memory)
    ├── Split Footer Cache (Memory)
    ├── Leaf Search Cache (Memory)
    ├── List Fields Cache (Memory)
    └── Split Cache (Disk)
         ├── File Descriptor Cache
         └── Split File Storage
```

## Cache Components

### 1. Fast Fields Cache
- **Type**: `QuickwitCache` (in-memory)
- **Purpose**: Caches fast field data for rapid columnar access
- **Default Size**: 1 GB
- **Scope**: Global, shared across all searchers
- **Implementation**: Arc-wrapped for thread-safe sharing

### 2. Split Footer Cache
- **Type**: `MemorySizedCache<String>` (in-memory)
- **Purpose**: Caches split file footers containing metadata and offsets
- **Default Size**: 500 MB
- **Scope**: Global, shared across all searchers
- **Benefits**: Avoids repeated S3/network requests for footer data

### 3. Leaf Search Cache
- **Type**: `LeafSearchCache` (in-memory)
- **Purpose**: Caches partial search results at the leaf level
- **Default Size**: 64 MB
- **Scope**: Global, shared across all searchers
- **Key**: Query hash + split ID

### 4. List Fields Cache
- **Type**: `ListFieldsCache` (in-memory)
- **Purpose**: Caches field listings and schema information
- **Default Size**: 64 MB (shares capacity with leaf search cache)
- **Scope**: Global, shared across all searchers

### 5. Split Cache (Disk-Based)
- **Type**: `SplitCache` (disk-backed)
- **Purpose**: Caches entire split files on local disk to avoid repeated downloads
- **Default Size**: 10 GB
- **Max Splits**: 10,000
- **Location**: Configurable (temp directory by default)
- **Features**:
  - Automatic eviction when size limits are reached
  - Concurrent download management
  - File descriptor pooling (max 100 FDs)
  - LRU eviction policy

## Implementation Details

### Global Cache Module (`global_cache.rs`)

The core module that manages all global caches:

```rust
pub struct GlobalCacheConfig {
    pub fast_field_cache_capacity: ByteSize,      // Default: 1GB
    pub split_footer_cache_capacity: ByteSize,    // Default: 500MB
    pub partial_request_cache_capacity: ByteSize, // Default: 64MB
    pub max_concurrent_splits: usize,             // Default: 100
    pub aggregation_memory_limit: ByteSize,       // Default: 500MB
    pub aggregation_bucket_limit: u32,            // Default: 65000
    pub warmup_memory_budget: ByteSize,           // Default: 100GB
    pub split_cache_limits: Option<SplitCacheLimits>,
    pub split_cache_root_path: Option<PathBuf>,
}
```

### Initialization Flow

1. **Application Startup**: Java code can optionally configure cache sizes
2. **First Access**: If not configured, uses default settings
3. **Component Creation**: Creates all cache instances wrapped in Arc
4. **Context Creation**: SearcherContext instances reference the global caches

```java
// Java configuration example
GlobalCacheConfig config = new GlobalCacheConfig()
    .withFastFieldCacheMB(2048)      // 2GB fast fields
    .withSplitFooterCacheMB(1024)    // 1GB split footers
    .withSplitCacheGB(20)             // 20GB disk cache
    .withSplitCachePath("/var/cache/tantivy");

config.initialize(); // Must be called before any searcher creation
```

### Thread Safety

All caches are thread-safe through different mechanisms:
- **Arc Wrapping**: Non-clonable caches are wrapped in Arc for safe sharing
- **Internal Synchronization**: Caches use internal mutexes/RwLocks
- **OnceCell**: Global initialization uses OnceCell to prevent races

## Cache Sharing Patterns

### StorageResolver (Global Singleton)
```rust
pub static GLOBAL_STORAGE_RESOLVER: Lazy<StorageResolver> = Lazy::new(|| {
    StorageResolver::configured(&StorageConfigs::default())
});
```

### SearcherComponents (Configurable Singleton)
```rust
static GLOBAL_SEARCHER_COMPONENTS: OnceCell<GlobalSearcherComponents> = OnceCell::new();

// Initialize with custom config from Java
pub fn initialize_global_cache(config: GlobalCacheConfig) -> bool {
    GLOBAL_SEARCHER_COMPONENTS.set(GlobalSearcherComponents::new(config)).is_ok()
}
```

## Memory Management

### Cache Size Limits
Each cache has configurable size limits to prevent unbounded memory growth:

| Cache | Default Size | Configurable | Eviction Policy |
|-------|-------------|--------------|-----------------|
| Fast Fields | 1 GB | Yes | LRU |
| Split Footer | 500 MB | Yes | LRU |
| Leaf Search | 64 MB | Yes | LRU |
| List Fields | 64 MB | Yes | LRU |
| Split Cache (Disk) | 10 GB | Yes | LRU by access time |

### Eviction Strategies

1. **Memory Caches**: Use size-based LRU eviction
2. **Split Cache**: Uses both size and count limits
   - Evicts least recently accessed splits
   - Maintains file access timestamps
   - Cleans up temporary files on shutdown

## Performance Benefits

### 1. Reduced Memory Usage
- Single cache instance vs. multiple per-searcher instances
- Shared data structures eliminate duplication
- Configurable limits prevent memory exhaustion

### 2. Improved Cache Hit Rates
- Larger effective cache size through sharing
- Cross-searcher cache warming
- Persistent disk cache survives searcher restarts

### 3. Reduced I/O Operations
- Split cache eliminates repeated S3 downloads
- Footer cache reduces metadata requests
- Fast field cache speeds up columnar operations

### 4. Network Optimization
- Cached S3 objects reduce bandwidth usage
- Concurrent download limits prevent network saturation
- Local disk cache serves as L2 cache for remote storage

## Configuration Best Practices

### Development Environment
```java
new GlobalCacheConfig()
    .withFastFieldCacheMB(512)       // Smaller caches for dev
    .withSplitFooterCacheMB(256)
    .withSplitCacheGB(5)              // 5GB disk cache
```

### Production Environment
```java
new GlobalCacheConfig()
    .withFastFieldCacheMB(4096)      // 4GB for production
    .withSplitFooterCacheMB(2048)    // 2GB footer cache
    .withSplitCacheGB(100)            // 100GB disk cache
    .withSplitCachePath("/mnt/ssd/cache")  // Fast SSD storage
    .withMaxConcurrentSplits(200)     // Higher concurrency
```

### Memory-Constrained Environment
```java
new GlobalCacheConfig()
    .withFastFieldCacheMB(256)       // Minimal memory usage
    .withSplitFooterCacheMB(128)
    .withPartialRequestCacheMB(32)
    .withSplitCacheGB(0)              // Disable disk cache
```

## Monitoring and Debugging

### Cache Statistics
The system provides cache statistics for monitoring:

```java
// Get cache statistics (when implemented)
CacheStats stats = cacheManager.getCacheStats();
System.out.println("Fast field cache: " + stats.fastFieldBytes);
System.out.println("Cache hit rate: " + stats.hitRate);
```

### Debug Logging
Enable debug logging to trace cache operations:

```bash
export TANTIVY4JAVA_DEBUG=1
java -jar myapp.jar
```

Debug output includes:
- Cache initialization
- Cache hits/misses
- Eviction events
- Split downloads
- Memory usage

## Integration with Quickwit

The caching architecture is designed to be compatible with Quickwit's patterns:

1. **StorageResolver**: Uses Quickwit's storage abstraction
2. **SearcherContext**: Compatible with Quickwit's searcher context
3. **SplitCache**: Direct use of Quickwit's split cache implementation
4. **Metrics**: Integrates with Quickwit's metrics system

## Future Enhancements

### Planned Improvements
1. **Distributed Cache**: Redis/Memcached backend for multi-node deployments
2. **Adaptive Sizing**: Dynamic cache size adjustment based on usage patterns
3. **Preloading**: Predictive cache warming based on query patterns
4. **Compression**: Compressed cache entries for larger effective cache size
5. **Tiered Storage**: Multiple cache tiers (RAM -> SSD -> HDD)

### Monitoring Enhancements
1. **JMX Integration**: Expose cache metrics via JMX
2. **Prometheus Metrics**: Export cache statistics for monitoring
3. **Cache Visualization**: UI for cache utilization analysis

## Conclusion

The global caching architecture in Tantivy4Java provides:
- **Efficiency**: Shared caches reduce memory usage
- **Performance**: High cache hit rates improve query speed
- **Scalability**: Configurable limits support various deployment sizes
- **Compatibility**: Follows Quickwit's proven patterns
- **Flexibility**: Java-configurable for different use cases

By following Quickwit's architecture, Tantivy4Java ensures that Java applications can achieve the same level of performance and efficiency as native Quickwit deployments.