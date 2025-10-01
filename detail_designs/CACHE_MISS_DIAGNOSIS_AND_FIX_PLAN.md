# Cache Miss Diagnosis and Comprehensive Fix Plan

## 🔍 **CACHE MISS ROOT CAUSE ANALYSIS**

### **Critical Issue: Split Footer Cache Not Persisting**

The debug output shows **15 consecutive cache misses** for the exact same split `part-00000-3371-09871f9b-997a-412f-a121-4978b7001d25` with identical footer ranges `961935015..962826095`. This indicates the footer cache is **not retaining data between requests**.

**Key Evidence:**
1. **Same Split ID**: All 15 misses are for identical split `part-00000-3371-09871f9b-997a-412f-a121-4978b7001d25`
2. **Same Footer Range**: Every request fetches the exact same 891KB footer data
3. **Cache Should Work**: After the first fetch, subsequent requests should hit cache
4. **Using Quickwit's Code**: The debug output shows Quickwit's `get_split_footer_from_cache_or_fetch` is being called

### **Root Cause: Cache Instance Isolation**

The issue is likely that **each Spark task/executor is creating its own cache instance** instead of sharing a global cache, or the cache is being **cleared/reset between operations**.

## 🎯 **COMPREHENSIVE CACHING FIX PLAN**

### **Phase 1: Immediate Cache Sharing Fix**

**Problem**: Each Spark executor/task creates isolated cache instances
**Solution**: Implement proper global cache sharing following Quickwit patterns

#### **1.1 Fix Cache Manager Singleton Pattern**
```rust
// In native/src/global_cache.rs - ensure true singleton behavior
static GLOBAL_CACHE_INSTANCE: OnceLock<Arc<GlobalSearcherComponents>> = OnceLock::new();

pub fn get_global_searcher_context() -> Arc<SearcherContext> {
    let components = GLOBAL_CACHE_INSTANCE.get_or_init(|| {
        Arc::new(GlobalSearcherComponents::new(GlobalCacheConfig::default()))
    });

    // Return a SearcherContext that shares the same cache instances
    Arc::new(SearcherContext::from_global_components(components.clone()))
}
```

#### **1.2 Ensure Cache Persistence Across JNI Calls**
```rust
// Fix potential cache eviction during JNI operations
impl SearcherContext {
    pub fn from_global_components(components: Arc<GlobalSearcherComponents>) -> Self {
        Self {
            searcher_config: components.searcher_config.clone(),
            fast_fields_cache: components.fast_fields_cache.clone(),
            split_footer_cache: components.split_footer_cache.clone(), // Shared reference
            // ... other fields
        }
    }
}
```

### **Phase 2: Quickwit-Compatible Cache Architecture**

**Align exactly with Quickwit's proven caching patterns**

#### **2.1 Implement Quickwit's Cache Hierarchy**
```rust
// Follow quickwit-search/src/service.rs patterns exactly
pub struct SearcherContext {
    // Multi-level cache hierarchy matching Quickwit
    pub fast_fields_cache: Arc<dyn StorageCache>,           // Long-term component cache
    pub split_footer_cache: MemorySizedCache<String>,       // Split metadata cache
    pub storage_long_term_cache: Arc<dyn StorageCache>,     // Storage byte cache
    pub split_cache_opt: Option<Arc<SplitCache>>,           // Split file cache
}
```

#### **2.2 Cache Key Consistency**
```rust
// Ensure cache keys match Quickwit's expectations exactly
fn get_footer_cache_key(split_id: &str) -> String {
    // Must match Quickwit's key generation logic
    split_id.to_string() // Simple split ID as key
}
```

### **Phase 3: Debug & Monitoring Enhancements**

#### **3.1 Enhanced Cache Debug Logging**
```rust
// Add comprehensive cache state logging
fn debug_cache_operation(operation: &str, split_id: &str, cache: &MemorySizedCache<String>) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1").unwrap_or(false) {
        let stats = get_cache_stats(cache);
        eprintln!("🔍 CACHE DEBUG: {} for split '{}' - Size: {}/{} items, {}% hit rate",
                 operation, split_id, stats.current_items, stats.max_items,
                 stats.hit_rate_percent);
    }
}
```

#### **3.2 Cache Lifecycle Verification**
```rust
// Verify cache instance identity across calls
fn verify_cache_consistency(cache: &MemorySizedCache<String>) {
    let cache_ptr = Arc::as_ptr(cache) as usize;
    debug_println!("🔍 CACHE IDENTITY: Using cache instance at address: 0x{:x}", cache_ptr);
}
```

### **Phase 4: Spark Integration Fixes**

#### **4.1 Ensure Cache Survival Across Spark Tasks**
```java
// In SplitCacheManager.java - use stronger singleton pattern
public class SplitCacheManager {
    private static final AtomicReference<SplitCacheManager> GLOBAL_INSTANCE = new AtomicReference<>();

    public static SplitCacheManager getInstance(CacheConfig config) {
        return GLOBAL_INSTANCE.updateAndGet(existing -> {
            if (existing == null || !existing.config.equals(config)) {
                return new SplitCacheManager(config);
            }
            return existing;
        });
    }
}
```

#### **4.2 JVM-Wide Cache Persistence**
```java
// Ensure cache survives Spark executor restarts
static {
    // Force cache initialization at class loading time
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        // Graceful cache cleanup on JVM shutdown
        if (GLOBAL_INSTANCE.get() != null) {
            GLOBAL_INSTANCE.get().close();
        }
    }));
}
```

### **Phase 5: Performance Optimizations**

#### **5.1 Cache Size Tuning**
```rust
// Optimize cache sizes based on typical Spark workload
impl Default for GlobalCacheConfig {
    fn default() -> Self {
        Self {
            fast_field_cache_capacity: ByteSize::gb(2),      // Increased for Spark
            split_footer_cache_capacity: ByteSize::gb(1),    // Larger footer cache
            partial_request_cache_capacity: ByteSize::mb(256), // More byte range cache
            max_concurrent_splits: 1000,                     // Higher concurrency
            aggregation_memory_limit: ByteSize::gb(1),
        }
    }
}
```

#### **5.2 Cache Prewarming**
```rust
// Implement footer cache prewarming for known splits
pub async fn prewarm_footer_cache(
    searcher_context: &SearcherContext,
    split_metadata: &[SplitMetadata]
) -> anyhow::Result<()> {
    for split in split_metadata {
        if !searcher_context.split_footer_cache.contains_key(&split.split_id) {
            // Fetch footer data proactively
            let _ = get_split_footer_from_cache_or_fetch(
                storage.clone(),
                &split.into(),
                &searcher_context.split_footer_cache
            ).await;
        }
    }
    Ok(())
}
```

## 🎯 **IMPLEMENTATION PRIORITY**

### **Immediate (Phase 1)**: Critical Cache Sharing Fix
- Fix singleton cache instance creation
- Ensure cache persistence across JNI calls
- **Expected Impact**: Eliminate all 15 footer cache misses

### **Short-term (Phase 2-3)**: Architecture Alignment
- Implement Quickwit-compatible cache hierarchy
- Add comprehensive debug logging
- **Expected Impact**: Match Quickwit's proven caching performance

### **Medium-term (Phase 4-5)**: Spark Optimization
- Optimize for Spark distributed execution
- Implement cache prewarming strategies
- **Expected Impact**: Minimize cold-start cache misses in distributed environment

## 🔍 **SUCCESS METRICS**

**Before Fix**: 15/15 cache misses (0% hit rate)
**Target After Fix**: 14/15 cache hits (93% hit rate) - only first request should miss

**Performance Impact**:
- Eliminate ~13.4MB of redundant S3 downloads (15 × 891KB)
- Reduce query latency by ~300-500ms per subsequent request
- Improve Spark job performance for multi-split queries

## 📊 **DEBUG OUTPUT ANALYSIS**

### **Observed Pattern**
```
[12:29:50.586] 🔍 QUICKWIT DEBUG: ❌ Footer not in cache, will fetch from storage
[12:29:50.929] 🔍 QUICKWIT DEBUG: ❌ Footer not in cache, will fetch from storage
[12:29:51.357] 🔍 QUICKWIT DEBUG: ❌ Footer not in cache, will fetch from storage
... (15 total cache misses for same split)
```

### **Expected Pattern After Fix**
```
[12:29:50.586] 🔍 QUICKWIT DEBUG: ❌ Footer not in cache, will fetch from storage
[12:29:50.929] 🔍 QUICKWIT DEBUG: ✅ Footer found in cache, size: 891080 bytes
[12:29:51.357] 🔍 QUICKWIT DEBUG: ✅ Footer found in cache, size: 891080 bytes
... (14 cache hits for same split)
```

## 🔬 **TECHNICAL DETAILS**

### **Cache Implementation Status**
- **Current**: Each Spark task creates isolated cache instances
- **Target**: Shared global cache instances across all Spark tasks
- **Architecture**: Follow Quickwit's multi-level caching exactly

### **Integration Points**
1. **Spark Executor Level**: Ensure cache persistence across tasks
2. **JNI Layer**: Maintain cache references across native calls
3. **Rust Layer**: Use Quickwit's proven cache architecture
4. **Java Layer**: Implement proper singleton patterns

This comprehensive plan addresses the root cause while aligning with Quickwit's proven caching architecture and optimizing for Spark's distributed execution model.