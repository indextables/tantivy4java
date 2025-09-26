# Multi-Split Search Analysis and Recommendations

## Key Findings from Analysis

### 1. Quickwit's Production Multi-Split Search Path

**Production Entry Point**: `single_doc_mapping_leaf_search` 
- **Location**: `quickwit-search/src/leaf.rs:1309`
- **Used by**: Production Quickwit searcher service for handling multiple splits
- **Benefits**: 
  - Proper resource coordination with IncrementalCollector
  - Better permit management across splits
  - Split optimization with CanSplitDoBetter
  - Parallel execution with proper synchronization

**Our Current Approach**: `leaf_search_single_split`
- **Location**: Direct single-split call
- **Issues**:
  - No cross-split coordination
  - Direct BundleStorage creation per split
  - Potential storage instance conflicts

### 2. Storage Instance and BundleStorage Issue Analysis

**Problem**: Multiple storage instances accessing the same S3 data concurrently
- **Instance Creation**: New S3 storage per `resolve_storage_for_split()` call
- **BundleStorage Wrapper**: Each creates its own wrapper around storage
- **Concurrent Access**: Multiple BundleStorage instances call `get_slice()` on different storage instances

**Root Cause**: 
```
Your Code: resolve_storage_for_split() → New S3 Storage Instance (0x6000033f3790)
                                      ↓
Quickwit:   open_split_bundle() → BundleStorage { storage: your_instance }
                                      ↓
           BundleStorage.get_slice() → your_instance.get_slice()
```

### 3. Cache Reuse Requirement

**Critical Issue**: Current approach creates new caches per invocation
- **SearcherContext**: Contains caches that should be reused
- **Fast Fields Cache**: Should persist across searches
- **Split Footer Cache**: Should accumulate split metadata
- **Leaf Search Cache**: Should cache query results

## Recommended Solution

### Option 1: Storage Instance Reuse (Immediate Fix)

Instead of creating new SearcherContext, modify storage resolution to reuse instances:

```rust
// Add to StandaloneSearchContext
pub storage_instances: Arc<RwLock<HashMap<String, Arc<dyn Storage>>>>

// Modify resolve_storage_for_split to check cache first
pub async fn resolve_storage_for_split_cached(
    storage_cache: &Arc<RwLock<HashMap<String, Arc<dyn Storage>>>>,
    storage_resolver: &StorageResolver,
    split_uri: &str,
) -> Result<Arc<dyn Storage>> {
    let cache_key = extract_storage_key(split_uri); // e.g., "s3://bucket/"
    
    // Check cache first
    {
        let cache = storage_cache.read().unwrap();
        if let Some(storage) = cache.get(&cache_key) {
            eprintln!("STORAGE DEBUG: Reusing cached storage instance: {:p}", &**storage);
            return Ok(storage.clone());
        }
    }
    
    // Create new instance
    let storage = resolve_storage_for_split(storage_resolver, split_uri).await?;
    eprintln!("STORAGE DEBUG: Created new storage instance: {:p}", &*storage);
    
    // Cache it
    {
        let mut cache = storage_cache.write().unwrap();
        cache.insert(cache_key, storage.clone());
    }
    
    Ok(storage)
}
```

### Option 2: Use Production Multi-Split Path (Better Long-term)

Modify our approach to use `single_doc_mapping_leaf_search` with reused SearcherContext:

```rust
impl StandaloneSearchContext {
    // Add SearcherContext as a field, created once
    pub searcher_context: Arc<SearcherContext>,
}

impl StandaloneSearcher {
    pub async fn search_split(&self, ...) -> Result<LeafSearchResponse> {
        // Use the pre-created, cached SearcherContext
        let result = single_doc_mapping_leaf_search(
            self.context.searcher_context.clone(), // Reuse cached instance
            Arc::new(search_request),
            storage,
            vec![split_offsets],
            doc_mapper,
            aggregations_limits,
        ).await?;
    }
}
```

## Immediate Action Plan

1. **Test Option 1 First** - Add storage instance caching to resolve conflicts
2. **Monitor Debug Output** - Track storage instance reuse vs creation
3. **Measure Performance** - Compare cache hit rates before/after
4. **Implement Option 2** - If Option 1 works, gradually migrate to multi-split path

## Expected Benefits

1. **Reduced Storage Conflicts**: Single storage instance per S3 endpoint
2. **Better Cache Utilization**: Reuse of split footers, fast fields, and query results
3. **Improved Performance**: Fewer S3 connections and HTTP overhead
4. **Production Alignment**: Use same patterns as working Quickwit production code

## Debug Monitoring

Add these debug statements to track improvement:
```rust
eprintln!("STORAGE REUSE: Cache hit for {}, instance: {:p}", cache_key, &**storage);
eprintln!("STORAGE REUSE: Cache miss for {}, creating new instance", cache_key);
eprintln!("CACHE STATS: Fast fields hits: {}, Split footer hits: {}", ...);
```

This approach maintains our existing cache infrastructure while fixing the storage conflicts that are likely causing the data corruption issues.