# Advanced Cache Optimizations Implementation Plan

## 🎯 Objective
Implement three high-impact cache optimizations from Quickwit to enhance tantivy4java performance:
1. **Time-Based Eviction Protection** - Prevent scan pattern cache thrashing
2. **Document-Count-Aware Memory Allocation** - Optimize memory usage based on split size
3. **ByteRange Cache Merging** - Intelligent range combining for partial cache hits

## 📊 Expected Performance Impact
- **Cache hit rate improvement**: 15-30% for mixed workloads
- **Memory efficiency**: 20-40% better allocation for varied split sizes
- **Network reduction**: 30-50% fewer S3 requests through range merging
- **Multi-thread stability**: Eliminates cache thrashing in concurrent scenarios

## 🔧 Technical Implementation Plan

### **Phase 1: Time-Based Eviction Protection**

#### **Problem Analysis**
Current LRU eviction can cause "scan pattern thrashing" where:
- Large query scans through many documents
- Evicts recently cached data from other concurrent operations
- Results in 0% cache hit rates for normal queries

#### **Quickwit Solution Pattern**
```rust
const MIN_TIME_SINCE_LAST_ACCESS_SECS: u64 = 60;

// Only evict items older than 60 seconds under memory pressure
fn should_evict(item: &CacheItem, current_time: SystemTime) -> bool {
    current_time.duration_since(item.last_accessed).unwrap_or_default().as_secs()
        >= MIN_TIME_SINCE_LAST_ACCESS_SECS
}
```

#### **Implementation Steps**
1. **Add timestamp tracking to cache items**
   - Extend cache structures with `last_accessed: SystemTime`
   - Update timestamp on each cache access
   - Location: Global cache structures in `global_cache.rs`

2. **Implement time-aware eviction policy**
   - Modify LRU eviction to check access time
   - Only evict items older than 60 seconds under memory pressure
   - Fallback to normal LRU if no old items exist

3. **Add configuration constants**
   ```rust
   /// Minimum time since last access before item can be evicted (prevents scan thrashing)
   const MIN_CACHE_ITEM_LIFETIME_SECS: u64 = 60;

   /// Emergency eviction threshold when cache is critically full
   const EMERGENCY_EVICTION_THRESHOLD: f64 = 0.95; // 95% full
   ```

#### **Files to Modify**
- `native/src/global_cache.rs` - Cache structures and eviction logic
- `native/src/split_searcher_replacement.rs` - Cache access patterns

---

### **Phase 2: Document-Count-Aware Memory Allocation**

#### **Problem Analysis**
Current fixed memory allocation doesn't scale with split characteristics:
- Small splits (1K docs) get same memory as large splits (1M docs)
- Wastes memory on small splits, under-allocates for large splits
- No consideration for actual data size

#### **Quickwit Solution Pattern**
```rust
fn compute_memory_allocation(split_metadata: &SplitMetadata) -> usize {
    let base_memory_mb = 10; // 10MB minimum
    let per_doc_bytes = 50; // 50 bytes per document
    let doc_based_mb = (split_metadata.num_docs * per_doc_bytes) / (1024 * 1024);

    (base_memory_mb + doc_based_mb).min(100).max(15) // 15MB-100MB range
}
```

#### **Implementation Steps**
1. **Add memory calculation function**
   - Create adaptive memory sizing based on document count
   - Include minimum (15MB) and maximum (100MB) bounds
   - Factor in split file size when available

2. **Integrate with cache block calculation**
   - Modify `get_batch_doc_cache_blocks()` to consider document count
   - Add document-count-aware scaling factor
   - Preserve thread-based scaling from previous optimization

3. **Add split metadata integration**
   - Extract document count from split metadata where available
   - Fallback to adaptive thread-based sizing when metadata unavailable
   - Cache metadata for reuse across operations

#### **Memory Allocation Formula**
```rust
fn calculate_adaptive_memory_allocation(
    split_metadata: Option<&SplitMetadata>,
    thread_count: usize
) -> usize {
    let base_blocks = BASE_CONCURRENT_REQUESTS * thread_count;

    if let Some(metadata) = split_metadata {
        let doc_count = metadata.num_docs as f64;
        let doc_scale_factor = (doc_count / 100_000.0).sqrt().max(0.5).min(3.0);
        (base_blocks as f64 * doc_scale_factor * 1.2) as usize
    } else {
        (base_blocks as f64 * 1.2) as usize // Existing logic
    }
}
```

#### **Files to Modify**
- `native/src/split_searcher_replacement.rs` - Memory allocation logic
- Add split metadata extraction utilities

---

### **Phase 3: ByteRange Cache Merging & Gap Detection**

#### **Problem Analysis**
Current cache serves exact byte ranges only:
- Request for bytes 100-200 + cached bytes 150-250 = cache miss + full download
- Cannot combine multiple cached segments to serve larger requests
- Each document retrieval may trigger separate storage requests

#### **Quickwit Solution Pattern**
```rust
pub struct CachedRange {
    start: usize,
    end: usize,
    data: Arc<[u8]>,
}

impl ByteRangeCache {
    fn get_merged_range(&self, requested: Range<usize>) -> Option<Vec<u8>> {
        let mut result = vec![0u8; requested.len()];
        let mut covered = 0;

        for cached in self.find_overlapping_ranges(requested.clone()) {
            let overlap = intersect_ranges(requested.clone(), cached.range());
            if overlap.start == requested.start + covered {
                // Contiguous coverage
                let data_slice = &cached.data[overlap.start - cached.start..];
                result[covered..covered + data_slice.len()].copy_from_slice(data_slice);
                covered += data_slice.len();
            }
        }

        if covered == requested.len() { Some(result) } else { None }
    }
}
```

#### **Implementation Steps**
1. **Enhance ByteRangeCache structure**
   - Add range overlap detection
   - Implement range merging algorithms
   - Add gap detection and filling logic

2. **Implement intelligent cache retrieval**
   - Check for partial coverage from multiple cached segments
   - Merge segments when they cover the requested range
   - Only fetch missing gaps from storage

3. **Add range optimization strategies**
   - **Prefetch adjacent ranges** when access patterns suggest sequential reads
   - **Coalesce small ranges** into larger cached blocks
   - **Range expansion** for commonly accessed areas

#### **Range Merging Algorithm**
```rust
pub fn get_with_merging(&self, requested_range: Range<usize>) -> CacheResult {
    let overlapping = self.find_overlapping_cached_ranges(&requested_range);

    if let Some(merged_data) = self.try_merge_ranges(&requested_range, &overlapping) {
        CacheResult::Hit(merged_data)
    } else {
        let gaps = self.calculate_missing_gaps(&requested_range, &overlapping);
        if gaps.len() <= MAX_ACCEPTABLE_GAPS {
            CacheResult::PartialHit { cached_segments: overlapping, missing_gaps: gaps }
        } else {
            CacheResult::Miss
        }
    }
}
```

#### **Files to Modify**
- `native/src/global_cache.rs` - ByteRangeCache enhancement
- Range merging utilities and algorithms
- Storage request optimization

---

## 🧪 Testing Strategy

### **Phase 1 Testing: Time-Based Eviction**
- **Scan pattern test**: Large query followed by small queries
- **Multi-thread test**: Concurrent operations with mixed query sizes
- **Memory pressure test**: Validate emergency eviction works
- **Performance benchmark**: Compare cache hit rates before/after

### **Phase 2 Testing: Memory Allocation**
- **Variable split sizes**: Test with 1K, 100K, 1M document splits
- **Memory usage validation**: Ensure allocations stay within bounds
- **Performance test**: Compare memory efficiency across split sizes
- **Integration test**: Verify thread-based scaling still works

### **Phase 3 Testing: Range Merging**
- **Range overlap scenarios**: Test various overlap patterns
- **Gap detection test**: Validate gap identification and filling
- **Performance benchmark**: Measure S3 request reduction
- **Correctness validation**: Ensure merged data integrity

### **Integration Testing**
- **End-to-end performance**: RealS3EndToEndTest with all optimizations
- **Concurrent workload**: Multiple threads with mixed operation types
- **Memory efficiency**: Overall memory usage patterns
- **Regression testing**: Ensure no performance regressions

## ✅ Success Criteria

### **Quantitative Goals**
1. **Cache hit rate improvement**: +15-30% for mixed workloads
2. **Memory allocation efficiency**: ±20% allocation accuracy based on split size
3. **Network request reduction**: -30-50% S3 requests through range merging
4. **Multi-thread stability**: Zero cache thrashing in concurrent scenarios

### **Qualitative Goals**
1. **Code maintainability**: Clean integration with existing architecture
2. **Configuration flexibility**: Easy tuning of optimization parameters
3. **Debug visibility**: Clear logging of optimization effectiveness
4. **Backward compatibility**: No breaking changes to existing API

## 🚀 Implementation Timeline

### **Phase 1: Time-Based Eviction Protection (2-3 days)**
- Day 1: Cache structure modifications and timestamp tracking
- Day 2: Eviction policy implementation and testing
- Day 3: Integration testing and performance validation

### **Phase 2: Document-Count-Aware Memory Allocation (2-3 days)**
- Day 1: Memory calculation logic and integration
- Day 2: Split metadata extraction and caching
- Day 3: Testing and performance benchmarking

### **Phase 3: ByteRange Cache Merging (3-4 days)**
- Day 1-2: Range overlap detection and merging algorithms
- Day 3: Gap detection and intelligent fetching
- Day 4: Integration testing and performance validation

### **Integration & Testing (1-2 days)**
- Comprehensive end-to-end testing
- Performance benchmarking and optimization
- Documentation and debug output enhancement

**Total Estimated Timeline: 8-12 days**

## 📝 Implementation Notes

### **Configuration Constants**
```rust
// Time-based eviction
const MIN_CACHE_ITEM_LIFETIME_SECS: u64 = 60;
const EMERGENCY_EVICTION_THRESHOLD: f64 = 0.95;

// Memory allocation
const MIN_MEMORY_ALLOCATION_MB: usize = 15;
const MAX_MEMORY_ALLOCATION_MB: usize = 100;
const BYTES_PER_DOCUMENT: usize = 50;

// Range merging
const MAX_ACCEPTABLE_GAPS: usize = 3;
const PREFETCH_ADJACENT_THRESHOLD: f64 = 0.8; // 80% cache hit rate triggers prefetch
```

### **Debug Output Enhancements**
- Cache eviction decisions with timing information
- Memory allocation calculations showing factors
- Range merging success/failure reasons with gap analysis
- Performance metrics for each optimization

This implementation plan provides a systematic approach to implementing all three high-impact cache optimizations, with clear testing strategies and success criteria. Each phase builds upon the previous optimizations while maintaining backward compatibility.