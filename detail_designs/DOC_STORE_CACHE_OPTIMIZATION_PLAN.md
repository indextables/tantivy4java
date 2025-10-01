# Doc Store Cache Optimization Implementation Plan

## 🎯 Objective
Implement Quickwit's doc store cache optimization to prevent cache thrashing during bulk document retrieval operations, improving performance by 2-5x for batch operations.

## 📊 Current State vs Target State

### Current Implementation
- Uses default Tantivy cache settings for all operations
- No differentiation between single-doc and bulk retrieval
- Cache size not tuned for concurrent bulk operations
- Potential cache thrashing with large batch sizes

### Target Implementation (Quickwit Pattern)
```rust
let index_reader = index
    .reader_builder()
    .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS) // 30 blocks
    .reload_policy(ReloadPolicy::Manual)
    .try_into()?;
```

## 🔧 Technical Implementation Steps

### Step 1: Add Cache Configuration Constants
- Add `NUM_CONCURRENT_REQUESTS` constant (30) to match Quickwit
- Add configurable cache block size for different operation types
- **File**: `split_searcher_replacement.rs`

### Step 2: Implement Optimized Index Reader Builder
- Create specialized index reader configuration for bulk operations
- Apply `doc_store_cache_num_blocks` optimization
- Set `ReloadPolicy::Manual` for consistency with Quickwit
- **Location**: `retrieve_documents_batch_from_split_optimized` function

### Step 3: Update Batch Document Retrieval Path
- Modify the fallback path (non-cached searcher) in batch function
- Apply cache optimization when creating new index readers
- Ensure cache settings are applied consistently
- **File**: Lines ~1420-1450 in `split_searcher_replacement.rs`

### Step 4: Add Cache Configuration for Individual Retrieval
- Apply similar optimization to individual document retrieval when applicable
- Differentiate between single-doc and batch cache strategies
- **Location**: `perform_doc_retrieval_async_impl_thread_safe` function

### Step 5: Performance Validation
- Add debug logging for cache configuration
- Measure performance improvements in existing tests
- Validate that cache thrashing is eliminated

## 🎯 Expected Performance Improvements

- **Batch Operations**: 2-5x improvement for large batches (>50 documents)
- **Cache Hit Rate**: Increased from ~60-70% to 85-95%
- **Memory Efficiency**: Reduced cache churn and garbage collection pressure
- **Consistency**: Aligned with Quickwit's proven optimization patterns

## 📝 Implementation Details

### Cache Block Sizing Strategy
```rust
const NUM_CONCURRENT_REQUESTS: usize = 30;
const SINGLE_DOC_CACHE_BLOCKS: usize = 10; // For individual retrieval
const BATCH_DOC_CACHE_BLOCKS: usize = NUM_CONCURRENT_REQUESTS; // For batch operations
```

### Integration Points
1. **Batch Document Retrieval**: Primary target for optimization
2. **Individual Document Retrieval**: Secondary optimization for consistency
3. **Index Opening**: Ensure cache settings are applied during index creation
4. **Cache Validation**: Add metrics and debugging for cache effectiveness

## 🧪 Testing Strategy

### Performance Tests
- Measure batch document retrieval performance before/after
- Test with varying batch sizes (10, 50, 100, 500 documents)
- Validate cache hit rates and memory usage
- Use existing `RealS3EndToEndTest` for real-world validation

### Regression Tests
- Ensure individual document retrieval performance is maintained
- Verify no regressions in existing functionality
- Test with various split sizes and configurations

## ✅ Success Criteria

1. **Performance**: 2x+ improvement in batch document retrieval (100+ docs)
2. **Cache Hit Rate**: >85% hit rate for bulk operations
3. **Compatibility**: No regressions in existing functionality
4. **Code Quality**: Clean integration following Quickwit patterns
5. **Documentation**: Clear debug output showing cache optimization active

## 🚀 Implementation Order

1. **Phase 1**: Add constants and basic cache configuration
2. **Phase 2**: Implement cache optimization in batch retrieval path
3. **Phase 3**: Apply optimization to individual retrieval path
4. **Phase 4**: Add performance validation and testing
5. **Phase 5**: Documentation and debug output enhancement

This optimization directly addresses cache thrashing issues identified in bulk document retrieval operations and aligns our implementation with Quickwit's proven performance patterns.