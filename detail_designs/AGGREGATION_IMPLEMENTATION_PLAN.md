# Tantivy4Java Aggregation Implementation Plan

## Executive Summary
This plan outlines the implementation of aggregation functionality in tantivy4java, following Quickwit's architecture patterns and leveraging the existing searcher infrastructure to provide efficient statistical and analytical capabilities over indexed data.

## Current State Analysis

### Existing Infrastructure
1. **Java Layer Components (Already Created)**:
   - Aggregation class hierarchy: `SplitAggregation` (base), `StatsAggregation`, `SumAggregation`, `AverageAggregation`, `MinAggregation`, `MaxAggregation`, `CountAggregation`
   - Result classes: `AggregationResult` (base), `StatsResult`, `SumResult`, `AverageResult`, `MinResult`, `MaxResult`, `CountResult`
   - Bucket aggregation classes: `TermsAggregation`, `HistogramAggregation`, `DateHistogramAggregation`, `RangeAggregation`
   - Test suite: `SplitSearcherAggregationTest` with comprehensive test scenarios

2. **SplitSearcher API (Defined but Not Implemented)**:
   - `search(query, limit, aggregations)` - Search with aggregations
   - `aggregate(query, aggregations)` - Aggregation-only search
   - `searchWithAggregations()` native method stub

3. **Native Layer**:
   - Existing searcher infrastructure with Tantivy integration
   - Access to Quickwit's collector and aggregation modules

## Architecture Design

### 1. Aggregation Flow Architecture
```
Java Layer (API & Model)
    ↓
JNI Binding (Parameter Marshalling)
    ↓
Rust Native Layer (Orchestration)
    ↓
Tantivy Aggregation Collectors
    ↓
Quickwit Intermediate Results
    ↓
Result Transformation
    ↓
Java Result Objects
```

### 2. Key Design Principles
- **Zero-Copy Where Possible**: Use ByteBuffers for bulk data transfer
- **Lazy Evaluation**: Only compute requested aggregations
- **Memory Efficiency**: Stream results rather than buffer everything
- **Cache Integration**: Leverage existing split cache for aggregation results
- **Quickwit Compatibility**: Follow Quickwit's aggregation JSON format

## Implementation Plan

### Phase 1: Core Infrastructure (Foundation)
**Goal**: Establish the native-to-Java aggregation pipeline

#### 1.1 Native Aggregation Collector Setup
- [ ] Create `aggregation_collector.rs` module
- [ ] Implement aggregation JSON parsing from Java layer
- [ ] Set up Tantivy `AggregationCollector` integration
- [ ] Handle Quickwit's `IntermediateAggregationResults`

#### 1.2 JNI Bridge Implementation
- [ ] Implement `Java_com_tantivy4java_SplitSearcher_searchWithAggregations` in JNI
- [ ] Create aggregation parameter marshalling (Map<String, SplitAggregation> → JSON)
- [ ] Implement result unmarshalling (bytes → Java objects)

#### 1.3 Result Transformation Layer
- [ ] Create result deserializer for `IntermediateAggregationResults`
- [ ] Transform Tantivy results to Java result objects
- [ ] Handle nested aggregation results properly

### Phase 2: Metric Aggregations (Stats)
**Goal**: Implement all statistical aggregations

#### 2.1 Stats Aggregation
- [ ] Parse stats aggregation JSON: `{"stats": {"field": "fieldName"}}`
- [ ] Create Tantivy stats collector
- [ ] Compute: count, sum, avg, min, max
- [ ] Return `StatsResult` object

#### 2.2 Individual Metric Aggregations
- [ ] Count aggregation (document count)
- [ ] Sum aggregation (field sum)
- [ ] Average aggregation (field average)
- [ ] Min aggregation (field minimum)
- [ ] Max aggregation (field maximum)

#### 2.3 Performance Optimizations
- [ ] Fast field access optimization
- [ ] Column-oriented data processing
- [ ] Parallel segment processing

### Phase 3: Bucket Aggregations
**Goal**: Implement grouping/bucketing aggregations

#### 3.1 Terms Aggregation
- [ ] Parse terms aggregation JSON
- [ ] Create term buckets with doc counts
- [ ] Support sub-aggregations per bucket
- [ ] Handle top-k terms efficiently

#### 3.2 Histogram Aggregations
- [ ] Numeric histogram with fixed intervals
- [ ] Date histogram with calendar intervals
- [ ] Range aggregation with custom buckets
- [ ] Extended bounds support

#### 3.3 Nested Aggregations
- [ ] Support sub-aggregations within buckets
- [ ] Recursive aggregation processing
- [ ] Memory-efficient result tree building

### Phase 4: Query Integration
**Goal**: Integrate aggregations with existing query infrastructure

#### 4.1 Query Context Support
- [ ] Apply query filters before aggregation
- [ ] Support match-all queries for full dataset
- [ ] Handle complex boolean queries

#### 4.2 Mixed Search/Aggregation
- [ ] Combine document hits with aggregations
- [ ] Optimize for aggregation-only queries (limit=0)
- [ ] Handle search_after with aggregations

### Phase 5: Caching & Performance
**Goal**: Optimize aggregation performance

#### 5.1 Aggregation Result Caching
- [ ] Integrate with existing `LeafSearchCache`
- [ ] Cache key generation for aggregation queries
- [ ] Partial result caching for large aggregations

#### 5.2 Memory Management
- [ ] Implement aggregation memory limits
- [ ] Circuit breaker for expensive aggregations
- [ ] Streaming result processing

## Technical Implementation Details

### 1. Native Method Signature
```rust
#[no_mangle]
pub extern "C" fn Java_com_tantivy4java_SplitSearcher_searchWithAggregations(
    env: JNIEnv,
    _obj: JObject,
    searcher_ptr: jlong,
    query_obj: JObject,
    limit: jint,
    aggregations_map: JObject,
) -> jobject {
    // Implementation
}
```

### 2. Aggregation JSON Format (Quickwit-Compatible)
```json
{
  "score_stats": {
    "stats": {
      "field": "score"
    }
  },
  "category_terms": {
    "terms": {
      "field": "category",
      "size": 10
    },
    "aggs": {
      "avg_score": {
        "avg": {
          "field": "score"
        }
      }
    }
  }
}
```

### 3. Collector Integration Pattern
```rust
// Based on Quickwit's collector pattern
let agg_req: Aggregations = serde_json::from_str(&agg_json)?;
let agg_collector = AggregationCollector::from_aggs(
    agg_req,
    None, // No sub-aggregations initially
    aggregation_limits,
);

let (top_docs_collector, aggregation_collector) = match aggregations_opt {
    Some(aggs) => {
        let agg_collector = create_aggregation_collector(aggs)?;
        (top_docs_collector, Some(agg_collector))
    }
    None => (top_docs_collector, None)
};

// Combine collectors
let multi_collector = (top_docs_collector, aggregation_collector);
let results = searcher.search(&query, &multi_collector)?;
```

### 4. Result Processing Flow
```rust
// Extract intermediate results
let intermediate_results = if let Some(agg_collector) = results.1 {
    agg_collector.harvest()?
} else {
    IntermediateAggregationResults::default()
};

// Convert to final results
let final_results = intermediate_results.into_final_results(agg_req)?;

// Serialize for Java layer
let result_json = serde_json::to_string(&final_results)?;
```

## Testing Strategy

### 1. Unit Tests
- Test each aggregation type individually
- Verify JSON serialization/deserialization
- Test error handling for invalid fields

### 2. Integration Tests
- Test aggregations with different query types
- Verify aggregation accuracy against known data
- Test performance with large datasets

### 3. Compatibility Tests
- Ensure Quickwit JSON format compatibility
- Test with existing SplitSearcher infrastructure
- Verify cache integration works correctly

## Performance Considerations

### 1. Memory Usage
- Aggregations can consume significant memory for high-cardinality fields
- Implement memory circuit breakers (following Quickwit's `AggregationLimitsGuard`)
- Use streaming where possible for large result sets

### 2. Fast Field Access
- Leverage Tantivy's columnar storage for numeric aggregations
- Ensure fast fields are properly configured for aggregated fields
- Use type-specific collectors for optimal performance

### 3. Caching Strategy
- Cache aggregation results at the split level
- Use query + aggregation as cache key
- Implement TTL-based eviction for aggregation cache

## Risk Mitigation

### 1. Technical Risks
- **Risk**: Complex JNI marshalling for nested results
- **Mitigation**: Start with flat aggregations, add nesting incrementally

### 2. Performance Risks
- **Risk**: Aggregations slowing down regular searches
- **Mitigation**: Separate code paths for search vs. aggregation-only queries

### 3. Memory Risks
- **Risk**: OOM from unbounded aggregations
- **Mitigation**: Implement strict memory limits and circuit breakers

## Success Metrics

1. **Functional Completeness**
   - All test cases in `SplitSearcherAggregationTest` passing
   - Support for all Quickwit aggregation types

2. **Performance Targets**
   - Aggregation-only queries < 100ms for 1M documents
   - Memory overhead < 50MB for typical aggregations
   - Cache hit rate > 80% for repeated aggregations

3. **Code Quality**
   - Zero memory leaks in native layer
   - Comprehensive error handling
   - Clear documentation and examples

## Next Steps

1. **Immediate Actions**:
   - Set up aggregation collector infrastructure in native layer
   - Implement basic stats aggregation end-to-end
   - Create JNI bridge for aggregation parameters

2. **Short-term Goals** (Week 1-2):
   - Complete all metric aggregations
   - Add comprehensive testing
   - Document API usage

3. **Long-term Goals** (Week 3-4):
   - Implement bucket aggregations
   - Add caching layer
   - Performance optimization

## Code References

Key files to modify:
- `/native/src/split_searcher_replacement.rs` - Add aggregation support
- `/native/src/lib.rs` - Register new JNI methods
- `/native/src/aggregation_collector.rs` - New module for aggregation logic
- `/src/main/java/com/tantivy4java/SplitSearcher.java` - Already has API defined

Quickwit references:
- `/quickwit/quickwit-search/src/collector.rs` - Aggregation collector patterns
- `/quickwit/quickwit-search/src/leaf.rs` - Leaf search with aggregations
- `tantivy::aggregation` module - Core aggregation functionality

## Conclusion

This implementation plan provides a structured approach to adding aggregation functionality to tantivy4java. By following Quickwit's proven patterns and leveraging the existing searcher infrastructure, we can deliver a robust, performant aggregation system that seamlessly integrates with the current codebase.

The phased approach allows for incremental delivery of value while maintaining system stability. Starting with metric aggregations provides immediate value, while the later phases add more sophisticated analytical capabilities.