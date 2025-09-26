# Problem Statement: Multiple Storage Instances Preventing Fast Field Cache Sharing

## Issue Summary
The fast field cache (file fragment cache) is not sharing cached data across queries, resulting in redundant S3 downloads for the same file ranges. This occurs despite successful implementation of shared SearcherContext and footer cache sharing.

## Root Cause Analysis
Multiple distinct storage instances are being created, each with their own separate cache, instead of sharing a single global cache instance:

**Evidence:**
- **Storage Instance A** (`0x6000001cead0`): Used for footer fetches (891KB file) - cache working correctly
- **Storage Instance B** (`0x600000e4b310`): Used for fast field fetches (small 2-4KB ranges) - cache not shared

**Impact:**
- Same file ranges fetched **3 times** instead of **1 time + 2 cache hits**
- Each query performs ~47 redundant S3 GET requests taking ~50ms each
- Total performance penalty: ~2.35 seconds per query in unnecessary I/O

## Expected vs Actual Behavior

**Expected (with proper cache sharing):**
```
Query 1: 47 S3 requests (cache misses) + store in cache
Query 2: 0 S3 requests (47 cache hits)
Query 3: 0 S3 requests (47 cache hits)
Total: 47 S3 requests
```

**Actual (current broken state):**
```
Query 1: 47 S3 requests + store in Storage Instance B cache
Query 2: 47 S3 requests + store in NEW Storage Instance B cache
Query 3: 47 S3 requests + store in NEW Storage Instance B cache
Total: 141 S3 requests (200% redundant I/O)
```

## Technical Analysis

### Cache Architecture Status
- ✅ **SearcherContext sharing**: Single global instance (`0x14bccd620`) correctly reused
- ✅ **Footer cache sharing**: Same cache instance (`0x133ff5378`) with 90.9% hit rate
- ❌ **Fast field cache sharing**: Multiple storage instances bypass shared cache

### Storage Instance Creation Points
The issue stems from storage instances being created in multiple locations without coordination:

1. **Split bundle opening** - Creates storage for footer access
2. **Fast field access** - Creates separate storage for file fragment access
3. **Directory initialization** - May create additional storage instances
4. **Index opening** - Potential additional storage creation

Each creation point likely instantiates its own storage with independent cache, rather than using the shared `fast_fields_cache` from `GlobalSearcherComponents`.

## Scope of Required Changes

### Minimal Fix (Recommended)
**Objective**: Ensure all storage instances use the same shared cache instance

**Approach**:
1. **Identify storage creation points** - Find where `0x600000e4b310` vs `0x6000001cead0` are created
2. **Centralize storage creation** - Route all storage creation through global cache configuration
3. **Enforce shared cache usage** - Ensure `GlobalSearcherComponents.fast_fields_cache` is used consistently

**Files likely affected**:
- Storage creation in split searcher initialization
- Directory setup in bundle opening
- Index storage configuration in Tantivy integration

### Comprehensive Fix (Alternative)
**Objective**: Implement storage instance pooling/sharing

**Approach**:
1. Create global storage instance registry
2. Implement storage instance sharing by key (S3 bucket + credentials)
3. Ensure cache sharing through storage instance sharing

## Success Criteria

**Verification Method**:
```bash
# After fix, should see same storage instance across all requests
fgrep 'storage instance:' /tmp/dbg | sort | uniq -c
# Expected: Single storage instance address for all operations

# Fast field cache hit rate should improve dramatically
fgrep 'get_slice range:' /tmp/dbg | sort | uniq -c
# Expected: Each range fetched only once instead of 3 times
```

**Performance Target**:
- **Current**: 141 S3 requests for 3 identical queries
- **Target**: 47 S3 requests for 3 identical queries (66% reduction)
- **Expected speedup**: ~2.35 seconds saved per query after first query

## Priority Assessment
- **Impact**: High - 200% redundant I/O overhead affects all query performance
- **Complexity**: Medium - Requires storage architecture coordination but within single codebase
- **Risk**: Low - Changes isolated to storage creation/configuration, existing cache logic proven working

**Recommended Timeline**: Address after footer cache sharing success is validated in production, as footer cache provides immediate 90.9% hit rate improvement for the largest file operations.

## Current Success: Footer Cache Sharing
**Note**: The footer cache sharing has been successfully implemented and is working correctly:
- **1 cache miss** on first request (expected)
- **10 cache hits** on subsequent requests (90.9% hit rate)
- **Same SearcherContext instance** shared across all operations
- **Zero SearchPermitProvider panics** with sync implementation

This provides immediate performance benefits for the largest file operations (891KB footer fetches), while the fast field cache issue represents an additional optimization opportunity.