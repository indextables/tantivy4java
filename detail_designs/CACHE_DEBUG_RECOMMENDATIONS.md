# Cache Debug Recommendations

## Current Issue Analysis

The cache implementation has been successfully updated, but the search_test is still showing 15 consecutive cache misses for the same split footer. This suggests one of several possible issues:

### 1. Build Integration Issue
**Problem**: The changes may not be reflected in the running code.

**Solutions**:
```bash
# Ensure the native library is rebuilt with changes
cd /Users/schenksj/tmp/x/tantivy4java
mvn clean package -DskipTests

# Verify the updated library is being used
ls -la target/classes/native/
```

### 2. Search Test Integration Path
**Problem**: The search_test might be using a different code path that bypasses our cache fixes.

**Investigation**:
- The test uses `com.tantivy4spark.core.Tantivy4SparkTableProvider`
- This likely goes through tantivy4spark → tantivy4java → native layer
- Need to verify the integration chain is using our updated cache

### 3. Cache Instance Isolation (Most Likely)
**Problem**: Even with our fixes, Spark tasks might still be creating separate JVM processes or classloader contexts.

**Enhanced Debug Strategy**:

Add this environment variable to see our new debug output:
```bash
export TANTIVY4JAVA_DEBUG=1
```

Then look for these specific debug messages in the output:
- `🔍 CACHE ENTRY: get_global_searcher_context() called`
- `🔍 CACHE IDENTITY: Using shared split_footer_cache Arc at address: 0x...`
- `📊 CACHE SUMMARY: Footer Cache: ... items, ... KB, ...% hit rate`

### 4. Storage Metrics Sharing Verification
**Problem**: Even if we share the same `STORAGE_METRICS.split_footer_cache`, the cache instances might not be properly synchronized.

**Debug Check**:
Look for debug output showing:
- Same cache instance addresses across calls
- Cache reference counts increasing (multiple Arc references)
- Cache hit rate improving over time

## Recommended Testing Approach

### Phase 1: Verify Debug Output
1. Rebuild with `mvn clean package -DskipTests`
2. Set `TANTIVY4JAVA_DEBUG=1`
3. Run the search_test and check for our new debug messages

### Phase 2: Check Cache Identity
Look for these patterns in the debug output:
```
🔍 CACHE ENTRY: get_global_searcher_context() called - using SHARED global caches
🔍 CACHE IDENTITY: Using shared split_footer_cache Arc at address: 0x12345678, ref_count: 2
📊 CACHE SUMMARY:
  Footer Cache: 0 items, 0 KB, 0% hit rate
```

If the cache addresses are different across calls, cache sharing isn't working.

### Phase 3: Alternative Testing
If the issue persists, test with a simpler tantivy4java-only test:
1. Create a minimal Java test that directly uses SplitSearcher
2. Search the same split multiple times
3. Verify cache hits occur

### Phase 4: Advanced Debugging
If cache sharing still doesn't work, the issue might be:
1. **Spark Isolation**: Each Spark task creates separate JVMs
2. **ClassLoader Issues**: Multiple classloaders creating separate singletons
3. **Maven Dependency Issues**: Using old tantivy4java version

## Expected Behavior After Fix

With working cache sharing, you should see:
```bash
# First query - cache miss (expected)
🔍 QUICKWIT DEBUG: ❌ Footer not in cache, will fetch from storage

# Subsequent queries - cache hits
🔍 QUICKWIT DEBUG: ✅ Footer found in cache, size: 891080 bytes
🔍 QUICKWIT DEBUG: ✅ Footer found in cache, size: 891080 bytes
# ... 14 more cache hits instead of misses
```

## Troubleshooting Steps

1. **Verify rebuild**: Check that `mvn package` completes without errors
2. **Check debug output**: Ensure `TANTIVY4JAVA_DEBUG=1` shows our new messages
3. **Test simplified case**: Create a direct tantivy4java test (not through Spark)
4. **Check Maven dependencies**: Ensure search_test is using the updated tantivy4java

If none of these resolve the issue, the problem may be architectural - Spark's distributed nature might require a different caching approach.