# Storage Cache Sharing Implementation Status

## Problem Summary

The STORAGE_INSTANCE_CACHE_SHARING_ISSUE.md identified that multiple distinct storage instances are being created, each with their own separate cache, preventing effective cache sharing for fast field operations.

**Evidence**:
- Storage Instance A (`0x6000001cead0`): Used for footer fetches - working correctly
- Storage Instance B (`0x600000e4b310`): Used for fast field fetches - cache not shared
- Result: Same file ranges fetched 3 times instead of 1 time + 2 cache hits

## Implementation Attempts

### Attempt 1: StorageResolver Caching (FAILED - DEADLOCK)

**Implementation**: Added global cache of StorageResolver instances by configuration key.

```rust
static CONFIGURED_STORAGE_RESOLVERS: Lazy<Arc<RwLock<HashMap<String, StorageResolver>>>> = ...;

pub fn get_configured_storage_resolver(s3_config_opt: Option<S3StorageConfig>) -> StorageResolver {
    // Cache StorageResolver instances by S3 config key
    // Use double-checked locking pattern with RwLock
}
```

**Result**: ❌ **DEADLOCK** - Test hangs during "Cache Manager Reuse Validation" phase

**Root Cause**: Even with RwLock and double-checked locking, the StorageResolver caching causes deadlocks. User confirmed: "the hang goes away if i stash your code"

**Status**: ✅ **REVERTED** - Deadlock resolved, test now passes completely

## Current Status

✅ **No Deadlock**: RealS3EndToEndTest passes completely
❌ **No Cache Sharing**: Original problem remains - multiple storage instances still created
🔄 **Need Alternative**: Must find different approach to achieve cache sharing

## Alternative Approaches to Investigate

### Option A: Storage-Level Caching
Instead of caching StorageResolver instances, cache the actual Storage instances returned by `resolver.resolve(uri)`.

**Pros**:
- Caches closer to the actual problem
- Might avoid StorageResolver creation complexity
- Could be simpler synchronization

**Cons**:
- Need to track storage instance lifecycle
- URI-based caching might be complex

### Option B: Explicit Cache Injection
Modify the SplitSearcher creation to explicitly inject shared cache instances.

**Pros**:
- Direct control over cache sharing
- No complex caching logic needed
- Clear ownership model

**Cons**:
- Requires changes to SplitSearcher API
- Might break existing patterns

### Option C: Different Synchronization Strategy
Use a different approach for caching that avoids the deadlock:
- Lazy static with Once initialization
- Lock-free data structures
- Message passing for cache management

### Option D: Investigation First
Before implementing new approaches, investigate:
1. **Where exactly** are multiple storage instances being created?
2. **Why** does StorageResolver caching cause deadlocks?
3. **What** is the actual call path that leads to multiple instances?

## Recommendation

**Next Step**: Investigate the exact call paths that create multiple storage instances before implementing a new caching strategy. This will help ensure we solve the right problem at the right level.

**Action Items**:
1. Add debug logging to track storage instance creation points
2. Analyze the call stack that leads to multiple instances
3. Determine if the issue is in tantivy4java code or Quickwit behavior
4. Choose the most appropriate level for intervention

## Test Validation

The RealS3EndToEndTest can be used to validate any new approach:
- ✅ **Test must not hang** (deadlock prevention)
- 🔍 **Debug logging should show cache sharing** (problem resolution)
- ⚡ **Performance should improve** (66% reduction in S3 I/O)

```bash
# Test command for validation
TANTIVY4JAVA_DEBUG=1 timeout 600s mvn test -Dtest="RealS3EndToEndTest" -DfailIfNoTests=false
```