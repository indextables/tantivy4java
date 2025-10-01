# Comprehensive Debug Logging for Storage Instance Cache Sharing

## Overview

I've added extensive debug logging to track thread progression, identify deadlocks, and validate that the storage instance cache sharing fix is working correctly. The logging focuses on critical paths where the previous deadlock occurred.

## Debug Logging Features Added

### 🧵 **Thread ID Tracking**
- **Purpose**: Identify which threads are executing which operations
- **Format**: `Thread ThreadId(123)` in all log messages
- **Benefit**: Track concurrent access patterns and identify potential deadlock scenarios

### ⏱️ **Comprehensive Timing Measurements**
- **Purpose**: Measure lock acquisition times and total operation duration
- **Granularity**: Microsecond precision for lock operations, millisecond for total operations
- **Benefit**: Validate that lock duration improvements are working

### 🔒 **Lock Acquisition/Release Tracking**
- **Purpose**: Monitor exactly when locks are acquired and released
- **Detail Level**: Individual lock scopes with entry/exit logging
- **Benefit**: Identify if threads are waiting for locks or progressing normally

## Detailed Logging Areas

### 1. **StorageResolver Caching** (`global_cache.rs:99-193`)

**Entry Point Logging:**
```
🧵 STORAGE_RESOLVER: Thread ThreadId(123) ENTRY into get_configured_storage_resolver [0ms]
🔑 STORAGE_RESOLVER: Thread ThreadId(123) generated cache key: us-east-1:default:AKIA...:false [1ms]
```

**First Cache Check (with timing):**
```
🔒 STORAGE_RESOLVER: Thread ThreadId(123) ATTEMPTING first cache lock [2ms]
✅ STORAGE_RESOLVER: Thread ThreadId(123) ACQUIRED first cache lock in 45μs [2ms total]
🎯 STORAGE_RESOLVER: Thread ThreadId(123) CACHE HIT - reusing existing resolver for key: us-east-1:... [3ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(123) RELEASING first cache lock after HIT [3ms]
```

**Cache Miss Path:**
```
❌ STORAGE_RESOLVER: Thread ThreadId(123) CACHE MISS for key: us-east-1:... [2ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(123) RELEASED first cache lock after MISS in 38μs [2ms total]
🏗️  STORAGE_RESOLVER: Thread ThreadId(123) STARTING expensive resolver creation (NO LOCK HELD) [3ms]
📋 STORAGE_RESOLVER: Thread ThreadId(123) StorageConfigs created in 125μs [5ms total]
⚙️  STORAGE_RESOLVER: Thread ThreadId(123) StorageResolver::configured completed in 45ms [50ms total]
```

**Second Lock for Insertion:**
```
🔒 STORAGE_RESOLVER: Thread ThreadId(123) ATTEMPTING second cache lock for insertion [50ms]
✅ STORAGE_RESOLVER: Thread ThreadId(123) ACQUIRED second cache lock in 23μs [50ms total]
💾 STORAGE_RESOLVER: Thread ThreadId(123) INSERTED new resolver into cache for key: us-east-1:... [51ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(123) RELEASED second cache lock after insertion in 15μs [51ms total]
🏁 STORAGE_RESOLVER: Thread ThreadId(123) COMPLETED with NEW resolver in 51ms total
```

**Race Condition Detection:**
```
🏃 STORAGE_RESOLVER: Thread ThreadId(456) RACE CONDITION - another thread created resolver, using existing [45ms]
```

### 2. **SplitSearcher Creation** (`split_searcher_replacement.rs:114-339`)

**Entry Logging:**
```
🧵 SPLIT_SEARCHER: Thread ThreadId(123) ENTRY into createNativeWithSharedCache [0ms]
🔗 SPLIT_SEARCHER: Thread ThreadId(123) cache_manager_ptr: 0x6000001ab64d0 [1ms]
```

**Success Completion:**
```
🏁 SPLIT_SEARCHER: Thread ThreadId(123) COMPLETED successfully in 234ms - pointer: 0x600000e4b310
```

**Error Handling:**
```
❌ SPLIT_SEARCHER: Thread ThreadId(123) FAILED after 156ms - error: Failed to resolve storage
```

### 3. **SplitSearcher Closure** (`split_searcher_replacement.rs:343-380`)

**Close Operations:**
```
🧵 SPLIT_SEARCHER_CLOSE: Thread ThreadId(123) ENTRY into closeNative - pointer: 0x600000e4b310
🏁 SPLIT_SEARCHER_CLOSE: Thread ThreadId(123) COMPLETED successfully - pointer: 0x600000e4b310
```

## Expected Debug Output Patterns

### ✅ **Successful Cache Sharing (No Deadlock)**
```
🧵 STORAGE_RESOLVER: Thread ThreadId(1) ENTRY into get_configured_storage_resolver [0ms]
🔑 STORAGE_RESOLVER: Thread ThreadId(1) generated cache key: us-east-1:default:AKIA...:false [1ms]
🔒 STORAGE_RESOLVER: Thread ThreadId(1) ATTEMPTING first cache lock [2ms]
✅ STORAGE_RESOLVER: Thread ThreadId(1) ACQUIRED first cache lock in 25μs [2ms total]
❌ STORAGE_RESOLVER: Thread ThreadId(1) CACHE MISS for key: us-east-1:... [2ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(1) RELEASED first cache lock after MISS in 30μs [2ms total]
🏗️  STORAGE_RESOLVER: Thread ThreadId(1) STARTING expensive resolver creation (NO LOCK HELD) [3ms]
⚙️  STORAGE_RESOLVER: Thread ThreadId(1) StorageResolver::configured completed in 45ms [48ms total]
🔒 STORAGE_RESOLVER: Thread ThreadId(1) ATTEMPTING second cache lock for insertion [48ms]
✅ STORAGE_RESOLVER: Thread ThreadId(1) ACQUIRED second cache lock in 18μs [48ms total]
💾 STORAGE_RESOLVER: Thread ThreadId(1) INSERTED new resolver into cache for key: us-east-1:... [49ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(1) RELEASED second cache lock after insertion in 12μs [49ms total]
🏁 STORAGE_RESOLVER: Thread ThreadId(1) COMPLETED with NEW resolver in 49ms total

[Second searcher creation - should show cache hit]
🧵 STORAGE_RESOLVER: Thread ThreadId(2) ENTRY into get_configured_storage_resolver [0ms]
🔑 STORAGE_RESOLVER: Thread ThreadId(2) generated cache key: us-east-1:default:AKIA...:false [1ms]
🔒 STORAGE_RESOLVER: Thread ThreadId(2) ATTEMPTING first cache lock [2ms]
✅ STORAGE_RESOLVER: Thread ThreadId(2) ACQUIRED first cache lock in 15μs [2ms total]
🎯 STORAGE_RESOLVER: Thread ThreadId(2) CACHE HIT - reusing existing resolver for key: us-east-1:... [2ms]
🔓 STORAGE_RESOLVER: Thread ThreadId(2) RELEASING first cache lock after HIT [2ms]
```

### ❌ **Deadlock Pattern (Should NOT occur after fix)**
```
🧵 STORAGE_RESOLVER: Thread ThreadId(1) ENTRY into get_configured_storage_resolver [0ms]
🔒 STORAGE_RESOLVER: Thread ThreadId(1) ATTEMPTING first cache lock [2ms]
✅ STORAGE_RESOLVER: Thread ThreadId(1) ACQUIRED first cache lock in 25μs [2ms total]
[... long pause - thread hangs here, never releases lock ...]
```

## Validation Points

### 🎯 **Cache Sharing Success Indicators**
1. **Second searcher creation shows CACHE HIT**: Proves StorageResolver caching is working
2. **Lock acquisition times < 100μs**: Proves no contention/deadlock
3. **Fast completion times**: Second searcher creation should be much faster
4. **Same cache key**: Both searchers generate identical cache keys for same S3 config

### ⚠️ **Potential Issues to Watch For**
1. **Lock acquisition > 1ms**: May indicate contention
2. **Missing CACHE HIT on second searcher**: Cache sharing not working
3. **Thread hangs**: Look for threads that don't complete their operations
4. **Different cache keys**: Configuration inconsistency

## Testing Command

```bash
TANTIVY4JAVA_DEBUG=1 mvn test -Dtest="RealS3EndToEndTest" -DfailIfNoTests=false
```

This will show all the detailed debug output and validate that:
1. ✅ Threads progress through operations without hanging
2. ✅ Lock durations are minimal (microseconds not milliseconds)
3. ✅ Cache sharing works (second searcher gets CACHE HIT)
4. ✅ No deadlock occurs during "Cache Manager Reuse Validation" phase

## Benefits

- **Deadlock Detection**: Immediate visibility if threads hang on lock acquisition
- **Performance Validation**: Proof that lock duration improvements are working
- **Cache Effectiveness**: Confirmation that StorageResolver instances are being reused
- **Thread Safety**: Multi-threaded operation tracking for concurrent searcher creation
- **Debugging Support**: Comprehensive information for troubleshooting any issues

The logging provides complete visibility into the critical path that was previously causing deadlocks, ensuring the fix is working correctly.