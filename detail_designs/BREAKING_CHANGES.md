# Breaking Changes Notice

## Version 0.24.1 - Complete Quickwit Integration Migration

### Summary
Major overhaul of the `QuickwitSplit.SplitMetadata` class to fully integrate with Quickwit's native implementation, including constructor signature changes, deprecated method behavior updates, and new field additions.

### Breaking Change Details

**Affected Class:** `com.tantivy4java.QuickwitSplit.SplitMetadata`

## Change 1: Complete Constructor Migration to Quickwit Format

**Original Constructor (Pre-0.24.0):**
```java
public SplitMetadata(String splitId, long numDocs, long uncompressedSizeBytes,
                   Instant timeRangeStart, Instant timeRangeEnd, Set<String> tags,
                   long deleteOpstamp, int numMergeOps,
                   long footerStartOffset, long footerEndOffset,
                   long hotcacheStartOffset, long hotcacheLength, String docMappingJson,
                   List<String> skippedSplits)
```

**Quickwit-Compatible Constructor (0.24.0):**
```java
public SplitMetadata(String splitId, String indexUid, long partitionId, String sourceId,
                   String nodeId, long numDocs, long uncompressedSizeBytes,
                   Instant timeRangeStart, Instant timeRangeEnd, long createTimestamp,
                   String maturity, Set<String> tags, long footerStartOffset, long footerEndOffset,
                   long deleteOpstamp, int numMergeOps, String docMappingUid,
                   List<String> skippedSplits)
```

**Latest Constructor (0.24.1):**
```java
public SplitMetadata(String splitId, String indexUid, long partitionId, String sourceId,
                   String nodeId, long numDocs, long uncompressedSizeBytes,
                   Instant timeRangeStart, Instant timeRangeEnd, long createTimestamp,
                   String maturity, Set<String> tags, long footerStartOffset, long footerEndOffset,
                   long deleteOpstamp, int numMergeOps, String docMappingUid, String docMappingJson,
                   List<String> skippedSplits)
```

## Change 2: New Required Fields Added

**New Quickwit-compatible fields that must be provided:**
- `String indexUid` - Required by Quickwit metadata structure
- `long partitionId` - Required by Quickwit metadata structure
- `String sourceId` - Required by Quickwit metadata structure
- `String nodeId` - Required by Quickwit metadata structure
- `long createTimestamp` - Required by Quickwit metadata structure
- `String maturity` - Required by Quickwit metadata structure ("Mature" or "Immature")
- `String docMappingUid` - Required by Quickwit metadata structure

## Change 3: Deprecated Methods Behavior Change

**Hotcache Methods (Now Deprecated):**
```java
// These methods now throw UnsupportedOperationException
public long getHotcacheStartOffset() // Previously returned actual values
public long getHotcacheLength()      // Previously returned actual values
```

**Document Mapping JSON Method (Restored):**
```java
// This method was previously deprecated but is now functional again
public String getDocMappingJson()    // Now returns actual JSON content
```

### Required Actions

## Action 1: Update Direct Constructor Calls

### Migration from Original (Pre-0.24.0) to Latest (0.24.1):

**Before (Original Constructor):**
```java
QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
    "split-id",                    // splitId
    100L,                          // numDocs
    1000000L,                      // uncompressedSizeBytes
    timeStart, timeEnd,            // timeRange
    tags,                          // tags
    0L, 1,                         // deleteOpstamp, numMergeOps
    5000L, 10000L,                // footerStartOffset, footerEndOffset
    3000L, 2000L,                 // hotcacheStartOffset, hotcacheLength
    "{\"fields\":[]}",            // docMappingJson
    skippedSplits                 // skippedSplits
);
```

**After (Latest Constructor):**
```java
QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
    "split-id",                    // splitId
    "my-index-uid",               // indexUid (NEW - required)
    0L,                           // partitionId (NEW - required)
    "my-source-id",               // sourceId (NEW - required)
    "my-node-id",                 // nodeId (NEW - required)
    100L,                         // numDocs
    1000000L,                     // uncompressedSizeBytes
    timeStart, timeEnd,           // timeRange
    System.currentTimeMillis() / 1000, // createTimestamp (NEW - required)
    "Mature",                     // maturity (NEW - required)
    tags,                         // tags
    5000L, 10000L,               // footerStartOffset, footerEndOffset
    0L, 1,                       // deleteOpstamp, numMergeOps
    "doc-mapping-uid",           // docMappingUid (NEW - required)
    "{\"fields\":[]}",           // docMappingJson (MOVED - for performance)
    skippedSplits                // skippedSplits
);
```

### Incremental Migration (If upgrading from 0.24.0):

**Before (0.24.0 Constructor):**
```java
QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
    "split-id", "index-uid", 0L, "source-id", "node-id",
    100L, 1000000L, timeStart, timeEnd, timestamp, "Mature", tags,
    5000L, 10000L, 0L, 1, "doc-mapping-uid", skippedSplits
);
```

**After (0.24.1 Constructor):**
```java
QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
    "split-id", "index-uid", 0L, "source-id", "node-id",
    100L, 1000000L, timeStart, timeEnd, timestamp, "Mature", tags,
    5000L, 10000L, 0L, 1, "doc-mapping-uid", null,  // Added docMappingJson parameter
    skippedSplits
);
```

## Action 2: Handle Deprecated Method Behavior Changes

### Update Hotcache Method Usage:

**Before (Working Code):**
```java
// These calls worked in previous versions
long hotcacheStart = metadata.getHotcacheStartOffset();
long hotcacheLength = metadata.getHotcacheLength();
```

**After (Required Changes):**
```java
// Use footer offsets instead - hotcache is deprecated in Quickwit format
long footerStart = metadata.getFooterStartOffset();
long footerEnd = metadata.getFooterEndOffset();

// If you specifically need hotcache-like behavior, calculate from footer offsets
// or migrate to Quickwit's footer-based caching system
```

### Update Document Mapping Access:

**Before (Threw Exception):**
```java
// This threw UnsupportedOperationException in 0.24.0
String docMapping = metadata.getDocMappingJson();
```

**After (Now Working):**
```java
// This now returns actual JSON content for performance
String docMapping = metadata.getDocMappingJson();
if (docMapping != null) {
    // Use for tokenization or other operations
}
```

## Action 3: Update Test Code

**Search for these patterns in your test code:**
1. `new QuickwitSplit.SplitMetadata(` - Update constructor calls
2. `getHotcacheStartOffset()` - Replace with footer offset usage
3. `getHotcacheLength()` - Replace with footer offset calculations
4. Try-catch blocks around `getDocMappingJson()` - Remove as it no longer throws exceptions

#### If you're using factory methods or library-generated metadata:
- **No action required** - The library automatically populates all fields during split creation and merge operations.

### New Functionality and Improvements

#### 1. Enhanced getDocMappingJson() Method
```java
// Previously threw UnsupportedOperationException in 0.24.0
// Now returns actual JSON content for tokenization performance
String docMappingJson = metadata.getDocMappingJson();
```

#### 2. Complete Quickwit Metadata Structure
All new fields provide full compatibility with Quickwit's distributed search infrastructure:
- `indexUid` - Unique identifier for the index
- `partitionId` - Partition information for distributed processing
- `sourceId` - Source identifier for data provenance
- `nodeId` - Node identifier for distributed operations
- `createTimestamp` - Creation timestamp for versioning
- `maturity` - Split maturity status for lifecycle management
- `docMappingUid` - Document mapping unique identifier

#### 3. Performance Improvements
- **Tokenization Performance:** SplitSearcher now uses cached document mapping JSON instead of expensive schema extraction
- **Native Integration:** Full integration with Quickwit's native merge and split operations
- **Memory Optimization:** Eliminates redundant schema loading during tokenization operations

#### 4. New Getter Methods
```java
// New Quickwit-compatible getters
String indexUid = metadata.getIndexUid();
long partitionId = metadata.getPartitionId();
String sourceId = metadata.getSourceId();
String nodeId = metadata.getNodeId();
long createTimestamp = metadata.getCreateTimestamp();
String maturity = metadata.getMaturity();
String docMappingUid = metadata.getDocMappingUid();
```

### Comprehensive Migration Guide

#### Step 1: Audit Your Codebase
Search for these patterns:
```bash
# Find constructor usage
grep -r "new QuickwitSplit.SplitMetadata(" src/

# Find hotcache method usage
grep -r "getHotcacheStartOffset\|getHotcacheLength" src/

# Find doc mapping JSON usage
grep -r "getDocMappingJson" src/
```

#### Step 2: Update Constructor Calls
Follow the examples in Action 1 above to add required Quickwit fields.

#### Step 3: Replace Deprecated Methods
Follow the examples in Action 2 above to migrate from hotcache to footer offsets.

#### Step 4: Test and Validate
1. Ensure your code compiles with the new signatures
2. Run your test suite to validate behavior
3. Check that tokenization performance has improved
4. Verify that split merge operations work correctly

#### Step 5: Optional Performance Enhancement
If you have document mapping JSON available during split creation, pass it directly instead of `null` for optimal tokenization performance.

### Migration Timeline Recommendations

1. **Immediate (Required):** Update constructor calls to compile
2. **Short-term (Recommended):** Replace hotcache method usage with footer offsets
3. **Medium-term (Optional):** Optimize by providing document mapping JSON when available
4. **Long-term (Best Practice):** Migrate to using library factory methods instead of direct constructor calls

### Rationale for Changes

These breaking changes were implemented to:

1. **Quickwit Integration:** Achieve full compatibility with Quickwit's distributed search infrastructure
2. **Performance Optimization:** Eliminate expensive schema extraction operations during tokenization
3. **Future-Proofing:** Align with Quickwit's roadmap and native implementations
4. **Standards Compliance:** Follow Quickwit's metadata structure standards
5. **Operational Excellence:** Enable better monitoring, debugging, and distributed operations

### Support

If you encounter issues during migration:
1. Check that you've added the `docMappingJson` parameter in the correct position
2. Ensure you're passing `null` if you don't have document mapping JSON available
3. Report issues at: https://github.com/anthropics/claude-code/issues

### Version History
- **0.24.0:** Original Quickwit-compatible constructor
- **0.24.1:** Added `docMappingJson` parameter for tokenization performance