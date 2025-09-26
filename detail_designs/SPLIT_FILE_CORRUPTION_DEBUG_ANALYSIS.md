# Split File Corruption Debug Analysis

**Date**: September 6, 2025  
**Issue**: Subtraction overflow panic in `BundleStorageFileOffsets::open`  
**Root Cause**: Corrupted split file with invalid footer metadata length  

## Problem Summary

During split search operations, the system encounters a panic with the message:
```
thread '<unnamed>' panicked at /Users/schenksj/.cargo/git/checkouts/tantivy-9cf9af06161d52ce/80f5f1e/common/src/file_slice.rs:296:20:
attempt to subtract with overflow
```

## Debug Session Trace

### Initial Debug Output
```
QUICKWIT DEBUG: [Thread ThreadId(1)] BundleStorageFileOffsets::open - file size: 101
QUICKWIT DEBUG: [Thread ThreadId(1)] BundleStorageFileOffsets::open - file slice len: 101
QUICKWIT DEBUG: Splitting file from end for metadata length...
QUICKWIT DEBUG: Reading footer num bytes...
QUICKWIT DEBUG: Footer num bytes: 540
QUICKWIT DEBUG: Reading bundle storage file offsets data...
[PANIC OCCURS HERE]
```

### Key Values Analysis
- **Available data**: 101 bytes (footer data from split file)
- **After removing 4-byte length field**: 97 bytes
- **Footer metadata claims**: 540 bytes
- **Problem**: 540 > 97 → Subtraction overflow when calling `slice_from_end(540)`

## Root Cause Analysis

### The File Size Value Origin (101 bytes)

The 101-byte value represents footer data extracted from a Quickwit split file through this path:

1. **Original Source**: Split file footer extraction
   ```rust
   let footer_data = get_split_footer_from_cache_or_fetch(
       index_storage.clone(),
       split_and_footer_offsets,
       &searcher_context.split_footer_cache,
   ).await?;
   ```

2. **Byte Range Extraction**: Specific slice from split file
   ```rust
   let footer_data_opt = index_storage
       .get_slice(
           &split_file,
           split_and_footer_offsets.split_footer_start as usize
               ..split_and_footer_offsets.split_footer_end as usize,
       ).await?;
   ```

3. **FileSlice Creation**: Wrapping footer data
   ```rust
   FileSlice::new(Arc::new(footer_data))  // 101 bytes
   ```

4. **Split Processing**: Body and metadata separation
   ```rust
   let (body_and_bundle_metadata, hotcache) = split_footer(footer_data)?;
   // body_and_bundle_metadata.len() = 101
   ```

5. **Bundle Storage Processing**: Where the panic occurs
   ```rust
   let file_offsets = BundleStorageFileOffsets::open(body_and_bundle_metadata)?;
   // file.len() = 101, but footer claims 540 bytes needed
   ```

### What the 101 Bytes Should Contain

The 101 bytes should represent the **bundle metadata** portion containing:
- File offsets and ranges for all files in the bundle
- JSON-serialized metadata about file storage within the split
- Information needed to locate and extract individual files

### The Corruption

The corruption occurs in the footer length field:
- **Expected**: Footer length ≤ 97 bytes (available data after removing 4-byte length field)
- **Actual**: Footer length = 540 bytes
- **Result**: Arithmetic overflow when trying to extract 540 bytes from 97 bytes

## Stack Trace Analysis

### Thread Creation Path
The panic occurs in Thread ThreadId(1), which is created by the tokio runtime during S3 storage resolution:

1. **Java Test**: `RealS3EndToEndTest.step3_mergeSplitsFromS3()`
2. **Split Merge**: `QuickwitSplit.mergeSplits()` → `nativeMergeSplits()`
3. **Tokio Runtime**: `tokio::runtime::Builder::new_current_thread().build()`
4. **S3 Resolution**: `storage_resolver.resolve(&storage_uri).await`
5. **Split Search**: Eventually leads to bundle processing and panic

### Complete Call Stack
```
tantivy_common::file_slice::FileSlice::slice_from_end (PANIC HERE)
↓
quickwit_storage::bundle_storage::BundleStorageFileOffsets::open
↓
quickwit_storage::bundle_storage::BundleStorageFileOffsets::open_from_split_data  
↓
quickwit_storage::bundle_storage::BundleStorage::open_from_split_data
↓
quickwit_search::leaf::open_split_bundle
↓
quickwit_search::leaf::leaf_search_single_split
↓
tantivy4java::standalone_searcher::StandaloneSearcher::search_single_split_with_permit
```

## Technical Details

### Bundle Storage File Format
Expected format: `[Files, FileMetadata, FileMetadata Len, HotCache, HotCache Len]`

### Processing Steps
1. Extract footer data (101 bytes total)
2. Remove last 4 bytes to get metadata length: `101 - 4 = 97 bytes available`
3. Read metadata length from last 4 bytes: `540 bytes claimed`
4. **FAILURE**: Attempt `97.slice_from_end(540)` → overflow

### The Failing Code
In `/quickwit/quickwit-storage/src/bundle_storage.rs:199`:
```rust
let mut bundle_storage_file_offsets_data = tantivy_files_data
    .slice_from_end(footer_num_bytes as usize); // 540 > 97 = OVERFLOW
```

## Fix Requirements

### 1. Split Generation Validation
- Verify footer length calculation in `merge_splits_impl()`
- Ensure metadata size matches actual serialized JSON size
- Add validation checks before writing footer length

### 2. Error Handling Improvement
- Add bounds checking before calling `slice_from_end()`
- Provide meaningful error messages for corrupted split files
- Implement recovery mechanisms for invalid metadata

### 3. Testing Enhancements
- Add test cases for corrupted split files
- Validate split file format after generation
- Test boundary conditions and edge cases

## Code Locations to Investigate

### Primary Suspects
1. **Split Merge Implementation**: `/native/src/quickwit_split.rs:merge_splits_impl()`
2. **Bundle Storage Creation**: `/quickwit/quickwit-storage/src/bundle_storage.rs:BundleStorageFileOffsets::open()`
3. **Footer Length Calculation**: Wherever the 4-byte footer length is written to split files

### Debug Additions Needed
1. Add validation in `BundleStorageFileOffsets::open()`:
   ```rust
   if footer_num_bytes as usize > tantivy_files_data.len() {
       return Err(anyhow!(
           "Corrupted split: footer claims {} bytes but only {} available", 
           footer_num_bytes, tantivy_files_data.len()
       ));
   }
   ```

2. Add footer size validation during split creation
3. Implement split file integrity checks

## Prevention Strategy

### During Split Creation
- Calculate actual serialized metadata size
- Write correct footer length (should be much less than 540 bytes)
- Add checksums or validation markers

### During Split Reading  
- Validate footer length against available data
- Implement graceful error handling for corrupted files
- Add recovery mechanisms where possible

## Impact Assessment

### Current Impact
- **Severity**: High - Causes complete failure of split search operations
- **Scope**: Affects all operations using corrupted split files
- **Recovery**: Requires regeneration of affected split files

### Risk Areas
- Split merge operations creating corrupted files
- Any code relying on split file footer metadata
- Production deployments using merged splits

## Recommended Actions

1. **Immediate**: Add bounds checking to prevent panic
2. **Short-term**: Fix split generation to write correct footer lengths  
3. **Long-term**: Implement comprehensive split file validation and recovery

## Test Cases to Add

1. **Corrupted Footer Length**: Split with footer length > available data
2. **Zero Footer Length**: Split with footer length = 0
3. **Missing Footer**: Split without proper footer structure
4. **Boundary Conditions**: Footer length = available data exactly

---

**Note**: This issue was discovered during integration of tantivy4java with Quickwit split functionality. The root cause is in the split file generation process writing incorrect metadata lengths.