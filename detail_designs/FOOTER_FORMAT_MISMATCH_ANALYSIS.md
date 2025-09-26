# Footer Format Mismatch Analysis

**Date**: September 6, 2025  
**Issue**: Critical format mismatch between split creation and reading  
**Root Cause**: Incorrect footer format assumptions in merge process  
**Impact**: Subtraction overflow panic causing split search failures  

## Executive Summary

The split file corruption issue identified in `SPLIT_FILE_CORRUPTION_DEBUG_ANALYSIS.md` has been traced to a **critical format mismatch** between what Quickwit's `SplitPayloadBuilder` actually creates and what the tantivy4java merge process assumes. This mismatch causes incorrect footer offset calculations, leading to corrupted split files that fail during reading.

## Format Comparison Analysis

### **What Quickwit's SplitPayloadBuilder Actually Creates**

**Location**: `/quickwit/quickwit-storage/src/split.rs` - `finalize()` method

**Actual Format Structure**:
```rust
let mut footer_bytes = Vec::new();
footer_bytes.extend(&metadata_json);                           // BundleStorageFileOffsets JSON
footer_bytes.extend((metadata_json.len() as u32).to_le_bytes()); // 4-byte u32 LE length
footer_bytes.extend(hotcache);                                 // Hotcache data  
footer_bytes.extend((hotcache.len() as u32).to_le_bytes());     // 4-byte u32 LE length
```

**Real Format**: `[Files][BundleStorageFileOffsets JSON][u32 metadata_len][Hotcache][u32 hotcache_len]`

### **What Tantivy4Java Merge Process Assumes**

**Location**: `/native/src/quickwit_split.rs` - `create_quickwit_split()` lines 520-570

**Assumed Format Structure**:
```rust
let metadata_length = split_fields.len() as u64;        // ❌ WRONG: Uses field metadata size
let hotcache_length = hotcache.len() as u64;

// Assumes 8-byte u64 lengths (WRONG)
let hotcache_start_offset = total_file_size - 8 - hotcache_length;    // ❌ Should be -4
let footer_start_offset = hotcache_start_offset - 8 - metadata_length; // ❌ Wrong calculation
```

**Assumed Format**: `[Files][FieldMetadata][u64 metadata_len][Hotcache][u64 hotcache_len]`

## Critical Differences Identified

### **1. Length Field Size Mismatch**
- **Quickwit Uses**: 4-byte `u32` length fields
- **Tantivy4Java Assumes**: 8-byte `u64` length fields
- **Impact**: Footer offset calculations are off by 4 bytes per length field

### **2. Metadata Content Mismatch**
- **Quickwit Uses**: `BundleStorageFileOffsets` JSON serialization (file mapping data)
- **Tantivy4Java Assumes**: `split_fields` serialized data (field schema data)
- **Impact**: Metadata size calculation is completely wrong

### **3. Serialization Format Mismatch**
- **Quickwit Creates**: Complex JSON structure with file offset mappings
- **Tantivy4Java Calculates**: Simple field metadata byte array size
- **Impact**: The actual metadata is much larger than assumed

## Detailed Error Chain Analysis

### **Phase 1: Split Creation (Incorrect)**
1. **Wrong Metadata Size**: Uses `split_fields.len()` instead of actual `BundleStorageFileOffsets` JSON size
2. **Wrong Length Fields**: Assumes 8-byte lengths instead of 4-byte lengths
3. **Incorrect Footer Offsets**: Calculates wrong `footer_start_offset` and `footer_end_offset`
4. **Invalid Split File**: Writes split with corrupted footer metadata

### **Phase 2: Split Reading (Failure)**
1. **Footer Extraction**: Reads actual footer data (101 bytes available)
2. **Length Field Reading**: Reads corrupted length value (540 bytes claimed)
3. **Buffer Overflow**: Attempts `slice_from_end(540)` on 97-byte buffer
4. **Panic**: Arithmetic overflow in Tantivy's `FileSlice::slice_from_end`

## Code Analysis

### **Problematic Code in Split Creation**

**File**: `/native/src/quickwit_split.rs:520-570`

```rust
// ❌ PROBLEM 1: Wrong metadata size calculation
let metadata_length = split_fields.len() as u64;  // Should use BundleStorageFileOffsets JSON size

// ❌ PROBLEM 2: Wrong length field assumptions  
let hotcache_start_offset = total_file_size - 8 - hotcache_length;    // Should be -4 (u32)
let footer_start_offset = hotcache_start_offset - 8 - metadata_length; // Should be -4 (u32)
```

### **Correct Implementation Should Be**

```rust
// ✅ CORRECT: Calculate actual BundleStorageFileOffsets JSON size
let metadata_with_fixed_paths = /* construct file mappings */;
let bundle_storage_file_offsets = BundleStorageFileOffsets {
    files: metadata_with_fixed_paths,
};
let metadata_json = BundleStorageFileOffsetsVersions::serialize(&bundle_storage_file_offsets);
let metadata_length = metadata_json.len() as u64;  // Use actual JSON size

// ✅ CORRECT: Use 4-byte u32 length fields
let hotcache_start_offset = total_file_size - 4 - hotcache_length;    // 4 bytes for u32
let footer_start_offset = hotcache_start_offset - 4 - metadata_length; // 4 bytes for u32
```

### **Failing Code in Split Reading**

**File**: `/quickwit/quickwit-storage/src/bundle_storage.rs:199`

```rust
// This fails because footer_num_bytes (540) > available data (97)
let mut bundle_storage_file_offsets_data = tantivy_files_data
    .slice_from_end(footer_num_bytes as usize); // PANIC: 540 > 97
```

## Impact Assessment

### **Immediate Impact**
- **Severity**: Critical - Complete failure of split search operations
- **Scope**: All merged split files are corrupted and unusable
- **Symptoms**: Arithmetic overflow panics during split opening

### **Data Integrity Impact**
- **Split Files**: All created split files have invalid footer metadata
- **Search Operations**: Cannot read any merged split files
- **Recovery**: Requires regeneration of all affected split files

### **Performance Impact**
- **Zero Functionality**: Split search completely non-functional
- **Error Recovery**: No graceful degradation, hard panics
- **Debug Difficulty**: Misleading error messages hide root cause

## Validation Evidence

### **Debug Output Confirming Issues**
```
QUICKWIT DEBUG: BundleStorageFileOffsets::open - file slice len: 101  ← Available data
QUICKWIT DEBUG: Footer num bytes: 540                                ← Corrupted length claim
QUICKWIT DEBUG: Reading bundle storage file offsets data...
[PANIC: attempt to subtract with overflow]                           ← 540 > 97
```

### **Stack Trace Confirmation**
The panic occurs specifically in:
- `tantivy_common::file_slice::FileSlice::slice_from_end` (line 296)
- Called from `quickwit_storage::bundle_storage::BundleStorageFileOffsets::open` (line 199)

### **Format Structure Validation**
Analysis of Quickwit test cases in `/quickwit/quickwit-storage/src/split.rs` confirms:
- Uses `u32` length fields consistently
- Creates `BundleStorageFileOffsets` JSON metadata
- Footer structure matches documented format

## Required Fixes

### **1. Fix Footer Format Assumptions**

**File**: `/native/src/quickwit_split.rs`  
**Function**: `create_quickwit_split()`  
**Lines**: 520-570

```rust
// Replace incorrect calculations with correct format matching
let metadata_json = BundleStorageFileOffsetsVersions::serialize(&bundle_storage_file_offsets);
let metadata_length = metadata_json.len() as u64;  // Use actual metadata size
let hotcache_length = hotcache.len() as u64;

// Use correct 4-byte u32 length fields
let footer_end_offset = total_file_size;
let hotcache_start_offset = total_file_size - 4 - hotcache_length;    // 4-byte u32
let footer_start_offset = hotcache_start_offset - 4 - metadata_length; // 4-byte u32
```

### **2. Add Footer Format Validation**

Add validation to prevent future format mismatches:

```rust
// Validate footer offsets before writing
if footer_start_offset >= hotcache_start_offset {
    return Err(anyhow!(
        "Invalid footer calculation: footer_start({}) >= hotcache_start({})", 
        footer_start_offset, hotcache_start_offset
    ));
}

// Validate against expected Quickwit format
let expected_footer_size = metadata_length + 4 + hotcache_length + 4;
let actual_footer_size = total_file_size - footer_start_offset;
if expected_footer_size != actual_footer_size {
    return Err(anyhow!(
        "Footer size mismatch: expected {} bytes, calculated {} bytes",
        expected_footer_size, actual_footer_size
    ));
}
```

### **3. Add Error Handling in Reading**

**File**: `/quickwit/quickwit-storage/src/bundle_storage.rs`  
**Function**: `BundleStorageFileOffsets::open()`  
**Line**: 199

```rust
// Add bounds checking before slice_from_end
if footer_num_bytes as usize > tantivy_files_data.len() {
    return Err(anyhow!(
        "Corrupted split file: footer claims {} bytes but only {} available. \
         This indicates a footer format mismatch during split creation.",
        footer_num_bytes, tantivy_files_data.len()
    ));
}

let mut bundle_storage_file_offsets_data = tantivy_files_data
    .slice_from_end(footer_num_bytes as usize);
```

## Testing Strategy

### **1. Unit Tests for Format Compliance**
Create tests that validate split files match Quickwit format exactly:

```rust
#[test]
fn test_split_format_compliance() {
    let split_file = create_test_split();
    
    // Verify footer structure matches Quickwit format
    let footer_data = extract_footer(split_file);
    let (metadata, metadata_len, hotcache, hotcache_len) = parse_footer(footer_data);
    
    assert_eq!(metadata_len, metadata.len() as u32);
    assert_eq!(hotcache_len, hotcache.len() as u32);
    
    // Verify can be read by BundleStorageFileOffsets::open
    assert!(BundleStorageFileOffsets::open(footer_data).is_ok());
}
```

### **2. Integration Tests with Real Quickwit**
Test split files created by tantivy4java can be read by actual Quickwit:

```rust
#[test] 
fn test_quickwit_compatibility() {
    let split_file = merge_splits_with_tantivy4java();
    let bundle_dir = BundleDirectory::open_split(split_file).expect("Should open successfully");
    // Verify all files can be accessed
}
```

### **3. Regression Tests**
Prevent future format mismatches:

```rust
#[test]
fn test_footer_offset_calculation() {
    let (footer_offsets, actual_split_data) = create_split_with_known_data();
    
    // Verify calculated offsets match actual data structure
    let extracted_metadata = extract_at_offset(actual_split_data, footer_offsets.footer_start_offset);
    let extracted_hotcache = extract_at_offset(actual_split_data, footer_offsets.hotcache_start_offset);
    
    assert!(validate_metadata_structure(extracted_metadata));
    assert!(validate_hotcache_structure(extracted_hotcache));
}
```

## Prevention Measures

### **1. Format Documentation**
Create clear documentation of expected split file format with byte-level specifications.

### **2. Automated Format Validation**
Add CI tests that validate created split files against Quickwit format specifications.

### **3. Cross-Platform Testing** 
Test split creation and reading across different endianness and architecture combinations.

### **4. Version Compatibility Matrix**
Document which versions of tantivy4java splits are compatible with which Quickwit versions.

## Recovery Plan

### **1. Immediate Fix**
Apply the footer format corrections to prevent new corrupted split files.

### **2. Data Recovery**
All existing corrupted split files must be regenerated from source data.

### **3. Validation Deployment**
Deploy with additional validation to catch similar issues early.

### **4. Monitoring**
Add monitoring for split file creation and reading success rates.

---

**Conclusion**: This analysis confirms that the split file corruption is caused by a fundamental format mismatch between the creation and reading processes. The fix requires aligning the footer format calculations with Quickwit's actual implementation, using correct length field sizes, and proper metadata size calculations.