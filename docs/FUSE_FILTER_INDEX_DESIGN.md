# Binary Fuse Filter XRef Design

## Overview

Replace the current Tantivy-index-based XRef system with a Binary Fuse Filter-based approach for faster indexing and smaller file sizes.

**Key decisions:**
- **Filter type**: Binary Fuse8 filters via `xorf` crate (~3% space overhead vs 44% for Bloom)
- **Filter granularity**: One filter per split with field-qualified keys (`"fieldname:value"`)
- **File format**: Quickwit bundle format for S3/Azure compatibility
- **Query support**: Full SplitQuery except range/wildcard queries (return "exists" for those)

---

## 1. File Format Specification

### 1.1 Bundle Layout (Quickwit-Compatible)

```
+-------------------------------------------------------+
| Files Section                                         |
|   fuse_filters.bin    - Binary fuse filter data       |
|   split_metadata.json - Per-split metadata array      |
|   xref_header.json    - Global XRef metadata          |
+-------------------------------------------------------+
| BundleStorageFileOffsets (JSON)                       |
+-------------------------------------------------------+
| Metadata length (4 bytes, little endian)              |
+-------------------------------------------------------+
| HotCache (optional)                                   |
+-------------------------------------------------------+
| HotCache length (4 bytes, little endian)              |
+-------------------------------------------------------+
```

### 1.2 fuse_filters.bin Structure

```
+---------------------------------------------------------------+
| Header (32 bytes)                                              |
|   Magic: "FXRF" (4 bytes)                                      |
|   Version: u32 (4 bytes) - currently 1                         |
|   Num Filters: u32 (4 bytes)                                   |
|   Filter Type: u32 (4 bytes) - 1=Fuse8, 2=Fuse16               |
|   Reserved: 16 bytes                                           |
+---------------------------------------------------------------+
| Filter Index Table (num_filters * 24 bytes each)               |
|   For each filter:                                             |
|     Offset: u64 (8 bytes) - byte offset into filter data       |
|     Size: u64 (8 bytes) - size in bytes                        |
|     Num Keys: u64 (8 bytes) - number of keys in filter         |
+---------------------------------------------------------------+
| Filter Data (variable length, serde-serialized BinaryFuse8)    |
+---------------------------------------------------------------+
```

### 1.3 split_metadata.json

```json
{
  "splits": [
    {
      "split_idx": 0,
      "uri": "s3://bucket/splits/split-001.split",
      "split_id": "split-001",
      "footer_start": 1000000,
      "footer_end": 1050000,
      "num_docs": 150000,
      "num_terms": 45000,
      "filter_size_bytes": 55800
    }
  ]
}
```

### 1.4 xref_header.json

```json
{
  "magic": "FXRF",
  "format_version": 1,
  "xref_id": "daily-xref-2024-01-15",
  "index_uid": "logs-index",
  "num_splits": 1000,
  "total_terms": 45000000,
  "filter_type": "BinaryFuse8",
  "created_at": 1705312000,
  "footer_start_offset": 67000000,
  "footer_end_offset": 67100000
}
```

---

## 2. Rust Implementation

### 2.1 New Files to Create

| File | Purpose |
|------|---------|
| `native/src/fuse_xref/mod.rs` | Module root, re-exports |
| `native/src/fuse_xref/types.rs` | Core data structures |
| `native/src/fuse_xref/builder.rs` | Build logic with term streaming |
| `native/src/fuse_xref/query.rs` | Query evaluation |
| `native/src/fuse_xref/storage.rs` | Serialization/bundle format |
| `native/src/fuse_xref/jni.rs` | JNI bindings |

### 2.2 Core Types (`types.rs`)

```rust
use xorf::{BinaryFuse8, Filter};
use serde::{Serialize, Deserialize};

pub const FUSE_XREF_MAGIC: &[u8; 4] = b"FXRF";
pub const FUSE_XREF_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
pub struct SplitFilterMetadata {
    pub split_idx: u32,
    pub uri: String,
    pub split_id: String,
    pub footer_start: u64,
    pub footer_end: u64,
    pub num_docs: u64,
    pub num_terms: u64,
    pub filter_size_bytes: u64,
}

pub struct FuseXRef {
    pub header: FuseXRefHeader,
    pub filters: Vec<BinaryFuse8>,
    pub metadata: Vec<SplitFilterMetadata>,
}

impl FuseXRef {
    /// Check if a key possibly exists in a split's filter
    pub fn check(&self, split_idx: usize, field: &str, value: &str) -> bool {
        let key = format!("{}:{}", field, value);
        let hash = fxhash::hash64(&key);
        self.filters[split_idx].contains(&hash)
    }
}
```

### 2.3 Builder (`builder.rs`)

```rust
pub struct FuseXRefBuilder {
    config: XRefBuildConfig,
}

impl FuseXRefBuilder {
    pub async fn build(&self, output_path: &Path) -> Result<XRefMetadata> {
        let mut all_filters = Vec::new();
        let mut all_metadata = Vec::new();

        for (idx, source) in self.config.source_splits.iter().enumerate() {
            // 1. Stream terms from source split (reuse existing HotDirectory pattern)
            let terms = self.collect_terms_from_split(source).await?;

            // 2. Hash all terms as "field:value"
            let hashes: Vec<u64> = terms.iter()
                .map(|(field, value)| fxhash::hash64(&format!("{}:{}", field, value)))
                .collect();

            // 3. Build Binary Fuse8 filter
            let filter = BinaryFuse8::try_from(&hashes)
                .map_err(|e| anyhow!("Failed to build filter: {:?}", e))?;

            all_filters.push(filter);
            all_metadata.push(SplitFilterMetadata {
                split_idx: idx as u32,
                uri: source.uri.clone(),
                split_id: source.split_id.clone(),
                footer_start: source.footer_start,
                footer_end: source.footer_end,
                num_docs: source.num_docs.unwrap_or(0),
                num_terms: terms.len() as u64,
                filter_size_bytes: 0, // Set after serialization
            });
        }

        // 4. Write to Quickwit bundle format
        self.write_bundle(output_path, all_filters, all_metadata).await
    }
}
```

### 2.4 Query Evaluation (`query.rs`)

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum FilterResult {
    NotPresent,      // Definitely not in split
    PossiblyPresent, // May be present (could be false positive)
    Exists,          // Cannot evaluate (range/wildcard)
}

pub fn evaluate_query(xref: &FuseXRef, split_idx: usize, query: &QueryAst) -> FilterResult {
    match query {
        QueryAst::Term { field, value } => {
            if xref.check(split_idx, field, value) {
                FilterResult::PossiblyPresent
            } else {
                FilterResult::NotPresent
            }
        }

        QueryAst::Bool { must, should, must_not } => {
            // All MUST clauses must pass
            for clause in must {
                if evaluate_query(xref, split_idx, clause) == FilterResult::NotPresent {
                    return FilterResult::NotPresent;
                }
            }

            // At least one SHOULD clause must pass (if any exist)
            if !should.is_empty() {
                let any_present = should.iter()
                    .any(|c| evaluate_query(xref, split_idx, c) != FilterResult::NotPresent);
                if !any_present {
                    return FilterResult::NotPresent;
                }
            }

            // MUST_NOT: Cannot definitively exclude with filters
            FilterResult::PossiblyPresent
        }

        QueryAst::Phrase { field, terms } => {
            // Check all terms exist (cannot verify positions)
            for term in terms {
                if !xref.check(split_idx, field, term) {
                    return FilterResult::NotPresent;
                }
            }
            FilterResult::PossiblyPresent
        }

        // Cannot evaluate these query types
        QueryAst::Range { .. } => FilterResult::Exists,
        QueryAst::Wildcard { .. } => FilterResult::Exists,
        QueryAst::Regex { .. } => FilterResult::Exists,

        QueryAst::MatchAll => FilterResult::PossiblyPresent,
        QueryAst::MatchNone => FilterResult::NotPresent,
    }
}

/// Search all splits and return matching ones
pub fn search(xref: &FuseXRef, query: &QueryAst, limit: usize) -> Vec<MatchingSplit> {
    let mut results = Vec::new();

    for (idx, meta) in xref.metadata.iter().enumerate() {
        match evaluate_query(xref, idx, query) {
            FilterResult::NotPresent => continue, // Skip this split
            FilterResult::PossiblyPresent | FilterResult::Exists => {
                results.push(MatchingSplit {
                    uri: meta.uri.clone(),
                    split_id: meta.split_id.clone(),
                    footer_start: meta.footer_start,
                    footer_end: meta.footer_end,
                });
            }
        }

        if results.len() >= limit {
            break;
        }
    }

    results
}
```

---

## 3. Java API Changes

### 3.1 XRefBuildConfig.java (Minor changes)

```java
// Remove: includePositions (not applicable for filters)
// Keep everything else the same - API is compatible
```

### 3.2 XRefSearchResult.java (Add field)

```java
/**
 * True if query contained range/wildcard clauses that could not be evaluated.
 * When true, results may include splits that don't actually match.
 */
private boolean hasUnevaluatedClauses;
```

### 3.3 Native Method Signatures (Update)

```java
// In XRefSplit.java - signature stays the same, implementation changes
private static native String nativeBuildXRefSplit(String configJson, String outputPath);

// In XRefSearcher.java - signature stays the same
private static native String nativeSearchXRef(long handle, String queryJson, int limit);
```

---

## 4. Cargo.toml Dependencies

```toml
[dependencies]
xorf = { version = "0.11", features = ["serde"] }
fxhash = "0.2"
```

---

## 5. Files to Modify/Delete

### Delete (old XRef implementation):
- `native/src/xref_streaming.rs` (~1400 lines)
- `native/src/xref_split.rs` (~300 lines)

### Modify:
- `native/src/lib.rs` - Replace xref modules with fuse_xref
- `native/src/xref_types.rs` - Keep metadata types, remove Tantivy-specific types
- `native/Cargo.toml` - Add xorf, fxhash; can remove some tantivy dependencies
- `src/main/java/.../xref/XRefBuildConfig.java` - Remove includePositions field
- `src/main/java/.../xref/XRefSearchResult.java` - Add hasUnevaluatedClauses

### Keep unchanged:
- `src/main/java/.../xref/XRefSplit.java` - API unchanged
- `src/main/java/.../xref/XRefSearcher.java` - API unchanged
- `src/main/java/.../xref/XRefSourceSplit.java` - API unchanged

---

## 6. Implementation Order

### Phase 1: Core Rust Implementation
1. Add `xorf` and `fxhash` to Cargo.toml
2. Create `native/src/fuse_xref/mod.rs` and types
3. Implement builder (reuse term streaming from xref_streaming.rs)
4. Implement query evaluation
5. Implement bundle serialization

### Phase 2: JNI Integration
1. Create JNI bindings in `fuse_xref/jni.rs`
2. Update `lib.rs` to expose new functions
3. Test with existing Java test infrastructure

### Phase 3: Java Updates
1. Update XRefSearchResult with hasUnevaluatedClauses
2. Remove includePositions from XRefBuildConfig
3. Update tests

### Phase 4: Cleanup
1. Delete xref_streaming.rs and xref_split.rs
2. Remove unused dependencies
3. Update documentation

---

## 7. Size Comparison

For 10,000 splits with average 50,000 terms each at effective 0.4% FPR:

| Metric | Current (Tantivy) | Binary Fuse8 |
|--------|-------------------|--------------|
| Per-split size | ~200 KB | ~62 KB |
| Total size | ~2 GB | ~620 MB |
| Build time | ~15 min | ~3 min (est.) |
| Query time | ~5ms | ~1ms (est.) |

**~70% size reduction, ~5x faster queries expected.**
