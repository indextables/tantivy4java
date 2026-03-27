# Rust Transaction Log Design

Drop-in replacement for the Scala `TransactionLog` in `search_test`. Implements protocol v4 (Avro state format), backward-compatible with existing txlogs. No JSON protocol support.

**Naming:**
- **Rust crate**: `indextables-native` (module: `src/txlog/`, referenced as `indextables_native::txlog`)
- **Java package**: `io.indextables.jni.txlog`
- **JNI prefix**: `io_indextables_jni_txlog_`

---

## 1. Module Layout

```
native/src/txlog/
├── mod.rs                    # Public API surface
├── actions.rs                # Action types (Add, Remove, Skip, Protocol, Metadata)
├── avro/
│   ├── mod.rs
│   ├── schemas.rs            # Avro schema definitions (field IDs 100-159)
│   ├── manifest_reader.rs    # Read single Avro manifest → Vec<FileEntry>
│   ├── manifest_writer.rs    # Write Avro manifests with partition bounds
│   ├── state_reader.rs       # Read state-v<N>/ directory (manifest list + parts)
│   └── state_writer.rs       # Write state-v<N>/ directory atomically
├── version_file.rs           # Read/write individual version JSON files (post-checkpoint)
├── checkpoint.rs             # Checkpoint creation, compaction detection
├── log_replay.rs             # Action replay: latest-action-per-path wins
├── schema_dedup.rs           # SHA-256 docMappingJson ↔ docMappingRef
├── partition_pruning.rs      # Numeric-aware partition filter evaluation
├── skip.rs                   # SkipAction management (merge/read skip tracking)
├── cache.rs                  # Unified cache with read/invalidate/refresh semantics
├── storage.rs                # Integration with existing StorageResolver + ObjectStore
├── compression.rs            # Gzip compress/decompress (Scala-compatible)
├── distributed.rs            # Distributable primitives (driver + executor)
├── serialization.rs          # TANT byte buffer serialization for JNI
├── arrow_ffi.rs              # Arrow FFI export (RecordBatch of FileEntry columns)
├── metrics.rs                # Write retries, cache stats, timing
├── garbage_collection.rs     # Orphaned manifest + old version cleanup
├── tombstone_distributor.rs  # Distribute RemoveActions across manifests
├── jni.rs                    # JNI entry points
└── error.rs                  # TxLogError enum
```

---

## 2. Action Types

All five action types from Scala, represented as Rust enums/structs.

```rust
// actions.rs

/// Top-level action envelope — one per line in version files,
/// one per record in Avro manifests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Action {
    Protocol(ProtocolAction),
    MetaData(MetadataAction),
    Add(AddAction),
    Remove(RemoveAction),
    MergeSkip(SkipAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolAction {
    pub min_reader_version: u32,    // 4 for Avro state
    pub min_writer_version: u32,    // 4 for Avro state
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataAction {
    pub id: String,
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    pub format: FormatSpec,
    pub configuration: HashMap<String, String>, // includes docMappingSchema.<hash> entries
    pub created_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddAction {
    // Core fields
    pub path: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub modification_time: i64,
    pub data_change: bool,

    // Statistics
    pub stats: Option<String>,          // JSON stats blob
    pub min_values: Option<HashMap<String, String>>,
    pub max_values: Option<HashMap<String, String>>,
    pub num_records: Option<i64>,

    // Footer optimization
    pub footer_start_offset: Option<i64>,
    pub footer_end_offset: Option<i64>,
    pub has_footer_offsets: Option<bool>,

    // Split metadata
    pub split_tags: Option<HashMap<String, String>>,
    pub num_merge_ops: Option<i32>,
    pub doc_mapping_json: Option<String>,   // full JSON (pre-dedup)
    pub doc_mapping_ref: Option<String>,    // 16-char Base64 hash (post-dedup)
    pub uncompressed_size_bytes: Option<i64>,

    // Time range (partition pruning)
    pub time_range_start: Option<i64>,
    pub time_range_end: Option<i64>,

    // Companion mode
    pub companion_source_files: Option<Vec<String>>,
    pub companion_delta_version: Option<i64>,
    pub companion_fast_field_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveAction {
    pub path: String,
    pub deletion_timestamp: Option<i64>,
    pub data_change: bool,
    pub partition_values: Option<HashMap<String, String>>,
    pub size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkipAction {
    pub path: String,
    pub skip_timestamp: i64,
    pub reason: String,         // "merge", "read", custom
    pub operation: Option<String>,
    pub partition_values: Option<HashMap<String, String>>,
    pub size: Option<i64>,
    pub retry_after: Option<i64>,
    pub skip_count: Option<i32>,
}

/// Avro state-specific: FileEntry with streaming metadata.
/// Superset of AddAction for state file serialization.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub add: AddAction,
    pub added_at_version: i64,
    pub added_at_timestamp: i64,
}
```

---

## 3. Avro Schema & State Format

### 3.1 Avro Schema (Scala-compatible field IDs)

```rust
// avro/schemas.rs

/// Field ID ranges (matching Scala AvroSchemas.scala):
///   100-109: Basic file info (path, size, modTime, dataChange, partitionValues)
///   110-119: Statistics (stats, minValues, maxValues, numRecords)
///   120-129: Footer offsets
///   130-139: Split metadata (splitTags, numMergeOps, docMapping*, uncompressedSize)
///   140-149: Streaming info (addedAtVersion, addedAtTimestamp)
///   150-159: Companion mode fields
pub fn file_entry_avro_schema() -> apache_avro::Schema {
    // Exact replica of Scala's AvroSchemas.FileEntrySchema
    // Must match field names, types, field IDs, and defaults precisely
    // for backward compatibility with existing state-v<N>/ directories.
    ...
}
```

### 3.2 State Directory Layout

```
<table_path>/_transaction_log/
├── 00000000000000000000.json      # Version 0 (protocol + metadata + initial adds)
├── 00000000000000000001.json      # Version 1 (incremental actions)
├── ...
├── state-v<N>/                    # Avro state checkpoint at version N
│   ├── _manifest                  # StateManifest: list of part files + global bounds
│   ├── manifest-0000.avro         # Avro-serialized Vec<FileEntry>
│   ├── manifest-0001.avro         # Partition-grouped for pruning
│   └── ...
├── _last_checkpoint               # JSON: {version, format:"avro-state", stateDir}
└── <post-checkpoint versions>.json
```

### 3.3 StateManifest

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateManifest {
    pub version: i64,
    pub manifests: Vec<ManifestInfo>,
    pub partition_bounds: Option<PartitionBounds>,
    pub created_time: i64,
    pub total_file_count: i64,
    pub format: String, // "avro-state"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestInfo {
    pub path: String,               // relative: "manifest-0000.avro"
    pub file_count: i64,
    pub partition_bounds: Option<PartitionBounds>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionBounds {
    pub min_values: HashMap<String, String>,
    pub max_values: HashMap<String, String>,
}
```

### 3.4 LastCheckpointInfo

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastCheckpointInfo {
    pub version: i64,
    pub size: i64,           // total action count
    pub size_in_bytes: Option<i64>,
    pub num_files: Option<i64>,
    pub format: String,      // "avro-state"
    pub state_dir: Option<String>,
    pub created_time: Option<i64>,
}
```

---

## 4. Storage Integration

Reuses the existing `StorageResolver` and `ObjectStore` infrastructure from `global_cache/storage_resolver.rs`.

```rust
// storage.rs

use crate::global_cache::storage_resolver::get_or_create_storage_resolver;
use quickwit_storage::StorageResolver;
use object_store::ObjectStore;

/// Credential-aware storage handle for txlog operations.
/// Same credential caching and isolation as SplitCacheManager.
pub struct TxLogStorage {
    resolver: StorageResolver,
    table_path: String,
    txlog_path: String,       // <table_path>/_transaction_log/
}

impl TxLogStorage {
    /// Create from config map (same keys as delta/iceberg readers).
    pub fn new(table_path: &str, config: &StorageConfig) -> Result<Self> {
        let resolver = get_or_create_storage_resolver(config)?;
        let txlog_path = format!("{}/_transaction_log/", table_path.trim_end_matches('/'));
        Ok(Self { resolver, table_path: table_path.to_string(), txlog_path })
    }

    // -- Primitive operations --

    /// Read bytes at path (relative to txlog root).
    pub async fn get(&self, relative_path: &str) -> Result<Bytes>;

    /// Write bytes atomically. Uses conditional put (if-not-exists) for version files.
    pub async fn put_if_absent(&self, relative_path: &str, data: Bytes) -> Result<bool>;

    /// Unconditional write (for checkpoints, manifests).
    pub async fn put(&self, relative_path: &str, data: Bytes) -> Result<()>;

    /// List entries under prefix (relative to txlog root).
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check existence.
    pub async fn exists(&self, relative_path: &str) -> Result<bool>;

    /// Delete (for GC only).
    pub async fn delete(&self, relative_path: &str) -> Result<()>;

    /// Version file path: "00000000000000000042.json"
    pub fn version_path(version: i64) -> String {
        format!("{:020}.json", version)
    }
}

/// Unified credential config (same keys as delta/iceberg readers in common.rs).
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub azure_account_name: Option<String>,
    pub azure_account_key: Option<String>,
    pub azure_bearer_token: Option<String>,
    pub azure_connection_string: Option<String>,
}
```

---

## 5. Schema Deduplication

Exact replica of Scala's `SchemaDeduplication.scala`.

```rust
// schema_dedup.rs

use sha2::{Sha256, Digest};
use base64::Engine;

const DOC_MAPPING_SCHEMA_PREFIX: &str = "docMappingSchema.";

/// On write: replace docMappingJson with docMappingRef in AddActions,
/// store schema in MetadataAction.configuration.
pub fn deduplicate_schemas(
    actions: &mut Vec<Action>,
    existing_schemas: &HashMap<String, String>, // from current metadata config
) -> HashMap<String, String> {
    // 1. Collect unique docMappingJson values across all AddActions
    // 2. Compute SHA-256 hash → 16-char Base64 ref for each
    // 3. Replace docMappingJson with docMappingRef in each AddAction
    // 4. Return new schema entries to merge into MetadataAction.configuration
    ...
}

/// On read: restore docMappingJson from docMappingRef using metadata config.
pub fn restore_schemas(
    entries: &mut [FileEntry],
    metadata_config: &HashMap<String, String>,
) {
    let schema_map: HashMap<&str, &str> = metadata_config.iter()
        .filter(|(k, _)| k.starts_with(DOC_MAPPING_SCHEMA_PREFIX))
        .map(|(k, v)| (k[DOC_MAPPING_SCHEMA_PREFIX.len()..].as_ref(), v.as_str()))
        .collect();

    for entry in entries {
        if let Some(ref doc_ref) = entry.add.doc_mapping_ref {
            if let Some(json) = schema_map.get(doc_ref.as_str()) {
                entry.add.doc_mapping_json = Some(json.to_string());
            }
        }
    }
}

fn compute_schema_hash(json: &str) -> String {
    // Canonical JSON normalization (sorted keys, no whitespace)
    // then SHA-256 → first 12 bytes → Base64 (16 chars)
    let canonical = canonical_json(json);
    let hash = Sha256::digest(canonical.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&hash[..12])
}
```

---

## 6. Partition Pruning

Numeric-aware filter evaluation matching Scala's `PartitionPruning.scala`.

```rust
// partition_pruning.rs

#[derive(Debug, Clone)]
pub enum PartitionFilter {
    EqualTo(String, String),
    GreaterThan(String, String),
    LessThan(String, String),
    GreaterThanOrEqual(String, String),
    LessThanOrEqual(String, String),
    In(String, Vec<String>),
    And(Box<PartitionFilter>, Box<PartitionFilter>),
    Or(Box<PartitionFilter>, Box<PartitionFilter>),
    Not(Box<PartitionFilter>),
}

impl PartitionFilter {
    /// Evaluate filter against concrete partition values.
    pub fn evaluate(&self, partition_values: &HashMap<String, String>) -> bool { ... }

    /// Evaluate filter against partition bounds (for manifest pruning).
    /// Returns true if the bounds MAY contain matching data.
    pub fn may_match_bounds(&self, bounds: &PartitionBounds) -> bool { ... }
}

/// Numeric-aware comparison: "2" < "10" (not lexicographic).
fn compare_values(a: &str, b: &str) -> Ordering {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(na), Ok(nb)) => na.partial_cmp(&nb).unwrap_or(Ordering::Equal),
        _ => a.cmp(b),
    }
}

/// Prune manifest list to only those that may contain matching data.
pub fn prune_manifests(
    manifests: &[ManifestInfo],
    filters: &[PartitionFilter],
) -> Vec<&ManifestInfo> {
    manifests.iter()
        .filter(|m| {
            match &m.partition_bounds {
                Some(bounds) => filters.iter().all(|f| f.may_match_bounds(bounds)),
                None => true, // no bounds → can't prune
            }
        })
        .collect()
}
```

---

## 7. Cache Architecture

Unified cache replacing Scala's 5+ separate caches. Provides read/invalidate/refresh semantics.

```rust
// cache.rs

use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use lru::LruCache;

/// Unified txlog cache. One instance per table path.
/// Thread-safe, time-expiring, with LRU eviction.
pub struct TxLogCache {
    inner: RwLock<CacheInner>,
    config: CacheConfig,
    stats: CacheStats,
}

struct CacheInner {
    /// Parsed actions per version number. LRU, TTL 5 min.
    versions: TimedLruCache<i64, Vec<Action>>,

    /// Computed file snapshot at a version: the fully-replayed file list.
    /// LRU(100), TTL 10 min.
    snapshots: TimedLruCache<i64, Arc<Vec<FileEntry>>>,

    /// Latest computed file list (with partition filter key). TTL 2 min.
    file_lists: TimedLruCache<FileListKey, Arc<Vec<FileEntry>>>,

    /// Table metadata (Protocol + Metadata actions). TTL 30 min.
    metadata: TimedLruCache<String, Arc<(ProtocolAction, MetadataAction)>>,

    /// Last known checkpoint info. Single entry, refreshed on invalidate.
    last_checkpoint: Option<Timed<LastCheckpointInfo>>,
}

/// Cache key for filtered file lists.
#[derive(Hash, Eq, PartialEq)]
struct FileListKey {
    version: i64,
    filter_hash: u64, // hash of partition filters, 0 = no filters
}

#[derive(Clone)]
pub struct CacheConfig {
    pub version_capacity: usize,     // default 1000
    pub version_ttl: Duration,       // default 5 min
    pub snapshot_capacity: usize,    // default 100
    pub snapshot_ttl: Duration,      // default 10 min
    pub file_list_capacity: usize,   // default 50
    pub file_list_ttl: Duration,     // default 2 min
    pub metadata_ttl: Duration,      // default 30 min
    pub enabled: bool,               // default true
}

/// Global cache for immutable data (Avro manifests, checkpoint actions).
/// These never change once written, so no TTL needed.
static GLOBAL_MANIFEST_CACHE: OnceLock<RwLock<LruCache<String, Arc<Vec<FileEntry>>>>> =
    OnceLock::new();

impl TxLogCache {
    pub fn new(config: CacheConfig) -> Self { ... }

    // -- Read --
    pub fn get_version(&self, version: i64) -> Option<Vec<Action>> { ... }
    pub fn get_snapshot(&self, version: i64) -> Option<Arc<Vec<FileEntry>>> { ... }
    pub fn get_file_list(&self, version: i64, filters: &[PartitionFilter]) -> Option<Arc<Vec<FileEntry>>> { ... }
    pub fn get_metadata(&self, table: &str) -> Option<Arc<(ProtocolAction, MetadataAction)>> { ... }
    pub fn get_last_checkpoint(&self) -> Option<LastCheckpointInfo> { ... }

    // -- Write / Cache --
    pub fn put_version(&self, version: i64, actions: Vec<Action>) { ... }
    pub fn put_snapshot(&self, version: i64, entries: Arc<Vec<FileEntry>>) { ... }
    pub fn put_file_list(&self, version: i64, filters: &[PartitionFilter], entries: Arc<Vec<FileEntry>>) { ... }
    pub fn put_metadata(&self, table: &str, protocol: ProtocolAction, metadata: MetadataAction) { ... }
    pub fn put_last_checkpoint(&self, info: LastCheckpointInfo) { ... }

    // -- Invalidate --
    /// Drop all cached data for this table. Next read triggers full reload.
    pub fn invalidate_all(&self) { ... }

    /// Drop only version/snapshot/file_list caches. Metadata and manifests kept.
    pub fn invalidate_mutable(&self) { ... }

    /// Drop entries at or after a version (e.g., after conflict retry).
    pub fn invalidate_from_version(&self, version: i64) { ... }

    // -- Refresh --
    /// Invalidate then pre-populate from storage. Returns new file list.
    pub async fn refresh(&self, storage: &TxLogStorage) -> Result<Arc<Vec<FileEntry>>> { ... }

    // -- Stats --
    pub fn stats(&self) -> CacheStatsSnapshot { ... }
    pub fn reset_stats(&self) { ... }
}

/// Global manifest cache operations.
/// Manifests are immutable — safe to cache indefinitely without TTL.
pub fn get_cached_manifest(path: &str) -> Option<Arc<Vec<FileEntry>>> { ... }
pub fn put_cached_manifest(path: &str, entries: Arc<Vec<FileEntry>>) { ... }
pub fn invalidate_manifest_cache() { ... }

/// Time-bounded LRU cache entry.
struct Timed<T> {
    value: T,
    inserted_at: Instant,
}

struct TimedLruCache<K: Hash + Eq, V> {
    inner: LruCache<K, Timed<V>>,
    ttl: Duration,
}
```

---

## 8. Core Read Path

```rust
// mod.rs (public API)

pub struct TransactionLog {
    storage: TxLogStorage,
    cache: TxLogCache,
}

impl TransactionLog {
    pub fn new(table_path: &str, config: &StorageConfig, cache_config: CacheConfig) -> Result<Self> { ... }

    /// List all live files at the latest version.
    /// Cache-first: returns cached file list if fresh.
    pub async fn list_files(&self) -> Result<Arc<Vec<FileEntry>>> {
        // 1. Check cache for unexpired file list
        // 2. If miss: load checkpoint → replay post-checkpoint versions → compute file list
        // 3. Cache result
        ...
    }

    /// List files with partition filter pruning.
    /// Uses manifest-level bounds to skip entire manifest files.
    pub async fn list_files_with_filters(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Arc<Vec<FileEntry>>> {
        // 1. Check cache (keyed by filter hash)
        // 2. If miss: load checkpoint with manifest pruning → replay → filter → cache
        ...
    }

    /// Get table metadata (Protocol + Metadata actions).
    pub async fn get_metadata(&self) -> Result<(ProtocolAction, MetadataAction)> { ... }

    /// Get current version number.
    pub async fn get_current_version(&self) -> Result<i64> { ... }

    /// Get total row count across all live files.
    pub async fn get_row_count(&self) -> Result<i64> { ... }

    /// Read streaming versions: files added at or after `since_version`.
    pub async fn list_files_added_since(&self, since_version: i64) -> Result<Vec<FileEntry>> { ... }

    /// Get all skip actions at current version.
    pub async fn list_skips(&self) -> Result<Vec<SkipAction>> { ... }

    /// Invalidate all caches, forcing next read to go to storage.
    pub fn invalidate_cache(&self) { ... }

    /// Invalidate and reload.
    pub async fn refresh(&self) -> Result<Arc<Vec<FileEntry>>> { ... }
}
```

### 8.1 Read Pipeline Detail

```
list_files()
  │
  ├─ cache hit? → return cached Arc<Vec<FileEntry>>
  │
  └─ cache miss:
       │
       ├─ read _last_checkpoint → LastCheckpointInfo
       │
       ├─ read state-v<N>/_manifest → StateManifest
       │
       ├─ read each manifest-XXXX.avro in parallel
       │   └─ per manifest: global cache check → read → deserialize → restore schemas
       │
       ├─ list post-checkpoint version files (N+1, N+2, ...)
       │
       ├─ read each version file → parse Actions
       │
       ├─ log_replay: apply Add/Remove/Skip to build final file map
       │   └─ latest action per path wins
       │   └─ Remove supersedes Add
       │   └─ SkipActions tracked separately
       │
       ├─ deduplicate_schemas: restore docMappingJson from refs
       │
       └─ cache snapshot + file list → return
```

---

## 9. Core Write Path

```rust
impl TransactionLog {
    /// Initialize a new transaction log (version 0: Protocol + Metadata).
    pub async fn initialize(
        &self,
        schema_string: &str,
        partition_columns: &[String],
    ) -> Result<i64> { ... }

    /// Add files atomically. Returns new version number.
    /// Retries on conflict with exponential backoff.
    pub async fn add_files(&self, adds: Vec<AddAction>) -> Result<i64> { ... }

    /// Remove a file by path.
    pub async fn remove_file(&self, path: &str) -> Result<i64> { ... }

    /// Overwrite: remove all existing files + add new files atomically.
    pub async fn overwrite_files(&self, adds: Vec<AddAction>) -> Result<i64> { ... }

    /// Record a skip action.
    pub async fn skip_file(&self, skip: SkipAction) -> Result<i64> { ... }

    /// Create an Avro state checkpoint at the current version.
    pub async fn create_checkpoint(&self) -> Result<LastCheckpointInfo> { ... }
}
```

### 9.1 Write Pipeline Detail

```
add_files(adds)
  │
  ├─ schema deduplication: deduplicate_schemas(&mut actions, existing_metadata)
  │
  ├─ determine target version:
  │   └─ list version files → find max → target = max + 1
  │
  ├─ serialize actions → gzip-compressed JSON lines
  │   (each line: {"add": {...}}, {"remove": {...}}, {"mergeskip": {...}})
  │
  ├─ conditional write: put_if_absent(version_path(target), bytes)
  │   │
  │   ├─ success → invalidate_from_version(target) in cache → return target
  │   │
  │   └─ conflict (file exists) → retry with backoff
  │       ├─ attempt 1: 100ms delay
  │       ├─ attempt 2: 200ms delay
  │       ├─ ...up to 10 attempts, max 5000ms delay
  │       └─ re-read current version → target = new_max + 1 → retry write
  │
  └─ optional: trigger checkpoint if post-checkpoint version count > threshold
```

### 9.2 Checkpoint Write

```
create_checkpoint()
  │
  ├─ compute full file list at current version (via list_files)
  │
  ├─ group FileEntries by partition key → create manifest partitions
  │
  ├─ distribute tombstones (RemoveActions) across manifests
  │   └─ tombstone_distributor: spread removes to improve partition pruning
  │
  ├─ write each manifest-XXXX.avro with:
  │   ├─ Avro-serialized FileEntry records
  │   └─ computed PartitionBounds per manifest
  │
  ├─ write _manifest (StateManifest) with manifest list + global bounds
  │
  ├─ write _last_checkpoint → {version, format: "avro-state", stateDir}
  │
  └─ update cache with new checkpoint info
```

---

## 10. Distributable Primitives

Following the exact same 2-phase pattern as delta/iceberg readers.

```rust
// distributed.rs

// ═══════════════════════════════════════════════════════════════
// DRIVER-SIDE PRIMITIVES (lightweight, single-threaded)
// ═══════════════════════════════════════════════════════════════

/// Read _last_checkpoint + list post-checkpoint version file paths.
/// Returns everything needed to distribute manifest + version reads.
///
/// Cost: 1 GET (_last_checkpoint) + 1 LIST (version files)
pub async fn get_txlog_snapshot_info(
    table_path: &str,
    config: &StorageConfig,
) -> Result<TxLogSnapshotInfo> { ... }

#[derive(Debug, Clone)]
pub struct TxLogSnapshotInfo {
    /// Version of the checkpoint
    pub checkpoint_version: i64,

    /// Manifest file paths relative to state-v<N>/ directory
    pub manifest_paths: Vec<ManifestPathInfo>,

    /// Post-checkpoint version file paths relative to _transaction_log/
    pub post_checkpoint_version_paths: Vec<String>,

    /// Protocol and metadata from the checkpoint
    pub protocol: ProtocolAction,
    pub metadata: MetadataAction,

    /// State directory path (e.g., "state-v42/")
    pub state_dir: String,
}

#[derive(Debug, Clone)]
pub struct ManifestPathInfo {
    pub path: String,               // "manifest-0000.avro"
    pub file_count: i64,
    pub partition_bounds: Option<PartitionBounds>,
}

/// Get current version without reading full state.
/// Cost: 1 GET (_last_checkpoint) + 1 LIST (post-checkpoint files)
pub async fn get_current_version(
    table_path: &str,
    config: &StorageConfig,
) -> Result<i64> { ... }

/// Read post-checkpoint version files and compute incremental changes.
/// Driver-side because version files are small and few.
///
/// Cost: N GETs (one per post-checkpoint version file)
pub async fn read_post_checkpoint_changes(
    table_path: &str,
    config: &StorageConfig,
    version_paths: &[String],
    metadata_config: &HashMap<String, String>, // for schema dedup restore
) -> Result<TxLogChanges> { ... }

#[derive(Debug, Clone)]
pub struct TxLogChanges {
    pub added_files: Vec<FileEntry>,
    pub removed_paths: Vec<String>,
    pub skip_actions: Vec<SkipAction>,
    pub max_version: i64,
}

/// Write a new version file (driver-only, requires conflict detection).
pub async fn write_version(
    table_path: &str,
    config: &StorageConfig,
    actions: Vec<Action>,
    retry_config: RetryConfig,
) -> Result<WriteResult> { ... }

#[derive(Debug, Clone)]
pub struct WriteResult {
    pub version: i64,
    pub retries: u32,
    pub conflicted_versions: Vec<i64>,
}

/// Create Avro state checkpoint at current version (driver-only).
pub async fn write_checkpoint(
    table_path: &str,
    config: &StorageConfig,
    entries: Vec<FileEntry>,
    metadata: MetadataAction,
    protocol: ProtocolAction,
) -> Result<LastCheckpointInfo> { ... }


// ═══════════════════════════════════════════════════════════════
// EXECUTOR-SIDE PRIMITIVES (parallelizable, distributable)
// ═══════════════════════════════════════════════════════════════

/// Read ONE Avro manifest file → Vec<FileEntry>.
/// Highly parallelizable: each executor reads one manifest.
///
/// Cost: 1 GET (manifest avro file, typically 1-50MB)
pub async fn read_manifest(
    table_path: &str,
    config: &StorageConfig,
    state_dir: &str,           // "state-v42/"
    manifest_path: &str,       // "manifest-0000.avro"
    metadata_config: &HashMap<String, String>, // for schema dedup restore
) -> Result<Vec<FileEntry>> { ... }
```

### 10.1 Distribution Flow (Spark Integration)

```
Spark Driver                           Spark Executors
────────────                           ───────────────
get_txlog_snapshot_info()
  → TxLogSnapshotInfo
     ├─ manifest_paths: [m0, m1, m2]
     ├─ post_checkpoint_versions: [v43, v44]
     └─ metadata (for schema dedup)
                │
                ├──────── broadcast metadata_config ────────────►
                │
                ├─ partition_prune(manifest_paths, filters)
                │   → [m0, m2] (m1 pruned)
                │
                ├──────── distribute m0 ──────────────────────► read_manifest(m0)
                ├──────── distribute m2 ──────────────────────► read_manifest(m2)
                │                                                → Vec<FileEntry>
                │
read_post_checkpoint_changes([v43, v44])
  → TxLogChanges
                │
                └─ log_replay(manifests ∪ changes)
                   → final Vec<FileEntry>
```

---

## 11. TANT Byte Buffer Serialization

For non-Arrow consumers (lightweight Java callers, non-Spark use cases).

```rust
// serialization.rs

use crate::common::tant_buffer::*;

/// Serialize TxLogSnapshotInfo → TANT byte buffer.
/// Format:
///   [header: "TANT" magic + version byte]
///   [checkpoint_version: i64]
///   [num_manifests: u32]
///   [manifest entries...]
///   [num_post_checkpoint: u32]
///   [version paths...]
///   [protocol JSON bytes]
///   [metadata JSON bytes]
///   [state_dir string]
pub fn serialize_snapshot_info(info: &TxLogSnapshotInfo) -> Vec<u8> { ... }

/// Serialize Vec<FileEntry> → TANT byte buffer.
/// One entry per file: all AddAction fields + streaming metadata.
pub fn serialize_file_entries(entries: &[FileEntry]) -> Vec<u8> { ... }

/// Serialize TxLogChanges → TANT byte buffer.
pub fn serialize_changes(changes: &TxLogChanges) -> Vec<u8> { ... }

/// Serialize WriteResult → TANT byte buffer.
pub fn serialize_write_result(result: &WriteResult) -> Vec<u8> { ... }

// Deserialization on Java side via existing TANT buffer reader pattern.
```

---

## 12. Arrow FFI Export

For Spark consumption: file list as Arrow RecordBatch.

```rust
// arrow_ffi.rs

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

/// Arrow schema for FileEntry RecordBatch.
pub fn file_entry_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modification_time", DataType::Int64, false),
        Field::new("data_change", DataType::Boolean, false),
        Field::new("num_records", DataType::Int64, true),
        Field::new("partition_values", DataType::Utf8, true), // JSON-encoded map
        Field::new("stats", DataType::Utf8, true),
        Field::new("min_values", DataType::Utf8, true),       // JSON-encoded map
        Field::new("max_values", DataType::Utf8, true),       // JSON-encoded map
        Field::new("footer_start_offset", DataType::Int64, true),
        Field::new("footer_end_offset", DataType::Int64, true),
        Field::new("split_tags", DataType::Utf8, true),       // JSON-encoded map
        Field::new("num_merge_ops", DataType::Int32, true),
        Field::new("doc_mapping_json", DataType::Utf8, true),
        Field::new("uncompressed_size_bytes", DataType::Int64, true),
        Field::new("time_range_start", DataType::Int64, true),
        Field::new("time_range_end", DataType::Int64, true),
        Field::new("companion_source_files", DataType::Utf8, true), // JSON array
        Field::new("companion_delta_version", DataType::Int64, true),
        Field::new("companion_fast_field_mode", DataType::Utf8, true),
        Field::new("added_at_version", DataType::Int64, false),
        Field::new("added_at_timestamp", DataType::Int64, false),
        // Skip fields (null for non-skip entries, populated for skip report)
        Field::new("skip_reason", DataType::Utf8, true),
        Field::new("skip_count", DataType::Int32, true),
        Field::new("retry_after", DataType::Int64, true),
    ])
}

/// Convert file entries to Arrow RecordBatch.
pub fn file_entries_to_record_batch(entries: &[FileEntry]) -> Result<RecordBatch> {
    // Build columnar arrays from entries
    // Schema dedup already resolved at this point
    ...
}

/// Export RecordBatch via Arrow FFI to pre-allocated Java addresses.
/// Same pattern as parquet_companion/arrow_ffi_export.rs.
pub unsafe fn export_file_entries_ffi(
    entries: &[FileEntry],
    array_addrs: &[i64],   // pre-allocated by Java
    schema_addrs: &[i64],  // pre-allocated by Java
) -> Result<()> {
    let batch = file_entries_to_record_batch(entries)?;

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let data = col.to_data();

        // Normalize non-zero offsets for FFI consumer compatibility
        let data = if data.offset() != 0 {
            arrow::compute::take(col, &UInt32Array::from((0..col.len() as u32).collect::<Vec<_>>()), None)?.to_data()
        } else {
            data
        };

        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        if array_ptr.is_null() || schema_ptr.is_null() {
            bail!("Null FFI address for column {i} ({})", field.name());
        }

        std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
        std::ptr::write_unaligned(
            schema_ptr,
            FFI_ArrowSchema::try_from(field.as_ref())?,
        );
    }
    Ok(())
}

/// Streaming variant: produce batches via channel for large file lists.
/// Same StreamingRetrievalSession pattern as parquet_companion/streaming_ffi.rs.
pub fn start_streaming_file_list(
    entries: Vec<FileEntry>,
    batch_size: usize,          // default 128K entries per batch
) -> Result<StreamingRetrievalSession> {
    let schema = Arc::new(file_entry_arrow_schema());
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);

    let handle = tokio::spawn(async move {
        for chunk in entries.chunks(batch_size) {
            match file_entries_to_record_batch(chunk) {
                Ok(batch) => { if tx.send(Ok(batch)).await.is_err() { break; } }
                Err(e) => { let _ = tx.send(Err(e)).await; break; }
            }
        }
    });

    Ok(StreamingRetrievalSession::new(rx, schema, handle))
}
```

---

## 13. JNI Bridge

```rust
// jni.rs

// ═══════════════════════════════════════════════════════════════
// LIFECYCLE
// ═══════════════════════════════════════════════════════════════

/// Create a TransactionLog handle. Returns jlong pointer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeCreate(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,       // HashMap<String, String> with AWS/Azure creds
    cache_config_map: JObject, // HashMap<String, String> with cache settings
) -> jlong { ... }

/// Close and free the TransactionLog handle.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeClose(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) { ... }

// ═══════════════════════════════════════════════════════════════
// READS (stateful, uses cache)
// ═══════════════════════════════════════════════════════════════

/// List all live files. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListFiles(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jbyteArray { ... }

/// List files with partition filters. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListFilesWithFilters(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
    filters_json: JString,     // JSON-serialized Vec<PartitionFilter>
) -> jbyteArray { ... }

/// List files via Arrow FFI (for Spark). Writes to pre-allocated FFI addresses.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListFilesArrowFfi(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jint { ... } // returns row count

/// Start streaming file list (for very large tables).
/// Returns streaming session handle.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeStartStreamingFileList(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
    filters_json: JString,
    batch_size: jint,
) -> jlong { ... }

/// Get next batch from streaming session.
/// Returns number of rows, 0 = done, -1 = error.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeStreamingNext(
    mut env: JNIEnv, _class: JClass,
    session_handle: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jint { ... }

/// Get metadata. Returns TANT byte buffer with Protocol + Metadata.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetMetadata(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jbyteArray { ... }

/// Get current version number.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetCurrentVersion(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jlong { ... }

/// Get row count across all live files.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetRowCount(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jlong { ... }

/// Get skip actions. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeListSkips(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jbyteArray { ... }

// ═══════════════════════════════════════════════════════════════
// WRITES
// ═══════════════════════════════════════════════════════════════

/// Initialize new txlog with schema.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeInitialize(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    schema_string: JString,
    partition_columns: JObjectArray,
) -> jlong { ... } // returns version

/// Add files. Returns TANT buffer with WriteResult (version, retries, conflicts).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeAddFiles(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    adds_json: JString,        // JSON array of AddAction
) -> jbyteArray { ... }

/// Remove file by path.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeRemoveFile(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    path: JString,
) -> jlong { ... }

/// Overwrite all files.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeOverwriteFiles(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    adds_json: JString,
) -> jbyteArray { ... }

/// Record skip action.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeSkipFile(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    skip_json: JString,
) -> jlong { ... }

/// Create checkpoint at current version.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeCreateCheckpoint(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
) -> jbyteArray { ... } // returns TANT-serialized LastCheckpointInfo

// ═══════════════════════════════════════════════════════════════
// DISTRIBUTABLE PRIMITIVES (stateless, for Spark distribution)
// ═══════════════════════════════════════════════════════════════

/// Driver: get snapshot info. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetSnapshotInfo(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
) -> jbyteArray { ... }

/// Executor: read one manifest. Returns TANT byte buffer of FileEntries.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadManifest(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    state_dir: JString,
    manifest_path: JString,
    metadata_config_json: JString, // JSON map for schema dedup restore
) -> jbyteArray { ... }

/// Executor: read one manifest via Arrow FFI.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadManifestArrowFfi(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    state_dir: JString,
    manifest_path: JString,
    metadata_config_json: JString,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jint { ... }

/// Driver: read post-checkpoint changes. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeReadPostCheckpointChanges(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    version_paths: JObjectArray,
    metadata_config_json: JString,
) -> jbyteArray { ... }

// ═══════════════════════════════════════════════════════════════
// CACHE MANAGEMENT
// ═══════════════════════════════════════════════════════════════

/// Invalidate all caches for a handle.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeInvalidateCache(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) { ... }

/// Get cache statistics. Returns TANT byte buffer.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeGetCacheStats(
    mut env: JNIEnv, _class: JClass,
    handle: jlong,
) -> jbyteArray { ... }

/// Invalidate global manifest cache (all tables).
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogReader_nativeInvalidateGlobalCache(
    mut env: JNIEnv, _class: JClass,
) { ... }

// ═══════════════════════════════════════════════════════════════
// GARBAGE COLLECTION
// ═══════════════════════════════════════════════════════════════

/// Clean up orphaned manifests and old version files.
#[no_mangle]
pub extern "system" fn Java_io_indextables_jni_txlog_TransactionLogWriter_nativeGarbageCollect(
    mut env: JNIEnv, _class: JClass,
    table_path: JString,
    config_map: JObject,
    log_retention_days: jint,       // default 30
    checkpoint_retention_hours: jint, // default 2
) -> jbyteArray { ... } // returns TANT buffer with GC stats
```

---

## 14. Log Replay

```rust
// log_replay.rs

/// Replay actions to compute the live file set.
/// Latest action per path wins. Removes supersede Adds.
pub fn replay(
    checkpoint_entries: Vec<FileEntry>,
    post_checkpoint_actions: Vec<(i64, Vec<Action>)>, // (version, actions)
) -> ReplayResult {
    let mut file_map: HashMap<String, FileEntry> = HashMap::new();
    let mut skip_map: HashMap<String, SkipAction> = HashMap::new();

    // Start with checkpoint entries
    for entry in checkpoint_entries {
        file_map.insert(entry.add.path.clone(), entry);
    }

    // Apply post-checkpoint actions in version order
    for (version, actions) in post_checkpoint_actions {
        let timestamp = current_timestamp_ms();
        for action in actions {
            match action {
                Action::Add(add) => {
                    let entry = FileEntry {
                        add,
                        added_at_version: version,
                        added_at_timestamp: timestamp,
                    };
                    file_map.insert(entry.add.path.clone(), entry);
                }
                Action::Remove(remove) => {
                    file_map.remove(&remove.path);
                }
                Action::MergeSkip(skip) => {
                    skip_map.insert(skip.path.clone(), skip);
                }
                Action::Protocol(_) | Action::MetaData(_) => {
                    // Protocol/Metadata tracked separately
                }
            }
        }
    }

    ReplayResult {
        files: file_map.into_values().collect(),
        skips: skip_map.into_values().collect(),
    }
}

pub struct ReplayResult {
    pub files: Vec<FileEntry>,
    pub skips: Vec<SkipAction>,
}
```

---

## 15. Compression

Gzip for Scala compatibility.

```rust
// compression.rs

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

/// Gzip compress bytes (Scala-compatible, level 6 default).
pub fn gzip_compress(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

/// Gzip decompress bytes.
pub fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Detect if bytes are gzip-compressed (magic bytes 0x1f 0x8b).
pub fn is_gzip(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

/// Read version file: decompress if gzip, parse JSON lines.
pub fn read_version_file(data: &[u8]) -> Result<Vec<Action>> {
    let text = if is_gzip(data) {
        String::from_utf8(gzip_decompress(data)?)?
    } else {
        String::from_utf8(data.to_vec())?
    };

    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<ActionEnvelope>(l).map(|e| e.into_action()))
        .collect()
}
```

---

## 16. Garbage Collection

```rust
// garbage_collection.rs

pub struct GcConfig {
    pub log_retention_days: u32,        // default 30
    pub checkpoint_retention_hours: u32, // default 2
}

pub struct GcResult {
    pub deleted_version_files: u32,
    pub deleted_state_dirs: u32,
    pub deleted_manifest_files: u32,
    pub bytes_freed: i64,
}

/// Clean up old version files and orphaned state directories.
/// Preserves: current checkpoint + all post-checkpoint versions.
pub async fn garbage_collect(
    storage: &TxLogStorage,
    config: &GcConfig,
) -> Result<GcResult> {
    // 1. Read _last_checkpoint → current checkpoint version
    // 2. List all version files → delete those older than retention AND before checkpoint
    // 3. List all state-v<N>/ dirs → delete those not referenced by _last_checkpoint
    //    AND older than checkpoint_retention_hours
    // 4. Within surviving state dirs: no cleanup (manifests are immutable)
    ...
}
```

---

## 17. Metrics

```rust
// metrics.rs

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct TxLogMetrics {
    // Write metrics
    pub write_attempts: AtomicU64,
    pub write_conflicts: AtomicU64,
    pub write_successes: AtomicU64,
    pub total_retry_delay_ms: AtomicU64,

    // Read metrics
    pub versions_read: AtomicU64,
    pub manifests_read: AtomicU64,
    pub file_entries_loaded: AtomicU64,

    // Cache metrics (delegated to TxLogCache.stats)

    // Timing
    pub last_list_files_ms: AtomicU64,
    pub last_write_ms: AtomicU64,
    pub last_checkpoint_ms: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxLogMetricsSnapshot {
    pub write_attempts: u64,
    pub write_conflicts: u64,
    pub write_successes: u64,
    pub avg_retry_delay_ms: f64,
    pub versions_read: u64,
    pub manifests_read: u64,
    pub file_entries_loaded: u64,
    pub cache: CacheStatsSnapshot,
    pub last_list_files_ms: u64,
    pub last_write_ms: u64,
    pub last_checkpoint_ms: u64,
}
```

---

## 18. Error Types

```rust
// error.rs

#[derive(Debug, thiserror::Error)]
pub enum TxLogError {
    #[error("Version conflict: attempted {attempted}, current is {current}")]
    VersionConflict { attempted: i64, current: i64 },

    #[error("Max retries exceeded ({retries}) for version write")]
    MaxRetriesExceeded { retries: u32, last_conflict: i64 },

    #[error("Transaction log not initialized at {path}")]
    NotInitialized { path: String },

    #[error("Incompatible protocol: requires reader v{required}, have v4")]
    IncompatibleProtocol { required: u32 },

    #[error("Checkpoint corrupted at version {version}: {detail}")]
    CorruptedCheckpoint { version: i64, detail: String },

    #[error("Manifest read failed for {path}: {source}")]
    ManifestReadFailed { path: String, source: Box<dyn std::error::Error + Send + Sync> },

    #[error("Storage error: {0}")]
    Storage(#[from] quickwit_storage::StorageError),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

---

## 19. Crate Dependencies (additions to native/Cargo.toml)

```toml
[dependencies]
# Avro (Scala-compatible state format)
apache-avro = "0.16"

# Gzip compression (Scala-compatible)
flate2 = "1.0"

# Schema deduplication
sha2 = "0.10"
base64 = "0.22"

# Cache
lru = "0.12"
parking_lot = "0.12"

# Already present in project:
# arrow, serde, serde_json, tokio, thiserror, jni, quickwit-storage, object_store
```

---

## 20. Concurrency Model

| Operation | Threading | Rationale |
|-----------|-----------|-----------|
| `list_files()` | async, single caller | Cache serializes concurrent reads |
| `list_files_with_filters()` | async, manifest reads parallelized | Multiple manifests read concurrently via `futures::join_all` |
| `add_files()` | async, retry loop | Conditional write + conflict detection |
| `create_checkpoint()` | async, manifest writes parallelized | Multiple manifests written concurrently |
| `read_manifest()` (distributed) | async, one per executor | Stateless, no shared state |
| Cache access | `parking_lot::RwLock` | Read-heavy workload, multiple concurrent readers |
| Global manifest cache | `OnceLock<RwLock<LruCache>>` | Process-wide singleton, immutable data |
| Metrics | `AtomicU64` | Lock-free counters |

---

## 21. Backward Compatibility Matrix

| Scala Feature | Rust Support | Notes |
|---------------|-------------|-------|
| JSON version files (v1-v3) | Read-only | Parse JSON lines, detect gzip |
| Avro state-v<N>/ (v4) | Read + Write | Primary format |
| _last_checkpoint | Read + Write | JSON format, same fields |
| Schema deduplication | Read + Write | SHA-256, 16-char Base64 ref |
| SkipAction ("mergeskip") | Read + Write | Same JSON key |
| Streaming timestamps (addedAtVersion/Timestamp) | Read + Write | Avro FileEntry fields |
| Partition bounds | Read + Write | Same manifest structure |
| Gzip compression | Read + Write | Same level, magic byte detection |
| Conditional writes (if-not-exists) | Write | Same atomic semantics |
| Exponential backoff retry | Write | Same defaults: 10 attempts, 100-5000ms |

**Not supported (by design):**
- JSON checkpoint format (v1-v3) — only Avro state format for new checkpoints
- Multi-part JSON checkpoints (v3) — superseded by Avro state

**Migration path for pre-v4 tables:**
- Rust reads existing JSON versions (v1-v3 files still parseable as JSON lines)
- First checkpoint written by Rust will be Avro state format (v4)
- _last_checkpoint updated to `format: "avro-state"`
- Protocol bumped to `minReaderVersion: 4, minWriterVersion: 4`
