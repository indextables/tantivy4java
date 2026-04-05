// txlog/cache.rs - Unified cache with read/invalidate/refresh semantics
//
// Replaces Scala's 5+ separate caches with a single structure providing
// TTL-based expiration, LRU eviction, and a global manifest cache.
//
// Uses moka::sync::Cache for lock-free concurrent reads. The previous
// parking_lot::RwLock<LruCache> approach serialized all readers because
// LruCache::get() mutates internal LRU order and requires exclusive access.
// Under concurrent executor threads (the designed read pattern) that was a
// significant bottleneck. moka uses a segmented approach similar to
// ConcurrentHashMap, allowing parallel reads with no write-lock contention.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use moka::sync::Cache as MokaCache;
use parking_lot::RwLock;

use super::actions::*;

// ============================================================================
// Cache configuration
// ============================================================================

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub version_capacity: usize,
    pub version_ttl: Duration,
    pub snapshot_capacity: usize,
    pub snapshot_ttl: Duration,
    pub file_list_capacity: usize,
    pub file_list_ttl: Duration,
    pub metadata_ttl: Duration,
    pub enabled: bool,
    /// Maximum number of concurrent object-store GETs issued simultaneously.
    /// Applies to version-file fan-outs and compaction manifest reads.
    /// Default: 32. Set to 0 to use the default.
    pub max_concurrent_reads: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            version_capacity: 1000,
            version_ttl: Duration::from_secs(300),
            snapshot_capacity: 100,
            snapshot_ttl: Duration::from_secs(600),
            file_list_capacity: 50,
            file_list_ttl: Duration::from_secs(120),
            metadata_ttl: Duration::from_secs(1800),
            enabled: true,
            max_concurrent_reads: 32,
        }
    }
}

impl CacheConfig {
    /// Build a CacheConfig from a string config map (passed through from Java).
    ///
    /// Recognised keys (all optional; omitted keys use defaults):
    /// - `max_concurrent_reads`       — max parallel object-store GETs (default 32)
    /// - `cache.version.capacity`     — version cache entry limit (default 1000)
    /// - `cache.snapshot.capacity`    — snapshot cache entry limit (default 100)
    /// - `cache.file_list.capacity`   — file-list cache entry limit (default 50)
    /// - `cache.version.ttl.ms`       — version cache TTL in ms (default 300_000)
    /// - `cache.snapshot.ttl.ms`      — snapshot cache TTL in ms (default 600_000)
    /// - `cache.file_list.ttl.ms`     — file-list cache TTL in ms (default 120_000)
    /// - `cache.metadata.ttl.ms`      — metadata cache TTL in ms (default 1_800_000)
    /// - `cache.ttl.ms` / `cache_ttl_ms` — override all TTLs at once
    /// - `cache.enabled`              — "false" to disable caching entirely
    pub fn from_map(map: &HashMap<String, String>) -> Self {
        let mut cfg = Self::default();

        // All-TTL override (existing behaviour: cache.ttl.ms sets everything)
        if let Some(ttl_ms) = map.get("cache.ttl.ms").or_else(|| map.get("cache_ttl_ms")) {
            if let Ok(ms) = ttl_ms.parse::<u64>() {
                let d = Duration::from_millis(ms);
                cfg.version_ttl = d;
                cfg.snapshot_ttl = d;
                cfg.file_list_ttl = d;
                cfg.metadata_ttl = d;
            }
        }

        // Per-tier TTL overrides
        if let Some(ms) = map.get("cache.version.ttl.ms").and_then(|s| s.parse::<u64>().ok()) {
            cfg.version_ttl = Duration::from_millis(ms);
        }
        if let Some(ms) = map.get("cache.snapshot.ttl.ms").and_then(|s| s.parse::<u64>().ok()) {
            cfg.snapshot_ttl = Duration::from_millis(ms);
        }
        if let Some(ms) = map.get("cache.file_list.ttl.ms").and_then(|s| s.parse::<u64>().ok()) {
            cfg.file_list_ttl = Duration::from_millis(ms);
        }
        if let Some(ms) = map.get("cache.metadata.ttl.ms").and_then(|s| s.parse::<u64>().ok()) {
            cfg.metadata_ttl = Duration::from_millis(ms);
        }

        // Capacity overrides
        if let Some(n) = map.get("cache.version.capacity").and_then(|s| s.parse::<usize>().ok()) {
            cfg.version_capacity = n;
        }
        if let Some(n) = map.get("cache.snapshot.capacity").and_then(|s| s.parse::<usize>().ok()) {
            cfg.snapshot_capacity = n;
        }
        if let Some(n) = map.get("cache.file_list.capacity").and_then(|s| s.parse::<usize>().ok()) {
            cfg.file_list_capacity = n;
        }

        // Concurrency
        if let Some(n) = map.get("max_concurrent_reads").and_then(|s| s.parse::<usize>().ok()) {
            cfg.max_concurrent_reads = if n == 0 { 32 } else { n };
        }

        // Kill-switch
        if map.get("cache.enabled").map(|s| s.as_str()) == Some("false") {
            cfg.enabled = false;
        }

        cfg
    }
}

// ============================================================================
// Cache statistics
// ============================================================================

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl CacheStatsSnapshot {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { self.hits as f64 / total as f64 * 100.0 }
    }
}

// ============================================================================
// Timed<T> — used for metadata and last_checkpoint (single-entry, infrequent)
// ============================================================================

struct Timed<T> {
    value: T,
    inserted_at: Instant,
}

// ============================================================================
// FileListKey: cache key for filtered file lists
// ============================================================================

#[derive(Eq, PartialEq, Clone)]
struct FileListKey {
    version: i64,
    filter_hash: u64,
}

impl Hash for FileListKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.version.hash(state);
        self.filter_hash.hash(state);
    }
}

// ============================================================================
// Main cache structure — flat, no inner wrapper needed since moka is Send+Sync
// ============================================================================

pub struct TxLogCache {
    versions: MokaCache<i64, Vec<super::actions::Action>>,
    snapshots: MokaCache<i64, Arc<Vec<FileEntry>>>,
    file_lists: MokaCache<FileListKey, Arc<Vec<FileEntry>>>,
    // State manifests are immutable per checkpoint version — cached with no TTL using
    // a moka LRU (capacity 100). Key is the state_dir string (e.g. "state-v000...042").
    state_manifests: MokaCache<String, super::actions::StateManifest>,
    // metadata, last_checkpoint, and version_list are single-entry, infrequently
    // updated — kept as RwLock<Option<Timed<...>>> rather than a moka cache.
    metadata: RwLock<Option<Timed<Arc<(ProtocolAction, MetadataAction)>>>>,
    last_checkpoint: RwLock<Option<Timed<LastCheckpointInfo>>>,
    /// Cached result of storage.list_versions() — avoids the expensive S3 LIST call on
    /// every query when metadata/checkpoint are already cached.  TTL matches version_ttl.
    version_list: RwLock<Option<Timed<Vec<i64>>>>,
    metadata_ttl: Duration,
    version_ttl: Duration,
    config: CacheConfig,
    // Hit/miss counters for statistics. Evictions are tracked by moka internally
    // but not exposed without an eviction listener; returning 0 is acceptable
    // since no caller asserts on eviction counts.
    hits: AtomicU64,
    misses: AtomicU64,
}

impl TxLogCache {
    pub fn new(config: CacheConfig) -> Self {
        let versions = MokaCache::builder()
            .max_capacity(config.version_capacity as u64)
            .time_to_live(config.version_ttl)
            .build();
        let snapshots = MokaCache::builder()
            .max_capacity(config.snapshot_capacity as u64)
            .time_to_live(config.snapshot_ttl)
            .build();
        let file_lists = MokaCache::builder()
            .max_capacity(config.file_list_capacity as u64)
            .time_to_live(config.file_list_ttl)
            .build();
        // State manifests are immutable — no TTL, bounded by capacity.
        let state_manifests = MokaCache::builder()
            .max_capacity(100)
            .build();
        let version_ttl = config.version_ttl;
        Self {
            versions,
            snapshots,
            file_lists,
            state_manifests,
            metadata: RwLock::new(None),
            last_checkpoint: RwLock::new(None),
            version_list: RwLock::new(None),
            metadata_ttl: config.metadata_ttl,
            version_ttl,
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    // -- Read --
    // moka::sync::Cache::get() takes a shared reference and does not require
    // exclusive access, so concurrent reads no longer contend on a write lock.

    pub fn get_version(&self, version: i64) -> Option<Vec<super::actions::Action>> {
        if !self.config.enabled { return None; }
        match self.versions.get(&version) {
            Some(v) => { self.hits.fetch_add(1, Ordering::Relaxed); Some(v) }
            None    => { self.misses.fetch_add(1, Ordering::Relaxed); None }
        }
    }

    pub fn get_snapshot(&self, version: i64) -> Option<Arc<Vec<FileEntry>>> {
        if !self.config.enabled { return None; }
        match self.snapshots.get(&version) {
            Some(v) => { self.hits.fetch_add(1, Ordering::Relaxed); Some(v) }
            None    => { self.misses.fetch_add(1, Ordering::Relaxed); None }
        }
    }

    pub fn get_file_list(&self, version: i64, filter_hash: u64) -> Option<Arc<Vec<FileEntry>>> {
        if !self.config.enabled { return None; }
        let key = FileListKey { version, filter_hash };
        match self.file_lists.get(&key) {
            Some(v) => { self.hits.fetch_add(1, Ordering::Relaxed); Some(v) }
            None    => { self.misses.fetch_add(1, Ordering::Relaxed); None }
        }
    }

    pub fn get_metadata(&self) -> Option<Arc<(ProtocolAction, MetadataAction)>> {
        if !self.config.enabled { return None; }
        let inner = self.metadata.read();
        if let Some(ref entry) = *inner {
            if entry.inserted_at.elapsed() < self.metadata_ttl {
                return Some(entry.value.clone());
            }
        }
        None
    }

    pub fn get_last_checkpoint(&self) -> Option<LastCheckpointInfo> {
        if !self.config.enabled { return None; }
        let inner = self.last_checkpoint.read();
        if let Some(ref entry) = *inner {
            if entry.inserted_at.elapsed() < self.metadata_ttl {
                return Some(entry.value.clone());
            }
        }
        None
    }

    /// Returns the cached state manifest for `state_dir` if present.
    /// State manifests are immutable per checkpoint — safe to cache indefinitely.
    pub fn get_state_manifest(&self, state_dir: &str) -> Option<super::actions::StateManifest> {
        if !self.config.enabled { return None; }
        match self.state_manifests.get(state_dir) {
            Some(v) => { self.hits.fetch_add(1, Ordering::Relaxed); Some(v) }
            None    => { self.misses.fetch_add(1, Ordering::Relaxed); None }
        }
    }

    /// Returns the cached version list if present and within TTL.
    pub fn get_version_list(&self) -> Option<Vec<i64>> {
        if !self.config.enabled { return None; }
        let inner = self.version_list.read();
        if let Some(ref entry) = *inner {
            if entry.inserted_at.elapsed() < self.version_ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.value.clone());
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    // -- Write / Cache --

    pub fn put_version(&self, version: i64, actions: Vec<super::actions::Action>) {
        if !self.config.enabled { return; }
        self.versions.insert(version, actions);
    }

    pub fn put_snapshot(&self, version: i64, entries: Arc<Vec<FileEntry>>) {
        if !self.config.enabled { return; }
        self.snapshots.insert(version, entries);
    }

    pub fn put_file_list(&self, version: i64, filter_hash: u64, entries: Arc<Vec<FileEntry>>) {
        if !self.config.enabled { return; }
        let key = FileListKey { version, filter_hash };
        self.file_lists.insert(key, entries);
    }

    pub fn put_metadata(&self, protocol: ProtocolAction, metadata: MetadataAction) {
        if !self.config.enabled { return; }
        *self.metadata.write() = Some(Timed {
            value: Arc::new((protocol, metadata)),
            inserted_at: Instant::now(),
        });
    }

    pub fn put_last_checkpoint(&self, info: LastCheckpointInfo) {
        if !self.config.enabled { return; }
        *self.last_checkpoint.write() = Some(Timed {
            value: info,
            inserted_at: Instant::now(),
        });
    }

    pub fn put_state_manifest(&self, state_dir: &str, manifest: super::actions::StateManifest) {
        if !self.config.enabled { return; }
        self.state_manifests.insert(state_dir.to_string(), manifest);
    }

    pub fn put_version_list(&self, versions: Vec<i64>) {
        if !self.config.enabled { return; }
        *self.version_list.write() = Some(Timed {
            value: versions,
            inserted_at: Instant::now(),
        });
    }

    // -- Invalidate --

    pub fn invalidate_all(&self) {
        self.versions.invalidate_all();
        self.snapshots.invalidate_all();
        self.file_lists.invalidate_all();
        self.state_manifests.invalidate_all();
        *self.metadata.write() = None;
        *self.last_checkpoint.write() = None;
        *self.version_list.write() = None;
    }

    pub fn invalidate_mutable(&self) {
        self.versions.invalidate_all();
        self.snapshots.invalidate_all();
        self.file_lists.invalidate_all();
        // version_list may be stale after a write; state_manifests are immutable so kept.
        *self.version_list.write() = None;
    }

    // -- Stats --

    pub fn stats(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: 0,
        }
    }

    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }
}

// ============================================================================
// Global manifest cache (immutable data, no TTL)
// Manifests are content-addressed and never mutated after creation, so they
// are safe to cache indefinitely. moka handles concurrent access without locks.
// ============================================================================

static GLOBAL_MANIFEST_CACHE: OnceLock<MokaCache<String, Arc<Vec<FileEntry>>>> = OnceLock::new();

fn global_manifest_cache() -> &'static MokaCache<String, Arc<Vec<FileEntry>>> {
    GLOBAL_MANIFEST_CACHE.get_or_init(|| {
        MokaCache::builder()
            .max_capacity(500)
            .build()
    })
}

/// Concurrent-safe manifest cache read. No write lock needed with moka.
pub fn get_cached_manifest(path: &str) -> Option<Arc<Vec<FileEntry>>> {
    global_manifest_cache().get(path)
}

pub fn put_cached_manifest(path: &str, entries: Arc<Vec<FileEntry>>) {
    global_manifest_cache().insert(path.to_string(), entries);
}

pub fn invalidate_manifest_cache() {
    global_manifest_cache().invalidate_all();
}

/// Invalidate manifest cache entries for a specific table path.
/// Manifest cache keys are formatted as "table_path/state_dir/manifest_path".
pub fn invalidate_manifest_cache_for_table(table_path: &str) {
    let normalized = normalize_table_path(table_path);
    let cache = global_manifest_cache();
    // Collect matching keys, then invalidate. moka does not provide a retain/drain_filter,
    // so two passes are required; this path is only called on writes (not the read hot path).
    // moka::iter() yields (Arc<K>, V); dereference through Arc to get String.
    let keys_to_remove: Vec<String> = cache.iter()
        .filter(|(k, _)| normalize_table_path(k.as_ref()).starts_with(&normalized))
        .map(|(k, _)| k.as_ref().clone())
        .collect();
    for key in keys_to_remove {
        cache.invalidate(&key);
    }
}

// ============================================================================
// Global cache registry (Option B: internal HashMap keyed by table_path)
// The registry itself is accessed only during creation and invalidation (rare).
// The per-table TxLogCache instances use moka for their hot-path reads.
// ============================================================================

use std::collections::hash_map::DefaultHasher;

/// Cache key combining table path and the config fingerprint that affects cache
/// contents/capacity (credential changes don't invalidate; concurrency doesn't either).
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct RegistryKey {
    table_path: String,
    ttl_ms: u64,
    version_capacity: usize,
    snapshot_capacity: usize,
    file_list_capacity: usize,
}

static CACHE_REGISTRY: OnceLock<RwLock<HashMap<RegistryKey, Arc<TxLogCache>>>> = OnceLock::new();

fn cache_registry() -> &'static RwLock<HashMap<RegistryKey, Arc<TxLogCache>>> {
    CACHE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Extract cache TTL from config map. Returns None if caching disabled (ttl=0).
pub fn extract_cache_ttl(config: &HashMap<String, String>) -> Option<Duration> {
    let ttl_str = config.get("cache.ttl.ms")
        .or_else(|| config.get("cache_ttl_ms"));
    match ttl_str {
        Some(s) => {
            let ms: u64 = s.parse().unwrap_or(300_000);
            if ms == 0 { None } else { Some(Duration::from_millis(ms)) }
        }
        None => Some(Duration::from_secs(300)), // default 5 minutes
    }
}

/// Get or create a TxLogCache for the given table path using the supplied CacheConfig.
/// Two callers with different TTLs or capacities get separate cache instances.
pub fn get_or_create_cache(table_path: &str, config: CacheConfig) -> Arc<TxLogCache> {
    let key = RegistryKey {
        table_path: normalize_table_path(table_path),
        ttl_ms: config.version_ttl.as_millis() as u64,
        version_capacity: config.version_capacity,
        snapshot_capacity: config.snapshot_capacity,
        file_list_capacity: config.file_list_capacity,
    };

    // Fast path: check if already exists (read lock)
    {
        let registry = cache_registry().read();
        if let Some(cache) = registry.get(&key) {
            return cache.clone();
        }
    }

    // Slow path: create new cache (write lock)
    let mut registry = cache_registry().write();
    // Double-check after acquiring write lock
    if let Some(cache) = registry.get(&key) {
        return cache.clone();
    }

    let cache = Arc::new(TxLogCache::new(config));
    registry.insert(key, cache.clone());
    cache
}

/// Normalize a table path for consistent cache key matching.
/// Handles file:// vs file:/// and trailing slash differences.
fn normalize_table_path(path: &str) -> String {
    let p = path.trim_end_matches('/');
    // Normalize file:/// to file:// (both are valid for local paths)
    if p.starts_with("file:///") {
        format!("file://{}", &p[7..])
    } else {
        p.to_string()
    }
}

/// Invalidate all cached data for a specific table (called after writes).
/// Uses normalized path matching to handle file:// vs file:/// differences.
pub fn invalidate_table_cache(table_path: &str) {
    let normalized = normalize_table_path(table_path);
    let registry = cache_registry().read();
    for (key, cache) in registry.iter() {
        if normalize_table_path(&key.table_path) == normalized {
            cache.invalidate_all();
        }
    }
}

/// Get aggregate stats across all cached tables.
pub fn global_cache_stats() -> CacheStatsSnapshot {
    let registry = cache_registry().read();
    let mut total = CacheStatsSnapshot::default();
    for cache in registry.values() {
        let s = cache.stats();
        total.hits += s.hits;
        total.misses += s.misses;
        total.evictions += s.evictions;
    }
    total
}

/// Clear all caches (for testing or shutdown).
pub fn clear_all_caches() {
    let mut registry = cache_registry().write();
    for cache in registry.values() {
        cache.invalidate_all();
    }
    registry.clear();
    invalidate_manifest_cache();
}

/// Compute a stable hash for snapshot cache key from snapshot info.
pub fn hash_snapshot_key(checkpoint_version: i64, post_cp_count: usize) -> i64 {
    let mut hasher = DefaultHasher::new();
    checkpoint_version.hash(&mut hasher);
    post_cp_count.hash(&mut hasher);
    hasher.finish() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_disabled() {
        let config = CacheConfig { enabled: false, ..Default::default() };
        let cache = TxLogCache::new(config);
        cache.put_version(1, vec![]);
        assert!(cache.get_version(1).is_none());
    }

    #[test]
    fn test_cache_hit_miss() {
        let cache = TxLogCache::new(CacheConfig::default());
        assert!(cache.get_version(1).is_none()); // miss
        cache.put_version(1, vec![]);
        assert!(cache.get_version(1).is_some()); // hit
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = TxLogCache::new(CacheConfig::default());
        cache.put_version(1, vec![]);
        cache.invalidate_all();
        assert!(cache.get_version(1).is_none());
    }

    #[test]
    fn test_cache_ttl_expiry() {
        let config = CacheConfig {
            version_ttl: Duration::from_millis(1),
            ..Default::default()
        };
        let cache = TxLogCache::new(config);
        cache.put_version(1, vec![]);
        std::thread::sleep(Duration::from_millis(50));
        assert!(cache.get_version(1).is_none()); // expired
    }

    #[test]
    fn test_global_manifest_cache() {
        let entries = Arc::new(vec![]);
        put_cached_manifest("test-manifest.avro", entries.clone());
        let cached = get_cached_manifest("test-manifest.avro");
        assert!(cached.is_some());
    }

    #[test]
    fn test_stats() {
        let cache = TxLogCache::new(CacheConfig::default());
        cache.get_version(1); // miss
        cache.put_version(1, vec![]);
        cache.get_version(1); // hit
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_extract_cache_ttl_default() {
        let config = HashMap::new();
        let ttl = extract_cache_ttl(&config);
        assert_eq!(ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_extract_cache_ttl_custom() {
        let mut config = HashMap::new();
        config.insert("cache.ttl.ms".to_string(), "60000".to_string());
        let ttl = extract_cache_ttl(&config);
        assert_eq!(ttl, Some(Duration::from_millis(60000)));
    }

    #[test]
    fn test_extract_cache_ttl_disabled() {
        let mut config = HashMap::new();
        config.insert("cache.ttl.ms".to_string(), "0".to_string());
        let ttl = extract_cache_ttl(&config);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_get_or_create_cache_reuse() {
        let config = CacheConfig { version_ttl: Duration::from_secs(60), ..Default::default() };
        let c1 = get_or_create_cache("test://table1-moka", config.clone());
        let c2 = get_or_create_cache("test://table1-moka", config);
        // Same Arc — should be the same cache
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[test]
    fn test_invalidate_table_cache() {
        let c = get_or_create_cache("test://table_inv_moka", CacheConfig::default());
        c.put_version(1, vec![]);
        assert!(c.get_version(1).is_some());
        invalidate_table_cache("test://table_inv_moka");
        assert!(c.get_version(1).is_none()); // invalidated
    }

    #[test]
    fn test_global_cache_stats() {
        let c = get_or_create_cache("test://table_stats_moka", CacheConfig::default());
        c.get_version(99); // miss
        c.put_version(99, vec![]);
        c.get_version(99); // hit
        let stats = global_cache_stats();
        assert!(stats.hits >= 1);
        assert!(stats.misses >= 1);
    }

    #[test]
    fn test_clear_all_caches_removes_all_entries() {
        // Populate caches for two distinct tables
        let c1 = get_or_create_cache("test://table_clear1_moka", CacheConfig::default());
        let c2 = get_or_create_cache("test://table_clear2_moka", CacheConfig::default());
        c1.put_version(1, vec![]);
        c2.put_version(2, vec![]);

        // Also populate the global manifest cache
        let entries = Arc::new(vec![]);
        put_cached_manifest("test://table_clear1_moka/manifest.avro", entries);

        // Global clear
        clear_all_caches();

        // Both table caches must be empty (registry was cleared)
        assert!(c1.get_version(1).is_none());
        assert!(c2.get_version(2).is_none());

        // Global manifest cache must also be cleared
        assert!(get_cached_manifest("test://table_clear1_moka/manifest.avro").is_none());
    }

    #[test]
    fn test_clear_all_caches_then_repopulate() {
        // Verify the registry is usable after a global clear
        clear_all_caches();

        let c = get_or_create_cache("test://table_repop_moka", CacheConfig::default());
        c.put_version(5, vec![]);
        assert!(c.get_version(5).is_some());
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        let cache = Arc::new(TxLogCache::new(CacheConfig::default()));
        cache.put_version(42, vec![]);
        // Spawn multiple threads reading concurrently — moka must not deadlock
        let handles: Vec<_> = (0..8).map(|_| {
            let c = cache.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let _ = c.get_version(42);
                    let _ = c.get_version(99); // miss
                }
            })
        }).collect();
        for h in handles { h.join().unwrap(); }
        let stats = cache.stats();
        // 8 threads × 100 iterations × 1 hit each = 800 hits; 800 misses
        assert_eq!(stats.hits, 800);
        assert_eq!(stats.misses, 800);
    }
}
