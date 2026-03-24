// txlog/cache.rs - Unified cache with read/invalidate/refresh semantics
//
// Replaces Scala's 5+ separate caches with a single structure providing
// TTL-based expiration, LRU eviction, and a global manifest cache.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;

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
        }
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
// Time-bounded LRU cache
// ============================================================================

struct Timed<T> {
    value: T,
    inserted_at: Instant,
}

struct TimedLruCache<K: Hash + Eq, V> {
    inner: LruCache<K, Timed<V>>,
    ttl: Duration,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl<K: Hash + Eq, V: Clone> TimedLruCache<K, V> {
    fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            inner: LruCache::new(NonZeroUsize::new(capacity.max(1)).expect("capacity.max(1) is always >= 1")),
            ttl,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        // Use get() directly which both checks existence and promotes in LRU order.
        // Then check TTL — if expired, remove and count as miss.
        if let Some(entry) = self.inner.get(key) {
            if entry.inserted_at.elapsed() < self.ttl {
                self.hits += 1;
                return Some(entry.value.clone());
            }
            // Expired — remove it
            self.inner.pop(key);
            self.evictions += 1;
        }
        self.misses += 1;
        None
    }

    fn put(&mut self, key: K, value: V) {
        if self.inner.len() >= self.inner.cap().get() {
            self.evictions += 1;
        }
        self.inner.put(key, Timed { value, inserted_at: Instant::now() });
    }

    fn invalidate_all(&mut self) {
        self.inner.clear();
    }

    fn stats(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }

    fn reset_stats(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
    }
}

// ============================================================================
// FileListKey: cache key for filtered file lists
// ============================================================================

#[derive(Eq, PartialEq)]
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
// Main cache structure
// ============================================================================

pub struct TxLogCache {
    inner: RwLock<CacheInner>,
    config: CacheConfig,
}

struct CacheInner {
    versions: TimedLruCache<i64, Vec<super::actions::Action>>,
    snapshots: TimedLruCache<i64, Arc<Vec<FileEntry>>>,
    file_lists: TimedLruCache<FileListKey, Arc<Vec<FileEntry>>>,
    metadata: Option<Timed<Arc<(ProtocolAction, MetadataAction)>>>,
    last_checkpoint: Option<Timed<LastCheckpointInfo>>,
    metadata_ttl: Duration,
}

impl TxLogCache {
    pub fn new(config: CacheConfig) -> Self {
        let inner = CacheInner {
            versions: TimedLruCache::new(config.version_capacity, config.version_ttl),
            snapshots: TimedLruCache::new(config.snapshot_capacity, config.snapshot_ttl),
            file_lists: TimedLruCache::new(config.file_list_capacity, config.file_list_ttl),
            metadata: None,
            last_checkpoint: None,
            metadata_ttl: config.metadata_ttl,
        };
        Self {
            inner: RwLock::new(inner),
            config,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    // -- Read --

    // Note: get_version/get_snapshot/get_file_list take a write lock because
    // LRU::get() updates internal access order for eviction tracking.
    // This serializes all cache reads. Acceptable for txlog workload (infrequent reads).

    pub fn get_version(&self, version: i64) -> Option<Vec<super::actions::Action>> {
        if !self.config.enabled { return None; }
        self.inner.write().versions.get(&version)
    }

    pub fn get_snapshot(&self, version: i64) -> Option<Arc<Vec<FileEntry>>> {
        if !self.config.enabled { return None; }
        self.inner.write().snapshots.get(&version)
    }

    pub fn get_file_list(&self, version: i64, filter_hash: u64) -> Option<Arc<Vec<FileEntry>>> {
        if !self.config.enabled { return None; }
        let key = FileListKey { version, filter_hash };
        self.inner.write().file_lists.get(&key)
    }

    pub fn get_metadata(&self) -> Option<Arc<(ProtocolAction, MetadataAction)>> {
        if !self.config.enabled { return None; }
        let inner = self.inner.read();
        if let Some(ref entry) = inner.metadata {
            if entry.inserted_at.elapsed() < inner.metadata_ttl {
                return Some(entry.value.clone());
            }
        }
        None
    }

    pub fn get_last_checkpoint(&self) -> Option<LastCheckpointInfo> {
        if !self.config.enabled { return None; }
        let inner = self.inner.read();
        if let Some(ref entry) = inner.last_checkpoint {
            if entry.inserted_at.elapsed() < inner.metadata_ttl {
                return Some(entry.value.clone());
            }
        }
        None
    }

    // -- Write / Cache --

    pub fn put_version(&self, version: i64, actions: Vec<super::actions::Action>) {
        if !self.config.enabled { return; }
        self.inner.write().versions.put(version, actions);
    }

    pub fn put_snapshot(&self, version: i64, entries: Arc<Vec<FileEntry>>) {
        if !self.config.enabled { return; }
        self.inner.write().snapshots.put(version, entries);
    }

    pub fn put_file_list(&self, version: i64, filter_hash: u64, entries: Arc<Vec<FileEntry>>) {
        if !self.config.enabled { return; }
        let key = FileListKey { version, filter_hash };
        self.inner.write().file_lists.put(key, entries);
    }

    pub fn put_metadata(&self, protocol: ProtocolAction, metadata: MetadataAction) {
        if !self.config.enabled { return; }
        self.inner.write().metadata = Some(Timed {
            value: Arc::new((protocol, metadata)),
            inserted_at: Instant::now(),
        });
    }

    pub fn put_last_checkpoint(&self, info: LastCheckpointInfo) {
        if !self.config.enabled { return; }
        self.inner.write().last_checkpoint = Some(Timed {
            value: info,
            inserted_at: Instant::now(),
        });
    }

    // -- Invalidate --

    pub fn invalidate_all(&self) {
        let mut inner = self.inner.write();
        inner.versions.invalidate_all();
        inner.snapshots.invalidate_all();
        inner.file_lists.invalidate_all();
        inner.metadata = None;
        inner.last_checkpoint = None;
    }

    pub fn invalidate_mutable(&self) {
        let mut inner = self.inner.write();
        inner.versions.invalidate_all();
        inner.snapshots.invalidate_all();
        inner.file_lists.invalidate_all();
    }

    // -- Stats --

    pub fn stats(&self) -> CacheStatsSnapshot {
        let inner = self.inner.read();
        let v = inner.versions.stats();
        let s = inner.snapshots.stats();
        let f = inner.file_lists.stats();
        CacheStatsSnapshot {
            hits: v.hits + s.hits + f.hits,
            misses: v.misses + s.misses + f.misses,
            evictions: v.evictions + s.evictions + f.evictions,
        }
    }

    pub fn reset_stats(&self) {
        let mut inner = self.inner.write();
        inner.versions.reset_stats();
        inner.snapshots.reset_stats();
        inner.file_lists.reset_stats();
    }
}

// ============================================================================
// Global manifest cache (immutable data, no TTL)
// ============================================================================

static GLOBAL_MANIFEST_CACHE: OnceLock<RwLock<LruCache<String, Arc<Vec<FileEntry>>>>> = OnceLock::new();

fn global_manifest_cache() -> &'static RwLock<LruCache<String, Arc<Vec<FileEntry>>>> {
    GLOBAL_MANIFEST_CACHE.get_or_init(|| {
        RwLock::new(LruCache::new(NonZeroUsize::new(500).expect("500 is non-zero")))
    })
}

/// Note: takes write lock because LRU::get() updates internal access order.
pub fn get_cached_manifest(path: &str) -> Option<Arc<Vec<FileEntry>>> {
    global_manifest_cache().write().get(&path.to_string()).cloned()
}

pub fn put_cached_manifest(path: &str, entries: Arc<Vec<FileEntry>>) {
    global_manifest_cache().write().put(path.to_string(), entries);
}

pub fn invalidate_manifest_cache() {
    global_manifest_cache().write().clear();
}

// ============================================================================
// Global cache registry (Option B: internal LRU keyed by table_path)
// ============================================================================

use std::collections::hash_map::DefaultHasher;

/// Cache key combining table path and TTL (credential changes don't invalidate cache).
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct RegistryKey {
    table_path: String,
    ttl_ms: u64,
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

/// Get or create a TxLogCache for the given table path and TTL.
pub fn get_or_create_cache(table_path: &str, ttl: Duration) -> Arc<TxLogCache> {
    let key = RegistryKey {
        table_path: table_path.to_string(),
        ttl_ms: ttl.as_millis() as u64,
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

    let config = CacheConfig {
        version_ttl: ttl,
        snapshot_ttl: ttl,
        file_list_ttl: ttl,
        metadata_ttl: ttl,
        ..Default::default()
    };
    let cache = Arc::new(TxLogCache::new(config));
    registry.insert(key, cache.clone());
    cache
}

/// Invalidate all cached data for a specific table (called after writes).
/// Clears everything including metadata and last_checkpoint, since any write
/// (version file or checkpoint) can change the table state.
pub fn invalidate_table_cache(table_path: &str) {
    let registry = cache_registry().read();
    for (key, cache) in registry.iter() {
        if key.table_path == table_path {
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
        std::thread::sleep(Duration::from_millis(5));
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
        let ttl = Duration::from_secs(60);
        let c1 = get_or_create_cache("test://table1", ttl);
        let c2 = get_or_create_cache("test://table1", ttl);
        // Same Arc — should be the same cache
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[test]
    fn test_invalidate_table_cache() {
        let ttl = Duration::from_secs(300);
        let c = get_or_create_cache("test://table_inv", ttl);
        c.put_version(1, vec![]);
        assert!(c.get_version(1).is_some());
        invalidate_table_cache("test://table_inv");
        assert!(c.get_version(1).is_none()); // invalidated
    }

    #[test]
    fn test_global_cache_stats() {
        let ttl = Duration::from_secs(300);
        let c = get_or_create_cache("test://table_stats", ttl);
        c.get_version(99); // miss
        c.put_version(99, vec![]);
        c.get_version(99); // hit
        let stats = global_cache_stats();
        // Stats are global aggregated — at least 1 hit and 1 miss from this test
        assert!(stats.hits >= 1);
        assert!(stats.misses >= 1);
    }
}
