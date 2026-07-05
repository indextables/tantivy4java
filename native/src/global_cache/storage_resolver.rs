// storage_resolver.rs - Storage resolver caching functions
// Extracted from global_cache.rs during refactoring

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;
use quickwit_config::{AzureStorageConfig, S3StorageConfig, StorageConfig, StorageConfigs};
use quickwit_storage::StorageResolver;

use crate::debug_println;

/// Maximum number of distinct configured `StorageResolver`s to keep cached. Each
/// resolver holds S3/Azure clients with connection pools, so an unbounded cache
/// leaks clients under credential rotation (e.g. Spark STS session tokens, which
/// mint a new key on every refresh). A bounded FIFO keeps reuse for the hot set
/// while ensuring stale rotated credentials are eventually dropped.
const MAX_CACHED_RESOLVERS: usize = 64;

/// Generate a cache key for a storage resolver that includes all credential
/// components. Different credentials therefore map to different cached resolvers,
/// and — critically — a config carrying BOTH S3 and Azure credentials produces a
/// key distinct from either alone, so mixed-cloud resolvers are not aliased onto an
/// S3-only (or Azure-only) cached instance.
///
/// Sensitive credentials (secrets, tokens) are hashed so they never reach logs while
/// still distinguishing different credential sets.
pub fn generate_storage_cache_key(
    s3_config: Option<&S3StorageConfig>,
    azure_config: Option<&AzureStorageConfig>,
) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(s3) = s3_config {
        let mut hasher = DefaultHasher::new();
        s3.secret_access_key.hash(&mut hasher);
        s3.session_token.hash(&mut hasher);
        let cred_hash = hasher.finish();

        parts.push(format!(
            "s3:{}:{}:{}:{}:{:x}",
            s3.region.as_deref().unwrap_or("default"),
            s3.endpoint.as_deref().unwrap_or("default"),
            s3.access_key_id.as_deref().unwrap_or("none"),
            s3.force_path_style_access,
            cred_hash
        ));
    }

    if let Some(az) = azure_config {
        let mut hasher = DefaultHasher::new();
        az.access_key.hash(&mut hasher);
        az.bearer_token.hash(&mut hasher);
        let cred_hash = hasher.finish();

        parts.push(format!(
            "azure:{}:{:x}",
            az.account_name.as_deref().unwrap_or("default"),
            cred_hash
        ));
    }

    if parts.is_empty() {
        "global".to_string()
    } else {
        parts.join("|")
    }
}

/// Global StorageResolver instance following Quickwit's pattern
/// This is a singleton that is shared across all searcher instances
pub static GLOBAL_STORAGE_RESOLVER: Lazy<StorageResolver> = Lazy::new(|| {
    debug_println!("RUST DEBUG: Initializing global StorageResolver singleton");
    let storage_configs = StorageConfigs::default();
    StorageResolver::configured(&storage_configs)
});

/// Bounded FIFO cache of configured resolvers, keyed by credential cache key.
struct ResolverCache {
    map: HashMap<String, StorageResolver>,
    order: VecDeque<String>,
}

impl ResolverCache {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, key: &str) -> Option<StorageResolver> {
        self.map.get(key).cloned()
    }

    fn insert(&mut self, key: String, resolver: StorageResolver) {
        if self.map.contains_key(&key) {
            return;
        }
        // Evict oldest entries until there is room for the newcomer.
        while self.order.len() >= MAX_CACHED_RESOLVERS {
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
                debug_println!("♻️  STORAGE_RESOLVER_EVICT: dropped cached resolver '{}'", evicted);
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, resolver);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

/// Single process-global resolver cache shared by both the sync and async entry
/// points (previously two independent maps, so the same credential could be
/// materialized twice). A `std::sync::Mutex` is safe in async callers here because
/// no `.await` happens while the lock is held — resolver construction occurs after
/// the lock is released.
static STORAGE_RESOLVERS: Lazy<Arc<Mutex<ResolverCache>>> =
    Lazy::new(|| Arc::new(Mutex::new(ResolverCache::new())));

/// Drop all cached configured resolvers. Called from the last-manager-close cleanup
/// so rotated credentials and their clients don't survive between test runs.
pub fn clear_storage_resolvers() {
    let mut cache = STORAGE_RESOLVERS.lock().unwrap();
    if !cache.map.is_empty() {
        debug_println!("🧹 CLEAR_STORAGE_RESOLVERS: dropping {} cached resolver(s)", cache.map.len());
    }
    cache.clear();
}

/// Core get-or-create shared by the sync and async wrappers. Builds a resolver from
/// BOTH the S3 and Azure configs when present (so mixed-cloud operation is
/// authenticated for both), caches it under a combined credential key, and reuses
/// the process-global unconfigured resolver when neither is supplied.
fn get_or_create_resolver(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>,
) -> StorageResolver {
    if s3_config_opt.is_none() && azure_config_opt.is_none() {
        return GLOBAL_STORAGE_RESOLVER.clone();
    }

    let cache_key = generate_storage_cache_key(s3_config_opt.as_ref(), azure_config_opt.as_ref());

    // Fast path: return a cached resolver if present.
    {
        let cache = STORAGE_RESOLVERS.lock().unwrap();
        if let Some(resolver) = cache.get(&cache_key) {
            debug_println!("🎯 STORAGE_RESOLVER_CACHE_HIT: reusing resolver for key: {}", cache_key);
            return resolver;
        }
    }

    // Build the resolver outside the lock (construction may be non-trivial).
    let mut configs: Vec<StorageConfig> = Vec::new();
    if let Some(s3) = s3_config_opt {
        debug_println!(
            "   📋 S3 Config: region={:?}, endpoint={:?}, path_style={}",
            s3.region, s3.endpoint, s3.force_path_style_access
        );
        configs.push(StorageConfig::S3(s3));
    }
    if let Some(az) = azure_config_opt {
        debug_println!("   📋 Azure Config: account={:?}", az.account_name);
        configs.push(StorageConfig::Azure(az));
    }
    let resolver = StorageResolver::configured(&StorageConfigs::new(configs));
    debug_println!("✅ STORAGE_RESOLVER_CREATED: new resolver for key: {}", cache_key);

    // Insert, double-checking for a concurrent creator.
    let mut cache = STORAGE_RESOLVERS.lock().unwrap();
    if let Some(existing) = cache.get(&cache_key) {
        debug_println!("🏃 STORAGE_RESOLVER_RACE: using resolver created by another thread");
        return existing;
    }
    cache.insert(cache_key.clone(), resolver.clone());
    debug_println!("💾 STORAGE_RESOLVER_CACHED: resolver cached for key: {}", cache_key);
    resolver
}

/// Helper function to track storage instance creation for debugging
/// This helps us understand when and where multiple storage instances are created
pub async fn tracked_storage_resolve(
    resolver: &StorageResolver,
    uri: &quickwit_common::uri::Uri,
    context: &str,
) -> Result<Arc<dyn quickwit_storage::Storage>, quickwit_storage::StorageResolverError> {
    static STORAGE_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let storage_id = STORAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    debug_println!(
        "🏗️  STORAGE_RESOLVE: Starting storage resolve #{} [{}]",
        storage_id, context
    );
    debug_println!("   🌐 URI: {}", uri);

    let result = resolver.resolve(uri).await;
    match &result {
        Ok(_) => debug_println!("✅ STORAGE_RESOLVED: #{} [{}]", storage_id, context),
        Err(e) => debug_println!("❌ STORAGE_RESOLVE_FAILED: #{} [{}]: {}", storage_id, context, e),
    }
    result
}

/// Get or create a cached StorageResolver (async wrapper).
///
/// 🚨 Use this (or the sync variant) for ALL configured storage resolver creation so
/// resolvers are shared. Direct `StorageResolver::configured()` calls bypass the
/// cache and create redundant storage instances.
pub async fn get_configured_storage_resolver_async(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>,
) -> StorageResolver {
    get_or_create_resolver(s3_config_opt, azure_config_opt)
}

/// Get or create a cached StorageResolver (sync).
pub fn get_configured_storage_resolver(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>,
) -> StorageResolver {
    get_or_create_resolver(s3_config_opt, azure_config_opt)
}
