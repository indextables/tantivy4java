// storage_resolver.rs - Storage resolver caching functions
// Extracted from global_cache.rs during refactoring

use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use quickwit_config::{AzureStorageConfig, S3StorageConfig, StorageConfigs};
use quickwit_storage::StorageResolver;
use tokio::sync::RwLock as TokioRwLock;

use crate::debug_println;

/// Generate a cache key for storage resolver that includes all credential components.
/// This ensures different credentials result in different cached StorageResolver instances.
///
/// The key includes:
/// - For S3: region, endpoint, full access_key_id, force_path_style, and a hash of secret+session_token
/// - For Azure: account_name and a hash of access_key+bearer_token
///
/// Sensitive credentials (secrets, tokens) are hashed to avoid leaking to logs while still
/// ensuring different credentials produce different cache keys.
pub fn generate_storage_cache_key(
    s3_config: Option<&S3StorageConfig>,
    azure_config: Option<&AzureStorageConfig>,
) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    if let Some(s3) = s3_config {
        let mut hasher = DefaultHasher::new();
        s3.secret_access_key.hash(&mut hasher);
        s3.session_token.hash(&mut hasher);
        let cred_hash = hasher.finish();

        format!(
            "s3:{}:{}:{}:{}:{:x}",
            s3.region.as_deref().unwrap_or("default"),
            s3.endpoint.as_deref().unwrap_or("default"),
            s3.access_key_id.as_deref().unwrap_or("none"),
            s3.force_path_style_access,
            cred_hash
        )
    } else if let Some(az) = azure_config {
        let mut hasher = DefaultHasher::new();
        az.access_key.hash(&mut hasher);
        az.bearer_token.hash(&mut hasher);
        let cred_hash = hasher.finish();

        format!(
            "azure:{}:{:x}",
            az.account_name.as_deref().unwrap_or("default"),
            cred_hash
        )
    } else {
        "global".to_string()
    }
}

/// Global StorageResolver instance following Quickwit's pattern
/// This is a singleton that is shared across all searcher instances
pub static GLOBAL_STORAGE_RESOLVER: Lazy<StorageResolver> = Lazy::new(|| {
    debug_println!("RUST DEBUG: Initializing global StorageResolver singleton");
    let storage_configs = StorageConfigs::default();
    StorageResolver::configured(&storage_configs)
});

/// Global storage resolver cache for configured S3 instances (async-compatible)
/// Uses tokio::sync::RwLock to prevent deadlocks in async context
static CONFIGURED_STORAGE_RESOLVERS: std::sync::OnceLock<
    TokioRwLock<std::collections::HashMap<String, StorageResolver>>,
> = std::sync::OnceLock::new();

/// Helper function to track storage instance creation for debugging
/// This helps us understand when and where multiple storage instances are created
pub async fn tracked_storage_resolve(
    resolver: &StorageResolver,
    uri: &quickwit_common::uri::Uri,
    context: &str,
) -> Result<Arc<dyn quickwit_storage::Storage>, quickwit_storage::StorageResolverError> {
    static STORAGE_COUNTER: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(1);
    let storage_id =
        STORAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    debug_println!(
        "🏗️  STORAGE_RESOLVE: Starting storage resolve #{} [{}]",
        storage_id,
        context
    );
    debug_println!("   📍 Resolver address: {:p}", resolver);
    debug_println!("   🌐 URI: {}", uri);

    let resolve_start = std::time::Instant::now();
    let result = resolver.resolve(uri).await;

    match &result {
        Ok(storage) => {
            debug_println!(
                "✅ STORAGE_RESOLVED: Storage instance #{} created in {}ms [{}]",
                storage_id,
                resolve_start.elapsed().as_millis(),
                context
            );
            debug_println!("   🏭 Storage address: {:p}", &**storage);
            debug_println!(
                "   📊 Storage type: {}",
                std::any::type_name::<dyn quickwit_storage::Storage>()
            );
        }
        Err(e) => {
            debug_println!(
                "❌ STORAGE_RESOLVE_FAILED: Storage resolve #{} failed in {}ms [{}]: {}",
                storage_id,
                resolve_start.elapsed().as_millis(),
                context,
                e
            );
        }
    }

    result
}

/// Get or create a cached StorageResolver with specific S3/Azure credentials (async version)
/// This follows Quickwit's pattern but enables caching for optimal storage instance reuse
///
/// 🚨 CRITICAL: This function should be used for ALL storage resolver creation in ASYNC contexts
/// to ensure consistent cache sharing. Direct calls to StorageResolver::configured()
/// bypass caching and cause multiple storage instances.
///
/// ✅ FIXED: Async-compatible using tokio::sync::RwLock to prevent deadlocks
pub async fn get_configured_storage_resolver_async(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>,
) -> StorageResolver {
    static RESOLVER_COUNTER: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(1);

    if let Some(s3_config) = s3_config_opt {
        // Create a cache key that includes ALL credential components
        let cache_key = generate_storage_cache_key(Some(&s3_config), None);

        // ✅ FIXED: Use async tokio RwLock to prevent deadlocks in async context
        // Initialize cache if needed
        let cache = CONFIGURED_STORAGE_RESOLVERS
            .get_or_init(|| TokioRwLock::new(std::collections::HashMap::new()));

        // Try to get from cache first (async read lock) - DEADLOCK FIXED
        {
            let read_cache = cache.read().await;
            if let Some(cached_resolver) = read_cache.get(&cache_key) {
                debug_println!(
                    "🎯 STORAGE_RESOLVER_CACHE_HIT: Reusing cached resolver for key: {} at address {:p}",
                    cache_key,
                    cached_resolver
                );
                return cached_resolver.clone();
            }
        } // <- async read lock released here

        // Cache miss - create new resolver (outside of any locks to prevent deadlock)
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "❌ STORAGE_RESOLVER_CACHE_MISS: Creating new resolver #{} for key: {}",
            resolver_id,
            cache_key
        );
        debug_println!(
            "   📋 S3 Config: region={:?}, endpoint={:?}, path_style={}",
            s3_config.region,
            s3_config.endpoint,
            s3_config.force_path_style_access
        );

        let storage_configs =
            StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config)]);
        let resolver = StorageResolver::configured(&storage_configs); // <- DEADLOCK SUSPECT: Quickwit resolver creation
        debug_println!(
            "✅ STORAGE_RESOLVER_CREATED: Resolver #{} created at address {:p}",
            resolver_id,
            &resolver
        );

        // Cache the new resolver (async write lock) - DEADLOCK FIXED
        {
            let mut write_cache = cache.write().await;
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = write_cache.get(&cache_key) {
                debug_println!(
                    "🏃 STORAGE_RESOLVER_RACE: Another thread created resolver, using existing at {:p}",
                    existing_resolver
                );
                return existing_resolver.clone();
            }
            write_cache.insert(cache_key.clone(), resolver.clone());
            debug_println!(
                "💾 STORAGE_RESOLVER_CACHED: Resolver #{} cached for key: {}",
                resolver_id,
                cache_key
            );
        } // <- async write lock released here

        resolver
    } else if let Some(azure_config) = azure_config_opt {
        // Create a cache key that includes ALL credential components
        let cache_key = generate_storage_cache_key(None, Some(&azure_config));

        let cache = CONFIGURED_STORAGE_RESOLVERS
            .get_or_init(|| TokioRwLock::new(HashMap::new()));

        // Try read lock first (async)
        {
            let read_cache = cache.read().await;
            if let Some(cached_resolver) = read_cache.get(&cache_key) {
                debug_println!(
                    "🎯 AZURE_RESOLVER_CACHE_HIT: Reusing resolver for key: {}",
                    cache_key
                );
                return cached_resolver.clone();
            }
        } // <- Lock released immediately

        // Create resolver OUTSIDE lock (deadlock prevention)
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "❌ AZURE_RESOLVER_CACHE_MISS: Creating resolver #{} for key: {}",
            resolver_id,
            cache_key
        );
        debug_println!(
            "   📋 Azure Config: account={:?}",
            azure_config.account_name
        );

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::Azure(azure_config),
        ]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!(
            "✅ AZURE_RESOLVER_CREATED: Resolver #{} at {:p}",
            resolver_id,
            &resolver
        );

        // Cache with write lock (async)
        {
            let mut write_cache = cache.write().await;
            // Double-check for race condition
            if let Some(existing_resolver) = write_cache.get(&cache_key) {
                debug_println!(
                    "🏃 AZURE_RESOLVER_RACE: Using existing at {:p}",
                    existing_resolver
                );
                return existing_resolver.clone();
            }
            write_cache.insert(cache_key.clone(), resolver.clone());
            debug_println!(
                "💾 AZURE_RESOLVER_CACHED: Resolver #{} cached",
                resolver_id
            );
        } // <- Lock released immediately

        resolver
    } else {
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "🌐 STORAGE_RESOLVER_GLOBAL: Resolver #{} - Using global unconfigured StorageResolver",
            resolver_id
        );
        let resolver = GLOBAL_STORAGE_RESOLVER.clone();
        debug_println!(
            "♻️  STORAGE_RESOLVER_REUSED: Resolver #{} reused global at address {:p}",
            resolver_id,
            &resolver
        );
        resolver
    }
}

/// Get or create a cached StorageResolver with specific S3/Azure credentials (sync version)
/// This version uses a simple sync-safe caching approach for sync contexts
///
/// ✅ FIXED: Now uses deadlock-safe sync caching approach
pub fn get_configured_storage_resolver(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>,
) -> StorageResolver {
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Mutex};

    static SYNC_STORAGE_RESOLVERS: Lazy<
        Arc<Mutex<std::collections::HashMap<String, StorageResolver>>>,
    > = Lazy::new(|| Arc::new(Mutex::new(std::collections::HashMap::new())));
    static RESOLVER_COUNTER: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(1);

    if let Some(s3_config) = s3_config_opt {
        // Create a cache key that includes ALL credential components
        let cache_key = generate_storage_cache_key(Some(&s3_config), None);

        // Try to get from sync cache (simple mutex approach)
        {
            let cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            if let Some(cached_resolver) = cache.get(&cache_key) {
                debug_println!(
                    "🎯 STORAGE_RESOLVER_SYNC_CACHE_HIT: Reusing cached sync resolver for key: {} at address {:p}",
                    cache_key,
                    cached_resolver
                );
                return cached_resolver.clone();
            }
        }

        // Cache miss - create new resolver
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "❌ STORAGE_RESOLVER_SYNC_CACHE_MISS: Creating new sync resolver #{} for key: {}",
            resolver_id,
            cache_key
        );
        debug_println!(
            "   📋 S3 Config: region={:?}, endpoint={:?}, path_style={}",
            s3_config.region,
            s3_config.endpoint,
            s3_config.force_path_style_access
        );

        let storage_configs =
            StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config)]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!(
            "✅ STORAGE_RESOLVER_CREATED: Sync resolver #{} created at address {:p}",
            resolver_id,
            &resolver
        );

        // Cache the new resolver
        {
            let mut cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = cache.get(&cache_key) {
                debug_println!(
                    "🏃 STORAGE_RESOLVER_SYNC_RACE: Another thread created sync resolver, using existing at {:p}",
                    existing_resolver
                );
                return existing_resolver.clone();
            }
            cache.insert(cache_key.clone(), resolver.clone());
            debug_println!(
                "💾 STORAGE_RESOLVER_SYNC_CACHED: Resolver #{} cached for key: {}",
                resolver_id,
                cache_key
            );
        }

        resolver
    } else if let Some(azure_config) = azure_config_opt {
        // Create a cache key that includes ALL credential components
        let cache_key = generate_storage_cache_key(None, Some(&azure_config));

        // Try to get from sync cache
        {
            let cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            if let Some(cached_resolver) = cache.get(&cache_key) {
                debug_println!(
                    "🎯 AZURE_RESOLVER_SYNC_CACHE_HIT: Reusing cached sync resolver for key: {} at address {:p}",
                    cache_key,
                    cached_resolver
                );
                return cached_resolver.clone();
            }
        }

        // Cache miss - create new resolver
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "❌ AZURE_RESOLVER_SYNC_CACHE_MISS: Creating new sync resolver #{} for key: {}",
            resolver_id,
            cache_key
        );
        debug_println!(
            "   📋 Azure Config: account={:?}",
            azure_config.account_name
        );

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::Azure(azure_config),
        ]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!(
            "✅ AZURE_RESOLVER_CREATED: Sync resolver #{} created at address {:p}",
            resolver_id,
            &resolver
        );

        // Cache the new resolver
        {
            let mut cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = cache.get(&cache_key) {
                debug_println!(
                    "🏃 AZURE_RESOLVER_SYNC_RACE: Another thread created sync resolver, using existing at {:p}",
                    existing_resolver
                );
                return existing_resolver.clone();
            }
            cache.insert(cache_key.clone(), resolver.clone());
            debug_println!(
                "💾 AZURE_RESOLVER_SYNC_CACHED: Resolver #{} cached for key: {}",
                resolver_id,
                cache_key
            );
        }

        resolver
    } else {
        let resolver_id =
            RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!(
            "🌐 STORAGE_RESOLVER_GLOBAL_SYNC: Resolver #{} - Using global unconfigured StorageResolver",
            resolver_id
        );
        let resolver = GLOBAL_STORAGE_RESOLVER.clone();
        debug_println!(
            "♻️  STORAGE_RESOLVER_REUSED_SYNC: Resolver #{} reused global at address {:p}",
            resolver_id,
            &resolver
        );
        resolver
    }
}
