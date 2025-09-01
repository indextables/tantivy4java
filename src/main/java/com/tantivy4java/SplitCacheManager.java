package com.tantivy4java;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global cache manager for Quickwit splits following Quickwit's multi-level caching strategy.
 * 
 * <h3>Architecture Overview</h3>
 * Implements Quickwit's proven caching architecture with GLOBAL shared caches:
 * <ul>
 *   <li><b>LeafSearchCache:</b> Global search result cache (per split_id + query)</li>
 *   <li><b>ByteRangeCache:</b> Global storage byte range cache (per file_path + range)</li>
 *   <li><b>ComponentCache:</b> Global component cache (fast fields, postings, etc.)</li>
 * </ul>
 * 
 * <h3>Cache Sharing Model</h3>
 * This matches Quickwit's design where caches are GLOBAL and shared across all splits.
 * Each SplitCacheManager instance represents a named cache configuration that can be
 * shared by multiple SplitSearcher instances. Cache instances are singleton per name.
 * 
 * <h3>Usage Pattern</h3>
 * <pre>{@code
 * // Single cache manager per application or logical grouping
 * SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("production-cache")
 *     .withMaxCacheSize(500_000_000)  // 500MB shared across all splits
 *     .withMaxConcurrentLoads(8)      // Parallel component loading
 *     .withAwsCredentials(key, secret) // AWS credentials
 *     .withAwsRegion(region);          // AWS region configured separately
 * 
 * // Or with session token for temporary credentials:
 * SplitCacheManager.CacheConfig configWithToken = new SplitCacheManager.CacheConfig("production-cache")
 *     .withMaxCacheSize(500_000_000)
 *     .withAwsCredentials(key, secret, sessionToken) // Credentials with session token
 *     .withAwsRegion(region);                        // Region configured separately
 * 
 * SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
 * 
 * // Multiple searchers sharing the same cache
 * List<SplitSearcher> searchers = cacheManager.createMultipleSplitSearchers(splitPaths);
 * 
 * // Global search across all managed splits
 * SearchResult results = cacheManager.searchAcrossAllSplits(query, 100);
 * }</pre>
 * 
 * <h3>Configuration Validation</h3>
 * The manager validates that cache instances with the same name have consistent
 * configurations to prevent conflicting cache settings.
 * 
 * <h3>AWS Credential Session Token Management</h3>
 * The cache manager automatically handles AWS credential rotation scenarios:
 * <ul>
 *   <li><b>Session Token Invalidation:</b> When creating a cache with the same AWS access_key 
 *       and secret_key but different session_token, existing caches are automatically closed 
 *       and invalidated</li>
 *   <li><b>Credential Tracking:</b> Caches are tracked by AWS credentials to enable automatic 
 *       cleanup during credential rotation</li>
 *   <li><b>Manual Invalidation:</b> {@link #invalidateAwsCredentialCaches(String, String)} 
 *       allows programmatic invalidation of caches using specific AWS credentials</li>
 * </ul>
 * 
 * <p><b>Example AWS credential rotation handling:</b></p>
 * <pre>{@code
 * // Initial cache with session token
 * SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("aws-cache")
 *     .withAwsCredentials("access-key", "secret-key", "session-token-1");
 * SplitCacheManager cache1 = SplitCacheManager.getInstance(config1);
 * 
 * // New cache with rotated session token - automatically invalidates cache1
 * SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("aws-cache")
 *     .withAwsCredentials("access-key", "secret-key", "session-token-2");
 * SplitCacheManager cache2 = SplitCacheManager.getInstance(config2); // cache1 is closed
 * 
 * // Manual invalidation of all caches using specific credentials
 * int invalidated = SplitCacheManager.invalidateAwsCredentialCaches("access-key", "secret-key");
 * }</pre>
 */
public class SplitCacheManager implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }
    
    private static final Map<String, SplitCacheManager> instances = new ConcurrentHashMap<>();
    
    // Track caches by AWS credential key (access_key + secret_key) for session token invalidation
    private static final Map<String, List<String>> awsCredentialToCacheKeys = new ConcurrentHashMap<>();
    
    private final String cacheName;
    private final String cacheKey; // Full cache key used for storage/retrieval
    private final long maxCacheSize;
    private final Map<String, SplitSearcher> managedSearchers;
    private final AtomicLong totalCacheSize;
    private final long nativePtr;
    private final Map<String, String> awsConfig;
    
    /**
     * Configuration for shared cache across multiple splits
     */
    public static class CacheConfig {
        private final String cacheName;
        private long maxCacheSize = 200_000_000; // 200MB default for shared cache
        private int maxConcurrentLoads = 8;
        private boolean enableQueryCache = true;
        private Map<String, String> awsConfig = new HashMap<>();
        private Map<String, String> azureConfig = new HashMap<>();
        private Map<String, String> gcpConfig = new HashMap<>();
        
        public CacheConfig(String cacheName) {
            this.cacheName = cacheName;
        }
        
        public CacheConfig withMaxCacheSize(long maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }
        
        public CacheConfig withMaxConcurrentLoads(int maxConcurrentLoads) {
            this.maxConcurrentLoads = maxConcurrentLoads;
            return this;
        }
        
        public CacheConfig withQueryCache(boolean enabled) {
            this.enableQueryCache = enabled;
            return this;
        }
        
        /**
         * Configure AWS credentials (2 parameters: access key and secret key).
         * For long-term credentials or IAM instance profiles.
         * 
         * @param accessKey AWS access key ID
         * @param secretKey AWS secret access key
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withAwsCredentials(String accessKey, String secretKey) {
            this.awsConfig.put("access_key", accessKey);
            this.awsConfig.put("secret_key", secretKey);
            return this;
        }
        
        /**
         * Configure AWS credentials (3 parameters: access key, secret key, and session token).
         * For temporary credentials from IAM roles, federated access, or STS-generated credentials.
         * 
         * @param accessKey AWS access key ID
         * @param secretKey AWS secret access key
         * @param sessionToken AWS session token for temporary credentials
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withAwsCredentials(String accessKey, String secretKey, String sessionToken) {
            this.awsConfig.put("access_key", accessKey);
            this.awsConfig.put("secret_key", secretKey);
            this.awsConfig.put("session_token", sessionToken);
            return this;
        }
        
        /**
         * Configure AWS region separately from credentials.
         * 
         * @param region AWS region (e.g., "us-east-1", "eu-west-1")
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withAwsRegion(String region) {
            this.awsConfig.put("region", region);
            return this;
        }
        
        
        public CacheConfig withAwsEndpoint(String endpoint) {
            this.awsConfig.put("endpoint", endpoint);
            return this;
        }
        
        // Azure Blob Storage configuration
        public CacheConfig withAzureCredentials(String accountName, String accountKey) {
            this.azureConfig.put("account_name", accountName);
            this.azureConfig.put("account_key", accountKey);
            return this;
        }
        
        public CacheConfig withAzureConnectionString(String connectionString) {
            this.azureConfig.put("connection_string", connectionString);
            return this;
        }
        
        public CacheConfig withAzureEndpoint(String endpoint) {
            this.azureConfig.put("endpoint", endpoint);
            return this;
        }
        
        // GCP Cloud Storage configuration
        public CacheConfig withGcpCredentials(String projectId, String serviceAccountKey) {
            this.gcpConfig.put("project_id", projectId);
            this.gcpConfig.put("service_account_key", serviceAccountKey);
            return this;
        }
        
        public CacheConfig withGcpCredentialsFile(String credentialsFilePath) {
            this.gcpConfig.put("credentials_file", credentialsFilePath);
            return this;
        }
        
        public CacheConfig withGcpEndpoint(String endpoint) {
            this.gcpConfig.put("endpoint", endpoint);
            return this;
        }
        
        // Getters
        public String getCacheName() { return cacheName; }
        public long getMaxCacheSize() { return maxCacheSize; }
        public int getMaxConcurrentLoads() { return maxConcurrentLoads; }
        public boolean isQueryCacheEnabled() { return enableQueryCache; }
        public Map<String, String> getAwsConfig() { return awsConfig; }
        public Map<String, String> getAzureConfig() { return azureConfig; }
        public Map<String, String> getGcpConfig() { return gcpConfig; }
        
        /**
         * Generate a comprehensive cache key based on all configuration parameters.
         * This ensures that different configurations don't accidentally share cache instances.
         * 
         * @return A unique cache key representing all configuration parameters
         */
        public String getCacheKey() {
            StringBuilder keyBuilder = new StringBuilder();
            keyBuilder.append("name=").append(cacheName);
            keyBuilder.append(",maxSize=").append(maxCacheSize);
            keyBuilder.append(",maxLoads=").append(maxConcurrentLoads);
            keyBuilder.append(",queryCache=").append(enableQueryCache);
            
            // Add AWS config to key (sorted for consistency)
            if (!awsConfig.isEmpty()) {
                keyBuilder.append(",aws={");
                awsConfig.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> keyBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append(","));
                keyBuilder.setLength(keyBuilder.length() - 1); // Remove last comma
                keyBuilder.append("}");
            }
            
            // Add Azure config to key (sorted for consistency)
            if (!azureConfig.isEmpty()) {
                keyBuilder.append(",azure={");
                azureConfig.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> keyBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append(","));
                keyBuilder.setLength(keyBuilder.length() - 1); // Remove last comma
                keyBuilder.append("}");
            }
            
            // Add GCP config to key (sorted for consistency)
            if (!gcpConfig.isEmpty()) {
                keyBuilder.append(",gcp={");
                gcpConfig.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> keyBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append(","));
                keyBuilder.setLength(keyBuilder.length() - 1); // Remove last comma
                keyBuilder.append("}");
            }
            
            return keyBuilder.toString();
        }
    }
    
    /**
     * Global cache statistics across all managed splits
     */
    public static class GlobalCacheStats {
        private final long totalHits;
        private final long totalMisses;
        private final long totalEvictions;
        private final long currentSize;
        private final long maxSize;
        private final int activeSplits;
        
        public GlobalCacheStats(long totalHits, long totalMisses, long totalEvictions, 
                               long currentSize, long maxSize, int activeSplits) {
            this.totalHits = totalHits;
            this.totalMisses = totalMisses;
            this.totalEvictions = totalEvictions;
            this.currentSize = currentSize;
            this.maxSize = maxSize;
            this.activeSplits = activeSplits;
        }
        
        public long getTotalHits() { return totalHits; }
        public long getTotalMisses() { return totalMisses; }
        public long getTotalEvictions() { return totalEvictions; }
        public long getCurrentSize() { return currentSize; }
        public long getMaxSize() { return maxSize; }
        public int getActiveSplits() { return activeSplits; }
        public double getHitRate() { 
            long total = totalHits + totalMisses;
            return total > 0 ? (double) totalHits / total : 0.0; 
        }
        
        public double getUtilization() {
            return maxSize > 0 ? (double) currentSize / maxSize : 0.0;
        }
    }
    
    private SplitCacheManager(CacheConfig config) {
        this.cacheName = config.getCacheName();
        this.cacheKey = config.getCacheKey();
        this.maxCacheSize = config.getMaxCacheSize();
        this.managedSearchers = new ConcurrentHashMap<>();
        this.totalCacheSize = new AtomicLong(0);
        this.awsConfig = new HashMap<>(config.getAwsConfig());
        this.nativePtr = createNativeCacheManager(config);
    }
    
    /**
     * Get or create a shared cache manager instance with AWS credential session token invalidation.
     * If AWS credentials (access_key + secret_key) already exist with a different session token,
     * the existing cache will be closed and replaced.
     */
    public static SplitCacheManager getInstance(CacheConfig config) {
        // Handle AWS credential session token invalidation
        String awsCredentialKey = getAwsCredentialKey(config);
        if (awsCredentialKey != null) {
            invalidateExistingAwsCredentialCaches(config, awsCredentialKey);
        }
        
        return instances.computeIfAbsent(config.getCacheKey(), 
            cacheKey -> {
                validateCacheConfig(config);
                SplitCacheManager manager = new SplitCacheManager(config);
                
                // Track this cache by AWS credentials if present
                if (awsCredentialKey != null) {
                    awsCredentialToCacheKeys.computeIfAbsent(awsCredentialKey, k -> new ArrayList<>())
                        .add(cacheKey);
                }
                
                return manager;
            });
    }
    
    /**
     * Generate AWS credential key for tracking (access_key + secret_key, excluding session_token)
     */
    private static String getAwsCredentialKey(CacheConfig config) {
        Map<String, String> awsConfig = config.getAwsConfig();
        if (awsConfig.isEmpty()) {
            return null;
        }
        
        String accessKey = awsConfig.get("access_key");
        String secretKey = awsConfig.get("secret_key");
        
        if (accessKey == null || secretKey == null) {
            return null;
        }
        
        return "aws:" + accessKey + ":" + secretKey;
    }
    
    /**
     * Invalidate existing caches with the same AWS credentials but different session tokens
     */
    private static void invalidateExistingAwsCredentialCaches(CacheConfig newConfig, String awsCredentialKey) {
        List<String> existingCacheKeys = awsCredentialToCacheKeys.get(awsCredentialKey);
        if (existingCacheKeys == null || existingCacheKeys.isEmpty()) {
            return;
        }
        
        String newSessionToken = newConfig.getAwsConfig().get("session_token");
        List<String> keysToRemove = new ArrayList<>();
        
        // Create a copy of the list to avoid ConcurrentModificationException
        List<String> cacheKeysCopy = new ArrayList<>(existingCacheKeys);
        
        for (String existingCacheKey : cacheKeysCopy) {
            SplitCacheManager existingManager = instances.get(existingCacheKey);
            if (existingManager == null) {
                keysToRemove.add(existingCacheKey);
                continue;
            }
            
            String existingSessionToken = existingManager.awsConfig.get("session_token");
            
            // If session tokens are different (including null vs non-null), invalidate the existing cache
            if (!Objects.equals(newSessionToken, existingSessionToken)) {
                System.out.println("🔄 AWS session token changed - invalidating existing cache: " + existingCacheKey);
                
                try {
                    existingManager.close();
                } catch (Exception e) {
                    System.err.println("Error closing cache during AWS credential invalidation: " + e.getMessage());
                }
                
                instances.remove(existingCacheKey);
                keysToRemove.add(existingCacheKey);
            }
        }
        
        // Clean up removed cache keys (safe since we're working with the original list)
        existingCacheKeys.removeAll(keysToRemove);
        if (existingCacheKeys.isEmpty()) {
            awsCredentialToCacheKeys.remove(awsCredentialKey);
        }
    }
    
    /**
     * Validate cache configuration for consistency.
     * With the comprehensive cache key approach, configurations are automatically 
     * unique and consistent - identical configs share instances, different configs get separate instances.
     */
    private static void validateCacheConfig(CacheConfig config) {
        // Basic validation - ensure required fields are present
        if (config.getCacheName() == null || config.getCacheName().trim().isEmpty()) {
            throw new IllegalArgumentException("Cache name cannot be null or empty");
        }
        if (config.getMaxCacheSize() <= 0) {
            throw new IllegalArgumentException("Max cache size must be positive, got: " + config.getMaxCacheSize());
        }
        if (config.getMaxConcurrentLoads() <= 0) {
            throw new IllegalArgumentException("Max concurrent loads must be positive, got: " + config.getMaxConcurrentLoads());
        }
        
        // Note: Configuration conflict validation is no longer needed since we use comprehensive 
        // cache keys. Identical configurations will share instances automatically, while different
        // configurations will get separate cache instances as intended.
    }
    
    /**
     * Create a split searcher that uses this shared cache
     */
    public SplitSearcher createSplitSearcher(String splitPath) {
        // Pass AWS configuration from the cache manager to the searcher
        Map<String, Object> splitConfig = new HashMap<>();
        if (!this.awsConfig.isEmpty()) {
            splitConfig.put("aws_config", this.awsConfig);
        }
        return createSplitSearcher(splitPath, splitConfig);
    }
    
    /**
     * Create a split searcher with custom configuration that uses this shared cache
     */
    public SplitSearcher createSplitSearcher(String splitPath, Map<String, Object> splitConfig) {
        SplitSearcher searcher = new SplitSearcher(splitPath, this, splitConfig);
        managedSearchers.put(splitPath, searcher);
        return searcher;
    }
    
    /**
     * Create multiple split searchers sharing the same cache (batch creation)
     */
    public List<SplitSearcher> createMultipleSplitSearchers(List<String> splitPaths) {
        List<SplitSearcher> searchers = new ArrayList<>();
        for (String splitPath : splitPaths) {
            searchers.add(createSplitSearcher(splitPath));
        }
        return searchers;
    }
    
    /**
     * Multi-split search across all managed splits
     * Implements distributed search pattern similar to Quickwit's merge_search
     */
    public SearchResult searchAcrossAllSplits(Query query, int totalLimit) {
        return searchAcrossAllSplitsNative(nativePtr, query.getNativePtr(), totalLimit);
    }
    
    /**
     * Multi-split search across specific splits
     */
    public SearchResult searchAcrossSplits(List<String> splitPaths, Query query, int totalLimit) {
        return searchAcrossSplitsNative(nativePtr, splitPaths, query.getNativePtr(), totalLimit);
    }
    
    /**
     * Remove a split searcher from management
     */
    void removeSplitSearcher(String splitPath) {
        managedSearchers.remove(splitPath);
    }
    
    /**
     * Get all currently managed split paths
     */
    public Set<String> getManagedSplitPaths() {
        return new HashSet<>(managedSearchers.keySet());
    }
    
    /**
     * Get global cache statistics across all managed splits
     */
    public GlobalCacheStats getGlobalCacheStats() {
        // Get native stats but pass the correct active splits count from Java
        GlobalCacheStats nativeStats = getGlobalCacheStatsNative(nativePtr);
        
        // Override the active splits with the correct Java-managed count
        return new GlobalCacheStats(
            nativeStats.getTotalHits(),
            nativeStats.getTotalMisses(), 
            nativeStats.getTotalEvictions(),
            nativeStats.getCurrentSize(),
            nativeStats.getMaxSize(),
            managedSearchers.size() // Use Java-managed count
        );
    }
    
    /**
     * Preload components for a specific split
     */
    public void preloadComponents(String splitPath, Set<SplitSearcher.IndexComponent> components) {
        preloadComponentsNative(nativePtr, splitPath, components);
    }
    
    /**
     * Evict components for a specific split or globally
     */
    public void evictComponents(String splitPath, Set<SplitSearcher.IndexComponent> components) {
        evictComponentsNative(nativePtr, splitPath, components);
    }
    
    /**
     * Force eviction to free up memory
     */
    public void forceEviction(long targetSizeBytes) {
        forceEvictionNative(nativePtr, targetSizeBytes);
    }
    
    @Override
    public void close() {
        // Close all managed searchers
        for (SplitSearcher searcher : managedSearchers.values()) {
            try {
                searcher.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
        managedSearchers.clear();
        
        // Clean up AWS credential tracking
        cleanupAwsCredentialTracking();
        
        // Close native cache manager
        if (nativePtr != 0) {
            closeNativeCacheManager(nativePtr);
        }
        
        // Remove from instances using the cache key
        instances.remove(cacheKey);
    }
    
    /**
     * Manually invalidate all caches using specific AWS credentials.
     * Useful for programmatic credential rotation scenarios.
     * 
     * @param accessKey AWS access key ID
     * @param secretKey AWS secret access key
     * @return Number of caches that were invalidated
     */
    public static int invalidateAwsCredentialCaches(String accessKey, String secretKey) {
        if (accessKey == null || secretKey == null) {
            return 0;
        }
        
        String awsCredentialKey = "aws:" + accessKey + ":" + secretKey;
        List<String> cacheKeys = awsCredentialToCacheKeys.get(awsCredentialKey);
        
        if (cacheKeys == null || cacheKeys.isEmpty()) {
            return 0;
        }
        
        int invalidatedCount = 0;
        List<String> keysToRemove = new ArrayList<>(cacheKeys);
        
        for (String cacheKey : keysToRemove) {
            SplitCacheManager manager = instances.get(cacheKey);
            if (manager != null) {
                System.out.println("🔄 Manually invalidating AWS credential cache: " + cacheKey);
                
                try {
                    manager.close();
                    invalidatedCount++;
                } catch (Exception e) {
                    System.err.println("Error closing cache during manual AWS credential invalidation: " + e.getMessage());
                }
            }
        }
        
        return invalidatedCount;
    }
    
    /**
     * Clean up AWS credential tracking when cache is closed
     */
    private void cleanupAwsCredentialTracking() {
        if (awsConfig.isEmpty()) {
            return;
        }
        
        String accessKey = awsConfig.get("access_key");
        String secretKey = awsConfig.get("secret_key");
        
        if (accessKey == null || secretKey == null) {
            return;
        }
        
        String awsCredentialKey = "aws:" + accessKey + ":" + secretKey;
        List<String> cacheKeys = awsCredentialToCacheKeys.get(awsCredentialKey);
        
        if (cacheKeys != null) {
            cacheKeys.remove(this.cacheKey);
            if (cacheKeys.isEmpty()) {
                awsCredentialToCacheKeys.remove(awsCredentialKey);
            }
        }
    }
    
    // Native method declarations
    private static native long createNativeCacheManager(CacheConfig config);
    private static native void closeNativeCacheManager(long ptr);
    private static native GlobalCacheStats getGlobalCacheStatsNative(long ptr);
    private static native void preloadComponentsNative(long ptr, String splitPath, Set<SplitSearcher.IndexComponent> components);
    private static native void evictComponentsNative(long ptr, String splitPath, Set<SplitSearcher.IndexComponent> components);
    private static native void forceEvictionNative(long ptr, long targetSizeBytes);
    private static native SearchResult searchAcrossAllSplitsNative(long ptr, long queryPtr, int totalLimit);
    private static native SearchResult searchAcrossSplitsNative(long ptr, List<String> splitPaths, long queryPtr, int totalLimit);
    
    // Package-private getters for SplitSearcher
    String getCacheName() { return cacheName; }
    String getCacheKey() { return cacheKey; }
    long getMaxCacheSize() { return maxCacheSize; }
    long getNativePtr() { return nativePtr; }
}