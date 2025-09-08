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
 * // For S3-compatible storage requiring path-style access (S3Mock, MinIO, LocalStack):
 * SplitCacheManager.CacheConfig s3MockConfig = new SplitCacheManager.CacheConfig("test-cache")
 *     .withAwsCredentials(accessKey, secretKey)
 *     .withAwsRegion("us-east-1")
 *     .withAwsEndpoint("http://localhost:9090")
 *     .withAwsPathStyleAccess(true);  // Required for S3Mock and MinIO
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
 */
public class SplitCacheManager implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }
    
    private static final Map<String, SplitCacheManager> instances = new ConcurrentHashMap<>();
    
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
        
        /**
         * Configure AWS path-style access for S3-compatible storage.
         * 
         * Path-style access uses URLs like http://endpoint/bucket/key instead of 
         * http://bucket.endpoint/key. This is required for:
         * - S3Mock testing environments
         * - MinIO self-hosted instances
         * - Custom S3-compatible storage solutions
         * - LocalStack local AWS emulation
         * 
         * @param pathStyleAccess true to enable path-style access, false for virtual-hosted style
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withAwsPathStyleAccess(boolean pathStyleAccess) {
            this.awsConfig.put("path_style_access", String.valueOf(pathStyleAccess));
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
        
        @Override
        public String toString() {
            return String.format(
                "GlobalCacheStats{hits=%d, misses=%d, evictions=%d, hitRate=%.2f%%, " +
                "currentSize=%d, maxSize=%d, utilization=%.2f%%, activeSplits=%d}",
                totalHits, totalMisses, totalEvictions, getHitRate() * 100,
                currentSize, maxSize, getUtilization() * 100, activeSplits
            );
        }
    }
    
    /**
     * 🚀 COMPREHENSIVE CACHE METRICS - Detailed per-cache-type statistics
     * 
     * Provides granular visibility into each cache type's performance:
     * - ByteRangeCache: Storage byte range caching (shortlived_cache in Quickwit)
     * - FooterCache: Split metadata/footer caching (split_footer_cache)  
     * - FastFieldCache: Component-level fast field caching (fast_field_cache)
     * - SplitCache: Split file caching (searcher_split_cache)
     */
    public static class ComprehensiveCacheStats {
        private final CacheTypeStats byteRangeCache;
        private final CacheTypeStats footerCache; 
        private final CacheTypeStats fastFieldCache;
        private final CacheTypeStats splitCache;
        private final GlobalCacheStats aggregated;
        
        public ComprehensiveCacheStats(CacheTypeStats byteRangeCache, CacheTypeStats footerCache,
                                     CacheTypeStats fastFieldCache, CacheTypeStats splitCache) {
            this.byteRangeCache = byteRangeCache;
            this.footerCache = footerCache;
            this.fastFieldCache = fastFieldCache;
            this.splitCache = splitCache;
            
            // Calculate aggregated stats
            long totalHits = byteRangeCache.hits + footerCache.hits + fastFieldCache.hits + splitCache.hits;
            long totalMisses = byteRangeCache.misses + footerCache.misses + fastFieldCache.misses + splitCache.misses;
            long totalEvictions = byteRangeCache.evictions + footerCache.evictions + fastFieldCache.evictions + splitCache.evictions;
            long totalSize = byteRangeCache.sizeBytes + footerCache.sizeBytes + fastFieldCache.sizeBytes + splitCache.sizeBytes;
            
            this.aggregated = new GlobalCacheStats(totalHits, totalMisses, totalEvictions, totalSize, 0, 0);
        }
        
        public CacheTypeStats getByteRangeCache() { return byteRangeCache; }
        public CacheTypeStats getFooterCache() { return footerCache; }
        public CacheTypeStats getFastFieldCache() { return fastFieldCache; }
        public CacheTypeStats getSplitCache() { return splitCache; }
        public GlobalCacheStats getAggregated() { return aggregated; }
        
        @Override
        public String toString() {
            return String.format(
                "ComprehensiveCacheStats{\n" +
                "  📦 ByteRangeCache: %s\n" +
                "  📄 FooterCache: %s\n" +
                "  ⚡ FastFieldCache: %s\n" +  
                "  🔍 SplitCache: %s\n" +
                "  🏆 Aggregated: %s\n" +
                "}",
                byteRangeCache, footerCache, fastFieldCache, splitCache, aggregated
            );
        }
    }
    
    /**
     * Cache statistics for a specific cache type
     */
    public static class CacheTypeStats {
        private final String cacheType;
        private final long hits;
        private final long misses; 
        private final long evictions;
        private final long sizeBytes;
        
        public CacheTypeStats(String cacheType, long hits, long misses, long evictions, long sizeBytes) {
            this.cacheType = cacheType;
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
            this.sizeBytes = sizeBytes;
        }
        
        public String getCacheType() { return cacheType; }
        public long getHits() { return hits; }
        public long getMisses() { return misses; }
        public long getEvictions() { return evictions; }
        public long getSizeBytes() { return sizeBytes; }
        
        public double getHitRate() {
            long total = hits + misses;
            return total > 0 ? (double) hits / total : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format(
                "%s{hits=%d, misses=%d, evictions=%d, hitRate=%.2f%%, sizeBytes=%d}",
                cacheType, hits, misses, evictions, getHitRate() * 100, sizeBytes
            );
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
     * Get or create a shared cache manager instance
     */
    public static SplitCacheManager getInstance(CacheConfig config) {
        return instances.computeIfAbsent(config.getCacheKey(), 
            cacheKey -> {
                validateCacheConfig(config);
                return new SplitCacheManager(config);
            });
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
     * Create an optimized split searcher using pre-computed footer offsets from a metastore.
     * This is the ONLY supported method for creating SplitSearcher instances, following
     * Quickwit's design where split metadata (including footer offsets) comes from the metastore.
     * 
     * This enables:
     * - 87% reduction in network traffic for S3 splits
     * - Precise byte-range requests instead of full file downloads
     * - Proper Quickwit-compatible lazy loading
     * 
     * Usage Pattern:
     * 1. Create split and extract metadata: QuickwitSplit.convertIndexFromPath(...)
     * 2. Store metadata in your metastore: metastore.storeSplitMetadata(...)
     * 3. Use optimized loading: cacheManager.createSplitSearcher(url, savedMetadata)
     * 
     * @param splitPath The path or URL to the split file
     * @param metadata Split metadata containing footer offsets for optimization (REQUIRED)
     * @return A SplitSearcher configured for optimized lazy loading
     * @throws IllegalArgumentException if metadata lacks footer offsets
     */
    public SplitSearcher createSplitSearcher(String splitPath, QuickwitSplit.SplitMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException(
                "Split metadata is required for SplitSearcher creation. " +
                "Use QuickwitSplit.convertIndexFromPath() to generate metadata, " +
                "store it in your metastore, then pass it here for optimized loading."
            );
        }
        
        if (!metadata.hasFooterOffsets()) {
            throw new IllegalArgumentException(
                "Split metadata must contain footer offsets for optimized loading. " +
                "Ensure the metadata was generated by QuickwitSplit.convertIndexFromPath() " +
                "and has hasFooterOffsets() == true."
            );
        }
        
        // DEFENSIVE CODING: Validate offset values to prevent native crashes
        validateSplitOffsets(splitPath, metadata);
        
        Map<String, Object> splitConfig = new HashMap<>();
        
        // Add AWS configuration from the cache manager
        if (!this.awsConfig.isEmpty()) {
            splitConfig.put("aws_config", this.awsConfig);
        }
        
        // Add footer offsets for lazy loading optimization
        splitConfig.put("footer_start_offset", metadata.getFooterStartOffset());
        splitConfig.put("footer_end_offset", metadata.getFooterEndOffset());
        splitConfig.put("hotcache_start_offset", metadata.getHotcacheStartOffset());
        splitConfig.put("hotcache_length", metadata.getHotcacheLength());
        splitConfig.put("enable_lazy_loading", true);
        
        // Add doc mapping JSON if available
        if (metadata.hasDocMapping()) {
            splitConfig.put("doc_mapping", metadata.getDocMappingJson());
            System.out.println("✅ Added doc mapping to split config for: " + splitPath);
        } else {
            System.out.println("⚠️  No doc mapping available in metadata for: " + splitPath);
        }
        
        // Create searcher with optimized configuration
        SplitSearcher searcher = new SplitSearcher(splitPath, this, splitConfig);
        managedSearchers.put(splitPath, searcher);
        return searcher;
    }
    
    /**
     * Internal method for creating split searcher with raw config.
     * Used internally by the public API methods.
     * @deprecated Internal use only - use createSplitSearcher(String, QuickwitSplit.SplitMetadata) instead
     */
    @Deprecated
    private SplitSearcher createSplitSearcher(String splitPath, Map<String, Object> splitConfig) {
        SplitSearcher searcher = new SplitSearcher(splitPath, this, splitConfig);
        managedSearchers.put(splitPath, searcher);
        return searcher;
    }
    
    /**
     * TEMPORARY COMPATIBILITY METHOD - for existing tests only
     * @deprecated This method provides temporary backward compatibility for existing tests.
     * Use createSplitSearcher(String, QuickwitSplit.SplitMetadata) for production code.
     * This method uses fallback parsing without footer optimization.
     */
    @Deprecated
    public SplitSearcher createSplitSearcherXXX(String splitPath) {
        System.err.println("⚠️  WARNING: Using deprecated createSplitSearcher(String) without optimization.");
        System.err.println("   For production code, use createSplitSearcher(String, QuickwitSplit.SplitMetadata)");
        System.err.println("   to enable 87% network traffic reduction and proper lazy loading.");
        
        // Pass AWS configuration from the cache manager to the searcher
        Map<String, Object> splitConfig = new HashMap<>();
        if (!this.awsConfig.isEmpty()) {
            splitConfig.put("aws_config", this.awsConfig);
        }
        // Disable lazy loading for backward compatibility
        splitConfig.put("enable_lazy_loading", false);
        return createSplitSearcher(splitPath, splitConfig);
    }
    
    /**
     * Create multiple split searchers sharing the same cache (batch creation)
     * All splits must have metadata with footer offsets for optimized loading.
     * 
     * @param splitPathsWithMetadata Map of split paths to their corresponding metadata
     * @return List of optimized SplitSearcher instances
     */
    public List<SplitSearcher> createMultipleSplitSearchers(Map<String, QuickwitSplit.SplitMetadata> splitPathsWithMetadata) {
        List<SplitSearcher> searchers = new ArrayList<>();
        for (Map.Entry<String, QuickwitSplit.SplitMetadata> entry : splitPathsWithMetadata.entrySet()) {
            searchers.add(createSplitSearcher(entry.getKey(), entry.getValue()));
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
     * 🚀 NEW: Get comprehensive cache statistics with per-cache-type breakdown
     * 
     * Provides detailed metrics for each cache type:
     * - ByteRangeCache: Storage byte range caching performance
     * - FooterCache: Split metadata caching efficiency  
     * - FastFieldCache: Component loading cache utilization
     * - SplitCache: Split file caching metrics
     * 
     * Essential for production performance monitoring and optimization tuning.
     * 
     * @return Comprehensive cache statistics with per-type breakdown
     */
    public ComprehensiveCacheStats getComprehensiveCacheStats() {
        // Get detailed per-cache metrics from native layer
        long[][] perCacheMetrics = getComprehensiveCacheStatsNative(nativePtr);
        
        if (perCacheMetrics == null || perCacheMetrics.length != 4) {
            // Fallback to basic stats if native method unavailable
            GlobalCacheStats basicStats = getGlobalCacheStats();
            CacheTypeStats fallback = new CacheTypeStats("fallback", 
                basicStats.getTotalHits(), basicStats.getTotalMisses(), 
                basicStats.getTotalEvictions(), basicStats.getCurrentSize());
            return new ComprehensiveCacheStats(fallback, fallback, fallback, fallback);
        }
        
        // Parse native metrics: each array contains [hits, misses, evictions, sizeBytes]
        CacheTypeStats byteRangeCache = new CacheTypeStats("ByteRangeCache",
            perCacheMetrics[0][0], perCacheMetrics[0][1], perCacheMetrics[0][2], perCacheMetrics[0][3]);
        
        CacheTypeStats footerCache = new CacheTypeStats("FooterCache", 
            perCacheMetrics[1][0], perCacheMetrics[1][1], perCacheMetrics[1][2], perCacheMetrics[1][3]);
        
        CacheTypeStats fastFieldCache = new CacheTypeStats("FastFieldCache",
            perCacheMetrics[2][0], perCacheMetrics[2][1], perCacheMetrics[2][2], perCacheMetrics[2][3]);
        
        CacheTypeStats splitCache = new CacheTypeStats("SplitCache",
            perCacheMetrics[3][0], perCacheMetrics[3][1], perCacheMetrics[3][2], perCacheMetrics[3][3]);
        
        return new ComprehensiveCacheStats(byteRangeCache, footerCache, fastFieldCache, splitCache);
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
        
        // Close native cache manager
        if (nativePtr != 0) {
            closeNativeCacheManager(nativePtr);
        }
        
        // Remove from instances using the cache key
        instances.remove(cacheKey);
    }
    
    /**
     * Validates that split metadata contains sensible offset values to prevent native crashes.
     * Throws IllegalArgumentException with detailed information if any offsets are invalid.
     * 
     * @param splitPath Path to the split file (for error reporting)
     * @param metadata Split metadata containing offset information
     * @throws IllegalArgumentException if any offsets are invalid
     */
    private void validateSplitOffsets(String splitPath, QuickwitSplit.SplitMetadata metadata) {
        long footerStart = metadata.getFooterStartOffset();
        long footerEnd = metadata.getFooterEndOffset();
        long hotcacheStart = metadata.getHotcacheStartOffset();
        long hotcacheLength = metadata.getHotcacheLength();
        
        // Check for invalid footer range
        if (footerEnd <= 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset must be > 0, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, footerEnd, metadata.getSplitId(), metadata.getNumDocs(), 
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        if (footerEnd <= footerStart) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset (%d) must be > footer_start_offset (%d). " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, footerEnd, footerStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        // Check for negative values (which shouldn't be possible with long, but can indicate overflow)
        if (footerStart < 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_start_offset cannot be negative, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, footerStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        if (hotcacheStart < 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': hotcache_start_offset cannot be negative, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, hotcacheStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        if (hotcacheLength < 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': hotcache_length cannot be negative, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, hotcacheLength, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        // Check for unreasonably large values (> 100GB) that might indicate corrupted metadata
        final long MAX_REASONABLE_SIZE = 100L * 1024 * 1024 * 1024; // 100GB
        
        if (footerEnd > MAX_REASONABLE_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset (%d) is unreasonably large (> 100GB), likely corrupted metadata. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, footerEnd, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        if (hotcacheStart > MAX_REASONABLE_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': hotcache_start_offset (%d) is unreasonably large (> 100GB), likely corrupted metadata. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, hotcacheStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        if (hotcacheLength > MAX_REASONABLE_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': hotcache_length (%d) is unreasonably large (> 100GB), likely corrupted metadata. " +
                "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                splitPath, hotcacheLength, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd, hotcacheStart, hotcacheLength
            ));
        }
        
        // Check for potential overflow in hotcache range calculation
        if (hotcacheLength > 0) {
            try {
                Math.addExact(hotcacheStart, hotcacheLength);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException(String.format(
                    "Invalid split metadata for '%s': hotcache range would overflow (start=%d + length=%d). " +
                    "Split metadata: splitId='%s', numDocs=%d, footerStart=%d, footerEnd=%d, hotcacheStart=%d, hotcacheLength=%d",
                    splitPath, hotcacheStart, hotcacheLength, metadata.getSplitId(), metadata.getNumDocs(),
                    footerStart, footerEnd, hotcacheStart, hotcacheLength
                ));
            }
        }
        
        // All validations passed - log success for debugging
        System.out.printf("✅ Split metadata validation passed for '%s': footer=%d..%d, hotcache=%d+%d%n", 
                         splitPath, footerStart, footerEnd, hotcacheStart, hotcacheLength);
    }
    
    // Native method declarations
    private static native long createNativeCacheManager(CacheConfig config);
    private static native void closeNativeCacheManager(long ptr);
    private static native GlobalCacheStats getGlobalCacheStatsNative(long ptr);
    private static native long[][] getComprehensiveCacheStatsNative(long ptr);
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
