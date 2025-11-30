package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.config.FileSystemConfig;

import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.core.Tantivy;import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    // Stronger thread-safety with explicit synchronization for Spark environments
    private static final Map<String, SplitCacheManager> instances = new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock instancesLock = new ReentrantReadWriteLock();

    // Global singleton for primary cache manager (used when no specific config is needed)
    private static final AtomicReference<SplitCacheManager> GLOBAL_INSTANCE = new AtomicReference<>();

    static {
        Tantivy.initialize();
        // Add shutdown hook to gracefully cleanup all cache instances
        // SAFETY FIX: Use the same lock (instancesLock) as normal access to prevent deadlocks
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            instancesLock.writeLock().lock();
            try {
                for (SplitCacheManager manager : instances.values()) {
                    try {
                        manager.close();
                    } catch (Exception e) {
                        System.err.println("Warning: Failed to close cache manager: " + e.getMessage());
                    }
                }
                instances.clear();
            } finally {
                instancesLock.writeLock().unlock();
            }
        }, "SplitCacheManager-Shutdown"));
    }
    
    private final String cacheName;
    private final String cacheKey; // Full cache key used for storage/retrieval
    private final long maxCacheSize;
    private final Map<String, SplitSearcher> managedSearchers;
    private final AtomicLong totalCacheSize;
    private final long nativePtr;
    private final Map<String, String> awsConfig;
    private final Map<String, String> azureConfig;

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
        private BatchOptimizationConfig batchOptimization = null; // null = use default when needed

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
            this.azureConfig.put("access_key", accountKey);  // Changed from "account_key" to "access_key" for consistency with native layer
            return this;
        }

        public CacheConfig withAzureBearerToken(String accountName, String bearerToken) {
            this.azureConfig.put("account_name", accountName);
            this.azureConfig.put("bearer_token", bearerToken);
            return this;
        }

        public CacheConfig withAzureConnectionString(String connectionString) {
            this.azureConfig.put("connection_string", connectionString);
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

        /**
         * Configure batch retrieval optimization.
         *
         * <p>Batch optimization significantly reduces S3 API calls and improves performance
         * for large document retrievals by consolidating nearby documents into fewer,
         * larger byte range requests.
         *
         * <p><b>Performance Impact:</b>
         * <ul>
         *   <li>1,000 docs: 1,000 S3 requests → 50-100 requests (90-95% reduction)</li>
         *   <li>Latency: 3.4s → 1.5-2.0s (1.7-2.3x faster)</li>
         * </ul>
         *
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * // Use preset configurations
         * config.withBatchOptimization(BatchOptimizationConfig.conservative());
         * config.withBatchOptimization(BatchOptimizationConfig.balanced());
         * config.withBatchOptimization(BatchOptimizationConfig.aggressive());
         *
         * // Or customize
         * BatchOptimizationConfig custom = new BatchOptimizationConfig()
         *     .setMaxRangeSize(8 * 1024 * 1024)  // 8MB ranges
         *     .setGapTolerance(256 * 1024)       // 256KB gap tolerance
         *     .setMinDocsForOptimization(100);    // Activate for 100+ docs
         * config.withBatchOptimization(custom);
         *
         * // Disable optimization
         * config.withBatchOptimization(BatchOptimizationConfig.disabled());
         * }</pre>
         *
         * @param batchOptimization batch optimization configuration
         * @return this CacheConfig for method chaining
         * @see BatchOptimizationConfig
         */
        public CacheConfig withBatchOptimization(BatchOptimizationConfig batchOptimization) {
            if (batchOptimization != null) {
                batchOptimization.validate(); // Validate configuration
            }
            this.batchOptimization = batchOptimization;
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
         * Gets the batch optimization configuration.
         *
         * @return batch optimization config, or null if using defaults
         */
        public BatchOptimizationConfig getBatchOptimization() {
            return batchOptimization;
        }
        
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
        this.azureConfig = new HashMap<>(config.getAzureConfig());
        this.nativePtr = createNativeCacheManager(config);
    }
    
    /**
     * Get or create a shared cache manager instance with stronger Spark-safe singleton guarantees
     */
    public static SplitCacheManager getInstance(CacheConfig config) {
        String cacheKey = config.getCacheKey();

        // First, try fast read path (most common case - instance already exists)
        instancesLock.readLock().lock();
        try {
            SplitCacheManager existing = instances.get(cacheKey);
            if (existing != null) {
                return existing;
            }
        } finally {
            instancesLock.readLock().unlock();
        }

        // Instance doesn't exist, acquire write lock for creation
        instancesLock.writeLock().lock();
        try {
            // Double-check pattern - another thread might have created it while we were waiting
            SplitCacheManager existing = instances.get(cacheKey);
            if (existing != null) {
                return existing;
            }

            // Create new instance with validation
            validateCacheConfig(config);
            SplitCacheManager newInstance = new SplitCacheManager(config);
            instances.put(cacheKey, newInstance);

            // Set as global instance if this is the first one
            GLOBAL_INSTANCE.compareAndSet(null, newInstance);

            return newInstance;
        } finally {
            instancesLock.writeLock().unlock();
        }
    }

    /**
     * Get the global cache manager instance, creating a default one if needed
     * This is useful for simple use cases that don't need specific configuration
     */
    public static SplitCacheManager getGlobalInstance() {
        SplitCacheManager existing = GLOBAL_INSTANCE.get();
        if (existing != null) {
            return existing;
        }

        // Create default instance if none exists
        CacheConfig defaultConfig = new CacheConfig("global-default")
            .withMaxCacheSize(500_000_000); // 500MB default
        return getInstance(defaultConfig);
    }
    
    /**
     * Get all active SplitCacheManager instances.
     * This method is used by GlobalCacheUtils to perform system-wide cache operations.
     * 
     * @return Map of cache keys to SplitCacheManager instances
     */
    public static Map<String, SplitCacheManager> getAllInstances() {
        return new HashMap<>(instances);
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
        if (!this.awsConfig.isEmpty()) {
            splitConfig.put("aws_config", this.awsConfig);
        }
        if (!this.azureConfig.isEmpty()) {
            splitConfig.put("azure_config", this.azureConfig);
        }

        // Extract specific values for native layer compatibility (based on commit b53bf9d)
        splitConfig.put("footer_start_offset", metadata.getFooterStartOffset());
        splitConfig.put("footer_end_offset", metadata.getFooterEndOffset());

        // CRITICAL: Extract docMappingUid and pass as "doc_mapping_uid" key for Quickwit compatibility
        if (metadata.getDocMappingUid() != null && !metadata.getDocMappingUid().trim().isEmpty()) {
            splitConfig.put("doc_mapping_uid", metadata.getDocMappingUid());
        }

        // CRITICAL: Extract docMappingJson and pass as "doc_mapping" key for tokenization performance
        if (metadata.getDocMappingJson() != null && !metadata.getDocMappingJson().trim().isEmpty()) {
            splitConfig.put("doc_mapping", metadata.getDocMappingJson());
        }

        // Pass the entire metadata object to the native layer
        splitConfig.put("split_metadata", metadata);
        
        // Resolve split path through file system root if it's a local path
        String resolvedSplitPath = splitPath;
        if (!splitPath.contains("://")) {
            resolvedSplitPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitPath) : splitPath;
        }

        // Create searcher with optimized configuration
        SplitSearcher searcher = new SplitSearcher(resolvedSplitPath, this, splitConfig);
        managedSearchers.put(resolvedSplitPath, searcher);
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
    
    /**
     * Flush all caches across the entire caching system.
     * This performs a comprehensive cache clear including:
     * - LeafSearchCache (query result cache)
     * - ByteRangeCache (storage byte range cache) 
     * - ComponentCache (fast fields, postings, etc.)
     * - All searcher-specific caches
     * 
     * @return CacheFlushStats containing information about what was flushed
     */
    public CacheFlushStats flushAllCaches() {
        GlobalCacheStats beforeStats = getGlobalCacheStats();
        
        // Force complete eviction (target size = 0 means flush everything)
        forceEvictionNative(nativePtr, 0);
        
        // Evict all components for all managed splits
        Set<SplitSearcher.IndexComponent> allComponents = EnumSet.allOf(SplitSearcher.IndexComponent.class);
        for (String splitPath : managedSearchers.keySet()) {
            evictComponents(splitPath, allComponents);
        }
        
        GlobalCacheStats afterStats = getGlobalCacheStats();
        
        return new CacheFlushStats(beforeStats, afterStats, managedSearchers.size());
    }
    
    /**
     * Flush caches for a specific split only.
     * 
     * @param splitPath Path to the split file
     * @return true if the split was found and caches were flushed, false if split not managed by this cache manager
     */
    public boolean flushSplitCaches(String splitPath) {
        if (!managedSearchers.containsKey(splitPath)) {
            return false;
        }
        
        // Evict all components for this specific split
        Set<SplitSearcher.IndexComponent> allComponents = EnumSet.allOf(SplitSearcher.IndexComponent.class);
        evictComponents(splitPath, allComponents);
        
        return true;
    }
    
    /**
     * Flush specific cache types across all splits.
     * 
     * @param cacheTypes Set of cache types to flush
     */
    public void flushCacheTypes(Set<CacheType> cacheTypes) {
        if (cacheTypes.contains(CacheType.LEAF_SEARCH) || cacheTypes.contains(CacheType.BYTE_RANGE)) {
            // These are global caches, force partial eviction
            forceEvictionNative(nativePtr, getGlobalCacheStats().getCurrentSize() / 2);
        }
        
        if (cacheTypes.contains(CacheType.COMPONENT)) {
            // Evict all components for all splits
            Set<SplitSearcher.IndexComponent> allComponents = EnumSet.allOf(SplitSearcher.IndexComponent.class);
            for (String splitPath : managedSearchers.keySet()) {
                evictComponents(splitPath, allComponents);
            }
        }
    }
    
    /**
     * Statistics about cache flushing operation
     */
    public static class CacheFlushStats {
        private final long bytesFreedTotal;
        private final long itemsEvicted;
        private final int splitsAffected;
        private final GlobalCacheStats beforeStats;
        private final GlobalCacheStats afterStats;
        
        public CacheFlushStats(GlobalCacheStats beforeStats, GlobalCacheStats afterStats, int splitsAffected) {
            this.beforeStats = beforeStats;
            this.afterStats = afterStats;
            this.splitsAffected = splitsAffected;
            this.bytesFreedTotal = beforeStats.getCurrentSize() - afterStats.getCurrentSize();
            this.itemsEvicted = (beforeStats.getTotalHits() + beforeStats.getTotalMisses()) - 
                               (afterStats.getTotalHits() + afterStats.getTotalMisses());
        }
        
        public long getBytesFreed() { return bytesFreedTotal; }
        public long getItemsEvicted() { return itemsEvicted; }
        public int getSplitsAffected() { return splitsAffected; }
        public GlobalCacheStats getBeforeStats() { return beforeStats; }
        public GlobalCacheStats getAfterStats() { return afterStats; }
        
        @Override
        public String toString() {
            return String.format("CacheFlushStats{bytesFreed=%d, itemsEvicted=%d, splitsAffected=%d, " +
                               "beforeSize=%d, afterSize=%d}", 
                               bytesFreedTotal, itemsEvicted, splitsAffected, 
                               beforeStats.getCurrentSize(), afterStats.getCurrentSize());
        }
    }
    
    /**
     * Cache types that can be selectively flushed
     */
    public enum CacheType {
        /** Query result cache (per split_id + query) */
        LEAF_SEARCH,
        /** Storage byte range cache (per file_path + range) */  
        BYTE_RANGE,
        /** Index component cache (fast fields, postings, etc.) */
        COMPONENT
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

        // Check for invalid footer range
        if (footerEnd <= 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset must be > 0, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerOffsets=%d-%d",
                splitPath, footerEnd, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd
            ));
        }

        if (footerEnd <= footerStart) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset (%d) must be > footer_start_offset (%d). " +
                "Split metadata: splitId='%s', numDocs=%d, footerOffsets=%d-%d",
                splitPath, footerEnd, footerStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd
            ));
        }

        // Check for negative values (which shouldn't be possible with long, but can indicate overflow)
        if (footerStart < 0) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_start_offset cannot be negative, got %d. " +
                "Split metadata: splitId='%s', numDocs=%d, footerOffsets=%d-%d",
                splitPath, footerStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd
            ));
        }

        // Check for unreasonably large values (> 100GB) that might indicate corrupted metadata
        final long MAX_REASONABLE_SIZE = 100L * 1024 * 1024 * 1024; // 100GB

        if (footerEnd > MAX_REASONABLE_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_end_offset (%d) is unreasonably large (> 100GB), likely corrupted metadata. " +
                "Split metadata: splitId='%s', numDocs=%d, footerOffsets=%d-%d",
                splitPath, footerEnd, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd
            ));
        }

        if (footerStart > MAX_REASONABLE_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Invalid split metadata for '%s': footer_start_offset (%d) is unreasonably large (> 100GB), likely corrupted metadata. " +
                "Split metadata: splitId='%s', numDocs=%d, footerOffsets=%d-%d",
                splitPath, footerStart, metadata.getSplitId(), metadata.getNumDocs(),
                footerStart, footerEnd
            ));
        }
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

    // Batch optimization metrics native methods
    static native long nativeGetBatchMetricsTotalOperations();
    static native long nativeGetBatchMetricsTotalDocuments();
    static native long nativeGetBatchMetricsTotalRequests();
    static native long nativeGetBatchMetricsConsolidatedRequests();
    static native long nativeGetBatchMetricsBytesTransferred();
    static native long nativeGetBatchMetricsBytesWasted();
    static native long nativeGetBatchMetricsTotalPrefetchDuration();
    static native long nativeGetBatchMetricsSegmentsProcessed();
    static native double nativeGetBatchMetricsConsolidationRatio();
    static native double nativeGetBatchMetricsCostSavingsPercent();
    static native double nativeGetBatchMetricsEfficiencyPercent();
    static native void nativeResetBatchMetrics();

    // ========================================
    // Searcher Cache Statistics Native Methods
    // ========================================
    static native long nativeGetSearcherCacheHits();
    static native long nativeGetSearcherCacheMisses();
    static native long nativeGetSearcherCacheEvictions();
    static native void nativeResetSearcherCacheStats();

    // ========================================
    // Object Storage Request Statistics Native Methods
    // ========================================
    static native long nativeGetObjectStorageRequestCount();
    static native long nativeGetObjectStorageBytesFetched();
    static native void nativeResetObjectStorageRequestStats();

    /**
     * Get global batch optimization metrics.
     *
     * <p>Returns cumulative statistics about batch retrieval optimization across all
     * SplitCacheManager instances. This provides visibility into cost savings,
     * efficiency, and performance.
     *
     * <p><strong>Example Usage:</strong>
     * <pre>{@code
     * BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();
     * System.out.println("S3 Cost Savings: " + metrics.getCostSavingsPercent() + "%");
     * System.out.println("Consolidation Ratio: " + metrics.getConsolidationRatio() + "x");
     * System.out.println(metrics.getSummary()); // Detailed multi-line summary
     * }</pre>
     *
     * @return snapshot of current batch optimization metrics
     * @see BatchOptimizationMetrics
     * @see #resetBatchMetrics()
     */
    public static BatchOptimizationMetrics getBatchMetrics() {
        return new BatchOptimizationMetrics();
    }

    /**
     * Get searcher cache statistics.
     *
     * <p>The searcher cache is an LRU cache (default size: 1000 entries) that stores
     * Tantivy searcher objects for efficient document retrieval. This method provides
     * visibility into cache performance.
     *
     * <p><strong>Example Usage:</strong>
     * <pre>{@code
     * SearcherCacheStats stats = SplitCacheManager.getSearcherCacheStats();
     * System.out.println("Cache Hit Rate: " + stats.getHitRate() + "%");
     * System.out.println("Total Hits: " + stats.getHits());
     * System.out.println("Total Misses: " + stats.getMisses());
     * System.out.println("Total Evictions: " + stats.getEvictions());
     * }</pre>
     *
     * @return snapshot of current searcher cache statistics
     * @see #resetSearcherCacheStats()
     */
    public static SearcherCacheStats getSearcherCacheStats() {
        long hits = nativeGetSearcherCacheHits();
        long misses = nativeGetSearcherCacheMisses();
        long evictions = nativeGetSearcherCacheEvictions();
        return new SearcherCacheStats(hits, misses, evictions);
    }

    /**
     * Reset searcher cache statistics to zero.
     *
     * <p>Useful for monitoring cache performance over specific time periods or
     * after configuration changes.
     *
     * <p><strong>Note:</strong> This only resets statistics counters, not the cache itself.
     * Cached searchers remain in the cache.
     */
    public static void resetSearcherCacheStats() {
        nativeResetSearcherCacheStats();
    }

    /**
     * Searcher cache performance statistics.
     *
     * <p>Tracks hits, misses, and evictions for the LRU searcher cache used in
     * document retrieval operations.
     */
    public static class SearcherCacheStats {
        private final long hits;
        private final long misses;
        private final long evictions;

        SearcherCacheStats(long hits, long misses, long evictions) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
        }

        /** Total cache hits (searcher found in cache). */
        public long getHits() { return hits; }

        /** Total cache misses (searcher had to be created). */
        public long getMisses() { return misses; }

        /** Total evictions (oldest entries removed due to cache size limit). */
        public long getEvictions() { return evictions; }

        /** Total cache accesses (hits + misses). */
        public long getTotalAccesses() { return hits + misses; }

        /**
         * Cache hit rate as percentage (0-100).
         * Returns 0 if no accesses have been recorded.
         */
        public double getHitRate() {
            long total = getTotalAccesses();
            return total == 0 ? 0.0 : (hits * 100.0 / total);
        }

        @Override
        public String toString() {
            return String.format(
                "SearcherCacheStats{hits=%d, misses=%d, evictions=%d, hitRate=%.1f%%}",
                hits, misses, evictions, getHitRate()
            );
        }
    }

    /**
     * Reset global batch optimization metrics to zero.
     *
     * <p>This is useful for periodic reporting or testing. After resetting,
     * metrics will start accumulating from zero again.
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * // Get current metrics
     * BatchOptimizationMetrics beforeMetrics = SplitCacheManager.getBatchMetrics();
     * System.out.println("Before: " + beforeMetrics.getCostSavingsPercent() + "%");
     *
     * // Reset for new measurement period
     * SplitCacheManager.resetBatchMetrics();
     *
     * // ... do some batch operations ...
     *
     * // Get metrics for this period
     * BatchOptimizationMetrics afterMetrics = SplitCacheManager.getBatchMetrics();
     * System.out.println("This period: " + afterMetrics.getCostSavingsPercent() + "%");
     * }</pre>
     */
    public static void resetBatchMetrics() {
        nativeResetBatchMetrics();
    }

    // ========================================
    // Object Storage Request Statistics API
    // ========================================

    /**
     * Get the total number of object storage get_slice requests made.
     *
     * <p>This is an accurate count from the storage layer that includes ALL object storage
     * requests (S3 and Azure), not just batch optimization requests. This includes:
     * <ul>
     *   <li>Footer/hotcache fetches during searcher initialization</li>
     *   <li>Consolidated store data fetches from batch optimization</li>
     *   <li>Any other storage layer requests</li>
     * </ul>
     *
     * <p><strong>Example Usage:</strong>
     * <pre>{@code
     * SplitCacheManager.resetObjectStorageRequestStats();
     * // ... perform some operations ...
     * long requestCount = SplitCacheManager.getObjectStorageRequestCount();
     * long bytesFetched = SplitCacheManager.getObjectStorageBytesFetched();
     * System.out.println("Object Storage Requests: " + requestCount);
     * System.out.println("Bytes Fetched: " + bytesFetched);
     * }</pre>
     *
     * @return total number of object storage get_slice requests since startup or last reset
     * @see #getObjectStorageBytesFetched()
     * @see #resetObjectStorageRequestStats()
     */
    public static long getObjectStorageRequestCount() {
        return nativeGetObjectStorageRequestCount();
    }

    /**
     * Get the total bytes fetched via object storage get_slice requests.
     *
     * @return total bytes fetched since startup or last reset
     * @see #getObjectStorageRequestCount()
     * @see #resetObjectStorageRequestStats()
     */
    public static long getObjectStorageBytesFetched() {
        return nativeGetObjectStorageBytesFetched();
    }

    /**
     * Reset object storage request statistics.
     *
     * <p>This resets both the request count and bytes fetched counters to zero.
     * Useful for per-operation tracking or testing.
     *
     * @see #getObjectStorageRequestCount()
     * @see #getObjectStorageBytesFetched()
     */
    public static void resetObjectStorageRequestStats() {
        nativeResetObjectStorageRequestStats();
    }

    // Package-private getters for SplitSearcher
    public String getCacheName() { return cacheName; }
    String getCacheKey() { return cacheKey; }
    long getMaxCacheSize() { return maxCacheSize; }
    long getNativePtr() { return nativePtr; }
}
