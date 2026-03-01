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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (instances) {
                for (SplitCacheManager manager : instances.values()) {
                    try {
                        manager.close();
                    } catch (Exception e) {
                        System.err.println("Warning: Failed to close cache manager: " + e.getMessage());
                    }
                }
                instances.clear();
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
    private final BatchOptimizationConfig batchOptimization;
    private final String parquetTableRoot;
    private final ParquetCompanionConfig.ParquetStorageConfig parquetStorageConfig;
    private final long coalesceMaxGapBytes;

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
        private String parquetTableRoot = null;
        private ParquetCompanionConfig.ParquetStorageConfig parquetStorageConfig = null;
        private long coalesceMaxGapBytes = 0; // 0 = use Rust default (512KB)

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

        /**
         * Configure the table root path for parquet companion mode.
         * This is where parquet files are located (local path, s3://, or azure://).
         * Decoupled from the split — the same split can be used with parquet files
         * at different locations.
         *
         * @param tableRoot root path for parquet files (e.g., "s3://bucket/tables/events/")
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withParquetTableRoot(String tableRoot) {
            this.parquetTableRoot = tableRoot;
            return this;
        }

        /**
         * Configure separate storage credentials for accessing parquet files.
         * If not set, the cache manager's AWS/Azure credentials are used as fallback.
         *
         * @param config parquet storage credentials
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withParquetStorage(ParquetCompanionConfig.ParquetStorageConfig config) {
            this.parquetStorageConfig = config;
            return this;
        }

        /**
         * Set the maximum gap (in bytes) for parquet byte-range coalescing.
         * Controls how nearby byte ranges are merged into single fetch requests.
         * <p>
         * Default (0): uses Rust default of 512KB, good for cloud storage.
         * For projected reads (few columns out of many), a smaller value like
         * 64KB reduces over-fetch at the cost of more round-trips.
         *
         * @param maxGapBytes maximum gap in bytes, or 0 for Rust default
         * @return this CacheConfig for method chaining
         */
        public CacheConfig withCoalesceMaxGap(long maxGapBytes) {
            this.coalesceMaxGapBytes = maxGapBytes;
            return this;
        }

        public long getCoalesceMaxGapBytes() { return coalesceMaxGapBytes; }

        public String getParquetTableRoot() { return parquetTableRoot; }
        public ParquetCompanionConfig.ParquetStorageConfig getParquetStorageConfig() { return parquetStorageConfig; }

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

        // Tiered disk cache configuration
        private TieredCacheConfig tieredCacheConfig = null;

        /**
         * Configure L2 tiered disk cache for persistent caching.
         *
         * <p>The tiered disk cache provides a second-level persistent cache on local disk
         * (typically NVMe) that survives process restarts and provides faster access than
         * remote storage (S3/Azure).
         *
         * <p><b>Cache Hierarchy:</b>
         * <ul>
         *   <li>L1: In-memory cache (fast, ephemeral)</li>
         *   <li>L2: Disk cache (configured here, persistent)</li>
         *   <li>L3: Remote storage (S3/Azure, slowest)</li>
         * </ul>
         *
         * <p><b>Usage Example:</b>
         * <pre>{@code
         * CacheConfig config = new CacheConfig("prod-cache")
         *     .withMaxCacheSize(500_000_000)  // 500MB L1 memory cache
         *     .withTieredCache(new TieredCacheConfig()
         *         .withDiskCachePath("/mnt/nvme/cache")
         *         .withMaxDiskSize(100_000_000_000L)  // 100GB disk cache
         *         .withCompression(CompressionAlgorithm.LZ4));  // Fast compression
         * }</pre>
         *
         * @param tieredCacheConfig the tiered cache configuration
         * @return this CacheConfig for method chaining
         * @see TieredCacheConfig
         * @see CompressionAlgorithm
         */
        public CacheConfig withTieredCache(TieredCacheConfig tieredCacheConfig) {
            this.tieredCacheConfig = tieredCacheConfig;
            return this;
        }

        /**
         * Gets the tiered cache configuration.
         *
         * @return tiered cache config, or null if not configured
         */
        public TieredCacheConfig getTieredCacheConfig() {
            return tieredCacheConfig;
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
            
            // Note: parquetTableRoot and parquetStorageConfig are intentionally excluded
            // from the cache key. They are per-split parameters passed to createSplitSearcher()
            // and do not affect the identity of the shared cache manager instance.

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
     * Compression algorithm for L2 disk cache.
     *
     * <p>Determines how cached data is compressed on disk:
     * <ul>
     *   <li>{@link #NONE} - No compression, fastest I/O but largest disk usage</li>
     *   <li>{@link #LZ4} - Fast compression (~400 MB/s), good compression ratio (default)</li>
     *   <li>{@link #ZSTD} - Better compression, slower (falls back to LZ4 currently)</li>
     * </ul>
     *
     * <p>The cache uses intelligent compression decisions based on component type:
     * <ul>
     *   <li>Small data (&lt;4KB): Never compressed (overhead exceeds benefit)</li>
     *   <li>Hot data (footer, metadata): Never compressed (CPU cost not worth it)</li>
     *   <li>Large components (.term, .idx, .pos): Always compressed (50-70% savings)</li>
     * </ul>
     */
    public enum CompressionAlgorithm {
        /** No compression - use for already-compressed or small data */
        NONE,
        /** LZ4 compression - fast, good for index data (default) */
        LZ4,
        /** Zstd compression - better ratio, slower (currently falls back to LZ4) */
        ZSTD
    }

    /**
     * Configuration for L2 tiered disk cache.
     *
     * <p>Provides persistent disk caching with intelligent compression for Quickwit split
     * components. The disk cache sits between the L1 memory cache and remote storage (S3/Azure).
     *
     * <p><b>Features:</b>
     * <ul>
     *   <li>Persistent across restarts - survives JVM shutdown</li>
     *   <li>Intelligent compression - LZ4 for large components, skips small/hot data</li>
     *   <li>Split-level LRU eviction - removes entire splits when disk space is needed</li>
     *   <li>Crash-safe manifest - periodic sync with backup for recovery</li>
     * </ul>
     *
     * <p><b>Directory Structure:</b>
     * <pre>
     * {diskCachePath}/
     *   manifest.json           # Cache manifest with metadata
     *   s3_bucket__hash/        # Storage location directory
     *     split-001/            # Split directory
     *       term_full.cache.lz4 # Compressed term dictionary
     *       idx_0-65536.cache   # Byte-range slice
     *       footer.cache        # Uncompressed footer (hot data)
     * </pre>
     *
     * <p><b>Usage Example:</b>
     * <pre>{@code
     * TieredCacheConfig tieredConfig = new TieredCacheConfig()
     *     .withDiskCachePath("/mnt/nvme/tantivy_cache")
     *     .withMaxDiskSize(100_000_000_000L)  // 100GB
     *     .withCompression(CompressionAlgorithm.LZ4)
     *     .withMinCompressSize(4096);  // Don't compress data < 4KB
     *
     * CacheConfig config = new CacheConfig("prod-cache")
     *     .withMaxCacheSize(500_000_000)  // 500MB L1
     *     .withTieredCache(tieredConfig);  // 100GB L2
     * }</pre>
     */
    /** Write queue backpressure strategy (mutually exclusive). */
    public enum WriteQueueMode {
        /** Bounded sync_channel with N fragment slots (default). */
        FRAGMENT,
        /** Unbounded channel, backpressure by total queued bytes. */
        SIZE_BASED
    }

    public static class TieredCacheConfig {
        private String diskCachePath;
        private long maxDiskSizeBytes = 0;  // 0 = auto (2/3 of available disk space)
        private CompressionAlgorithm compression = CompressionAlgorithm.LZ4;
        private int minCompressSizeBytes = 4096;  // Skip compression below 4KB
        private int manifestSyncIntervalSecs = 30;  // Sync manifest every 30 seconds
        private boolean disableL1Cache = false;  // Debugging: disable L1 ByteRangeCache
        private WriteQueueMode writeQueueMode = WriteQueueMode.FRAGMENT;
        private int writeQueueCapacity = 16;              // used ONLY when mode=FRAGMENT
        private long writeQueueMaxBytes = 2_147_483_648L; // used ONLY when mode=SIZE_BASED
        private boolean dropWritesWhenFull = false;        // query-path writes drop instead of block

        /**
         * Set the disk cache directory path.
         *
         * <p>This directory will contain all cached split components. It should be on
         * fast storage (NVMe SSD recommended) with sufficient space for the cache.
         *
         * @param path absolute path to disk cache directory
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withDiskCachePath(String path) {
            this.diskCachePath = path;
            return this;
        }

        /**
         * Set the maximum disk cache size in bytes.
         *
         * <p>When the cache exceeds this size, the least recently used splits are
         * evicted to make room. If set to 0 (default), the cache will automatically
         * use 2/3 of the available disk space.
         *
         * @param bytes maximum cache size in bytes (0 = auto-detect)
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withMaxDiskSize(long bytes) {
            this.maxDiskSizeBytes = bytes;
            return this;
        }

        /**
         * Set the compression algorithm for cached data.
         *
         * <p>The default is {@link CompressionAlgorithm#LZ4} which provides fast
         * compression (~400 MB/s) with good ratios (50-70% for index data).
         *
         * <p>Note: Compression is only applied to components where it provides benefit.
         * Small data (&lt;4KB), hot data (footer), and already-compact data (numeric fast fields)
         * are not compressed regardless of this setting.
         *
         * @param compression compression algorithm to use
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withCompression(CompressionAlgorithm compression) {
            this.compression = compression;
            return this;
        }

        /**
         * Disable compression entirely.
         *
         * <p>Use this if CPU is limited or if the data is already compressed.
         *
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withoutCompression() {
            this.compression = CompressionAlgorithm.NONE;
            return this;
        }

        /**
         * Set minimum data size for compression.
         *
         * <p>Data smaller than this threshold will not be compressed, as the
         * CPU overhead exceeds the I/O savings. Default is 4KB.
         *
         * @param bytes minimum size in bytes to consider for compression
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withMinCompressSize(int bytes) {
            this.minCompressSizeBytes = bytes;
            return this;
        }

        /**
         * Set the interval for syncing the cache manifest to disk.
         *
         * <p>The manifest tracks all cached components. Periodic sync ensures
         * the cache can be recovered after crashes. Default is 30 seconds.
         * Set to 0 to sync on every write (slower but safest).
         *
         * @param seconds sync interval in seconds (0 = sync every write)
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withManifestSyncInterval(int seconds) {
            this.manifestSyncIntervalSecs = seconds;
            return this;
        }

        /**
         * Disable the L1 ByteRangeCache (in-memory cache) for debugging.
         *
         * <p>When enabled, all storage requests bypass the L1 memory cache and go directly
         * to L2 disk cache or L3 remote storage. This helps debug cache key mismatches
         * between prewarm and query operations.
         *
         * <p><b>Debug Usage:</b>
         * <pre>{@code
         * TieredCacheConfig config = new TieredCacheConfig()
         *     .withDiskCachePath("/tmp/cache")
         *     .withDisableL1Cache(true);  // Bypass memory cache for debugging
         *
         * // Run with TANTIVY4JAVA_DEBUG=1 to see cache key details
         * }</pre>
         *
         * @param disable true to disable L1 cache, false to use normally
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withDisableL1Cache(boolean disable) {
            this.disableL1Cache = disable;
            return this;
        }

        /**
         * Set fragment-based write queue mode with the given capacity.
         *
         * <p>This is the default mode. The write queue uses a bounded channel with N slots.
         * Each slot holds one write request (typically a component ~10MB), so N slots ≈ N*10MB RAM.
         *
         * @param capacity number of fragment slots (default: 16)
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withWriteQueueFragmentCapacity(int capacity) {
            this.writeQueueMode = WriteQueueMode.FRAGMENT;
            this.writeQueueCapacity = capacity;
            return this;
        }

        /**
         * Set size-based write queue mode with the given byte limit.
         *
         * <p>Uses an unbounded channel with backpressure based on total queued bytes.
         * When queued bytes exceed the limit, senders block until the background writer
         * drains enough data. This provides more precise memory control than fragment mode.
         *
         * @param maxBytes maximum total queued bytes before backpressure kicks in (default: 2GB)
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withWriteQueueSizeLimit(long maxBytes) {
            this.writeQueueMode = WriteQueueMode.SIZE_BASED;
            this.writeQueueMaxBytes = maxBytes;
            return this;
        }

        /**
         * Enable dropping query-path writes when the write queue is full.
         *
         * <p>When enabled, query-path cache writes (data fetched during searches) are
         * silently dropped if the write queue is at capacity, instead of blocking the
         * search thread. Prewarm operations always block regardless of this setting.
         *
         * <p>This is useful for latency-sensitive workloads where search responsiveness
         * is more important than disk cache fill rate.
         *
         * @param drop true to drop writes when full, false to block (default: false)
         * @return this TieredCacheConfig for method chaining
         */
        public TieredCacheConfig withDropWritesWhenFull(boolean drop) {
            this.dropWritesWhenFull = drop;
            return this;
        }

        // Getters
        public String getDiskCachePath() { return diskCachePath; }
        public long getMaxDiskSizeBytes() { return maxDiskSizeBytes; }
        public CompressionAlgorithm getCompression() { return compression; }
        public int getMinCompressSizeBytes() { return minCompressSizeBytes; }
        public int getManifestSyncIntervalSecs() { return manifestSyncIntervalSecs; }
        public boolean isDisableL1Cache() { return disableL1Cache; }

        /** @return write queue mode ordinal (0=FRAGMENT, 1=SIZE_BASED) for native layer */
        public int getWriteQueueModeOrdinal() { return writeQueueMode.ordinal(); }
        /** @return fragment slot capacity (only meaningful when mode=FRAGMENT) */
        public int getWriteQueueCapacity() { return writeQueueCapacity; }
        /** @return max queued bytes (only meaningful when mode=SIZE_BASED) */
        public long getWriteQueueMaxBytes() { return writeQueueMaxBytes; }
        /** @return whether query-path writes are dropped when the queue is full */
        public boolean isDropWritesWhenFull() { return dropWritesWhenFull; }

        /**
         * Convert compression algorithm to ordinal for native layer.
         *
         * @return ordinal value (0=None, 1=LZ4, 2=ZSTD)
         */
        public int getCompressionOrdinal() {
            return compression.ordinal();
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
        // Store batch optimization config, using balanced default if not specified
        this.batchOptimization = config.getBatchOptimization() != null
                ? config.getBatchOptimization()
                : BatchOptimizationConfig.balanced();
        this.parquetTableRoot = config.getParquetTableRoot();
        this.parquetStorageConfig = config.getParquetStorageConfig();
        this.coalesceMaxGapBytes = config.getCoalesceMaxGapBytes();
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
    /**
     * Create a SplitSearcher with a per-split parquet table root.
     * This overload is used when different splits reference parquet files at different locations.
     * The per-split table root takes precedence over the CacheConfig-level table root.
     *
     * @param splitPath Path or URI of the split file
     * @param metadata Split metadata with footer offsets
     * @param parquetTableRoot Base path for resolving parquet file locations (overrides CacheConfig setting)
     */
    /**
     * Create a SplitSearcher with per-split parquet table root and storage config overrides.
     * This overload is used when different splits reference parquet files at different locations
     * or need different storage credentials (e.g., cross-account S3 access).
     *
     * <p>Per-split arguments take precedence over CacheConfig-level defaults. Pass {@code null}
     * for either parameter to fall back to the CacheConfig-level setting.
     *
     * @param splitPath Path or URI of the split file
     * @param metadata Split metadata with footer offsets
     * @param parquetTableRoot Base path for resolving parquet file locations (overrides CacheConfig setting), or null
     * @param parquetStorageConfig Storage credentials for parquet file access (overrides CacheConfig setting), or null
     */
    public SplitSearcher createSplitSearcher(
            String splitPath,
            QuickwitSplit.SplitMetadata metadata,
            String parquetTableRoot,
            ParquetCompanionConfig.ParquetStorageConfig parquetStorageConfig) {
        return createSplitSearcherInternal(splitPath, metadata, parquetTableRoot, parquetStorageConfig);
    }

    public SplitSearcher createSplitSearcher(String splitPath, QuickwitSplit.SplitMetadata metadata, String parquetTableRoot) {
        return createSplitSearcherInternal(splitPath, metadata, parquetTableRoot, null);
    }

    public SplitSearcher createSplitSearcher(String splitPath, QuickwitSplit.SplitMetadata metadata) {
        return createSplitSearcherInternal(splitPath, metadata, null, null);
    }

    private SplitSearcher createSplitSearcherInternal(String splitPath, QuickwitSplit.SplitMetadata metadata, String perSplitParquetTableRoot, ParquetCompanionConfig.ParquetStorageConfig perSplitParquetStorageConfig) {
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

        // Parquet companion mode: pass table_root and storage credentials
        // Per-split override takes precedence over CacheConfig-level setting
        String effectiveTableRoot = (perSplitParquetTableRoot != null && !perSplitParquetTableRoot.isEmpty())
                ? perSplitParquetTableRoot : this.parquetTableRoot;
        if (effectiveTableRoot != null && !effectiveTableRoot.isEmpty()) {
            splitConfig.put("parquet_table_root", effectiveTableRoot);
        }
        // Per-split parquet storage config takes precedence over cache-manager-level default
        ParquetCompanionConfig.ParquetStorageConfig effectiveParquetStorage =
                (perSplitParquetStorageConfig != null) ? perSplitParquetStorageConfig : this.parquetStorageConfig;
        if (effectiveParquetStorage != null) {
            if (!effectiveParquetStorage.getAwsConfig().isEmpty()) {
                splitConfig.put("parquet_aws_config", effectiveParquetStorage.getAwsConfig());
            }
            if (!effectiveParquetStorage.getAzureConfig().isEmpty()) {
                splitConfig.put("parquet_azure_config", effectiveParquetStorage.getAzureConfig());
            }
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

        // Parquet coalesce config
        long effectiveCoalesceMaxGap = this.coalesceMaxGapBytes;
        if (effectiveCoalesceMaxGap > 0) {
            splitConfig.put("parquet_coalesce_max_gap", String.valueOf(effectiveCoalesceMaxGap));
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

    // ========================================
    // L2 Disk Cache Statistics Native Methods
    // ========================================
    private static native long nativeGetDiskCacheTotalBytes(long ptr);
    private static native long nativeGetDiskCacheMaxBytes(long ptr);
    private static native int nativeGetDiskCacheSplitCount(long ptr);
    private static native int nativeGetDiskCacheComponentCount(long ptr);
    private static native boolean nativeIsDiskCacheEnabled(long ptr);
    private static native boolean nativeEvictSplitFromDiskCache(long ptr, String splitUri);

    // ========================================
    // Storage Download Metrics Native Methods
    // ========================================
    // Global counters for programmatic verification of S3/storage downloads
    static native long nativeGetStorageDownloadCount();
    static native long nativeGetStorageDownloadBytes();
    static native long nativeGetStoragePrewarmDownloadCount();
    static native long nativeGetStoragePrewarmDownloadBytes();
    static native long nativeGetStorageQueryDownloadCount();
    static native long nativeGetStorageQueryDownloadBytes();
    static native void nativeResetStorageDownloadMetrics();

    // ========================================
    // L1 ByteRangeCache Statistics Native Methods
    // ========================================
    static native long nativeGetL1CacheSize();
    static native long nativeGetL1CacheCapacity();
    static native long nativeGetL1CacheEvictions();
    static native long nativeGetL1CacheEvictedBytes();
    static native long nativeGetL1CacheHits();
    static native long nativeGetL1CacheMisses();
    static native void nativeClearL1Cache();
    static native void nativeResetL1Cache();

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

    // ========================================
    // Storage Download Metrics API
    // ========================================
    // Global counters to programmatically verify S3/storage download behavior.
    // Tracks downloads separately for prewarm vs query operations.

    /**
     * Storage download metrics for programmatic verification.
     *
     * <p>This class tracks actual storage downloads (S3, Azure, file) separately for
     * prewarm and query operations. Use this to verify that:
     * <ul>
     *   <li>Prewarm populates the cache without redundant downloads</li>
     *   <li>Queries use cached data (zero query downloads after prewarm)</li>
     *   <li>Cache key mismatches are detected (prewarm + query downloads for same data)</li>
     * </ul>
     *
     * <p><strong>Example Test Usage:</strong>
     * <pre>{@code
     * // Reset counters at start of test
     * SplitCacheManager.resetStorageDownloadMetrics();
     *
     * // Prewarm the split
     * searcher.preloadComponents(PrewarmComponent.ALL);
     *
     * StorageDownloadMetrics afterPrewarm = SplitCacheManager.getStorageDownloadMetrics();
     * System.out.println("Prewarm downloads: " + afterPrewarm.getPrewarmDownloads());
     *
     * // Run query
     * searcher.search(query, 10);
     *
     * StorageDownloadMetrics afterQuery = SplitCacheManager.getStorageDownloadMetrics();
     * assertEquals(0, afterQuery.getQueryDownloads(), "Queries should use cached data!");
     * }</pre>
     */
    public static class StorageDownloadMetrics {
        private final long totalDownloads;
        private final long totalBytes;
        private final long prewarmDownloads;
        private final long prewarmBytes;
        private final long queryDownloads;
        private final long queryBytes;

        public StorageDownloadMetrics(
                long totalDownloads, long totalBytes,
                long prewarmDownloads, long prewarmBytes,
                long queryDownloads, long queryBytes) {
            this.totalDownloads = totalDownloads;
            this.totalBytes = totalBytes;
            this.prewarmDownloads = prewarmDownloads;
            this.prewarmBytes = prewarmBytes;
            this.queryDownloads = queryDownloads;
            this.queryBytes = queryBytes;
        }

        /** Total storage downloads (all sources) */
        public long getTotalDownloads() { return totalDownloads; }

        /** Total bytes downloaded (all sources) */
        public long getTotalBytes() { return totalBytes; }

        /** Downloads during prewarm operations */
        public long getPrewarmDownloads() { return prewarmDownloads; }

        /** Bytes downloaded during prewarm */
        public long getPrewarmBytes() { return prewarmBytes; }

        /** Downloads during query operations (L3 cache misses) */
        public long getQueryDownloads() { return queryDownloads; }

        /** Bytes downloaded during queries */
        public long getQueryBytes() { return queryBytes; }

        /** Returns true if any query downloads occurred (indicates cache miss) */
        public boolean hasQueryDownloads() { return queryDownloads > 0; }

        /** Returns true if prewarm was fully effective (no query downloads after prewarm) */
        public boolean isPrewarmEffective() { return prewarmDownloads > 0 && queryDownloads == 0; }

        /** Formatted summary string */
        public String getSummary() {
            return String.format(
                "Downloads: %d total (%d bytes), %d prewarm (%d bytes), %d query (%d bytes)",
                totalDownloads, totalBytes,
                prewarmDownloads, prewarmBytes,
                queryDownloads, queryBytes
            );
        }

        @Override
        public String toString() {
            return getSummary();
        }
    }

    /**
     * Get current storage download metrics.
     *
     * <p>Returns a snapshot of all storage download counters. These counters track
     * actual downloads from remote storage (S3, Azure, file), separated by operation type.
     *
     * @return current storage download metrics
     * @see #resetStorageDownloadMetrics()
     */
    public static StorageDownloadMetrics getStorageDownloadMetrics() {
        return new StorageDownloadMetrics(
            nativeGetStorageDownloadCount(),
            nativeGetStorageDownloadBytes(),
            nativeGetStoragePrewarmDownloadCount(),
            nativeGetStoragePrewarmDownloadBytes(),
            nativeGetStorageQueryDownloadCount(),
            nativeGetStorageQueryDownloadBytes()
        );
    }

    /**
     * Reset all storage download metrics.
     *
     * <p>Resets all counters to zero. Use this at the start of tests to get
     * accurate per-test measurements.
     *
     * @see #getStorageDownloadMetrics()
     */
    public static void resetStorageDownloadMetrics() {
        nativeResetStorageDownloadMetrics();
    }

    // ========================================
    // L2 Disk Cache Statistics API
    // ========================================

    /**
     * Get L2 disk cache statistics.
     *
     * <p>The L2 disk cache provides persistent caching of split data on local disk,
     * reducing repeated fetches from remote storage (S3/Azure). This method returns
     * statistics about the current state of the disk cache.
     *
     * <p><strong>Example Usage:</strong>
     * <pre>{@code
     * DiskCacheStats stats = cacheManager.getDiskCacheStats();
     * if (stats.isEnabled()) {
     *     System.out.println("Disk Cache Usage: " + stats.getUsagePercent() + "%");
     *     System.out.println("Splits Cached: " + stats.getSplitCount());
     *     System.out.println("Components Cached: " + stats.getComponentCount());
     *     System.out.println("Total Bytes: " + stats.getTotalBytes());
     * }
     * }</pre>
     *
     * @return disk cache statistics, or empty stats if disk cache is not enabled
     */
    public DiskCacheStats getDiskCacheStats() {
        if (nativePtr == 0) {
            return new DiskCacheStats(false, 0, 0, 0, 0);
        }
        boolean enabled = nativeIsDiskCacheEnabled(nativePtr);
        if (!enabled) {
            return new DiskCacheStats(false, 0, 0, 0, 0);
        }
        return new DiskCacheStats(
            true,
            nativeGetDiskCacheTotalBytes(nativePtr),
            nativeGetDiskCacheMaxBytes(nativePtr),
            nativeGetDiskCacheSplitCount(nativePtr),
            nativeGetDiskCacheComponentCount(nativePtr)
        );
    }

    /**
     * Check if L2 disk cache is enabled for this cache manager.
     *
     * @return true if disk cache is enabled and configured
     */
    public boolean isDiskCacheEnabled() {
        return nativePtr != 0 && nativeIsDiskCacheEnabled(nativePtr);
    }

    /**
     * Evict a specific split from the disk cache.
     *
     * <p>This removes all cached components for the specified split from the L2 disk cache.
     * The split will be re-fetched from remote storage on next access.
     *
     * @param splitUri the URI of the split to evict (e.g., "s3://bucket/path/split.split")
     * @return true if the eviction was performed, false if disk cache is not enabled
     */
    public boolean evictSplitFromDiskCache(String splitUri) {
        if (nativePtr == 0 || !nativeIsDiskCacheEnabled(nativePtr)) {
            return false;
        }
        return nativeEvictSplitFromDiskCache(nativePtr, splitUri);
    }

    /**
     * L2 disk cache statistics.
     *
     * <p>Provides visibility into the state of the persistent disk cache layer.
     * The disk cache stores split components on local disk with optional LZ4 compression
     * to reduce repeated fetches from remote storage.
     */
    public static class DiskCacheStats {
        private final boolean enabled;
        private final long totalBytes;
        private final long maxBytes;
        private final int splitCount;
        private final int componentCount;

        DiskCacheStats(boolean enabled, long totalBytes, long maxBytes, int splitCount, int componentCount) {
            this.enabled = enabled;
            this.totalBytes = totalBytes;
            this.maxBytes = maxBytes;
            this.splitCount = splitCount;
            this.componentCount = componentCount;
        }

        /** Whether the disk cache is enabled and active. */
        public boolean isEnabled() { return enabled; }

        /** Total bytes currently used by the disk cache. */
        public long getTotalBytes() { return totalBytes; }

        /** Maximum bytes allowed for the disk cache. */
        public long getMaxBytes() { return maxBytes; }

        /** Number of splits currently cached on disk. */
        public int getSplitCount() { return splitCount; }

        /** Total number of components cached across all splits. */
        public int getComponentCount() { return componentCount; }

        /**
         * Cache usage as a percentage (0-100).
         * Returns 0 if disk cache is not enabled or max size is 0.
         */
        public double getUsagePercent() {
            if (!enabled || maxBytes == 0) {
                return 0.0;
            }
            return (totalBytes * 100.0 / maxBytes);
        }

        @Override
        public String toString() {
            if (!enabled) {
                return "DiskCacheStats{enabled=false}";
            }
            return String.format(
                "DiskCacheStats{totalBytes=%d, maxBytes=%d, usage=%.1f%%, splits=%d, components=%d}",
                totalBytes, maxBytes, getUsagePercent(), splitCount, componentCount
            );
        }
    }

    // ========================================
    // L1 Cache Statistics API
    // ========================================

    /**
     * Get L1 cache (ByteRangeCache) statistics.
     *
     * <p>The L1 cache is the in-memory cache layer that provides fastest access to
     * recently accessed data. It is bounded by the capacity configured via
     * {@link CacheConfig#withMaxCacheSize(long)} and automatically evicts entries
     * when capacity is exceeded.
     *
     * <p><strong>Example Usage:</strong>
     * <pre>{@code
     * L1CacheStats stats = SplitCacheManager.getL1CacheStats();
     * System.out.println("L1 Cache Usage: " + stats.getSizeBytes() + " / " + stats.getCapacityBytes());
     * System.out.println("Hit Rate: " + stats.getHitRate() + "%");
     * System.out.println("Total Evictions: " + stats.getEvictions());
     * }</pre>
     *
     * @return L1 cache statistics snapshot
     */
    public static L1CacheStats getL1CacheStats() {
        return new L1CacheStats(
            nativeGetL1CacheSize(),
            nativeGetL1CacheCapacity(),
            nativeGetL1CacheHits(),
            nativeGetL1CacheMisses(),
            nativeGetL1CacheEvictions(),
            nativeGetL1CacheEvictedBytes()
        );
    }

    /**
     * Clear the L1 cache (ByteRangeCache).
     *
     * <p>This frees all memory used by the L1 cache. Data is still available in
     * the L2 disk cache and will be reloaded on demand.
     */
    public static void clearL1Cache() {
        nativeClearL1Cache();
    }

    /**
     * Reset the L1 cache to allow reinitialization with new capacity.
     *
     * <p>This is primarily for testing scenarios where you need to change the L1
     * cache capacity between tests. In production, you typically configure the
     * capacity once via {@link CacheConfig#withMaxCacheSize(long)}.
     *
     * <p>After calling this method, the next {@link SplitCacheManager#getInstance(CacheConfig)}
     * call will create a new L1 cache with the capacity from the new CacheConfig.
     */
    public static void resetL1Cache() {
        nativeResetL1Cache();
    }

    /**
     * L1 cache (ByteRangeCache) statistics.
     *
     * <p>The L1 cache is a bounded in-memory cache that automatically evicts entries
     * when the configured capacity is exceeded. This prevents OOM during large prewarm
     * operations while still providing fast access to recently used data.
     *
     * <p><b>Key Metrics:</b>
     * <ul>
     *   <li><b>sizeBytes</b> - Current memory usage in bytes</li>
     *   <li><b>capacityBytes</b> - Maximum allowed size (configured via withMaxCacheSize)</li>
     *   <li><b>evictions</b> - Number of times the cache was cleared due to capacity exceeded</li>
     *   <li><b>evictedBytes</b> - Total bytes evicted from the cache</li>
     *   <li><b>hits/misses</b> - Cache access statistics for hit rate calculation</li>
     * </ul>
     */
    public static class L1CacheStats {
        private final long sizeBytes;
        private final long capacityBytes;
        private final long hits;
        private final long misses;
        private final long evictions;
        private final long evictedBytes;

        L1CacheStats(long sizeBytes, long capacityBytes, long hits, long misses,
                     long evictions, long evictedBytes) {
            this.sizeBytes = sizeBytes;
            this.capacityBytes = capacityBytes;
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
            this.evictedBytes = evictedBytes;
        }

        /** Current size of L1 cache in bytes. */
        public long getSizeBytes() { return sizeBytes; }

        /** Maximum capacity of L1 cache in bytes (0 if unlimited). */
        public long getCapacityBytes() { return capacityBytes; }

        /** Number of cache hits. */
        public long getHits() { return hits; }

        /** Number of cache misses. */
        public long getMisses() { return misses; }

        /** Number of items evicted from the cache. */
        public long getEvictions() { return evictions; }

        /** Total bytes evicted from the cache. */
        public long getEvictedBytes() { return evictedBytes; }

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

        /**
         * Cache usage as percentage (0-100).
         * Returns 0 if capacity is 0 (unlimited).
         */
        public double getUsagePercent() {
            return capacityBytes == 0 ? 0.0 : (sizeBytes * 100.0 / capacityBytes);
        }

        @Override
        public String toString() {
            return String.format(
                "L1CacheStats{size=%d, capacity=%d, usage=%.1f%%, hits=%d, misses=%d, hitRate=%.1f%%, evictions=%d, evictedBytes=%d}",
                sizeBytes, capacityBytes, getUsagePercent(), hits, misses, getHitRate(), evictions, evictedBytes
            );
        }
    }

    // Package-private getters for SplitSearcher
    public String getCacheName() { return cacheName; }
    String getCacheKey() { return cacheKey; }
    long getMaxCacheSize() { return maxCacheSize; }
    long getNativePtr() { return nativePtr; }

    /**
     * Get the batch optimization configuration for this cache manager.
     *
     * <p>This configuration controls threshold-based method selection for batch
     * document retrieval. When the number of documents exceeds the byteBufferThreshold,
     * the byte buffer protocol is used to reduce JNI overhead.
     *
     * @return batch optimization configuration (never null)
     */
    public BatchOptimizationConfig getBatchConfig() { return batchOptimization; }
}
