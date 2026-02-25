/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java.split.merge;

import io.indextables.tantivy4java.core.Tantivy;

import io.indextables.tantivy4java.core.Index;
import io.indextables.tantivy4java.config.FileSystemConfig;
import io.indextables.tantivy4java.split.ColumnStatistics;
import io.indextables.tantivy4java.split.ParquetCompanionConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Quickwit split functionality for converting Tantivy indices to Quickwit splits.
 * 
 * A Quickwit split is an immutable, self-contained piece of an index stored as a single .split file.
 * This class provides methods to convert existing Tantivy indices into Quickwit splits that can be
 * used with Quickwit's distributed search infrastructure.
 */
public class QuickwitSplit {
    static {
        Tantivy.initialize();
    }

    /**
     * Configuration for split conversion.
     */
    public static class SplitConfig {
        private final String indexUid;
        private final String sourceId;
        private final String nodeId;
        private final String docMappingUid;
        private final long partitionId;
        private final Instant timeRangeStart;
        private final Instant timeRangeEnd;
        private final Set<String> tags;
        private final Map<String, Object> metadata;

        // New streaming configuration fields
        private final long streamingChunkSize;        // 64MB default for optimal I/O
        private final boolean enableProgressTracking;  // Enable detailed progress logging
        private final boolean enableStreamingIO;       // Use streaming I/O instead of read_all()

        /**
         * Create a new split configuration.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that created the split
         * @param docMappingUid Document mapping unique identifier
         * @param partitionId Partition identifier (default: 0)
         * @param timeRangeStart Optional start time for time-based data
         * @param timeRangeEnd Optional end time for time-based data
         * @param tags Optional tags for the split
         * @param metadata Optional additional metadata
         */
        public SplitConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, Instant timeRangeStart, Instant timeRangeEnd,
                          Set<String> tags, Map<String, Object> metadata) {
            this(indexUid, sourceId, nodeId, docMappingUid, partitionId, timeRangeStart, timeRangeEnd,
                 tags, metadata, 64_000_000L, false, true); // Default: 64MB chunks, no progress tracking, streaming enabled
        }

        /**
         * Create a new split configuration with streaming options.
         */
        public SplitConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, Instant timeRangeStart, Instant timeRangeEnd,
                          Set<String> tags, Map<String, Object> metadata,
                          long streamingChunkSize, boolean enableProgressTracking, boolean enableStreamingIO) {
            this.indexUid = indexUid;
            this.sourceId = sourceId;
            this.nodeId = nodeId;
            this.docMappingUid = docMappingUid;
            this.partitionId = partitionId;
            this.timeRangeStart = timeRangeStart;
            this.timeRangeEnd = timeRangeEnd;
            this.tags = tags;
            this.metadata = metadata;
            this.streamingChunkSize = streamingChunkSize;
            this.enableProgressTracking = enableProgressTracking;
            this.enableStreamingIO = enableStreamingIO;
        }

        /**
         * Create a minimal split configuration.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that created the split
         */
        public SplitConfig(String indexUid, String sourceId, String nodeId) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, null, null, null,
                 64_000_000L, false, true); // Default: 64MB chunks, no progress tracking, streaming enabled
        }

        // Getters
        public String getIndexUid() { return indexUid; }
        public String getSourceId() { return sourceId; }
        public String getNodeId() { return nodeId; }
        public String getDocMappingUid() { return docMappingUid; }
        public long getPartitionId() { return partitionId; }
        public Instant getTimeRangeStart() { return timeRangeStart; }
        public Instant getTimeRangeEnd() { return timeRangeEnd; }
        public Set<String> getTags() { return tags; }
        public Map<String, Object> getMetadata() { return metadata; }

        // New streaming configuration getters
        public long getStreamingChunkSize() { return streamingChunkSize; }
        public boolean isProgressTrackingEnabled() { return enableProgressTracking; }
        public boolean isStreamingIOEnabled() { return enableStreamingIO; }
    }

    /**
     * Information about a split that was skipped during merge operations.
     * Contains the URL/path of the skipped split and the reason for skipping.
     */
    public static class SkippedSplit {
        private final String url;
        private final String reason;

        public SkippedSplit(String url, String reason) {
            this.url = url;
            this.reason = reason;
        }

        /** Returns the URL or path of the skipped split. */
        public String getUrl() { return url; }

        /** Returns the reason why this split was skipped (e.g., corruption, parsing error). */
        public String getReason() { return reason; }

        @Override
        public String toString() {
            return "SkippedSplit{url='" + url + "', reason='" + reason + "'}";
        }
    }

    /**
     * Metadata about a created split.
     */
    public static class SplitMetadata {
        // Core Quickwit fields (matching quickwit_metastore::SplitMetadata)
        private final String splitId;
        private final String indexUid;          // NEW: Required by Quickwit
        private final long partitionId;         // NEW: Required by Quickwit
        private final String sourceId;          // NEW: Required by Quickwit
        private final String nodeId;            // NEW: Required by Quickwit
        private final long numDocs;
        private final long uncompressedSizeBytes;
        private final Instant timeRangeStart;   // Extracted from Quickwit's Range<i64>
        private final Instant timeRangeEnd;     // Extracted from Quickwit's Range<i64>
        private final long createTimestamp;     // NEW: Required by Quickwit
        private final String maturity;          // NEW: Required by Quickwit ("Mature" or "Immature")
        private final Set<String> tags;
        private final long footerStartOffset;   // From Quickwit's footer_offsets.start
        private final long footerEndOffset;     // From Quickwit's footer_offsets.end
        private final long deleteOpstamp;
        private final int numMergeOps;
        private final String docMappingUid;     // NEW: Required by Quickwit

        // Legacy fields (for backward compatibility and custom features)
        @Deprecated
        private final long hotcacheStartOffset;  // DEPRECATED: Not in Quickwit format
        @Deprecated
        private final long hotcacheLength;       // DEPRECATED: Not in Quickwit format
        @Deprecated
        private final String docMappingJson;     // DEPRECATED: Use docMappingUid instead

        // Custom fields (for merge operations with parsing failures)
        private final List<SkippedSplit> skippedSplits;  // Splits that failed to parse with reasons

        // Parquet companion mode: column statistics for split pruning
        private Map<String, ColumnStatistics> columnStatistics;

        /**
         * Full constructor with all Quickwit-compatible fields and detailed skip information.
         */
        public SplitMetadata(String splitId, String indexUid, long partitionId, String sourceId,
                           String nodeId, long numDocs, long uncompressedSizeBytes,
                           Instant timeRangeStart, Instant timeRangeEnd, long createTimestamp,
                           String maturity, Set<String> tags, long footerStartOffset, long footerEndOffset,
                           long deleteOpstamp, int numMergeOps, String docMappingUid, String docMappingJson,
                           List<SkippedSplit> skippedSplits) {
            // Core Quickwit fields
            this.splitId = splitId;
            this.indexUid = indexUid;
            this.partitionId = partitionId;
            this.sourceId = sourceId;
            this.nodeId = nodeId;
            this.numDocs = numDocs;
            this.uncompressedSizeBytes = uncompressedSizeBytes;
            this.timeRangeStart = timeRangeStart;
            this.timeRangeEnd = timeRangeEnd;
            this.createTimestamp = createTimestamp;
            this.maturity = maturity;
            this.tags = tags != null ? new java.util.HashSet<>(tags) : new java.util.HashSet<>();
            this.footerStartOffset = footerStartOffset;
            this.footerEndOffset = footerEndOffset;
            this.deleteOpstamp = deleteOpstamp;
            this.numMergeOps = numMergeOps;
            this.docMappingUid = docMappingUid;

            // Legacy fields (deprecated)
            this.hotcacheStartOffset = -1L;  // Not supported in Quickwit format
            this.hotcacheLength = -1L;       // Not supported in Quickwit format
            this.docMappingJson = docMappingJson;  // Store for tokenization performance

            // Custom fields
            this.skippedSplits = skippedSplits != null ? new ArrayList<>(skippedSplits) : new ArrayList<>();
        }

        /**
         * Constructor with legacy hotcache fields (for backward compatibility).
         * @deprecated Use the full Quickwit-compatible constructor instead.
         */
        @Deprecated
        public SplitMetadata(String splitId, long numDocs, long uncompressedSizeBytes,
                           Instant timeRangeStart, Instant timeRangeEnd, Set<String> tags,
                           long deleteOpstamp, int numMergeOps,
                           long footerStartOffset, long footerEndOffset,
                           long hotcacheStartOffset, long hotcacheLength, String docMappingJson,
                           List<SkippedSplit> skippedSplits) {
            // Call new constructor with default Quickwit values
            this(splitId, "unknown-index", 0L, "unknown-source", "unknown-node",
                 numDocs, uncompressedSizeBytes, timeRangeStart, timeRangeEnd,
                 System.currentTimeMillis() / 1000, "Mature", tags,
                 footerStartOffset, footerEndOffset, deleteOpstamp, numMergeOps,
                 "unknown-doc-mapping", docMappingJson, skippedSplits);
        }
        
        /**
         * Backward compatibility constructor (for existing code).
         * @deprecated Use the full Quickwit-compatible constructor instead.
         */
        @Deprecated
        public SplitMetadata(String splitId, long numDocs, long uncompressedSizeBytes,
                           Instant timeRangeStart, Instant timeRangeEnd, Set<String> tags,
                           long deleteOpstamp, int numMergeOps) {
            this(splitId, "unknown-index", 0L, "unknown-source", "unknown-node",
                 numDocs, uncompressedSizeBytes, timeRangeStart, timeRangeEnd,
                 System.currentTimeMillis() / 1000, "Mature", tags,
                 -1L, -1L, deleteOpstamp, numMergeOps, "unknown-doc-mapping", null, null);
        }

        /**
         * Constructor with only footer offset information (for testing).
         * @deprecated Use the full Quickwit-compatible constructor instead.
         */
        @Deprecated
        public SplitMetadata(long footerStartOffset, long footerEndOffset,
                           long hotcacheStartOffset, long hotcacheLength) {
            this("", "test-index", 0L, "test-source", "test-node", 0L, 0L, null, null,
                 System.currentTimeMillis() / 1000, "Mature", new java.util.HashSet<>(),
                 footerStartOffset, footerEndOffset, 0L, 0, "test-doc-mapping", null, null);
        }

        // Core Quickwit field getters
        public String getSplitId() { return splitId; }
        public String getIndexUid() { return indexUid; }
        public long getPartitionId() { return partitionId; }
        public String getSourceId() { return sourceId; }
        public String getNodeId() { return nodeId; }
        public long getNumDocs() { return numDocs; }
        public long getUncompressedSizeBytes() { return uncompressedSizeBytes; }
        public Instant getTimeRangeStart() { return timeRangeStart; }
        public Instant getTimeRangeEnd() { return timeRangeEnd; }
        public long getCreateTimestamp() { return createTimestamp; }
        public String getMaturity() { return maturity; }
        public Set<String> getTags() { return new java.util.HashSet<>(tags); }
        public long getFooterStartOffset() { return footerStartOffset; }
        public long getFooterEndOffset() { return footerEndOffset; }
        public long getDeleteOpstamp() { return deleteOpstamp; }
        public int getNumMergeOps() { return numMergeOps; }
        public String getDocMappingUid() { return docMappingUid; }

        // Legacy getters (deprecated)
        @Deprecated
        public long getHotcacheStartOffset() {
            throw new UnsupportedOperationException("Hotcache offsets not supported in Quickwit format. Use footer offsets instead.");
        }

        @Deprecated
        public long getHotcacheLength() {
            throw new UnsupportedOperationException("Hotcache length not supported in Quickwit format. Use footer offsets instead.");
        }

        @Deprecated
        public String getDocMappingJson() {
            return docMappingJson;  // Return for tokenization performance needs
        }

        // Skipped splits getters (for merge operations)

        /**
         * Returns detailed information about splits that were skipped during merge operations.
         * Each SkippedSplit contains the URL/path and the reason for skipping.
         */
        public List<SkippedSplit> getSkippedSplitsDetailed() {
            return new ArrayList<>(skippedSplits);
        }

        /**
         * Returns just the URLs/paths of splits that were skipped during merge operations.
         * For more detail including the reason for each skip, use {@link #getSkippedSplitsDetailed()}.
         */
        public List<String> getSkippedSplits() {
            List<String> urls = new ArrayList<>(skippedSplits.size());
            for (SkippedSplit s : skippedSplits) {
                urls.add(s.getUrl());
            }
            return urls;
        }

        // Convenience methods
        public boolean hasFooterOffsets() {
            return footerStartOffset >= 0 && footerEndOffset >= 0;
        }

        public boolean hasTimeRange() {
            return timeRangeStart != null && timeRangeEnd != null;
        }

        public boolean isImmature() {
            return "Immature".equals(maturity);
        }

        public boolean isMature() {
            return "Mature".equals(maturity);
        }

        @Deprecated
        public boolean hasDocMapping() {
            // Always return false since we no longer store JSON doc mapping
            return false;
        }

        /**
         * Returns true if any splits were skipped during merge operations due to parsing failures.
         */
        public boolean hasSkippedSplits() {
            return !skippedSplits.isEmpty();
        }

        /**
         * Returns column statistics computed during parquet companion split creation.
         * Returns null if no statistics were computed (non-parquet-companion split).
         */
        public Map<String, ColumnStatistics> getColumnStatistics() {
            return columnStatistics;
        }

        /**
         * Set column statistics (used internally by createFromParquet).
         */
        void setColumnStatistics(Map<String, ColumnStatistics> stats) {
            this.columnStatistics = stats;
        }

        public long getMetadataSize() {
            return hasFooterOffsets() ? (footerEndOffset - footerStartOffset) : -1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("SplitMetadata{")
                    .append("splitId='").append(splitId).append('\'')
                    .append(", indexUid='").append(indexUid).append('\'')
                    .append(", sourceId='").append(sourceId).append('\'')
                    .append(", nodeId='").append(nodeId).append('\'')
                    .append(", partitionId=").append(partitionId)
                    .append(", numDocs=").append(numDocs)
                    .append(", uncompressedSizeBytes=").append(uncompressedSizeBytes)
                    .append(", timeRangeStart=").append(timeRangeStart)
                    .append(", timeRangeEnd=").append(timeRangeEnd)
                    .append(", createTimestamp=").append(createTimestamp)
                    .append(", maturity='").append(maturity).append('\'')
                    .append(", tags=").append(tags)
                    .append(", deleteOpstamp=").append(deleteOpstamp)
                    .append(", numMergeOps=").append(numMergeOps)
                    .append(", docMappingUid='").append(docMappingUid).append('\'')
                    .append(", footerOffsets=").append(footerStartOffset).append("-").append(footerEndOffset);

            if (hasSkippedSplits()) {
                sb.append(", skippedSplits=").append(skippedSplits.size()).append(" splits");
            }

            return sb.append('}').toString();
        }
    }

    /**
     * Convert a Tantivy index to a Quickwit split file.
     * 
     * @param index The Tantivy index to convert
     * @param outputPath Path where the split file should be written (must end with .split)
     * @param config Configuration for the split conversion
     * @return Metadata about the created split
     * @throws IllegalArgumentException if outputPath doesn't end with .split or other validation errors
     * @throws RuntimeException if conversion fails
     */
    public static SplitMetadata convertIndex(Index index, String outputPath, SplitConfig config) {
        if (outputPath == null || !outputPath.endsWith(".split")) {
            throw new IllegalArgumentException("Output path must end with .split");
        }
        if (config == null) {
            throw new IllegalArgumentException("Split configuration cannot be null");
        }

        String resolvedOutputPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(outputPath) : outputPath;
        if (config.getIndexUid() == null || config.getIndexUid().trim().isEmpty()) {
            throw new IllegalArgumentException("Index UID cannot be null or empty");
        }
        if (config.getSourceId() == null || config.getSourceId().trim().isEmpty()) {
            throw new IllegalArgumentException("Source ID cannot be null or empty");
        }
        if (config.getNodeId() == null || config.getNodeId().trim().isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        // Check if this index has a stored path (file-based index)
        String indexPath = index.getIndexPath();
        if (indexPath != null) {
            // Delegate to the working convertIndexFromPath method
            return convertIndexFromPath(indexPath, resolvedOutputPath, config);
        } else {
            // This is an in-memory index - cannot be converted directly
            throw new IllegalArgumentException(
                "Cannot convert in-memory index to Quickwit split. " +
                "For in-memory indices, first save the index to disk using IndexWriter.commit(), " +
                "then use QuickwitSplit.convertIndexFromPath(indexPath, outputPath, config) instead."
            );
        }
    }

    /**
     * Convert a Tantivy index directory to a Quickwit split file.
     * 
     * @param indexPath Path to the Tantivy index directory
     * @param outputPath Path where the split file should be written (must end with .split)
     * @param config Configuration for the split conversion
     * @return Metadata about the created split
     * @throws IllegalArgumentException if paths are invalid or other validation errors
     * @throws RuntimeException if conversion fails
     */
    public static SplitMetadata convertIndexFromPath(String indexPath, String outputPath, SplitConfig config) {
        if (indexPath == null || indexPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Index path cannot be null or empty");
        }
        if (outputPath == null || !outputPath.endsWith(".split")) {
            throw new IllegalArgumentException("Output path must end with .split");
        }
        if (config == null) {
            throw new IllegalArgumentException("Split configuration cannot be null");
        }

        String resolvedIndexPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(indexPath) : indexPath;
        String resolvedOutputPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(outputPath) : outputPath;

        return nativeConvertIndexFromPath(resolvedIndexPath, resolvedOutputPath, config);
    }

    /**
     * Extract information about a Quickwit split file without fully loading it.
     * 
     * @param splitPath Path to the split file
     * @return Metadata about the split
     * @throws IllegalArgumentException if splitPath is invalid
     * @throws RuntimeException if split reading fails
     */
    public static SplitMetadata readSplitMetadata(String splitPath) {
        if (splitPath == null || splitPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Split path cannot be null or empty");
        }
        if (!splitPath.endsWith(".split")) {
            throw new IllegalArgumentException("Split path must end with .split");
        }

        String resolvedSplitPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitPath) : splitPath;
        return nativeReadSplitMetadata(resolvedSplitPath);
    }

    /**
     * List the files contained within a Quickwit split.
     * 
     * @param splitPath Path to the split file
     * @return List of file names contained in the split
     * @throws IllegalArgumentException if splitPath is invalid
     * @throws RuntimeException if split reading fails
     */
    public static List<String> listSplitFiles(String splitPath) {
        if (splitPath == null || splitPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Split path cannot be null or empty");
        }
        if (!splitPath.endsWith(".split")) {
            throw new IllegalArgumentException("Split path must end with .split");
        }

        String resolvedSplitPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitPath) : splitPath;
        return nativeListSplitFiles(resolvedSplitPath);
    }

    /**
     * Extract a Quickwit split back to a Tantivy index directory.
     * 
     * @param splitPath Path to the split file
     * @param outputDir Directory where the Tantivy index should be extracted
     * @return Metadata about the extracted split
     * @throws IllegalArgumentException if paths are invalid
     * @throws RuntimeException if extraction fails
     */
    public static SplitMetadata extractSplit(String splitPath, String outputDir) {
        if (splitPath == null || splitPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Split path cannot be null or empty");
        }
        if (!splitPath.endsWith(".split")) {
            throw new IllegalArgumentException("Split path must end with .split");
        }
        if (outputDir == null || outputDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Output directory cannot be null or empty");
        }

        String resolvedSplitPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitPath) : splitPath;
        String resolvedOutputDir = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(outputDir) : outputDir;
        return nativeExtractSplit(resolvedSplitPath, resolvedOutputDir);
    }

    /**
     * Validate that a split file is well-formed and can be read.
     * 
     * @param splitPath Path to the split file
     * @return true if the split is valid, false otherwise
     */
    public static boolean validateSplit(String splitPath) {
        if (splitPath == null || splitPath.trim().isEmpty() || !splitPath.endsWith(".split")) {
            return false;
        }

        try {
            String resolvedSplitPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitPath) : splitPath;
            return nativeValidateSplit(resolvedSplitPath);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * AWS configuration for S3-compatible storage access.
     * 
     * <p>This class provides configuration for accessing S3 splits using AWS credentials.
     * It supports both basic AWS access (access key + secret key) and temporary credentials
     * (with session tokens from STS), as well as custom S3-compatible endpoints like MinIO.
     * 
     * <h3>Usage Examples:</h3>
     * 
     * <h4>Basic AWS Credentials:</h4>
     * <pre>{@code
     * AwsConfig config = new AwsConfig("AKIA...", "secret-key", "us-east-1");
     * }</pre>
     * 
     * <h4>Temporary Credentials (STS):</h4>
     * <pre>{@code
     * AwsConfig config = new AwsConfig(
     *     "temp-access-key", 
     *     "temp-secret-key", 
     *     "session-token",    // From STS
     *     "us-west-2", 
     *     null,              // Use default endpoint
     *     false              // Virtual-hosted style
     * );
     * }</pre>
     * 
     * <h4>MinIO or Custom S3 Endpoint:</h4>
     * <pre>{@code
     * AwsConfig config = new AwsConfig(
     *     "minioaccess", 
     *     "miniosecret", 
     *     null,                           // No session token
     *     "us-east-1", 
     *     "https://minio.example.com",    // Custom endpoint
     *     true                            // Force path-style URLs
     * );
     * }</pre>
     * 
     * @see MergeConfig#MergeConfig(String, String, String, AwsConfig) for integration
     * @since 0.24.0
     */
    public static class AwsConfig {
        private final String accessKey;
        private final String secretKey;
        private final String sessionToken;
        private final String region;
        private final String endpoint;
        private final boolean forcePathStyle;
        
        /**
         * Create AWS configuration for S3 access.
         *
         * @param accessKey AWS access key ID
         * @param secretKey AWS secret access key
         * @param region AWS region
         */
        public AwsConfig(String accessKey, String secretKey, String region) {
            this(accessKey, secretKey, null, region, null, false);
        }
        
        /**
         * Create AWS configuration with all options.
         *
         * @param accessKey AWS access key ID
         * @param secretKey AWS secret access key
         * @param sessionToken AWS session token (optional, for temporary credentials)
         * @param region AWS region
         * @param endpoint Custom S3 endpoint (optional, for S3-compatible storage)
         * @param forcePathStyle Force path-style URLs (for MinIO compatibility)
         */
        public AwsConfig(String accessKey, String secretKey, String sessionToken, String region, String endpoint, boolean forcePathStyle) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.sessionToken = sessionToken;
            this.region = region;
            this.endpoint = endpoint;
            this.forcePathStyle = forcePathStyle;
        }
        
        public String getAccessKey() { return accessKey; }
        public String getSecretKey() { return secretKey; }
        public String getSessionToken() { return sessionToken; }
        public String getRegion() { return region; }
        public String getEndpoint() { return endpoint; }
        public boolean isForcePathStyle() { return forcePathStyle; }
    }

    /**
     * Azure Blob Storage configuration for accessing Azure-hosted splits.
     *
     * <p>This class provides configuration for accessing Azure Blob Storage splits using
     * Azure credentials. It supports multiple authentication methods and custom endpoints
     * for testing with Azurite emulator.
     *
     * <h3>Usage Examples:</h3>
     *
     * <h4>Basic Azure Credentials:</h4>
     * <pre>{@code
     * AzureConfig config = new AzureConfig("storageaccount", "accesskey");
     * }</pre>
     *
     * <h4>Connection String Authentication:</h4>
     * <pre>{@code
     * AzureConfig config = AzureConfig.fromConnectionString(
     *     "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
     * );
     * }</pre>
     *
     * @see MergeConfig#MergeConfig(String, String, String, AwsConfig, AzureConfig) for integration
     * @since 0.25.0
     */
    public static class AzureConfig {
        private final String accountName;
        private final String accountKey;
        private final String bearerToken;
        private final String connectionString;

        /**
         * Create Azure configuration with account name and access key.
         *
         * @param accountName Azure storage account name
         * @param accountKey Azure storage account access key
         */
        public AzureConfig(String accountName, String accountKey) {
            this(accountName, accountKey, null, null);
        }

        /**
         * Create Azure configuration with OAuth bearer token authentication.
         *
         * @param accountName Azure storage account name
         * @param bearerToken OAuth bearer token for Azure AD authentication
         * @return AzureConfig instance with bearer token authentication
         */
        public static AzureConfig withBearerToken(String accountName, String bearerToken) {
            return new AzureConfig(accountName, null, bearerToken, null);
        }

        /**
         * Create Azure configuration with all options.
         *
         * @param accountName Azure storage account name
         * @param accountKey Azure storage account access key
         * @param bearerToken OAuth bearer token (optional, for Azure AD authentication)
         * @param connectionString Full connection string (optional, overrides other settings)
         */
        private AzureConfig(String accountName, String accountKey, String bearerToken, String connectionString) {
            this.accountName = accountName;
            this.accountKey = accountKey;
            this.bearerToken = bearerToken;
            this.connectionString = connectionString;
        }

        /**
         * Create Azure configuration from a connection string.
         *
         * @param connectionString Azure storage connection string
         * @return AzureConfig instance
         */
        public static AzureConfig fromConnectionString(String connectionString) {
            // Parse account name from connection string for convenience
            String accountName = extractAccountNameFromConnectionString(connectionString);
            return new AzureConfig(accountName, null, null, connectionString);
        }

        /**
         * Extract account name from connection string.
         */
        private static String extractAccountNameFromConnectionString(String connectionString) {
            if (connectionString == null || connectionString.isEmpty()) {
                return "unknown";
            }
            String[] parts = connectionString.split(";");
            for (String part : parts) {
                if (part.trim().startsWith("AccountName=")) {
                    return part.substring(part.indexOf('=') + 1).trim();
                }
            }
            return "unknown";
        }

        public String getAccountName() { return accountName; }
        public String getAccountKey() { return accountKey; }
        public String getBearerToken() { return bearerToken; }
        public String getConnectionString() { return connectionString; }

        /**
         * Check if this configuration uses a connection string.
         */
        public boolean usesConnectionString() {
            return connectionString != null && !connectionString.isEmpty();
        }
    }

    /**
     * Configuration for split merging operations with optional S3/AWS and Azure support.
     *
     * <p>This class configures split merge operations using a fluent builder pattern.
     *
     * <h3>Usage Examples:</h3>
     *
     * <h4>Local Split Merging:</h4>
     * <pre>{@code
     * MergeConfig config = MergeConfig.builder()
     *     .indexUid("my-index")
     *     .sourceId("my-source")
     *     .nodeId("my-node")
     *     .build();
     * }</pre>
     *
     * <h4>S3 Remote Split Merging:</h4>
     * <pre>{@code
     * AwsConfig awsConfig = new AwsConfig("access-key", "secret-key", "us-east-1");
     * MergeConfig config = MergeConfig.builder()
     *     .indexUid("my-index")
     *     .sourceId("my-source")
     *     .nodeId("my-node")
     *     .awsConfig(awsConfig)
     *     .build();
     * }</pre>
     *
     * <h4>Azure Remote Split Merging:</h4>
     * <pre>{@code
     * AzureConfig azureConfig = new AzureConfig("storageaccount", "accesskey");
     * MergeConfig config = MergeConfig.builder()
     *     .indexUid("my-index")
     *     .sourceId("my-source")
     *     .nodeId("my-node")
     *     .azureConfig(azureConfig)
     *     .build();
     * }</pre>
     *
     * <h4>Multi-Cloud Configuration:</h4>
     * <pre>{@code
     * MergeConfig config = MergeConfig.builder()
     *     .indexUid("distributed-index")
     *     .sourceId("data-source")
     *     .nodeId("worker-node")
     *     .docMappingUid("mapping-v1")
     *     .partitionId(42L)
     *     .awsConfig(awsConfig)
     *     .azureConfig(azureConfig)
     *     .deleteQueries(Arrays.asList("deleted:true"))
     *     .heapSizeBytes(256_000_000L)
     *     .debugEnabled(true)
     *     .build();
     * }</pre>
     *
     * @see AwsConfig for S3 credential configuration
     * @see AzureConfig for Azure Blob Storage credential configuration
     * @see #mergeSplits(List, String, MergeConfig) for usage
     * @since 0.25.0
     */
    public static class MergeConfig {
        private final String indexUid;
        private final String sourceId;
        private final String nodeId;
        private final String docMappingUid;
        private final long partitionId;
        private final List<String> deleteQueries;
        private final AwsConfig awsConfig;
        private final AzureConfig azureConfig;
        private final String tempDirectoryPath;
        private final Long heapSizeBytes;
        private final boolean debugEnabled;

        private MergeConfig(Builder builder) {
            this.indexUid = builder.indexUid;
            this.sourceId = builder.sourceId;
            this.nodeId = builder.nodeId;
            this.docMappingUid = builder.docMappingUid;
            this.partitionId = builder.partitionId;
            this.deleteQueries = builder.deleteQueries;
            this.awsConfig = builder.awsConfig;
            this.azureConfig = builder.azureConfig;
            this.tempDirectoryPath = builder.tempDirectoryPath;
            this.heapSizeBytes = builder.heapSizeBytes;
            this.debugEnabled = builder.debugEnabled;
        }

        /**
         * Create a new builder for MergeConfig.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Backward compatibility: Create a minimal merge configuration.
         * @deprecated Use {@link #builder()} instead
         */
        @Deprecated
        public MergeConfig(String indexUid, String sourceId, String nodeId) {
            this(builder().indexUid(indexUid).sourceId(sourceId).nodeId(nodeId));
        }

        /**
         * Backward compatibility: Create merge configuration with AWS config.
         * @deprecated Use {@link #builder()} instead
         */
        @Deprecated
        public MergeConfig(String indexUid, String sourceId, String nodeId, AwsConfig awsConfig) {
            this(builder().indexUid(indexUid).sourceId(sourceId).nodeId(nodeId).awsConfig(awsConfig));
        }

        /**
         * Backward compatibility: Full 10-parameter constructor.
         * @deprecated Use {@link #builder()} instead
         */
        @Deprecated
        public MergeConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, List<String> deleteQueries, AwsConfig awsConfig,
                          String tempDirectoryPath, Long heapSizeBytes, boolean debugEnabled) {
            this(builder()
                .indexUid(indexUid)
                .sourceId(sourceId)
                .nodeId(nodeId)
                .docMappingUid(docMappingUid)
                .partitionId(partitionId)
                .deleteQueries(deleteQueries)
                .awsConfig(awsConfig)
                .tempDirectoryPath(tempDirectoryPath)
                .heapSizeBytes(heapSizeBytes)
                .debugEnabled(debugEnabled));
        }

        /**
         * Backward compatibility: 7-parameter constructor with AWS config.
         * @deprecated Use {@link #builder()} instead
         */
        @Deprecated
        public MergeConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, List<String> deleteQueries, AwsConfig awsConfig) {
            this(builder()
                .indexUid(indexUid)
                .sourceId(sourceId)
                .nodeId(nodeId)
                .docMappingUid(docMappingUid)
                .partitionId(partitionId)
                .deleteQueries(deleteQueries)
                .awsConfig(awsConfig));
        }

        // Getters
        public String getIndexUid() { return indexUid; }
        public String getSourceId() { return sourceId; }
        public String getNodeId() { return nodeId; }
        public String getDocMappingUid() { return docMappingUid; }
        public long getPartitionId() { return partitionId; }
        public List<String> getDeleteQueries() { return deleteQueries; }
        public AwsConfig getAwsConfig() { return awsConfig; }
        public AzureConfig getAzureConfig() { return azureConfig; }
        public String getTempDirectoryPath() { return tempDirectoryPath; }
        public Long getHeapSizeBytes() { return heapSizeBytes; }
        public boolean isDebugEnabled() { return debugEnabled; }

        /**
         * Builder for MergeConfig using fluent API.
         */
        public static class Builder {
            private String indexUid;
            private String sourceId;
            private String nodeId;
            private String docMappingUid = "default";
            private long partitionId = 0L;
            private List<String> deleteQueries = null;
            private AwsConfig awsConfig = null;
            private AzureConfig azureConfig = null;
            private String tempDirectoryPath = null;
            private Long heapSizeBytes = null;
            private boolean debugEnabled = false;

            private Builder() {}

            public Builder indexUid(String indexUid) {
                this.indexUid = indexUid;
                return this;
            }

            public Builder sourceId(String sourceId) {
                this.sourceId = sourceId;
                return this;
            }

            public Builder nodeId(String nodeId) {
                this.nodeId = nodeId;
                return this;
            }

            public Builder docMappingUid(String docMappingUid) {
                this.docMappingUid = docMappingUid;
                return this;
            }

            public Builder partitionId(long partitionId) {
                this.partitionId = partitionId;
                return this;
            }

            public Builder deleteQueries(List<String> deleteQueries) {
                this.deleteQueries = deleteQueries;
                return this;
            }

            public Builder awsConfig(AwsConfig awsConfig) {
                this.awsConfig = awsConfig;
                return this;
            }

            public Builder azureConfig(AzureConfig azureConfig) {
                this.azureConfig = azureConfig;
                return this;
            }

            public Builder tempDirectoryPath(String tempDirectoryPath) {
                this.tempDirectoryPath = tempDirectoryPath;
                return this;
            }

            public Builder heapSizeBytes(Long heapSizeBytes) {
                this.heapSizeBytes = heapSizeBytes;
                return this;
            }

            public Builder debugEnabled(boolean debugEnabled) {
                this.debugEnabled = debugEnabled;
                return this;
            }

            public MergeConfig build() {
                if (indexUid == null || indexUid.trim().isEmpty()) {
                    throw new IllegalArgumentException("indexUid cannot be null or empty");
                }
                if (sourceId == null || sourceId.trim().isEmpty()) {
                    throw new IllegalArgumentException("sourceId cannot be null or empty");
                }
                if (nodeId == null || nodeId.trim().isEmpty()) {
                    throw new IllegalArgumentException("nodeId cannot be null or empty");
                }
                return new MergeConfig(this);
            }
        }

        // ============================================================
        // Global Concurrency Configuration (static methods)
        // ============================================================

        /**
         * Configure global concurrency settings for all merge operations.
         *
         * <p>These settings control the native Tokio runtime and global semaphores
         * that limit concurrent I/O operations across ALL merge operations.
         * Configuration must be applied before any merge operations are performed.</p>
         *
         * <h3>Example Usage:</h3>
         * <pre>{@code
         * // Configure at application startup
         * MergeConfig.configureGlobalConcurrency()
         *     .workerThreads(16)
         *     .maxConcurrentDownloads(32)
         *     .maxConcurrentUploads(16)
         *     .apply();
         *
         * // Then use MergeConfig normally for per-merge settings
         * MergeConfig config = MergeConfig.builder()
         *     .indexUid("my-index")
         *     .sourceId("source")
         *     .nodeId("node")
         *     .awsConfig(aws)
         *     .build();
         * }</pre>
         *
         * @return a builder for global concurrency configuration
         * @since 0.28.0
         */
        public static GlobalConcurrencyBuilder configureGlobalConcurrency() {
            return new GlobalConcurrencyBuilder();
        }

        /**
         * Gets the default thread count (number of CPU cores on this system).
         * @since 0.28.0
         */
        public static int getDefaultThreadCount() {
            return getDefaultThreadCountNative();
        }

        /**
         * Gets the currently configured number of worker threads.
         * @since 0.28.0
         */
        public static int getGlobalWorkerThreads() {
            return getGlobalWorkerThreadsNative();
        }

        /**
         * Gets the currently configured maximum concurrent downloads.
         * @since 0.28.0
         */
        public static int getGlobalMaxConcurrentDownloads() {
            return getGlobalMaxConcurrentDownloadsNative();
        }

        /**
         * Gets the currently configured maximum concurrent uploads.
         * @since 0.28.0
         */
        public static int getGlobalMaxConcurrentUploads() {
            return getGlobalMaxConcurrentUploadsNative();
        }

        /**
         * Builder for global concurrency configuration.
         *
         * <p>All settings default to the number of CPU cores on the system.
         * A value of 0 or negative means "use default" (num_cpus).</p>
         *
         * @since 0.28.0
         */
        public static class GlobalConcurrencyBuilder {
            private int workerThreads = 0;           // 0 = use default (num_cpus)
            private int maxConcurrentDownloads = 0;  // 0 = use default (num_cpus)
            private int maxConcurrentUploads = 0;    // 0 = use default (num_cpus)

            private GlobalConcurrencyBuilder() {}

            /**
             * Sets the number of Tokio worker threads for async I/O operations.
             *
             * <p>These threads handle all async I/O including downloads, uploads,
             * and storage access. A value of 0 or negative uses the default
             * (number of CPU cores).</p>
             *
             * @param threads number of worker threads, or 0 for default
             * @return this builder
             */
            public GlobalConcurrencyBuilder workerThreads(int threads) {
                this.workerThreads = threads;
                return this;
            }

            /**
             * Sets the maximum number of concurrent split downloads.
             *
             * <p>This limits how many splits can be downloaded simultaneously
             * across ALL merge operations. A value of 0 or negative uses the
             * default (number of CPU cores).</p>
             *
             * @param maxDownloads maximum concurrent downloads, or 0 for default
             * @return this builder
             */
            public GlobalConcurrencyBuilder maxConcurrentDownloads(int maxDownloads) {
                this.maxConcurrentDownloads = maxDownloads;
                return this;
            }

            /**
             * Sets the maximum number of concurrent split uploads.
             *
             * <p>This limits how many merged splits can be uploaded simultaneously
             * to cloud storage across ALL merge operations. A value of 0 or negative
             * uses the default (number of CPU cores).</p>
             *
             * @param maxUploads maximum concurrent uploads, or 0 for default
             * @return this builder
             */
            public GlobalConcurrencyBuilder maxConcurrentUploads(int maxUploads) {
                this.maxConcurrentUploads = maxUploads;
                return this;
            }

            /**
             * Applies this configuration to the native runtime.
             *
             * <p>This must be called before any merge operations are performed.
             * Can only be called once; subsequent calls will return false and
             * have no effect.</p>
             *
             * @return true if configuration was applied, false if runtime was already initialized
             */
            public boolean apply() {
                return configureGlobalConcurrencyNative(workerThreads, maxConcurrentDownloads, maxConcurrentUploads);
            }
        }

        // Native methods for global concurrency configuration
        private static native boolean configureGlobalConcurrencyNative(int workerThreads, int maxConcurrentDownloads, int maxConcurrentUploads);
        private static native int getGlobalWorkerThreadsNative();
        private static native int getGlobalMaxConcurrentDownloadsNative();
        private static native int getGlobalMaxConcurrentUploadsNative();
        private static native int getDefaultThreadCountNative();
    }

    /**
     * Create a Quickwit split from external parquet files (Parquet Companion Mode).
     *
     * <p>This creates a minimal split that references external parquet files for document
     * storage. The split contains only the tantivy index (for search) and a parquet manifest
     * (for document retrieval). This reduces split size by 80-90% compared to standard splits.
     *
     * <p>The pipeline:
     * <ol>
     *   <li>Reads parquet schema from the first file</li>
     *   <li>Validates schema consistency across all files</li>
     *   <li>Derives tantivy schema from parquet schema</li>
     *   <li>Iterates rows and builds tantivy index</li>
     *   <li>Computes column statistics for split pruning</li>
     *   <li>Creates split with embedded parquet manifest</li>
     * </ol>
     *
     * @param parquetFiles List of absolute paths to parquet files
     * @param outputPath Path where the split file should be written (must end with .split)
     * @param config Parquet companion configuration (table root, fast field mode, etc.)
     * @return Metadata about the created split, including column statistics
     * @throws IllegalArgumentException if validation fails
     * @throws RuntimeException if split creation fails
     */
    @SuppressWarnings("unchecked")
    public static SplitMetadata createFromParquet(List<String> parquetFiles, String outputPath,
                                                   ParquetCompanionConfig config) {
        if (parquetFiles == null || parquetFiles.isEmpty()) {
            throw new IllegalArgumentException("Parquet file list cannot be null or empty");
        }
        if (outputPath == null || !outputPath.endsWith(".split")) {
            throw new IllegalArgumentException("Output path must end with .split");
        }
        if (config == null) {
            throw new IllegalArgumentException("Parquet companion config cannot be null");
        }

        String resolvedOutputPath = FileSystemConfig.hasGlobalRoot()
                ? FileSystemConfig.resolvePath(outputPath) : outputPath;

        // Build config JSON for native layer
        String configJson = config.toIndexingConfigJson();

        // Call native method - returns HashMap with "metadata" and "columnStatistics"
        Map<String, Object> result = (Map<String, Object>) nativeCreateFromParquet(
                parquetFiles, resolvedOutputPath, configJson);

        if (result == null) {
            throw new RuntimeException("createFromParquet returned null result");
        }

        SplitMetadata metadata = (SplitMetadata) result.get("metadata");
        if (metadata == null) {
            throw new RuntimeException("createFromParquet result missing metadata");
        }

        // Attach column statistics to metadata
        Map<String, ColumnStatistics> stats =
                (Map<String, ColumnStatistics>) result.get("columnStatistics");
        if (stats != null) {
            metadata.setColumnStatistics(stats);
        }

        return metadata;
    }

    /**
     * Merge multiple Quickwit splits into a single split file with full S3 remote support.
     * 
     * This function implements Quickwit's split merging functionality with comprehensive 
     * support for local files, file URLs, and S3 remote splits. The process:
     * 1. Downloads/opens all source split files (from local disk or S3)
     * 2. Extracts Tantivy index directories from each split
     * 3. Uses Tantivy's segment merging to combine all content
     * 4. Applies any delete queries during the merge process
     * 5. Creates a new split file with merged content and updated metadata
     * 
     * <h3>Supported URL Types:</h3>
     * <ul>
     * <li><strong>Local paths:</strong> {@code /path/to/split.split}</li>
     * <li><strong>File URLs:</strong> {@code file:///path/to/split.split}</li>
     * <li><strong>S3 URLs:</strong> {@code s3://bucket/path/split.split} (requires AWS config)</li>
     * </ul>
     * 
     * <h3>S3 Remote Split Support:</h3>
     * When merging S3 splits, provide AWS credentials via {@link MergeConfig#getAwsConfig()}:
     * <pre>{@code
     * // Create AWS configuration
     * AwsConfig awsConfig = new AwsConfig("access-key", "secret-key", "us-east-1");
     * 
     * // Create merge config with AWS credentials  
     * MergeConfig config = new MergeConfig("index", "source", "node", awsConfig);
     * 
     * // Merge S3 splits
     * List<String> s3Splits = Arrays.asList(
     *     "s3://bucket/split1.split", 
     *     "s3://bucket/split2.split"
     * );
     * SplitMetadata result = mergeSplits(s3Splits, "/tmp/merged.split", config);
     * }</pre>
     * 
     * <h3>Mixed Protocol Support:</h3>
     * You can merge splits from multiple sources in a single operation:
     * <pre>{@code
     * List<String> mixedSplits = Arrays.asList(
     *     "/local/split1.split",                    // Local file
     *     "file:///shared/split2.split",           // File URL
     *     "s3://bucket/split3.split"               // S3 URL
     * );
     * }</pre>
     * 
     * The merged split will contain:
     * - Combined document count from all source splits
     * - Merged time ranges (if applicable)
     * - Aggregated size statistics
     * - Incremented merge operation count
     * - List of replaced split IDs for tracking
     * 
     * @param splitUrls List of split file URLs/paths to merge (local, file://, or s3://)
     * @param outputPath Path where the merged split file should be written (must end with .split)
     * @param config Configuration for the merge operation (include AwsConfig for S3 splits)
     * @return Metadata about the created merged split
     * @throws IllegalArgumentException if input validation fails or AWS credentials missing for S3 URLs
     * @throws RuntimeException if merge operation fails (network, S3 access, or merge errors)
     * 
     * @see AwsConfig for S3 credential configuration
     * @see MergeConfig#MergeConfig(String, String, String, AwsConfig) for AWS integration
     * @since 0.24.0
     */
    public static SplitMetadata mergeSplits(List<String> splitUrls, String outputPath, MergeConfig config) {
        if (splitUrls == null || splitUrls.isEmpty()) {
            throw new IllegalArgumentException("Split URLs list cannot be null or empty");
        }
        if (splitUrls.size() < 2) {
            throw new IllegalArgumentException("At least 2 splits are required for merging");
        }
        if (outputPath == null || !outputPath.endsWith(".split")) {
            throw new IllegalArgumentException("Output path must end with .split");
        }
        if (config == null) {
            throw new IllegalArgumentException("Merge configuration cannot be null");
        }
        if (config.getIndexUid() == null || config.getIndexUid().trim().isEmpty()) {
            throw new IllegalArgumentException("Index UID cannot be null or empty");
        }
        if (config.getSourceId() == null || config.getSourceId().trim().isEmpty()) {
            throw new IllegalArgumentException("Source ID cannot be null or empty");
        }
        if (config.getNodeId() == null || config.getNodeId().trim().isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        // Validate all split URLs/paths
        for (String splitUrl : splitUrls) {
            if (splitUrl == null || splitUrl.trim().isEmpty()) {
                throw new IllegalArgumentException("Split URL cannot be null or empty");
            }
            // Support both file paths and URLs
            if (!splitUrl.contains("://")) {
                // Treat as file path - must end with .split
                if (!splitUrl.endsWith(".split")) {
                    throw new IllegalArgumentException("Split file path must end with .split: " + splitUrl);
                }
            }
            // For URLs (s3://, file://), we'll validate format in native layer
        }

        // Resolve output path through file system root if it's a local path
        String resolvedOutputPath = outputPath;
        if (!outputPath.contains("://")) {
            resolvedOutputPath = FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(outputPath) : outputPath;
        }

        // Resolve local file paths in split URLs through file system root
        List<String> resolvedSplitUrls = new java.util.ArrayList<>();
        for (String splitUrl : splitUrls) {
            if (!splitUrl.contains("://")) {
                // Local file path - resolve through file system root
                resolvedSplitUrls.add(FileSystemConfig.hasGlobalRoot() ? FileSystemConfig.resolvePath(splitUrl) : splitUrl);
            } else {
                // URL (s3://, file://) - keep as is
                resolvedSplitUrls.add(splitUrl);
            }
        }

        return nativeMergeSplits(resolvedSplitUrls, resolvedOutputPath, config);
    }

    // Native method declarations
    private static native SplitMetadata nativeConvertIndex(long indexPtr, String outputPath, SplitConfig config);
    private static native SplitMetadata nativeConvertIndexFromPath(String indexPath, String outputPath, SplitConfig config);
    private static native SplitMetadata nativeReadSplitMetadata(String splitPath);
    private static native List<String> nativeListSplitFiles(String splitPath);
    private static native SplitMetadata nativeExtractSplit(String splitPath, String outputDir);
    private static native boolean nativeValidateSplit(String splitPath);
    private static native SplitMetadata nativeMergeSplits(List<String> splitUrls, String outputPath, MergeConfig config);
    private static native Object nativeCreateFromParquet(List<String> parquetFiles, String outputPath, String configJson);

    /**
     * Test helper: create a parquet file with test data.
     * Schema: id (i64), name (utf8), score (f64), active (bool).
     * Rows: numRows rows with ids starting from idOffset.
     */
    public static native void nativeWriteTestParquet(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file WITHOUT offset index (legacy format).
     * Same schema as nativeWriteTestParquet but with offset index explicitly disabled,
     * simulating legacy parquet files that don't have page-level offset information.
     * Schema: id (i64), name (utf8), score (f64), active (bool), category (utf8).
     * Rows: numRows rows with ids starting from idOffset.
     */
    public static native void nativeWriteTestParquetNoPageIndex(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file with ALL data types including complex ones.
     * Schema: id (i64), name (utf8), score (f64), active (bool),
     *         created_at (timestamp micros), tags (list&lt;utf8&gt;),
     *         address (struct{city:utf8, zip:utf8}), notes (utf8 nullable with nulls)
     */
    public static native void nativeWriteTestParquetComplex(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file with IP address columns (stored as UTF8 strings).
     * Schema: id (i64), src_ip (utf8), dst_ip (utf8), port (i64), label (utf8)
     */
    public static native void nativeWriteTestParquetWithIps(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file with JSON string columns (stored as UTF8).
     * Schema: id (i64), name (utf8), payload (utf8 — JSON objects), metadata (utf8 — JSON objects)
     */
    public static native void nativeWriteTestParquetWithJsonStrings(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file covering ALL data types.
     * Schema: id (i64), uint_val (u64), float_val (f64), bool_val (bool),
     *         text_val (utf8), binary_val (binary), ts_val (timestamp_us),
     *         date_val (date32), ip_val (utf8, for IP), tags (list<utf8>),
     *         address (struct{city,zip}), props (map<utf8,utf8>)
     */
    public static native void nativeWriteTestParquetAllTypes(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file with 10 realistic fields.
     * Schema: num_1 (i64), num_2 (i64), num_3 (f64), num_4 (f64),
     *         created_at (timestamp_us), ip_addr (utf8 — IPs),
     *         uuid_1..uuid_5 (utf8 — unique UUIDs per row)
     */
    public static native void nativeWriteTestParquetWide(String path, int numRows, long idOffset);

    /**
     * Test helper: write a parquet file for string indexing mode testing.
     * Schema: id (i64), trace_id (utf8 — pure UUIDs),
     *         message (utf8 — text with embedded UUID),
     *         error_log (utf8 — text with ERR-XXXX pattern),
     *         category (utf8 — cycling "info"/"warn"/"error")
     */
    public static native void nativeWriteTestParquetForStringIndexing(String path, int numRows, long idOffset);
}