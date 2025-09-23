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

package com.tantivy4java;

import java.time.Instant;
import java.util.ArrayList;
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
        private final List<String> skippedSplits;  // URLs/paths of splits that failed to parse

        /**
         * Full constructor with all Quickwit-compatible fields.
         */
        public SplitMetadata(String splitId, String indexUid, long partitionId, String sourceId,
                           String nodeId, long numDocs, long uncompressedSizeBytes,
                           Instant timeRangeStart, Instant timeRangeEnd, long createTimestamp,
                           String maturity, Set<String> tags, long footerStartOffset, long footerEndOffset,
                           long deleteOpstamp, int numMergeOps, String docMappingUid, String docMappingJson,
                           List<String> skippedSplits) {
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
                           List<String> skippedSplits) {
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

        // Skipped splits getter (for merge operations)
        public List<String> getSkippedSplits() { return new ArrayList<>(skippedSplits); }

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
     * Configuration for split merging operations with optional S3/AWS support.
     * 
     * <p>This class configures split merge operations, including metadata for the resulting
     * merged split and optional AWS credentials for accessing remote S3 splits.
     * 
     * <h3>Usage Examples:</h3>
     * 
     * <h4>Local Split Merging:</h4>
     * <pre>{@code
     * MergeConfig config = new MergeConfig("my-index", "my-source", "my-node");
     * }</pre>
     * 
     * <h4>S3 Remote Split Merging:</h4>
     * <pre>{@code
     * AwsConfig awsConfig = new AwsConfig("access-key", "secret-key", "us-east-1");
     * MergeConfig config = new MergeConfig("my-index", "my-source", "my-node", awsConfig);
     * }</pre>
     * 
     * <h4>Advanced Configuration:</h4>
     * <pre>{@code
     * AwsConfig awsConfig = new AwsConfig("access", "secret", "us-east-1");
     * List<String> deleteQueries = Arrays.asList("deleted:true");
     * 
     * MergeConfig config = new MergeConfig(
     *     "distributed-index",    // Index UID
     *     "data-source",         // Source ID  
     *     "worker-node",         // Node ID
     *     "mapping-v1",          // Document mapping UID
     *     42L,                   // Partition ID
     *     deleteQueries,         // Delete queries to apply
     *     awsConfig              // AWS credentials for S3
     * );
     * }</pre>
     * 
     * @see AwsConfig for S3 credential configuration
     * @see #mergeSplits(List, String, MergeConfig) for usage
     * @since 0.24.0
     */
    public static class MergeConfig {
        private final String indexUid;
        private final String sourceId;
        private final String nodeId;
        private final String docMappingUid;
        private final long partitionId;
        private final List<String> deleteQueries;
        private final AwsConfig awsConfig;
        private final String tempDirectoryPath;
        private final Long heapSizeBytes;

        /**
         * Create a new merge configuration.
         *
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param docMappingUid Document mapping unique identifier (must match across all splits)
         * @param partitionId Partition identifier (default: 0)
         * @param deleteQueries Optional list of delete queries to apply during merge
         * @param awsConfig AWS configuration for S3 access (optional)
         * @param tempDirectoryPath Custom path for temporary directories (optional, platform-specific)
         * @param heapSizeBytes Heap size in bytes for merge operations (optional, null for default 15MB)
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, List<String> deleteQueries, AwsConfig awsConfig, String tempDirectoryPath, Long heapSizeBytes) {
            this.indexUid = indexUid;
            this.sourceId = sourceId;
            this.nodeId = nodeId;
            this.docMappingUid = docMappingUid;
            this.partitionId = partitionId;
            this.deleteQueries = deleteQueries;
            this.awsConfig = awsConfig;
            this.tempDirectoryPath = tempDirectoryPath;
            this.heapSizeBytes = heapSizeBytes;
        }

        /**
         * Create a new merge configuration without temp directory customization.
         *
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param docMappingUid Document mapping unique identifier (must match across all splits)
         * @param partitionId Partition identifier (default: 0)
         * @param deleteQueries Optional list of delete queries to apply during merge
         * @param awsConfig AWS configuration for S3 access (optional)
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, List<String> deleteQueries, AwsConfig awsConfig) {
            this(indexUid, sourceId, nodeId, docMappingUid, partitionId, deleteQueries, awsConfig, null, null);
        }
        
        /**
         * Create a new merge configuration without AWS config.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param docMappingUid Document mapping unique identifier (must match across all splits)
         * @param partitionId Partition identifier (default: 0)
         * @param deleteQueries Optional list of delete queries to apply during merge
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, List<String> deleteQueries) {
            this(indexUid, sourceId, nodeId, docMappingUid, partitionId, deleteQueries, null, null, null);
        }

        /**
         * Create a minimal merge configuration.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, null, null, null);
        }
        
        /**
         * Create a merge configuration with AWS config for S3 splits.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param awsConfig AWS configuration for S3 access
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, AwsConfig awsConfig) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, awsConfig, null, null);
        }

        /**
         * Create a merge configuration with custom temp directory for platforms like Databricks.
         *
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param awsConfig AWS configuration for S3 access
         * @param tempDirectoryPath Custom temp directory path (e.g., "/local_disk0" for Databricks)
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, AwsConfig awsConfig, String tempDirectoryPath) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, awsConfig, tempDirectoryPath, null);
        }

        /**
         * Create a merge configuration with heap size control.
         *
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that will create the merged split
         * @param heapSizeBytes Heap size in bytes for merge operations
         */
        public MergeConfig(String indexUid, String sourceId, String nodeId, Long heapSizeBytes) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, null, null, heapSizeBytes);
        }

        // Getters
        public String getIndexUid() { return indexUid; }
        public String getSourceId() { return sourceId; }
        public String getNodeId() { return nodeId; }
        public String getDocMappingUid() { return docMappingUid; }
        public long getPartitionId() { return partitionId; }
        public List<String> getDeleteQueries() { return deleteQueries; }
        public AwsConfig getAwsConfig() { return awsConfig; }
        public String getTempDirectoryPath() { return tempDirectoryPath; }
        public Long getHeapSizeBytes() { return heapSizeBytes; }
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
}