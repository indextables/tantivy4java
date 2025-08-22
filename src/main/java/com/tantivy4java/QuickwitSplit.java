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
            this.indexUid = indexUid;
            this.sourceId = sourceId;
            this.nodeId = nodeId;
            this.docMappingUid = docMappingUid;
            this.partitionId = partitionId;
            this.timeRangeStart = timeRangeStart;
            this.timeRangeEnd = timeRangeEnd;
            this.tags = tags;
            this.metadata = metadata;
        }

        /**
         * Create a minimal split configuration.
         * 
         * @param indexUid Unique identifier for the index
         * @param sourceId Source identifier
         * @param nodeId Node identifier that created the split
         */
        public SplitConfig(String indexUid, String sourceId, String nodeId) {
            this(indexUid, sourceId, nodeId, "default", 0L, null, null, null, null);
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
    }

    /**
     * Metadata about a created split.
     */
    public static class SplitMetadata {
        private final String splitId;
        private final long numDocs;
        private final long uncompressedSizeBytes;
        private final Instant timeRangeStart;
        private final Instant timeRangeEnd;
        private final Set<String> tags;
        private final long deleteOpstamp;
        private final int numMergeOps;

        public SplitMetadata(String splitId, long numDocs, long uncompressedSizeBytes,
                           Instant timeRangeStart, Instant timeRangeEnd, Set<String> tags,
                           long deleteOpstamp, int numMergeOps) {
            this.splitId = splitId;
            this.numDocs = numDocs;
            this.uncompressedSizeBytes = uncompressedSizeBytes;
            this.timeRangeStart = timeRangeStart;
            this.timeRangeEnd = timeRangeEnd;
            this.tags = tags;
            this.deleteOpstamp = deleteOpstamp;
            this.numMergeOps = numMergeOps;
        }

        // Getters
        public String getSplitId() { return splitId; }
        public long getNumDocs() { return numDocs; }
        public long getUncompressedSizeBytes() { return uncompressedSizeBytes; }
        public Instant getTimeRangeStart() { return timeRangeStart; }
        public Instant getTimeRangeEnd() { return timeRangeEnd; }
        public Set<String> getTags() { return tags; }
        public long getDeleteOpstamp() { return deleteOpstamp; }
        public int getNumMergeOps() { return numMergeOps; }

        @Override
        public String toString() {
            return "SplitMetadata{" +
                    "splitId='" + splitId + '\'' +
                    ", numDocs=" + numDocs +
                    ", uncompressedSizeBytes=" + uncompressedSizeBytes +
                    ", timeRangeStart=" + timeRangeStart +
                    ", timeRangeEnd=" + timeRangeEnd +
                    ", tags=" + tags +
                    ", deleteOpstamp=" + deleteOpstamp +
                    ", numMergeOps=" + numMergeOps +
                    '}';
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
        if (config.getIndexUid() == null || config.getIndexUid().trim().isEmpty()) {
            throw new IllegalArgumentException("Index UID cannot be null or empty");
        }
        if (config.getSourceId() == null || config.getSourceId().trim().isEmpty()) {
            throw new IllegalArgumentException("Source ID cannot be null or empty");
        }
        if (config.getNodeId() == null || config.getNodeId().trim().isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        return nativeConvertIndex(index.getNativePtr(), outputPath, config);
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

        return nativeConvertIndexFromPath(indexPath, outputPath, config);
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

        return nativeReadSplitMetadata(splitPath);
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

        return nativeListSplitFiles(splitPath);
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

        return nativeExtractSplit(splitPath, outputDir);
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
            return nativeValidateSplit(splitPath);
        } catch (Exception e) {
            return false;
        }
    }

    // Native method declarations
    private static native SplitMetadata nativeConvertIndex(long indexPtr, String outputPath, SplitConfig config);
    private static native SplitMetadata nativeConvertIndexFromPath(String indexPath, String outputPath, SplitConfig config);
    private static native SplitMetadata nativeReadSplitMetadata(String splitPath);
    private static native List<String> nativeListSplitFiles(String splitPath);
    private static native SplitMetadata nativeExtractSplit(String splitPath, String outputDir);
    private static native boolean nativeValidateSplit(String splitPath);
}