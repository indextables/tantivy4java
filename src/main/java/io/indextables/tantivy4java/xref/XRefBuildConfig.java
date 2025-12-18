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

package io.indextables.tantivy4java.xref;

import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for building an XRef split.
 *
 * <p>An XRef split is a lightweight index that consolidates term dictionaries from
 * multiple source splits. Each document in the XRef split represents one source split,
 * enabling fast query routing to determine which splits contain matching documents.</p>
 *
 * <p><strong>Memory Efficiency:</strong> The build process uses a streaming architecture
 * with memory-mapped I/O, enabling efficient processing of thousands of source splits
 * with minimal memory footprint (~15MB for 10,000 splits). Term dictionaries are merged
 * using an N-way streaming merge algorithm that processes terms incrementally.</p>
 *
 * <p>IMPORTANT: Source splits must include footer offsets from their SplitMetadata.
 * This follows the same pattern as other split operations in tantivy4java.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Create source splits with metadata (recommended approach)
 * XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(
 *     "s3://bucket/splits/split-001.split", splitMetadata1);
 * XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(
 *     "s3://bucket/splits/split-002.split", splitMetadata2);
 *
 * XRefBuildConfig config = XRefBuildConfig.builder()
 *     .xrefId("daily-xref-2024-01-15")
 *     .indexUid("logs-index")
 *     .sourceSplits(Arrays.asList(source1, source2))
 *     .awsConfig(new QuickwitSplit.AwsConfig("access-key", "secret-key", "us-east-1"))
 *     .filterType(FilterType.FUSE16)  // Optional: lower FPR, larger size
 *     .build();
 * }</pre>
 */
public class XRefBuildConfig {

    /**
     * Filter type for XRef Binary Fuse filters.
     *
     * <p>Controls the trade-off between filter size and false positive rate (FPR).
     * The filter type determines how many bits are used per fingerprint.</p>
     */
    public enum FilterType {
        /**
         * 8-bit fingerprints (~1.24 bytes per key).
         * <ul>
         *   <li>False positive rate: ~0.39% (1/256)</li>
         *   <li>Size for 10K splits × 50K terms: ~620 MB</li>
         *   <li>Best for: Most use cases, good balance of size and accuracy</li>
         * </ul>
         */
        FUSE8("fuse8"),

        /**
         * 16-bit fingerprints (~2.24 bytes per key).
         * <ul>
         *   <li>False positive rate: ~0.0015% (1/65536)</li>
         *   <li>Size for 10K splits × 50K terms: ~1.1 GB</li>
         *   <li>Best for: When minimizing false positives is critical</li>
         * </ul>
         */
        FUSE16("fuse16");

        private final String value;

        FilterType(String value) {
            this.value = value;
        }

        /**
         * Get the string value used in JSON serialization.
         */
        public String getValue() {
            return value;
        }

        /**
         * Get the false positive rate for this filter type.
         */
        public double getFalsePositiveRate() {
            return this == FUSE8 ? 1.0 / 256 : 1.0 / 65536;
        }

        /**
         * Get approximate bytes per key for this filter type.
         */
        public double getBytesPerKey() {
            return this == FUSE8 ? 1.24 : 2.24;
        }
    }

    /**
     * Compression type for XRef storage.
     *
     * <p>Zstd compression provides excellent compression ratios with minimal overhead.
     * The compression type is embedded in the file header, so readers automatically
     * detect and decompress regardless of the setting used during build.</p>
     */
    public enum CompressionType {
        /**
         * No compression (fastest writes, largest files).
         */
        NONE("none"),

        /**
         * Zstd compression level 1 (fastest compression, good ratio).
         */
        ZSTD1("zstd1"),

        /**
         * Zstd compression level 3 (default - good balance of speed and ratio).
         * Typically achieves 30-50% size reduction on filter data.
         */
        ZSTD3("zstd3"),

        /**
         * Zstd compression level 6 (better compression, slower).
         */
        ZSTD6("zstd6"),

        /**
         * Zstd compression level 9 (best compression, slowest).
         */
        ZSTD9("zstd9");

        private final String value;

        CompressionType(String value) {
            this.value = value;
        }

        /**
         * Get the string value used in native calls.
         */
        public String getValue() {
            return value;
        }
    }

    /**
     * Default heap size for XRef index building (50MB).
     * This is sufficient for most XRef builds since XRef documents are small.
     */
    public static final long DEFAULT_HEAP_SIZE = 50_000_000L;

    /**
     * Minimum heap size required by Tantivy (15MB).
     */
    public static final long MIN_HEAP_SIZE = 15_000_000L;

    private final String xrefId;
    private final String indexUid;
    private final List<XRefSourceSplit> sourceSplits;
    private final QuickwitSplit.AwsConfig awsConfig;
    private final QuickwitSplit.AzureConfig azureConfig;
    private final List<String> includedFields;
    private final String tempDirectoryPath;
    private final long heapSize;
    private final FilterType filterType;
    private final CompressionType compression;

    private XRefBuildConfig(Builder builder) {
        this.xrefId = builder.xrefId;
        this.indexUid = builder.indexUid;
        this.sourceSplits = Collections.unmodifiableList(new ArrayList<>(builder.sourceSplits));
        this.awsConfig = builder.awsConfig;
        this.azureConfig = builder.azureConfig;
        this.includedFields = builder.includedFields != null
            ? Collections.unmodifiableList(new ArrayList<>(builder.includedFields))
            : Collections.emptyList();
        this.tempDirectoryPath = builder.tempDirectoryPath;
        this.heapSize = builder.heapSize;
        this.filterType = builder.filterType;
        this.compression = builder.compression;
    }

    /**
     * Create a new builder for XRefBuildConfig.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get the XRef split ID.
     */
    public String getXrefId() {
        return xrefId;
    }

    /**
     * Get the index UID for Quickwit compatibility.
     */
    public String getIndexUid() {
        return indexUid;
    }

    /**
     * Get the list of source splits with their metadata.
     */
    public List<XRefSourceSplit> getSourceSplits() {
        return sourceSplits;
    }

    /**
     * Get the AWS configuration for S3 access.
     */
    public QuickwitSplit.AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * Get the Azure configuration for Azure Blob Storage access.
     */
    public QuickwitSplit.AzureConfig getAzureConfig() {
        return azureConfig;
    }

    /**
     * Get the list of fields to include in the XRef (empty = all indexed fields).
     */
    public List<String> getIncludedFields() {
        return includedFields;
    }

    /**
     * Get the custom temporary directory path for intermediate files.
     * Returns null if the system default should be used.
     */
    public String getTempDirectoryPath() {
        return tempDirectoryPath;
    }

    /**
     * Get the heap size for the index writer in bytes.
     */
    public long getHeapSize() {
        return heapSize;
    }

    /**
     * Get the filter type for Binary Fuse filters.
     * Default is FUSE8 (~0.39% false positive rate).
     */
    public FilterType getFilterType() {
        return filterType;
    }

    /**
     * Get the compression type for output files.
     * Default is ZSTD3 (good balance of speed and compression).
     */
    public CompressionType getCompression() {
        return compression;
    }

    /**
     * Builder for XRefBuildConfig.
     */
    public static class Builder {
        private String xrefId;
        private String indexUid;
        private List<XRefSourceSplit> sourceSplits = new ArrayList<>();
        private QuickwitSplit.AwsConfig awsConfig;
        private QuickwitSplit.AzureConfig azureConfig;
        private List<String> includedFields;
        private String tempDirectoryPath;
        private long heapSize = DEFAULT_HEAP_SIZE;
        private FilterType filterType = FilterType.FUSE8;
        private CompressionType compression = CompressionType.ZSTD3;

        /**
         * Set the XRef split ID.
         */
        public Builder xrefId(String xrefId) {
            this.xrefId = xrefId;
            return this;
        }

        /**
         * Set the index UID for Quickwit compatibility.
         */
        public Builder indexUid(String indexUid) {
            this.indexUid = indexUid;
            return this;
        }

        /**
         * Set the list of source splits with their metadata.
         *
         * @param sourceSplits List of XRefSourceSplit with footer offsets
         */
        public Builder sourceSplits(List<XRefSourceSplit> sourceSplits) {
            this.sourceSplits = new ArrayList<>(sourceSplits);
            return this;
        }

        /**
         * Add a source split to the list.
         *
         * @param sourceSplit Source split with footer offsets
         */
        public Builder addSourceSplit(XRefSourceSplit sourceSplit) {
            this.sourceSplits.add(sourceSplit);
            return this;
        }

        /**
         * Add a source split using URI and SplitMetadata.
         *
         * This is a convenience method that creates an XRefSourceSplit
         * from existing SplitMetadata.
         *
         * @param uri      Split URI
         * @param metadata Split metadata with footer offsets
         */
        public Builder addSourceSplit(String uri, QuickwitSplit.SplitMetadata metadata) {
            this.sourceSplits.add(XRefSourceSplit.fromSplitMetadata(uri, metadata));
            return this;
        }

        /**
         * Set the AWS configuration for S3 access.
         */
        public Builder awsConfig(QuickwitSplit.AwsConfig awsConfig) {
            this.awsConfig = awsConfig;
            return this;
        }

        /**
         * Set the Azure configuration for Azure Blob Storage access.
         */
        public Builder azureConfig(QuickwitSplit.AzureConfig azureConfig) {
            this.azureConfig = azureConfig;
            return this;
        }

        /**
         * Set the list of fields to include in the XRef (empty = all indexed fields).
         */
        public Builder includedFields(List<String> includedFields) {
            this.includedFields = new ArrayList<>(includedFields);
            return this;
        }

        /**
         * Set a custom temporary directory path for intermediate files.
         * If not set, the system default temporary directory will be used.
         *
         * @param tempDirectoryPath Path to use for temporary files
         */
        public Builder tempDirectoryPath(String tempDirectoryPath) {
            this.tempDirectoryPath = tempDirectoryPath;
            return this;
        }

        /**
         * Set the heap size for the index writer.
         * Default is {@link #DEFAULT_HEAP_SIZE} (50MB).
         * Minimum is {@link #MIN_HEAP_SIZE} (15MB).
         *
         * @param heapSize Heap size in bytes
         * @throws IllegalArgumentException if heapSize is less than MIN_HEAP_SIZE
         */
        public Builder heapSize(long heapSize) {
            if (heapSize < MIN_HEAP_SIZE) {
                throw new IllegalArgumentException(
                    "Heap size must be at least " + MIN_HEAP_SIZE + " bytes (15MB). " +
                    "Consider using XRefBuildConfig.DEFAULT_HEAP_SIZE (" + DEFAULT_HEAP_SIZE + ")."
                );
            }
            this.heapSize = heapSize;
            return this;
        }

        /**
         * Set the filter type for Binary Fuse filters.
         *
         * <p>This controls the trade-off between filter size and false positive rate:</p>
         * <ul>
         *   <li>{@link FilterType#FUSE8} (default): ~0.39% FPR, ~1.24 bytes/key</li>
         *   <li>{@link FilterType#FUSE16}: ~0.0015% FPR, ~2.24 bytes/key (260× fewer false positives)</li>
         * </ul>
         *
         * @param filterType The filter type to use
         */
        public Builder filterType(FilterType filterType) {
            if (filterType == null) {
                throw new IllegalArgumentException("filterType cannot be null");
            }
            this.filterType = filterType;
            return this;
        }

        /**
         * Set the compression type for output files.
         *
         * <p>Compression is applied to the filter data section. The compression type
         * is embedded in the file header, so readers automatically detect and decompress
         * regardless of the setting used during build.</p>
         *
         * <ul>
         *   <li>{@link CompressionType#NONE}: Fastest writes, largest files</li>
         *   <li>{@link CompressionType#ZSTD1}: Fast compression, good ratio</li>
         *   <li>{@link CompressionType#ZSTD3} (default): Good balance of speed and compression</li>
         *   <li>{@link CompressionType#ZSTD6}: Better compression, slower</li>
         *   <li>{@link CompressionType#ZSTD9}: Best compression, slowest</li>
         * </ul>
         *
         * @param compression The compression type to use
         */
        public Builder compression(CompressionType compression) {
            if (compression == null) {
                throw new IllegalArgumentException("compression cannot be null");
            }
            this.compression = compression;
            return this;
        }

        /**
         * Build the XRefBuildConfig.
         */
        public XRefBuildConfig build() {
            if (xrefId == null || xrefId.isEmpty()) {
                throw new IllegalArgumentException("xrefId is required");
            }
            if (indexUid == null || indexUid.isEmpty()) {
                throw new IllegalArgumentException("indexUid is required");
            }
            if (sourceSplits.isEmpty()) {
                throw new IllegalArgumentException("At least one source split is required");
            }
            return new XRefBuildConfig(this);
        }
    }
}
