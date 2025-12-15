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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Metadata about an XRef split.
 *
 * Contains information about the source splits, fields, and build statistics.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class XRefMetadata {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("format_version")
    private int formatVersion;

    @JsonProperty("xref_id")
    private String xrefId;

    @JsonProperty("index_uid")
    private String indexUid;

    @JsonProperty("split_registry")
    private SplitRegistry splitRegistry;

    @JsonProperty("fields")
    private List<FieldInfo> fields;

    @JsonProperty("total_terms")
    private long totalTerms;

    @JsonProperty("build_stats")
    private BuildStats buildStats;

    @JsonProperty("created_at")
    private long createdAt;

    @JsonProperty("footer_start_offset")
    private long footerStartOffset;

    @JsonProperty("footer_end_offset")
    private long footerEndOffset;

    // Default constructor for Jackson
    public XRefMetadata() {
        this.fields = new ArrayList<>();
        this.splitRegistry = new SplitRegistry();
        this.buildStats = new BuildStats();
    }

    /**
     * Parse XRefMetadata from JSON string.
     */
    public static XRefMetadata fromJson(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, XRefMetadata.class);
    }

    /**
     * Get the format version.
     */
    public int getFormatVersion() {
        return formatVersion;
    }

    /**
     * Get the XRef split ID.
     */
    public String getXrefId() {
        return xrefId;
    }

    /**
     * Get the index UID.
     */
    public String getIndexUid() {
        return indexUid;
    }

    /**
     * Get the split registry.
     */
    public SplitRegistry getSplitRegistry() {
        return splitRegistry;
    }

    /**
     * Get the list of field information.
     */
    public List<FieldInfo> getFields() {
        return fields != null ? Collections.unmodifiableList(fields) : Collections.emptyList();
    }

    /**
     * Get the total number of unique terms.
     */
    public long getTotalTerms() {
        return totalTerms;
    }

    /**
     * Get the build statistics.
     */
    public BuildStats getBuildStats() {
        return buildStats;
    }

    /**
     * Get the creation timestamp (Unix seconds).
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Get the footer start offset of this XRef split.
     */
    public long getFooterStartOffset() {
        return footerStartOffset;
    }

    /**
     * Get the footer end offset of this XRef split.
     */
    public long getFooterEndOffset() {
        return footerEndOffset;
    }

    /**
     * Check if this metadata has valid footer offsets.
     */
    public boolean hasFooterOffsets() {
        return footerStartOffset >= 0 && footerEndOffset > footerStartOffset;
    }

    /**
     * Convert this XRefMetadata to a standard SplitMetadata for use with SplitSearcher.
     *
     * <p>This provides consistent metadata handling between XRef splits and regular splits.
     * The returned SplitMetadata can be passed directly to
     * {@link io.indextables.tantivy4java.split.SplitCacheManager#createSplitSearcher(String, QuickwitSplit.SplitMetadata)}.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * XRefMetadata xrefMetadata = XRefSplit.build(config, outputPath);
     *
     * // Use the standard SplitMetadata directly
     * QuickwitSplit.SplitMetadata splitMeta = xrefMetadata.toSplitMetadata();
     * try (SplitSearcher searcher = cacheManager.createSplitSearcher(xrefUri, splitMeta)) {
     *     // Search the XRef split
     * }
     * }</pre>
     *
     * @return A SplitMetadata representing this XRef split
     * @throws IllegalStateException if footer offsets are not available
     */
    @JsonIgnore
    public QuickwitSplit.SplitMetadata toSplitMetadata() {
        if (!hasFooterOffsets()) {
            throw new IllegalStateException(
                "XRefMetadata must have valid footer offsets to create SplitMetadata. " +
                "Ensure this metadata came from XRefSplit.build()."
            );
        }

        return new QuickwitSplit.SplitMetadata(
            xrefId,                                              // splitId
            indexUid,                                            // indexUid
            0L,                                                  // partitionId
            "xref",                                              // sourceId
            "xref-node",                                         // nodeId
            getNumSplits(),                                      // numDocs (in XRef, each doc = 1 source split)
            buildStats != null ? buildStats.getOutputSizeBytes() : 0L, // uncompressedSizeBytes
            null,                                                // timeRangeStart
            null,                                                // timeRangeEnd
            createdAt,                                           // createTimestamp
            "Mature",                                            // maturity
            Collections.emptySet(),                              // tags
            footerStartOffset,                                   // footerStartOffset
            footerEndOffset,                                     // footerEndOffset
            0L,                                                  // deleteOpstamp
            0,                                                   // numMergeOps
            "xref-doc-mapping",                                  // docMappingUid
            null,                                                // docMappingJson
            null                                                 // skippedSplits
        );
    }

    /**
     * Get the number of source splits.
     */
    public int getNumSplits() {
        return splitRegistry != null ? splitRegistry.getNumSplits() : 0;
    }

    /**
     * Get the total number of documents across all source splits.
     */
    public long getTotalSourceDocs() {
        return splitRegistry != null ? splitRegistry.getTotalDocs() : 0;
    }

    /**
     * Registry of source splits.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SplitRegistry {
        @JsonProperty("splits")
        private List<XRefSplitEntry> splits;

        public SplitRegistry() {
            this.splits = new ArrayList<>();
        }

        public List<XRefSplitEntry> getSplits() {
            return splits != null ? Collections.unmodifiableList(splits) : Collections.emptyList();
        }

        public int getNumSplits() {
            return splits != null ? splits.size() : 0;
        }

        public long getTotalDocs() {
            if (splits == null) return 0;
            return splits.stream().mapToLong(XRefSplitEntry::getNumDocs).sum();
        }
    }

    /**
     * Information about a field in the XRef.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldInfo {
        @JsonProperty("name")
        private String name;

        @JsonProperty("field_type")
        private String fieldType;

        @JsonProperty("num_terms")
        private long numTerms;

        @JsonProperty("total_postings")
        private long totalPostings;

        @JsonProperty("has_positions")
        private boolean hasPositions;

        public String getName() {
            return name;
        }

        public String getFieldType() {
            return fieldType;
        }

        public long getNumTerms() {
            return numTerms;
        }

        public long getTotalPostings() {
            return totalPostings;
        }

        public boolean hasPositions() {
            return hasPositions;
        }
    }

    /**
     * Build statistics.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BuildStats {
        @JsonProperty("build_duration_ms")
        private long buildDurationMs;

        @JsonProperty("bytes_read")
        private long bytesRead;

        @JsonProperty("output_size_bytes")
        private long outputSizeBytes;

        @JsonProperty("compression_ratio")
        private double compressionRatio;

        @JsonProperty("splits_processed")
        private int splitsProcessed;

        @JsonProperty("splits_skipped")
        private int splitsSkipped;

        @JsonProperty("unique_terms")
        private long uniqueTerms;

        public long getBuildDurationMs() {
            return buildDurationMs;
        }

        public long getBytesRead() {
            return bytesRead;
        }

        public long getOutputSizeBytes() {
            return outputSizeBytes;
        }

        public double getCompressionRatio() {
            return compressionRatio;
        }

        public int getSplitsProcessed() {
            return splitsProcessed;
        }

        public int getSplitsSkipped() {
            return splitsSkipped;
        }

        public long getUniqueTerms() {
            return uniqueTerms;
        }
    }
}
