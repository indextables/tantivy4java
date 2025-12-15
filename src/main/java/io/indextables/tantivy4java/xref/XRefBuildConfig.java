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
 * An XRef split is a lightweight index that consolidates term dictionaries from
 * multiple source splits. Each document in the XRef split represents one source split,
 * enabling fast query routing to determine which splits contain matching documents.
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
 *     .includePositions(false)
 *     .build();
 * }</pre>
 */
public class XRefBuildConfig {
    private final String xrefId;
    private final String indexUid;
    private final List<XRefSourceSplit> sourceSplits;
    private final QuickwitSplit.AwsConfig awsConfig;
    private final QuickwitSplit.AzureConfig azureConfig;
    private final List<String> includedFields;
    private final boolean includePositions;

    private XRefBuildConfig(Builder builder) {
        this.xrefId = builder.xrefId;
        this.indexUid = builder.indexUid;
        this.sourceSplits = Collections.unmodifiableList(new ArrayList<>(builder.sourceSplits));
        this.awsConfig = builder.awsConfig;
        this.azureConfig = builder.azureConfig;
        this.includedFields = builder.includedFields != null
            ? Collections.unmodifiableList(new ArrayList<>(builder.includedFields))
            : Collections.emptyList();
        this.includePositions = builder.includePositions;
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
     * Check if position data should be included (for phrase query support).
     */
    public boolean isIncludePositions() {
        return includePositions;
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
        private boolean includePositions = false;

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
         * Enable position data for phrase query support.
         * Note: Positions increase XRef size but enable phrase query routing.
         */
        public Builder includePositions(boolean includePositions) {
            this.includePositions = includePositions;
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
