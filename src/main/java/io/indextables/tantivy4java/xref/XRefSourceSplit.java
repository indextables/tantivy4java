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

/**
 * Source split information for XRef building.
 *
 * Contains the URI and footer offsets needed to efficiently open the split.
 * This class follows the same pattern as QuickwitSplit.SplitMetadata for
 * footer offset handling.
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Create from existing SplitMetadata
 * XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(
 *     "s3://bucket/splits/split-001.split",
 *     splitMetadata
 * );
 *
 * // Or create directly with required fields
 * XRefSourceSplit source = new XRefSourceSplit(
 *     "s3://bucket/splits/split-001.split",
 *     "split-001",
 *     footerStart,
 *     footerEnd
 * );
 * }</pre>
 */
public class XRefSourceSplit {
    private final String uri;
    private final String splitId;
    private final long footerStart;
    private final long footerEnd;
    private final Long numDocs;
    private final Long sizeBytes;
    private final String docMappingJson;  // Used to derive the schema

    /**
     * Create a new XRefSourceSplit with required fields.
     *
     * @param uri         Split URI (file paths, file://, s3://, azure://)
     * @param splitId     Split ID (for quick lookup)
     * @param footerStart Footer start offset (required for efficient split opening)
     * @param footerEnd   Footer end offset (required for efficient split opening)
     */
    public XRefSourceSplit(String uri, String splitId, long footerStart, long footerEnd) {
        this(uri, splitId, footerStart, footerEnd, null, null, null);
    }

    /**
     * Create a new XRefSourceSplit with all fields.
     *
     * @param uri            Split URI (file paths, file://, s3://, azure://)
     * @param splitId        Split ID (for quick lookup)
     * @param footerStart    Footer start offset (required for efficient split opening)
     * @param footerEnd      Footer end offset (required for efficient split opening)
     * @param numDocs        Optional document count in source split
     * @param sizeBytes      Optional size in bytes
     * @param docMappingJson Optional doc mapping JSON for schema derivation
     */
    public XRefSourceSplit(String uri, String splitId, long footerStart, long footerEnd,
                           Long numDocs, Long sizeBytes, String docMappingJson) {
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException("URI is required");
        }
        if (splitId == null || splitId.isEmpty()) {
            throw new IllegalArgumentException("Split ID is required");
        }
        if (footerEnd <= 0) {
            throw new IllegalArgumentException("Footer end offset must be > 0, got: " + footerEnd);
        }
        if (footerEnd <= footerStart) {
            throw new IllegalArgumentException(
                "Footer end (" + footerEnd + ") must be > footer start (" + footerStart + ")");
        }

        this.uri = uri;
        this.splitId = splitId;
        this.footerStart = footerStart;
        this.footerEnd = footerEnd;
        this.numDocs = numDocs;
        this.sizeBytes = sizeBytes;
        this.docMappingJson = docMappingJson;
    }

    /**
     * Create an XRefSourceSplit from existing SplitMetadata.
     *
     * This is the recommended way to create source splits when you have
     * existing split metadata from split creation or merge operations.
     * Includes the docMappingJson for schema derivation.
     *
     * @param uri      Split URI (file paths, file://, s3://, azure://)
     * @param metadata Split metadata with footer offsets and doc mapping
     * @return New XRefSourceSplit
     */
    public static XRefSourceSplit fromSplitMetadata(String uri, QuickwitSplit.SplitMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("SplitMetadata is required");
        }
        if (!metadata.hasFooterOffsets()) {
            throw new IllegalArgumentException(
                "SplitMetadata must have footer offsets for XRef building");
        }

        return new XRefSourceSplit(
            uri,
            metadata.getSplitId(),
            metadata.getFooterStartOffset(),
            metadata.getFooterEndOffset(),
            metadata.getNumDocs(),
            metadata.getUncompressedSizeBytes(),
            metadata.getDocMappingJson()
        );
    }

    /**
     * Get the split URI.
     */
    public String getUri() {
        return uri;
    }

    /**
     * Get the split ID.
     */
    public String getSplitId() {
        return splitId;
    }

    /**
     * Get the footer start offset.
     */
    public long getFooterStart() {
        return footerStart;
    }

    /**
     * Get the footer end offset.
     */
    public long getFooterEnd() {
        return footerEnd;
    }

    /**
     * Get the optional document count.
     */
    public Long getNumDocs() {
        return numDocs;
    }

    /**
     * Get the optional size in bytes.
     */
    public Long getSizeBytes() {
        return sizeBytes;
    }

    /**
     * Get the optional doc mapping JSON (used to derive the schema).
     */
    public String getDocMappingJson() {
        return docMappingJson;
    }

    /**
     * Check if this source split has doc mapping JSON.
     */
    public boolean hasDocMappingJson() {
        return docMappingJson != null && !docMappingJson.isEmpty();
    }

    @Override
    public String toString() {
        return "XRefSourceSplit{" +
            "uri='" + uri + '\'' +
            ", splitId='" + splitId + '\'' +
            ", footerStart=" + footerStart +
            ", footerEnd=" + footerEnd +
            ", numDocs=" + numDocs +
            ", sizeBytes=" + sizeBytes +
            ", hasDocMapping=" + hasDocMappingJson() +
            '}';
    }
}
