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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * Information about a source split in the XRef.
 *
 * Each document in the XRef split represents one source split.
 * In the XRef, doc_id = split_index directly.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class XRefSplitEntry {
    @JsonProperty("uri")
    private String uri;

    @JsonProperty("split_id")
    private String splitId;

    @JsonProperty("num_docs")
    private long numDocs;

    @JsonProperty("footer_start")
    private long footerStart;

    @JsonProperty("footer_end")
    private long footerEnd;

    @JsonProperty("time_range_start")
    private Long timeRangeStart;

    @JsonProperty("time_range_end")
    private Long timeRangeEnd;

    @JsonProperty("size_bytes")
    private long sizeBytes;

    @JsonProperty("schema_hash")
    private String schemaHash;

    // Default constructor for Jackson
    public XRefSplitEntry() {}

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
     * Get the document count in this source split.
     */
    public long getNumDocs() {
        return numDocs;
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
     * Get the time range start (Unix timestamp in microseconds).
     */
    public Optional<Long> getTimeRangeStart() {
        return Optional.ofNullable(timeRangeStart);
    }

    /**
     * Get the time range end (Unix timestamp in microseconds).
     */
    public Optional<Long> getTimeRangeEnd() {
        return Optional.ofNullable(timeRangeEnd);
    }

    /**
     * Get the size in bytes.
     */
    public long getSizeBytes() {
        return sizeBytes;
    }

    /**
     * Get the schema hash for compatibility checking.
     */
    public Optional<String> getSchemaHash() {
        return Optional.ofNullable(schemaHash);
    }

    @Override
    public String toString() {
        return "XRefSplitEntry{" +
            "uri='" + uri + '\'' +
            ", splitId='" + splitId + '\'' +
            ", numDocs=" + numDocs +
            ", sizeBytes=" + sizeBytes +
            '}';
    }
}
