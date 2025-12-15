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
 * Information about a matching split from XRef search.
 *
 * Contains the split's URI, ID, document count, and relevance score.
 * The footer offsets can be used to quickly open the split for searching.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MatchingSplit {
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

    @JsonProperty("score")
    private float score;

    // Default constructor for Jackson
    public MatchingSplit() {}

    // Setters for Java-side construction

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setSplitId(String splitId) {
        this.splitId = splitId;
    }

    public void setNumDocs(long numDocs) {
        this.numDocs = numDocs;
    }

    public void setFooterStart(long footerStart) {
        this.footerStart = footerStart;
    }

    public void setFooterEnd(long footerEnd) {
        this.footerEnd = footerEnd;
    }

    public void setTimeRangeStart(Long timeRangeStart) {
        this.timeRangeStart = timeRangeStart;
    }

    public void setTimeRangeEnd(Long timeRangeEnd) {
        this.timeRangeEnd = timeRangeEnd;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public void setScore(float score) {
        this.score = score;
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
     * Get the document count in this source split.
     */
    public long getNumDocs() {
        return numDocs;
    }

    /**
     * Get the footer start offset for fast split opening.
     */
    public long getFooterStart() {
        return footerStart;
    }

    /**
     * Get the footer end offset for fast split opening.
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
     * Get the search relevance score.
     * Higher scores indicate more relevant splits (based on term coverage).
     */
    public float getScore() {
        return score;
    }

    @Override
    public String toString() {
        return "MatchingSplit{" +
            "uri='" + uri + '\'' +
            ", splitId='" + splitId + '\'' +
            ", numDocs=" + numDocs +
            ", score=" + score +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchingSplit that = (MatchingSplit) o;
        return uri != null ? uri.equals(that.uri) : that.uri == null;
    }

    @Override
    public int hashCode() {
        return uri != null ? uri.hashCode() : 0;
    }
}
