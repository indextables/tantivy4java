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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result of searching an XRef split.
 *
 * Contains the list of source splits that contain matching documents,
 * along with search statistics.
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * XRefSearchResult result = xrefSearcher.search("error AND level:critical", 1000);
 *
 * System.out.println("Query matches documents across " +
 *     result.getNumMatchingSplits() + " splits");
 *
 * // Get the split URIs to search
 * List<String> splitsToSearch = result.getSplitUrisToSearch();
 * for (String splitUri : splitsToSearch) {
 *     try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher(splitUri)) {
 *         // Perform full search on this split
 *         SearchResult fullResult = splitSearcher.search(query, 100);
 *     }
 * }
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class XRefSearchResult {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("matching_splits")
    private List<MatchingSplit> matchingSplits;

    @JsonProperty("num_matching_splits")
    private int numMatchingSplits;

    @JsonProperty("search_time_ms")
    private long searchTimeMs;

    /**
     * True if the query contained clauses that could not be evaluated
     * (e.g., range queries, wildcard queries, regex queries).
     * When true, results may include splits that don't actually match.
     */
    @JsonProperty("has_unevaluated_clauses")
    private boolean hasUnevaluatedClauses;

    // Default constructor for Jackson
    public XRefSearchResult() {
        this.matchingSplits = new ArrayList<>();
        this.hasUnevaluatedClauses = false;
    }

    // Setters for Java-side construction

    public void setMatchingSplits(List<MatchingSplit> matchingSplits) {
        this.matchingSplits = matchingSplits;
    }

    public void setNumMatchingSplits(int numMatchingSplits) {
        this.numMatchingSplits = numMatchingSplits;
    }

    public void setSearchTimeMs(long searchTimeMs) {
        this.searchTimeMs = searchTimeMs;
    }

    public void setHasUnevaluatedClauses(boolean hasUnevaluatedClauses) {
        this.hasUnevaluatedClauses = hasUnevaluatedClauses;
    }

    /**
     * Parse XRefSearchResult from JSON string.
     */
    public static XRefSearchResult fromJson(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, XRefSearchResult.class);
    }

    /**
     * Get the list of matching splits.
     */
    public List<MatchingSplit> getMatchingSplits() {
        return matchingSplits != null
            ? Collections.unmodifiableList(matchingSplits)
            : Collections.emptyList();
    }

    /**
     * Get the number of matching splits.
     */
    public int getNumMatchingSplits() {
        return numMatchingSplits;
    }

    /**
     * Get the search execution time in milliseconds.
     */
    public long getSearchTimeMs() {
        return searchTimeMs;
    }

    /**
     * Check if the query contained clauses that could not be evaluated.
     *
     * When this returns true, it means the query contained range queries,
     * wildcard queries, regex queries, or other clauses that Binary Fuse
     * filters cannot evaluate. The results may include splits that don't
     * actually match the query.
     *
     * @return true if the query had unevaluatable clauses
     */
    public boolean hasUnevaluatedClauses() {
        return hasUnevaluatedClauses;
    }

    /**
     * Get the split URIs that should be searched.
     */
    public List<String> getSplitUrisToSearch() {
        if (matchingSplits == null) return Collections.emptyList();
        return matchingSplits.stream()
            .map(MatchingSplit::getUri)
            .collect(Collectors.toList());
    }

    /**
     * Get the split IDs that should be searched.
     */
    public List<String> getSplitIdsToSearch() {
        if (matchingSplits == null) return Collections.emptyList();
        return matchingSplits.stream()
            .map(MatchingSplit::getSplitId)
            .collect(Collectors.toList());
    }

    /**
     * Get the total estimated document count across matching splits.
     */
    public long getTotalEstimatedDocs() {
        if (matchingSplits == null) return 0;
        return matchingSplits.stream()
            .mapToLong(MatchingSplit::getNumDocs)
            .sum();
    }

    /**
     * Check if any splits matched the query.
     */
    public boolean hasMatches() {
        return numMatchingSplits > 0;
    }

    @Override
    public String toString() {
        return "XRefSearchResult{" +
            "numMatchingSplits=" + numMatchingSplits +
            ", searchTimeMs=" + searchTimeMs +
            ", estimatedDocs=" + getTotalEstimatedDocs() +
            ", hasUnevaluatedClauses=" + hasUnevaluatedClauses +
            '}';
    }
}
