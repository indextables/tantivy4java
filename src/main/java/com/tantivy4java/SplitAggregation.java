package com.tantivy4java;

import java.util.Map;

/**
 * Base interface for aggregations that can be performed on SplitSearcher results.
 * This follows Quickwit's aggregation model which wraps Tantivy aggregations.
 *
 * Aggregations allow computing metrics (count, sum, avg, min, max) and grouping
 * documents by field values or ranges, similar to Elasticsearch aggregations.
 *
 * Usage example:
 * <pre>{@code
 * // Create aggregations
 * SplitAggregation responseStats = new StatsAggregation("response");
 * SplitAggregation hostTerms = new TermsAggregation("host");
 *
 * // Execute search with aggregations
 * SearchResult result = searcher.search(query, 10,
 *     Map.of("stats", responseStats, "hosts", hostTerms));
 *
 * // Get aggregation results
 * StatsResult stats = (StatsResult) result.getAggregation("stats");
 * TermsResult hosts = (TermsResult) result.getAggregation("hosts");
 * }</pre>
 */
public abstract class SplitAggregation {

    protected final String name;

    protected SplitAggregation(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Aggregation name cannot be null or empty");
        }
        this.name = name.trim();
    }

    /**
     * Gets the name of this aggregation.
     */
    public String getName() {
        return name;
    }

    /**
     * Converts this aggregation to Quickwit's aggregation JSON format.
     * This mirrors Tantivy's aggregation request structure.
     */
    public abstract String toAggregationJson();

    /**
     * Gets the field name this aggregation operates on (if applicable).
     */
    public abstract String getFieldName();

    /**
     * Gets the aggregation type for JSON serialization.
     */
    public abstract String getAggregationType();
}