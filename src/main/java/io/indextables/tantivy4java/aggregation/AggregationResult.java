package io.indextables.tantivy4java.aggregation;

/**
 * Base interface for aggregation results returned from SplitSearcher.
 * This follows Quickwit's aggregation result model.
 */
public interface AggregationResult {

    /**
     * Gets the name of the aggregation that produced this result.
     */
    String getName();

    /**
     * Gets the aggregation type that produced this result.
     */
    String getType();
}