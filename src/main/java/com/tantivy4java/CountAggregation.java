package com.tantivy4java;

/**
 * Count aggregation that counts the number of documents.
 * This can be used for simple document counting or as a bucket count.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation docCount = new CountAggregation();
 * SearchResult result = searcher.search(query, 10, Map.of("count", docCount));
 * CountResult count = (CountResult) result.getAggregation("count");
 * System.out.println("Total documents: " + count.getCount());
 * }</pre>
 */
public class CountAggregation extends SplitAggregation {

    /**
     * Creates a count aggregation with default name.
     */
    public CountAggregation() {
        super("doc_count");
    }

    /**
     * Creates a named count aggregation.
     *
     * @param name The name for this aggregation
     */
    public CountAggregation(String name) {
        super(name);
    }

    @Override
    public String getFieldName() {
        return null; // Count doesn't operate on a specific field
    }

    @Override
    public String getAggregationType() {
        return "value_count";
    }

    @Override
    public String toAggregationJson() {
        // For count aggregation, we use value_count with _doc field
        return "{\"value_count\": {\"field\": \"_doc\"}}";
    }

    @Override
    public String toString() {
        return String.format("CountAggregation{name='%s'}", name);
    }
}