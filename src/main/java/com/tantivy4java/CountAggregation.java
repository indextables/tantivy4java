package com.tantivy4java;

/**
 * Count aggregation that counts the number of documents that have non-null values
 * for a specified field. This uses Tantivy's value_count aggregation internally.
 *
 * Example usage:
 * <pre>{@code
 * // Count documents that have a non-null "title" field
 * SplitAggregation docCount = new CountAggregation("title");
 *
 * // With custom name
 * SplitAggregation docCount = new CountAggregation("doc_count", "title");
 *
 * SearchResult result = searcher.search(query, 10, Map.of("count", docCount));
 * CountResult count = (CountResult) result.getAggregation("count");
 * System.out.println("Documents with title: " + count.getCount());
 * }</pre>
 */
public class CountAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a count aggregation for a specific field.
     *
     * @param fieldName The field to count non-null values from (must exist in schema)
     */
    public CountAggregation(String fieldName) {
        super("count_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named count aggregation for a specific field.
     *
     * @param name The name for this aggregation
     * @param fieldName The field to count non-null values from (must exist in schema)
     */
    public CountAggregation(String name, String fieldName) {
        super(name);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getAggregationType() {
        return "value_count";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"value_count\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("CountAggregation{name='%s', field='%s'}", name, fieldName);
    }
}