package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

/**
 * Max aggregation that finds the maximum value for a numeric field.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation responseMax = new MaxAggregation("response");
 * SearchResult result = searcher.search(query, 10, Map.of("max", responseMax));
 * MaxResult max = (MaxResult) result.getAggregation("max");
 * System.out.println("Maximum response: " + max.getMax());
 * }</pre>
 */
public class MaxAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a max aggregation for the specified field.
     *
     * @param fieldName The numeric field to find maximum for
     */
    public MaxAggregation(String fieldName) {
        super("max_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named max aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to find maximum for
     */
    public MaxAggregation(String name, String fieldName) {
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
        return "max";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"max\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("MaxAggregation{name='%s', field='%s'}", name, fieldName);
    }
}