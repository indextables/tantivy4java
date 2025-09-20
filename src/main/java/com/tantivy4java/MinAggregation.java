package com.tantivy4java;

/**
 * Min aggregation that finds the minimum value for a numeric field.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation responseMin = new MinAggregation("response");
 * SearchResult result = searcher.search(query, 10, Map.of("min", responseMin));
 * MinResult min = (MinResult) result.getAggregation("min");
 * System.out.println("Minimum response: " + min.getMin());
 * }</pre>
 */
public class MinAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a min aggregation for the specified field.
     *
     * @param fieldName The numeric field to find minimum for
     */
    public MinAggregation(String fieldName) {
        super("min_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named min aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to find minimum for
     */
    public MinAggregation(String name, String fieldName) {
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
        return "min";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"min\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("MinAggregation{name='%s', field='%s'}", name, fieldName);
    }
}