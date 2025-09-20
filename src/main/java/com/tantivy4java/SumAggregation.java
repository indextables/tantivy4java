package com.tantivy4java;

/**
 * Sum aggregation that computes the sum of values for a numeric field.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation responseSum = new SumAggregation("response");
 * SearchResult result = searcher.search(query, 10, Map.of("sum", responseSum));
 * SumResult sum = (SumResult) result.getAggregation("sum");
 * System.out.println("Total response: " + sum.getSum());
 * }</pre>
 */
public class SumAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a sum aggregation for the specified field.
     *
     * @param fieldName The numeric field to sum
     */
    public SumAggregation(String fieldName) {
        super("sum_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named sum aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to sum
     */
    public SumAggregation(String name, String fieldName) {
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
        return "sum";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"sum\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("SumAggregation{name='%s', field='%s'}", name, fieldName);
    }
}