package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

/**
 * Average aggregation that computes the average value for a numeric field.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation responseAvg = new AverageAggregation("response");
 * SearchResult result = searcher.search(query, 10, Map.of("avg", responseAvg));
 * AverageResult avg = (AverageResult) result.getAggregation("avg");
 * System.out.println("Average response: " + avg.getAverage());
 * }</pre>
 */
public class AverageAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates an average aggregation for the specified field.
     *
     * @param fieldName The numeric field to average
     */
    public AverageAggregation(String fieldName) {
        super("avg_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named average aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to average
     */
    public AverageAggregation(String name, String fieldName) {
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
        return "avg";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"avg\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("AverageAggregation{name='%s', field='%s'}", name, fieldName);
    }
}