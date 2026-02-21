package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

/**
 * Cardinality aggregation that estimates the number of distinct values for a field.
 * Uses HyperLogLog++ internally for an approximate count.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation cardinality = new CardinalityAggregation("category");
 *
 * SearchResult result = searcher.search(query, 10, Map.of("unique_cats", cardinality));
 * CardinalityResult cr = (CardinalityResult) result.getAggregation("unique_cats");
 * System.out.println("Distinct categories: " + cr.getValue());
 * }</pre>
 */
public class CardinalityAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a cardinality aggregation for a specific field.
     *
     * @param fieldName The field to count distinct values from
     */
    public CardinalityAggregation(String fieldName) {
        super("cardinality_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named cardinality aggregation for a specific field.
     *
     * @param name The name for this aggregation
     * @param fieldName The field to count distinct values from
     */
    public CardinalityAggregation(String name, String fieldName) {
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
        return "cardinality";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"cardinality\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("CardinalityAggregation{name='%s', field='%s'}", name, fieldName);
    }
}
