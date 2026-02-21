package io.indextables.tantivy4java.aggregation;

/**
 * Result of a cardinality aggregation (estimated distinct count).
 */
public class CardinalityResult implements AggregationResult {

    private final String name;
    private final long value;

    /**
     * Creates a cardinality result.
     *
     * @param name The name of the aggregation
     * @param value The estimated number of distinct values
     */
    public CardinalityResult(String name, long value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "cardinality";
    }

    /**
     * Gets the estimated number of distinct values.
     */
    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("CardinalityResult{name='%s', value=%d}", name, value);
    }
}
