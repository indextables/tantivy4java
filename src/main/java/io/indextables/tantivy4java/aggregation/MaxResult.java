package io.indextables.tantivy4java.aggregation;

/**
 * Result of a max aggregation.
 */
public class MaxResult implements AggregationResult {

    private final String name;
    private final double max;

    /**
     * Creates a max result.
     *
     * @param name The name of the aggregation
     * @param max The maximum value
     */
    public MaxResult(String name, double max) {
        this.name = name;
        this.max = max;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "max";
    }

    /**
     * Gets the maximum value.
     */
    public double getMax() {
        return max;
    }

    @Override
    public String toString() {
        return String.format("MaxResult{name='%s', max=%.2f}", name, max);
    }
}