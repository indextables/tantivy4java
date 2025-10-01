package io.indextables.tantivy4java.aggregation;

/**
 * Result of a min aggregation.
 */
public class MinResult implements AggregationResult {

    private final String name;
    private final double min;

    /**
     * Creates a min result.
     *
     * @param name The name of the aggregation
     * @param min The minimum value
     */
    public MinResult(String name, double min) {
        this.name = name;
        this.min = min;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "min";
    }

    /**
     * Gets the minimum value.
     */
    public double getMin() {
        return min;
    }

    @Override
    public String toString() {
        return String.format("MinResult{name='%s', min=%.2f}", name, min);
    }
}