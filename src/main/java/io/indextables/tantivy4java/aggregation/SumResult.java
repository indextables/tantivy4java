package io.indextables.tantivy4java.aggregation;

/**
 * Result of a sum aggregation.
 */
public class SumResult implements AggregationResult {

    private final String name;
    private final double sum;

    /**
     * Creates a sum result.
     *
     * @param name The name of the aggregation
     * @param sum The sum value
     */
    public SumResult(String name, double sum) {
        this.name = name;
        this.sum = sum;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "sum";
    }

    /**
     * Gets the sum value.
     */
    public double getSum() {
        return sum;
    }

    @Override
    public String toString() {
        return String.format("SumResult{name='%s', sum=%.2f}", name, sum);
    }
}