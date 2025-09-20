package com.tantivy4java;

/**
 * Result of a stats aggregation containing statistical metrics.
 * This mirrors Quickwit's stats aggregation result format.
 */
public class StatsResult implements AggregationResult {

    private final String name;
    private final long count;
    private final double sum;
    private final double min;
    private final double max;

    /**
     * Creates a stats result with the given metrics.
     *
     * @param name The name of the aggregation
     * @param count Number of documents that contributed to the stats
     * @param sum Sum of all values
     * @param min Minimum value
     * @param max Maximum value
     */
    public StatsResult(String name, long count, double sum, double min, double max) {
        this.name = name;
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "stats";
    }

    /**
     * Gets the number of documents that contributed to these stats.
     */
    public long getCount() {
        return count;
    }

    /**
     * Gets the sum of all values.
     */
    public double getSum() {
        return sum;
    }

    /**
     * Gets the average value (sum / count).
     */
    public double getAverage() {
        return count > 0 ? sum / count : 0.0;
    }

    /**
     * Gets the minimum value.
     */
    public double getMin() {
        return min;
    }

    /**
     * Gets the maximum value.
     */
    public double getMax() {
        return max;
    }

    @Override
    public String toString() {
        return String.format(
            "StatsResult{name='%s', count=%d, sum=%.2f, avg=%.2f, min=%.2f, max=%.2f}",
            name, count, sum, getAverage(), min, max
        );
    }
}