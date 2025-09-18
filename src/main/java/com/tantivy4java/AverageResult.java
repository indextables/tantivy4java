package com.tantivy4java;

/**
 * Result of an average aggregation.
 */
public class AverageResult implements AggregationResult {

    private final String name;
    private final double average;

    /**
     * Creates an average result.
     *
     * @param name The name of the aggregation
     * @param average The average value
     */
    public AverageResult(String name, double average) {
        this.name = name;
        this.average = average;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "avg";
    }

    /**
     * Gets the average value.
     */
    public double getAverage() {
        return average;
    }

    @Override
    public String toString() {
        return String.format("AverageResult{name='%s', average=%.2f}", name, average);
    }
}