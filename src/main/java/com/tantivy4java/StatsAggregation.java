package com.tantivy4java;

/**
 * Stats aggregation that computes statistical metrics (count, sum, avg, min, max)
 * for a numeric field. This mirrors Quickwit's stats aggregation functionality.
 *
 * Example usage:
 * <pre>{@code
 * SplitAggregation responseStats = new StatsAggregation("response");
 * SearchResult result = searcher.search(query, 10, Map.of("stats", responseStats));
 * StatsResult stats = (StatsResult) result.getAggregation("stats");
 *
 * System.out.println("Count: " + stats.getCount());
 * System.out.println("Sum: " + stats.getSum());
 * System.out.println("Average: " + stats.getAverage());
 * System.out.println("Min: " + stats.getMin());
 * System.out.println("Max: " + stats.getMax());
 * }</pre>
 */
public class StatsAggregation extends SplitAggregation {

    private final String fieldName;

    /**
     * Creates a stats aggregation for the specified field.
     *
     * @param fieldName The numeric field to compute stats for
     */
    public StatsAggregation(String fieldName) {
        super("stats_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named stats aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to compute stats for
     */
    public StatsAggregation(String name, String fieldName) {
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
        return "stats";
    }

    @Override
    public String toAggregationJson() {
        return String.format("{\"stats\": {\"field\": \"%s\"}}", fieldName);
    }

    @Override
    public String toString() {
        return String.format("StatsAggregation{name='%s', field='%s'}", name, fieldName);
    }
}