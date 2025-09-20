package com.tantivy4java;

/**
 * Histogram aggregation that groups documents by numeric intervals.
 * This creates buckets for fixed-size intervals in a numeric field.
 * Similar to Elasticsearch's histogram aggregation and Quickwit's histogram aggregation.
 *
 * Example usage:
 * <pre>{@code
 * HistogramAggregation priceHist = new HistogramAggregation("price_distribution", "price")
 *     .setInterval(50.0);
 *
 * SearchResult result = searcher.search(query, 10, Map.of("histogram", priceHist));
 * HistogramResult hist = (HistogramResult) result.getAggregation("histogram");
 * }</pre>
 */
public class HistogramAggregation extends SplitAggregation {

    private final String fieldName;
    private double interval;
    private Double offset;
    private Double minDocCount;

    /**
     * Creates a histogram aggregation for the specified field.
     *
     * @param fieldName The numeric field to create histogram for
     * @param interval The interval size for histogram buckets
     */
    public HistogramAggregation(String fieldName, double interval) {
        super("histogram_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive");
        }
        this.fieldName = fieldName.trim();
        this.interval = interval;
    }

    /**
     * Creates a named histogram aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to create histogram for
     * @param interval The interval size for histogram buckets
     */
    public HistogramAggregation(String name, String fieldName, double interval) {
        super(name);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive");
        }
        this.fieldName = fieldName.trim();
        this.interval = interval;
    }

    /**
     * Sets the interval size for histogram buckets.
     */
    public HistogramAggregation setInterval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive");
        }
        this.interval = interval;
        return this;
    }

    /**
     * Sets the offset for bucket boundaries.
     */
    public HistogramAggregation setOffset(double offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Sets the minimum document count for buckets to be included.
     */
    public HistogramAggregation setMinDocCount(double minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getAggregationType() {
        return "histogram";
    }

    @Override
    public String toAggregationJson() {
        StringBuilder json = new StringBuilder();
        json.append("{\"histogram\": {\"field\": \"").append(fieldName).append("\"");
        json.append(", \"interval\": ").append(interval);

        if (offset != null) {
            json.append(", \"offset\": ").append(offset);
        }
        if (minDocCount != null) {
            json.append(", \"min_doc_count\": ").append(minDocCount);
        }

        json.append("}}");
        return json.toString();
    }

    public double getInterval() {
        return interval;
    }

    public Double getOffset() {
        return offset;
    }

    public Double getMinDocCount() {
        return minDocCount;
    }

    @Override
    public String toString() {
        return String.format("HistogramAggregation{name='%s', field='%s', interval=%.2f}",
                           name, fieldName, interval);
    }
}