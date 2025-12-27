package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Histogram aggregation that groups documents by numeric intervals.
 * This creates buckets for fixed-size intervals in a numeric field.
 * Similar to Elasticsearch's histogram aggregation and Quickwit's histogram aggregation.
 *
 * <p>The bucket calculation formula is:</p>
 * <pre>
 * bucket_position = floor((value - offset) / interval)
 * bucket_key = bucket_position * interval + offset
 * </pre>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Basic histogram with interval of 50
 * HistogramAggregation priceHist = new HistogramAggregation("price_distribution", "price", 50.0);
 *
 * // Histogram with offset and sub-aggregation
 * HistogramAggregation detailed = new HistogramAggregation("detailed", "price", 100.0)
 *     .setOffset(25.0)  // Shift bucket boundaries by 25
 *     .addSubAggregation(new StatsAggregation("price_stats", "price"));
 *
 * // Histogram with bounds to filter values
 * HistogramAggregation bounded = new HistogramAggregation("bounded", "score", 10.0)
 *     .setHardBounds(0.0, 100.0)   // Only include scores 0-100
 *     .setMinDocCount(1);          // Exclude empty buckets
 *
 * SearchResult result = searcher.search(query, 10, Map.of("histogram", priceHist));
 * HistogramResult hist = (HistogramResult) result.getAggregation("histogram");
 * }</pre>
 */
public class HistogramAggregation extends SplitAggregation {

    private final String fieldName;
    private double interval;
    private Double offset;
    private Long minDocCount;
    private ExtendedBounds extendedBounds;
    private HardBounds hardBounds;
    private boolean keyed = false;
    private final Map<String, SplitAggregation> subAggregations = new LinkedHashMap<>();

    /**
     * Creates a histogram aggregation for the specified field.
     *
     * @param fieldName The numeric field to create histogram for
     * @param interval The interval size for histogram buckets (must be positive)
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
     * @param interval The interval size for histogram buckets (must be positive)
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
     *
     * @param interval The interval size (must be positive)
     * @return this aggregation for method chaining
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
     * The offset shifts the bucket grid by the specified amount.
     * Must be in the range [0, interval).
     *
     * @param offset The offset value
     * @return this aggregation for method chaining
     */
    public HistogramAggregation setOffset(double offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Sets the minimum document count for buckets to be included in results.
     * Buckets with fewer documents will be excluded.
     * Note: Cannot be combined with extended_bounds.
     *
     * @param minDocCount Minimum document count (default: 0)
     * @return this aggregation for method chaining
     */
    public HistogramAggregation setMinDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Sets extended bounds to include empty buckets within the specified range.
     * This ensures buckets are created even if no documents fall within them.
     * Note: Cannot be combined with min_doc_count > 0.
     *
     * @param min Minimum value for bucket range
     * @param max Maximum value for bucket range
     * @return this aggregation for method chaining
     */
    public HistogramAggregation setExtendedBounds(double min, double max) {
        this.extendedBounds = new ExtendedBounds(min, max);
        return this;
    }

    /**
     * Sets hard bounds to filter values outside the specified range.
     * Documents with values outside these bounds are excluded from aggregation.
     *
     * @param min Minimum value (inclusive)
     * @param max Maximum value (inclusive)
     * @return this aggregation for method chaining
     */
    public HistogramAggregation setHardBounds(double min, double max) {
        this.hardBounds = new HardBounds(min, max);
        return this;
    }

    /**
     * Sets whether to return buckets as a keyed map instead of an array.
     *
     * @param keyed true for keyed output, false for array output (default)
     * @return this aggregation for method chaining
     */
    public HistogramAggregation setKeyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /**
     * Adds a sub-aggregation to be computed within each histogram bucket.
     *
     * @param subAgg The sub-aggregation to add
     * @return this aggregation for method chaining
     */
    public HistogramAggregation addSubAggregation(SplitAggregation subAgg) {
        this.subAggregations.put(subAgg.getName(), subAgg);
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
        if (keyed) {
            json.append(", \"keyed\": true");
        }
        if (extendedBounds != null) {
            json.append(", \"extended_bounds\": {");
            json.append("\"min\": ").append(extendedBounds.min);
            json.append(", \"max\": ").append(extendedBounds.max);
            json.append("}");
        }
        if (hardBounds != null) {
            json.append(", \"hard_bounds\": {");
            json.append("\"min\": ").append(hardBounds.min);
            json.append(", \"max\": ").append(hardBounds.max);
            json.append("}");
        }

        json.append("}");

        // Add sub-aggregations if any
        if (!subAggregations.isEmpty()) {
            json.append(", \"aggs\": {");
            boolean first = true;
            for (Map.Entry<String, SplitAggregation> entry : subAggregations.entrySet()) {
                if (!first) json.append(", ");
                json.append("\"").append(entry.getKey()).append("\": ");
                json.append(entry.getValue().toAggregationJson());
                first = false;
            }
            json.append("}");
        }

        json.append("}");
        return json.toString();
    }

    public double getInterval() {
        return interval;
    }

    public Double getOffset() {
        return offset;
    }

    public Long getMinDocCount() {
        return minDocCount;
    }

    public ExtendedBounds getExtendedBounds() {
        return extendedBounds;
    }

    public HardBounds getHardBounds() {
        return hardBounds;
    }

    public boolean isKeyed() {
        return keyed;
    }

    public Map<String, SplitAggregation> getSubAggregations() {
        return subAggregations;
    }

    @Override
    public String toString() {
        return String.format("HistogramAggregation{name='%s', field='%s', interval=%.2f, subAggs=%d}",
                           name, fieldName, interval, subAggregations.size());
    }

    /**
     * Extended bounds for including empty buckets within a range.
     */
    public static class ExtendedBounds {
        public final double min;
        public final double max;

        public ExtendedBounds(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("ExtendedBounds{min=%.2f, max=%.2f}", min, max);
        }
    }

    /**
     * Hard bounds for filtering values outside the range.
     */
    public static class HardBounds {
        public final double min;
        public final double max;

        public HardBounds(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("HardBounds{min=%.2f, max=%.2f}", min, max);
        }
    }
}