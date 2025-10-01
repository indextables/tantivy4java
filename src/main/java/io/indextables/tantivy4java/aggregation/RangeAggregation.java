package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

import java.util.List;
import java.util.ArrayList;

/**
 * Range aggregation that groups documents into buckets based on numeric or date ranges.
 * This creates buckets for specified ranges in the field values.
 * Similar to Elasticsearch's range aggregation and Quickwit's range aggregation.
 *
 * Example usage:
 * <pre>{@code
 * RangeAggregation priceRanges = new RangeAggregation("price_ranges", "price")
 *     .addRange("cheap", null, 50.0)
 *     .addRange("medium", 50.0, 100.0)
 *     .addRange("expensive", 100.0, null);
 *
 * SearchResult result = searcher.search(query, 10, Map.of("ranges", priceRanges));
 * RangeResult ranges = (RangeResult) result.getAggregation("ranges");
 * }</pre>
 */
public class RangeAggregation extends SplitAggregation {

    private final String fieldName;
    private final List<RangeSpec> ranges;

    /**
     * Creates a range aggregation for the specified field.
     *
     * @param fieldName The numeric field to create ranges for
     */
    public RangeAggregation(String fieldName) {
        super("range_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
        this.ranges = new ArrayList<>();
    }

    /**
     * Creates a named range aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The numeric field to create ranges for
     */
    public RangeAggregation(String name, String fieldName) {
        super(name);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
        this.ranges = new ArrayList<>();
    }

    /**
     * Adds a range bucket to this aggregation.
     *
     * @param key Optional key for this range (can be null)
     * @param from Lower bound (inclusive), null for unbounded
     * @param to Upper bound (exclusive), null for unbounded
     * @return This aggregation for chaining
     */
    public RangeAggregation addRange(String key, Double from, Double to) {
        ranges.add(new RangeSpec(key, from, to));
        return this;
    }

    /**
     * Adds a range bucket without a key.
     *
     * @param from Lower bound (inclusive), null for unbounded
     * @param to Upper bound (exclusive), null for unbounded
     * @return This aggregation for chaining
     */
    public RangeAggregation addRange(Double from, Double to) {
        return addRange(null, from, to);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getAggregationType() {
        return "range";
    }

    @Override
    public String toAggregationJson() {
        if (ranges.isEmpty()) {
            throw new IllegalStateException("Range aggregation must have at least one range");
        }

        StringBuilder json = new StringBuilder();
        json.append("{\"range\": {\"field\": \"").append(fieldName).append("\", \"ranges\": [");

        for (int i = 0; i < ranges.size(); i++) {
            if (i > 0) json.append(", ");
            json.append(ranges.get(i).toJson());
        }

        json.append("]}}");
        return json.toString();
    }

    public List<RangeSpec> getRanges() {
        return new ArrayList<>(ranges);
    }

    @Override
    public String toString() {
        return String.format("RangeAggregation{name='%s', field='%s', ranges=%d}",
                           name, fieldName, ranges.size());
    }

    /**
     * Specification for a single range in the aggregation.
     */
    public static class RangeSpec {
        private final String key;
        private final Double from;
        private final Double to;

        public RangeSpec(String key, Double from, Double to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        public String getKey() {
            return key;
        }

        public Double getFrom() {
            return from;
        }

        public Double getTo() {
            return to;
        }

        public String toJson() {
            StringBuilder json = new StringBuilder("{");

            if (key != null) {
                json.append("\"key\": \"").append(key).append("\"");
            }

            if (from != null) {
                if (key != null) json.append(", ");
                json.append("\"from\": ").append(from);
            }

            if (to != null) {
                if (key != null || from != null) json.append(", ");
                json.append("\"to\": ").append(to);
            }

            json.append("}");
            return json.toString();
        }

        @Override
        public String toString() {
            return String.format("RangeSpec{key='%s', from=%s, to=%s}", key, from, to);
        }
    }
}