package io.indextables.tantivy4java.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Result of a range aggregation containing buckets for each range.
 */
public class RangeResult implements AggregationResult {

    private final String name;
    private final List<RangeBucket> buckets;

    /**
     * Creates a range result.
     *
     * @param name The name of the aggregation
     * @param buckets List of range buckets
     */
    public RangeResult(String name, List<RangeBucket> buckets) {
        this.name = name;
        this.buckets = buckets;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "range";
    }

    /**
     * Gets the list of range buckets.
     */
    public List<RangeBucket> getBuckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return String.format("RangeResult{name='%s', buckets=%d}", name, buckets.size());
    }

    /**
     * Represents a single bucket in a range aggregation.
     */
    public static class RangeBucket {
        private final String key;
        private final Double from;
        private final Double to;
        private final long docCount;
        private final Map<String, AggregationResult> subAggregations;

        /**
         * Creates a range bucket without sub-aggregations.
         */
        public RangeBucket(String key, Double from, Double to, long docCount) {
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.subAggregations = Collections.emptyMap();
        }

        /**
         * Creates a range bucket with sub-aggregations.
         *
         * @param key The bucket key/name
         * @param from The lower bound (inclusive), null for unbounded
         * @param to The upper bound (exclusive), null for unbounded
         * @param docCount The document count in this bucket
         * @param subAggregations Map of sub-aggregation results
         */
        public RangeBucket(String key, Double from, Double to, long docCount,
                          Map<String, AggregationResult> subAggregations) {
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.subAggregations = subAggregations != null ? subAggregations : Collections.emptyMap();
        }

        /**
         * Gets the key for this range bucket.
         */
        public String getKey() {
            return key;
        }

        /**
         * Gets the lower bound of this range (inclusive).
         */
        public Double getFrom() {
            return from;
        }

        /**
         * Gets the upper bound of this range (exclusive).
         */
        public Double getTo() {
            return to;
        }

        /**
         * Gets the document count for this range.
         */
        public long getDocCount() {
            return docCount;
        }

        /**
         * Gets the sub-aggregations for this bucket.
         */
        public Map<String, AggregationResult> getSubAggregations() {
            return subAggregations;
        }

        /**
         * Gets a specific sub-aggregation by name.
         *
         * @param name The sub-aggregation name
         * @param type The expected result type
         * @return The sub-aggregation result, or null if not found
         */
        @SuppressWarnings("unchecked")
        public <T extends AggregationResult> T getSubAggregation(String name, Class<T> type) {
            AggregationResult result = subAggregations.get(name);
            if (result != null && type.isInstance(result)) {
                return (T) result;
            }
            return null;
        }

        @Override
        public String toString() {
            return String.format("RangeBucket{key='%s', from=%s, to=%s, docCount=%d, subAggs=%d}",
                               key, from, to, docCount, subAggregations.size());
        }
    }
}