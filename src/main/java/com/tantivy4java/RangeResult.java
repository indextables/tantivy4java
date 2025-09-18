package com.tantivy4java;

import java.util.List;

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

        public RangeBucket(String key, Double from, Double to, long docCount) {
            this.key = key;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
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

        @Override
        public String toString() {
            return String.format("RangeBucket{key='%s', from=%s, to=%s, docCount=%d}",
                               key, from, to, docCount);
        }
    }
}