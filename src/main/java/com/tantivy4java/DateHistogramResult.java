package com.tantivy4java;

import java.util.List;

/**
 * Result of a date histogram aggregation containing time-based buckets.
 */
public class DateHistogramResult implements AggregationResult {

    private final String name;
    private final List<DateHistogramBucket> buckets;

    /**
     * Creates a date histogram result.
     *
     * @param name The name of the aggregation
     * @param buckets List of date histogram buckets
     */
    public DateHistogramResult(String name, List<DateHistogramBucket> buckets) {
        this.name = name;
        this.buckets = buckets;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "date_histogram";
    }

    /**
     * Gets the list of date histogram buckets.
     */
    public List<DateHistogramBucket> getBuckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return String.format("DateHistogramResult{name='%s', buckets=%d}", name, buckets.size());
    }

    /**
     * Represents a single bucket in a date histogram aggregation.
     */
    public static class DateHistogramBucket {
        private final double key;
        private final String keyAsString;
        private final long docCount;

        public DateHistogramBucket(double key, String keyAsString, long docCount) {
            this.key = key;
            this.keyAsString = keyAsString;
            this.docCount = docCount;
        }

        /**
         * Gets the timestamp key for this bucket (milliseconds since epoch).
         */
        public double getKey() {
            return key;
        }

        /**
         * Gets the formatted timestamp key as a string.
         */
        public String getKeyAsString() {
            return keyAsString;
        }

        /**
         * Gets the document count for this time bucket.
         */
        public long getDocCount() {
            return docCount;
        }

        @Override
        public String toString() {
            return String.format("DateHistogramBucket{key=%.0f, keyAsString='%s', docCount=%d}",
                               key, keyAsString, docCount);
        }
    }
}