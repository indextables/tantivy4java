package io.indextables.tantivy4java.aggregation;

import java.util.List;

/**
 * Result of a histogram aggregation containing numeric interval buckets.
 */
public class HistogramResult implements AggregationResult {

    private final String name;
    private final List<HistogramBucket> buckets;

    /**
     * Creates a histogram result.
     *
     * @param name The name of the aggregation
     * @param buckets List of histogram buckets
     */
    public HistogramResult(String name, List<HistogramBucket> buckets) {
        this.name = name;
        this.buckets = buckets;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "histogram";
    }

    /**
     * Gets the list of histogram buckets.
     */
    public List<HistogramBucket> getBuckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return String.format("HistogramResult{name='%s', buckets=%d}", name, buckets.size());
    }

    /**
     * Represents a single bucket in a histogram aggregation.
     */
    public static class HistogramBucket {
        private final double key;
        private final long docCount;

        public HistogramBucket(double key, long docCount) {
            this.key = key;
            this.docCount = docCount;
        }

        /**
         * Gets the numeric key for this bucket (the start of the interval).
         */
        public double getKey() {
            return key;
        }

        /**
         * Gets the document count for this histogram bucket.
         */
        public long getDocCount() {
            return docCount;
        }

        @Override
        public String toString() {
            return String.format("HistogramBucket{key=%.2f, docCount=%d}", key, docCount);
        }
    }
}