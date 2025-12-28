package io.indextables.tantivy4java.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        private final Map<String, AggregationResult> subAggregations;

        /**
         * Creates a histogram bucket without sub-aggregations.
         */
        public HistogramBucket(double key, long docCount) {
            this.key = key;
            this.docCount = docCount;
            this.subAggregations = Collections.emptyMap();
        }

        /**
         * Creates a histogram bucket with sub-aggregations.
         *
         * @param key The bucket key (start of interval)
         * @param docCount The document count in this bucket
         * @param subAggregations Map of sub-aggregation results
         */
        public HistogramBucket(double key, long docCount, Map<String, AggregationResult> subAggregations) {
            this.key = key;
            this.docCount = docCount;
            this.subAggregations = subAggregations != null ? subAggregations : Collections.emptyMap();
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
            return String.format("HistogramBucket{key=%.2f, docCount=%d, subAggs=%d}",
                               key, docCount, subAggregations.size());
        }
    }
}