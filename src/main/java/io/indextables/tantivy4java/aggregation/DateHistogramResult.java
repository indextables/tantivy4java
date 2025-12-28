package io.indextables.tantivy4java.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        private final Map<String, AggregationResult> subAggregations;

        /**
         * Creates a date histogram bucket without sub-aggregations.
         */
        public DateHistogramBucket(double key, String keyAsString, long docCount) {
            this.key = key;
            this.keyAsString = keyAsString;
            this.docCount = docCount;
            this.subAggregations = Collections.emptyMap();
        }

        /**
         * Creates a date histogram bucket with sub-aggregations.
         *
         * @param key The bucket key (milliseconds since epoch)
         * @param keyAsString The formatted timestamp string (RFC3339)
         * @param docCount The document count in this bucket
         * @param subAggregations Map of sub-aggregation results
         */
        public DateHistogramBucket(double key, String keyAsString, long docCount,
                                   Map<String, AggregationResult> subAggregations) {
            this.key = key;
            this.keyAsString = keyAsString;
            this.docCount = docCount;
            this.subAggregations = subAggregations != null ? subAggregations : Collections.emptyMap();
        }

        /**
         * Gets the timestamp key for this bucket (milliseconds since epoch).
         */
        public double getKey() {
            return key;
        }

        /**
         * Gets the formatted timestamp key as a string (RFC3339 format).
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
            return String.format("DateHistogramBucket{key=%.0f, keyAsString='%s', docCount=%d, subAggs=%d}",
                               key, keyAsString, docCount, subAggregations.size());
        }
    }
}