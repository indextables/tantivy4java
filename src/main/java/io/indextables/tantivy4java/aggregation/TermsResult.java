package io.indextables.tantivy4java.aggregation;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Result of a terms aggregation containing buckets for each term.
 * Each bucket contains the term value and document count.
 */
public class TermsResult implements AggregationResult {

    private final String name;
    private final List<TermsBucket> buckets;
    private final long docCountErrorUpperBound;
    private final long sumOtherDocCount;

    /**
     * Creates a terms result.
     *
     * @param name The name of the aggregation
     * @param buckets List of term buckets
     * @param docCountErrorUpperBound Upper bound of error in document counts
     * @param sumOtherDocCount Count of documents not included in returned buckets
     */
    public TermsResult(String name, List<TermsBucket> buckets, long docCountErrorUpperBound, long sumOtherDocCount) {
        this.name = name;
        this.buckets = buckets;
        this.docCountErrorUpperBound = docCountErrorUpperBound;
        this.sumOtherDocCount = sumOtherDocCount;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "terms";
    }

    /**
     * Gets the list of term buckets.
     */
    public List<TermsBucket> getBuckets() {
        return buckets;
    }

    /**
     * Gets the upper bound of error in document counts.
     */
    public long getDocCountErrorUpperBound() {
        return docCountErrorUpperBound;
    }

    /**
     * Gets the count of documents not included in returned buckets.
     */
    public long getSumOtherDocCount() {
        return sumOtherDocCount;
    }

    @Override
    public String toString() {
        return String.format("TermsResult{name='%s', buckets=%d, errorBound=%d, otherDocs=%d}",
                           name, buckets.size(), docCountErrorUpperBound, sumOtherDocCount);
    }

    /**
     * Represents a single bucket in a terms aggregation.
     */
    public static class TermsBucket {
        private final Object key;
        private final long docCount;
        private final Map<String, AggregationResult> subAggregations;

        public TermsBucket(Object key, long docCount) {
            this.key = key;
            this.docCount = docCount;
            this.subAggregations = new HashMap<>();
        }

        public TermsBucket(Object key, long docCount, Map<String, AggregationResult> subAggregations) {
            this.key = key;
            this.docCount = docCount;
            this.subAggregations = subAggregations != null ? subAggregations : new HashMap<>();
        }

        /**
         * Gets the term value for this bucket.
         */
        public Object getKey() {
            return key;
        }

        /**
         * Gets the term value as a string.
         */
        public String getKeyAsString() {
            return key != null ? key.toString() : null;
        }

        /**
         * Gets the document count for this bucket.
         */
        public long getDocCount() {
            return docCount;
        }

        /**
         * Gets a sub-aggregation by name.
         *
         * @param name The name of the sub-aggregation
         * @return The sub-aggregation result, or null if not found
         */
        public AggregationResult getSubAggregation(String name) {
            return subAggregations.get(name);
        }

        /**
         * Gets all sub-aggregations for this bucket.
         *
         * @return Map of sub-aggregation name to result
         */
        public Map<String, AggregationResult> getSubAggregations() {
            return subAggregations;
        }

        /**
         * Checks if this bucket has any sub-aggregations.
         *
         * @return true if sub-aggregations exist, false otherwise
         */
        public boolean hasSubAggregations() {
            return !subAggregations.isEmpty();
        }

        @Override
        public String toString() {
            if (hasSubAggregations()) {
                return String.format("TermsBucket{key='%s', docCount=%d, subAggs=%s}",
                                   key, docCount, subAggregations.keySet());
            } else {
                return String.format("TermsBucket{key='%s', docCount=%d}", key, docCount);
            }
        }
    }
}