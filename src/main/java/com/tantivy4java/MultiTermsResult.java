package com.tantivy4java;

import java.util.*;

/**
 * Result container for multi-dimensional terms aggregations.
 * Flattens nested TermsResult structures into a list of multi-dimensional buckets.
 *
 * Each bucket represents a unique combination of field values across all grouped fields,
 * similar to a row in a SQL GROUP BY result set.
 */
public class MultiTermsResult implements AggregationResult {

    private final List<MultiTermsBucket> buckets;
    private final String[] fieldNames;
    private final String name;

    /**
     * Creates a MultiTermsResult from nested TermsResult structures.
     *
     * @param name The name of the aggregation
     * @param rootTermsResult The root TermsResult from the nested aggregation
     * @param fieldNames Array of field names in nesting order
     */
    public MultiTermsResult(String name, TermsResult rootTermsResult, String[] fieldNames) {
        if (fieldNames == null || fieldNames.length < 2) {
            throw new IllegalArgumentException("MultiTermsResult requires at least 2 field names");
        }

        this.name = name != null ? name : "multi_terms";
        this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
        this.buckets = new ArrayList<>();

        if (rootTermsResult != null) {
            flattenNestedBuckets(rootTermsResult, new ArrayList<>(), 0);
        }
    }

    /**
     * Recursively flattens nested TermsResult buckets into multi-dimensional buckets.
     */
    private void flattenNestedBuckets(TermsResult termsResult, List<String> currentPath, int depth) {
        if (termsResult == null) {
            return;
        }

        for (TermsResult.TermsBucket bucket : termsResult.getBuckets()) {
            List<String> bucketPath = new ArrayList<>(currentPath);
            bucketPath.add(bucket.getKeyAsString());

            if (depth == fieldNames.length - 1) {
                // Reached leaf level - create final multi-dimensional bucket
                String[] fieldValues = bucketPath.toArray(new String[0]);
                Map<String, AggregationResult> subAggs = bucket.getSubAggregations();

                MultiTermsBucket multiTermsBucket = new MultiTermsBucket(
                    fieldValues,
                    bucket.getDocCount(),
                    subAggs
                );
                buckets.add(multiTermsBucket);
            } else {
                // Continue to next nesting level
                String nextFieldAggName = fieldNames[depth + 1] + "_terms";
                AggregationResult nextLevel = bucket.getSubAggregation(nextFieldAggName);

                if (nextLevel instanceof TermsResult) {
                    flattenNestedBuckets((TermsResult) nextLevel, bucketPath, depth + 1);
                }
            }
        }
    }

    /**
     * Gets all multi-dimensional buckets.
     */
    public List<MultiTermsBucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    /**
     * Gets the field names in grouping order.
     */
    public String[] getFieldNames() {
        return Arrays.copyOf(fieldNames, fieldNames.length);
    }

    /**
     * Gets the number of multi-dimensional buckets.
     */
    public int getBucketCount() {
        return buckets.size();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "multi_terms";
    }

    @Override
    public String toString() {
        return "MultiTermsResult{" +
                "name='" + name + '\'' +
                ", fields=" + Arrays.toString(fieldNames) +
                ", buckets=" + buckets.size() +
                '}';
    }

    /**
     * Represents a single multi-dimensional bucket with field values and metrics.
     */
    public static class MultiTermsBucket {
        private final String[] fieldValues;
        private final long docCount;
        private final Map<String, AggregationResult> subAggregations;

        public MultiTermsBucket(String[] fieldValues, long docCount, Map<String, AggregationResult> subAggregations) {
            this.fieldValues = Arrays.copyOf(fieldValues, fieldValues.length);
            this.docCount = docCount;
            this.subAggregations = subAggregations != null ?
                new HashMap<>(subAggregations) :
                new HashMap<>();
        }

        /**
         * Gets all field values for this bucket.
         * Index corresponds to the field order in the original MultiTermsAggregation.
         */
        public String[] getFieldValues() {
            return Arrays.copyOf(fieldValues, fieldValues.length);
        }

        /**
         * Gets the field value at the specified index.
         */
        public String getFieldValue(int index) {
            if (index < 0 || index >= fieldValues.length) {
                throw new IndexOutOfBoundsException("Field index " + index + " out of bounds for " + fieldValues.length + " fields");
            }
            return fieldValues[index];
        }

        /**
         * Gets field values as a map using field names as keys.
         * Requires the field names to be provided.
         */
        public Map<String, String> getFieldValuesMap(String[] fieldNames) {
            if (fieldNames.length != fieldValues.length) {
                throw new IllegalArgumentException("Field names length must match field values length");
            }

            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                map.put(fieldNames[i], fieldValues[i]);
            }
            return map;
        }

        /**
         * Gets the document count for this multi-dimensional bucket.
         */
        public long getDocCount() {
            return docCount;
        }

        /**
         * Gets a sub-aggregation result by name.
         */
        public AggregationResult getSubAggregation(String name) {
            return subAggregations.get(name);
        }

        /**
         * Gets all sub-aggregation results.
         */
        public Map<String, AggregationResult> getSubAggregations() {
            return Collections.unmodifiableMap(subAggregations);
        }

        /**
         * Checks if this bucket has any sub-aggregations.
         */
        public boolean hasSubAggregations() {
            return !subAggregations.isEmpty();
        }

        /**
         * Creates a composite key by joining field values with a delimiter.
         */
        public String getCompositeKey() {
            return getCompositeKey("|");
        }

        /**
         * Creates a composite key by joining field values with a custom delimiter.
         */
        public String getCompositeKey(String delimiter) {
            return String.join(delimiter, fieldValues);
        }

        @Override
        public String toString() {
            return "MultiTermsBucket{" +
                    "fieldValues=" + Arrays.toString(fieldValues) +
                    ", docCount=" + docCount +
                    ", subAggregations=" + subAggregations.keySet() +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MultiTermsBucket that = (MultiTermsBucket) o;
            return docCount == that.docCount &&
                    Arrays.equals(fieldValues, that.fieldValues) &&
                    Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(docCount, subAggregations);
            result = 31 * result + Arrays.hashCode(fieldValues);
            return result;
        }
    }
}