package com.tantivy4java;

/**
 * Terms aggregation that groups documents by distinct values in a field.
 * This creates buckets for each unique term value in the specified field.
 * Similar to Elasticsearch's terms aggregation and Quickwit's terms aggregation.
 *
 * Example usage:
 * <pre>{@code
 * TermsAggregation categoryTerms = new TermsAggregation("categories", "category");
 * SearchResult result = searcher.search(query, 10, Map.of("terms", categoryTerms));
 * TermsResult terms = (TermsResult) result.getAggregation("terms");
 *
 * for (TermsBucket bucket : terms.getBuckets()) {
 *     System.out.println("Category: " + bucket.getKey() + ", Count: " + bucket.getDocCount());
 * }
 * }</pre>
 */
public class TermsAggregation extends SplitAggregation {

    private final String fieldName;
    private final int size;
    private final int shardSize;

    /**
     * Creates a terms aggregation for the specified field with default size.
     *
     * @param fieldName The field to group by (text or keyword field)
     */
    public TermsAggregation(String fieldName) {
        this("terms_" + fieldName, fieldName, 10, 0);
    }

    /**
     * Creates a terms aggregation with custom size.
     *
     * @param fieldName The field to group by
     * @param size Maximum number of buckets to return (default: 10)
     */
    public TermsAggregation(String fieldName, int size) {
        this("terms_" + fieldName, fieldName, size, 0);
    }

    /**
     * Creates a named terms aggregation with full configuration.
     *
     * @param name The name for this aggregation
     * @param fieldName The field to group by
     * @param size Maximum number of buckets to return
     * @param shardSize Number of buckets to return from each shard (0 = auto)
     */
    public TermsAggregation(String name, String fieldName, int size, int shardSize) {
        super(name);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }
        this.fieldName = fieldName.trim();
        this.size = size;
        this.shardSize = shardSize;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getAggregationType() {
        return "terms";
    }

    @Override
    public String toAggregationJson() {
        StringBuilder json = new StringBuilder();
        json.append("{\"terms\": {\"field\": \"").append(fieldName).append("\"");
        json.append(", \"size\": ").append(size);
        if (shardSize > 0) {
            json.append(", \"shard_size\": ").append(shardSize);
        }
        json.append("}}");
        return json.toString();
    }

    public int getSize() {
        return size;
    }

    public int getShardSize() {
        return shardSize;
    }

    @Override
    public String toString() {
        return String.format("TermsAggregation{name='%s', field='%s', size=%d, shardSize=%d}",
                           name, fieldName, size, shardSize);
    }
}