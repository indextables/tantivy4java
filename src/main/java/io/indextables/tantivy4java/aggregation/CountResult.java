package io.indextables.tantivy4java.aggregation;

/**
 * Result of a count aggregation.
 */
public class CountResult implements AggregationResult {

    private final String name;
    private final long count;

    /**
     * Creates a count result.
     *
     * @param name The name of the aggregation
     * @param count The document count
     */
    public CountResult(String name, long count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "value_count";
    }

    /**
     * Gets the count of documents.
     */
    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return String.format("CountResult{name='%s', count=%d}", name, count);
    }
}