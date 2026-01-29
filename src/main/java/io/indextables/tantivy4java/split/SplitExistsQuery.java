package io.indextables.tantivy4java.split;

/**
 * A query that matches documents where a specific field has any non-null value.
 * This is equivalent to an IS NOT NULL check in SQL.
 *
 * <p>Designed for QueryAst conversion using Quickwit's FieldPresenceQuery.
 *
 * <p>Example usage:
 * <pre>
 * // Find documents where email field exists
 * SplitExistsQuery exists = new SplitExistsQuery("email");
 * SearchResult result = splitSearcher.search(exists, 100);
 *
 * // IS NULL pattern using boolean negation
 * SplitBooleanQuery isNull = new SplitBooleanQuery.Builder()
 *     .must(new SplitMatchAllQuery())
 *     .mustNot(new SplitExistsQuery("email"))
 *     .build();
 * </pre>
 */
public class SplitExistsQuery extends SplitQuery {
    private final String field;

    /**
     * Create a new exists query for the specified field.
     *
     * @param field The field name to check for existence
     * @throws IllegalArgumentException if field is null
     */
    public SplitExistsQuery(String field) {
        if (field == null) {
            throw new IllegalArgumentException("Field cannot be null");
        }
        this.field = field;
    }

    /**
     * Get the field name this query checks for existence.
     *
     * @return The field name
     */
    public String getField() {
        return field;
    }

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return String.format("SplitExistsQuery(field=%s)", field);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitExistsQuery that = (SplitExistsQuery) obj;
        return field.equals(that.field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
}
