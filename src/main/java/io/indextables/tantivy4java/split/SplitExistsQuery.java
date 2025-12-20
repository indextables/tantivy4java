package io.indextables.tantivy4java.split;

/**
 * A query that matches documents where the specified field exists (has a value).
 * This is equivalent to Quickwit's FieldPresence query.
 *
 * For prescan purposes, this query type returns conservative results since
 * checking field presence in the FST requires special handling.
 */
public class SplitExistsQuery extends SplitQuery {
    private final String field;

    /**
     * Create a new exists query for the specified field.
     *
     * @param field The field to check for existence
     */
    public SplitExistsQuery(String field) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Field cannot be null or empty");
        }
        this.field = field;
    }

    /**
     * Get the field name being checked for existence.
     */
    public String getField() {
        return field;
    }

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return "SplitExistsQuery(field=" + field + ")";
    }
}
