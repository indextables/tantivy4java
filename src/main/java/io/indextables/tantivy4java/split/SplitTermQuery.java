package io.indextables.tantivy4java.split;

/**
 * A term query for split searching that matches documents containing a specific term.
 * Equivalent to Tantivy's TermQuery but designed for QueryAst conversion.
 *
 * <p>IP address fields are detected automatically from the schema; no {@code fieldType} hint
 * is required from callers.
 */
public class SplitTermQuery extends SplitQuery {
    private final String field;
    private final String value;

    /**
     * Create a new term query.
     *
     * @param field The field name to search in
     * @param value The term value to search for
     */
    public SplitTermQuery(String field, String value) {
        if (field == null || value == null) {
            throw new IllegalArgumentException("Field and value cannot be null");
        }
        this.field = field;
        this.value = value;
    }

    /**
     * Get the field name this query searches in.
     */
    public String getField() {
        return field;
    }

    /**
     * Get the term value this query searches for.
     */
    public String getValue() {
        return value;
    }

    /**
     * Serialize this query to a Quickwit QueryAst JSON string.
     *
     * <p><strong>FOR TESTING ONLY.</strong> IP CIDR/wildcard expansion requires a live searcher
     * context and is not applied here. For production use, prefer {@code SplitSearcher.search()}.
     */
    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return String.format("SplitTermQuery(field=%s, value=%s)", field, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitTermQuery that = (SplitTermQuery) obj;
        return field.equals(that.field) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(field, value);
    }
}
