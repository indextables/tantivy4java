package io.indextables.tantivy4java.split;

/**
 * A term query for split searching that matches documents containing a specific term.
 * Equivalent to Tantivy's TermQuery but designed for QueryAst conversion.
 */
public class SplitTermQuery extends SplitQuery {
    private final String field;
    private final String value;
    private final String fieldType;

    /**
     * Create a new term query. IP CIDR/wildcard expansion is skipped (backwards compatible).
     *
     * @param field The field name to search in
     * @param value The term value to search for
     */
    public SplitTermQuery(String field, String value) {
        this(field, value, null);
    }

    /**
     * Create a new term query with an explicit field type.
     * When {@code fieldType} is {@code "ipaddr"}, {@code "ip_addr"}, or {@code "ip"},
     * CIDR and wildcard patterns in {@code value} are transparently expanded to a range query.
     *
     * @param field     The field name to search in
     * @param value     The term value to search for
     * @param fieldType The field type (nullable; pass {@code "ipaddr"} for IP address fields)
     */
    public SplitTermQuery(String field, String value, String fieldType) {
        if (field == null || value == null) {
            throw new IllegalArgumentException("Field and value cannot be null");
        }
        this.field = field;
        this.value = value;
        this.fieldType = fieldType;
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
     * Get the field type hint (may be null).
     */
    public String getFieldType() {
        return fieldType;
    }
    
    @Override
    public native String toQueryAstJson();
    
    @Override
    public String toString() {
        return String.format("SplitTermQuery(field=%s, value=%s, fieldType=%s)", field, value, fieldType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitTermQuery that = (SplitTermQuery) obj;
        return field.equals(that.field) && value.equals(that.value)
                && java.util.Objects.equals(fieldType, that.fieldType);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(field, value, fieldType);
    }
}