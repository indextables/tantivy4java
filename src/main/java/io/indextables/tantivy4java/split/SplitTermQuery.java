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
     * Explicit field type hint, as {@code FieldType.getValue()} from the tantivy4java
     * {@code FieldType} enum (e.g. {@code FieldType.IP_ADDR.getValue() == 10}).
     * {@code 0} means unknown — the native layer falls back to schema-based detection.
     */
    private final int fieldTypeValue;

    /**
     * Create a new term query with schema-based field type detection.
     *
     * @param field The field name to search in
     * @param value The term value to search for
     */
    public SplitTermQuery(String field, String value) {
        this(field, value, 0);
    }

    /**
     * Create a new term query with an explicit field type hint.
     *
     * <p>Pass {@code FieldType.getValue()} (e.g. {@code FieldType.IP_ADDR.getValue()}) to
     * enable IP CIDR/wildcard expansion without requiring a live searcher schema context.
     * Using the numeric ordinal avoids a shadow string mapping and is more efficient than
     * passing a type name string.
     *
     * @param field          The field name to search in
     * @param value          The term value to search for
     * @param fieldTypeValue The {@code FieldType.getValue()} ordinal, or {@code 0} for unknown
     */
    public SplitTermQuery(String field, String value, int fieldTypeValue) {
        if (field == null || value == null) {
            throw new IllegalArgumentException("Field and value cannot be null");
        }
        this.field = field;
        this.value = value;
        this.fieldTypeValue = fieldTypeValue;
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
     * <p><strong>FOR TESTING ONLY.</strong> When constructed with the two-argument constructor,
     * CIDR and wildcard IP expansion is not performed (no searcher context available).
     * When constructed with an explicit {@code fieldTypeValue}, IP expansion is applied
     * correctly without a searcher context. For production use, prefer
     * {@code SplitSearcher.search()}.
     */
    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return String.format("SplitTermQuery(field=%s, value=%s, fieldTypeValue=%d)", field, value, fieldTypeValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitTermQuery that = (SplitTermQuery) obj;
        return field.equals(that.field) && value.equals(that.value) && fieldTypeValue == that.fieldTypeValue;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(field, value, fieldTypeValue);
    }
}
