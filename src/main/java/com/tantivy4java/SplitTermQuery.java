package com.tantivy4java;

/**
 * A term query for split searching that matches documents containing a specific term.
 * Equivalent to Tantivy's TermQuery but designed for QueryAst conversion.
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
        return field.hashCode() * 31 + value.hashCode();
    }
}