package com.tantivy4java;

/**
 * A query that matches all documents in the index.
 * Equivalent to Tantivy's AllQuery but designed for QueryAst conversion.
 */
public class SplitMatchAllQuery extends SplitQuery {
    
    /**
     * Create a new match-all query.
     */
    public SplitMatchAllQuery() {
    }
    
    @Override
    public native String toQueryAstJson();
    
    @Override
    public String toString() {
        return "SplitMatchAllQuery(*)";
    }
    
    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass();
    }
    
    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}