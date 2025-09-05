package com.tantivy4java;

/**
 * Base class for queries that can be used with SplitSearcher.
 * These queries are designed to be convertible to Quickwit QueryAst format
 * for efficient split searching.
 */
public abstract class SplitQuery {
    
    /**
     * Convert this query to a Quickwit QueryAst JSON string for split searching.
     * This is handled natively using Quickwit's query parsing libraries.
     * Each subclass provides its own native implementation.
     */
    public abstract String toQueryAstJson();
    
    /**
     * Convert this query to a standard Tantivy Query for compatibility.
     * This allows using the same query objects with regular Index searches.
     */
    public native Query toTantivyQuery(Schema schema);
    
    /**
     * Parse a query string into a SplitQuery using Quickwit's query parser.
     * This leverages Quickwit's proven query parsing logic.
     * 
     * @param queryString The query string to parse (e.g., "title:hello", "age:[1 TO 100]")
     * @param schema The schema to validate field names against
     * @param defaultSearchFields Default fields to search if no field is specified
     * @return A SplitQuery that can be used with SplitSearcher
     */
    public static native SplitQuery parseQuery(String queryString, Schema schema, String[] defaultSearchFields);
    
    /**
     * Parse a query string with default search fields from schema.
     */
    public static SplitQuery parseQuery(String queryString, Schema schema) {
        return parseQuery(queryString, schema, new String[0]);
    }
}