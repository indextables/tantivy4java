package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.core.Tantivy;
/**
 * Base class for queries that can be used with SplitSearcher.
 * These queries are designed to be convertible to Quickwit QueryAst format
 * for efficient split searching.
 */
public abstract class SplitQuery {
    
    static {
        // Ensure native library is loaded for all SplitQuery subclasses
        Tantivy.initialize();
    }
    
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
    public static SplitQuery parseQuery(String queryString, Schema schema, String[] defaultSearchFields) {
        return nativeParseQuery(queryString, schema.getNativePtr(), defaultSearchFields);
    }

    /**
     * Parse a query string with default search fields from schema.
     */
    public static SplitQuery parseQuery(String queryString, Schema schema) {
        return parseQuery(queryString, schema, new String[0]);
    }

    /**
     * Count the number of unique fields that would be searched by a query.
     * This is useful for understanding the scope of a query before executing it.
     *
     * @param queryString The query string to analyze (e.g., "title:hello AND body:world")
     * @param schema The schema to validate field names against
     * @param defaultSearchFields Default fields to search if no field is specified in the query
     * @return The number of unique fields that the query would search
     * @throws RuntimeException if the query cannot be parsed
     */
    public static int countQueryFields(String queryString, Schema schema, String[] defaultSearchFields) {
        int count = nativeCountQueryFields(queryString, schema.getNativePtr(), defaultSearchFields);
        if (count < 0) {
            throw new RuntimeException("Failed to count query fields - query may be invalid");
        }
        return count;
    }

    /**
     * Count the number of unique fields that would be searched by a query.
     * Uses all indexed text fields from the schema as default search fields.
     *
     * @param queryString The query string to analyze
     * @param schema The schema to validate field names against
     * @return The number of unique fields that the query would search
     * @throws RuntimeException if the query cannot be parsed
     */
    public static int countQueryFields(String queryString, Schema schema) {
        return countQueryFields(queryString, schema, new String[0]);
    }

    // Native method that takes the schema pointer directly
    private static native SplitQuery nativeParseQuery(String queryString, long schemaPtr, String[] defaultSearchFields);

    // Native method to count fields in a query
    private static native int nativeCountQueryFields(String queryString, long schemaPtr, String[] defaultSearchFields);
}