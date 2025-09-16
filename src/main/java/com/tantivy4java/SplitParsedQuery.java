package com.tantivy4java;

/**
 * A SplitQuery that holds a pre-parsed Quickwit QueryAst as JSON.
 * This is used for queries that have been successfully parsed by Quickwit's 
 * query parser into QueryAst and serialized to JSON.
 * 
 * This approach leverages Quickwit's proven parsing capabilities instead of 
 * trying to recreate individual query types.
 */
public class SplitParsedQuery extends SplitQuery {
    
    private final String queryAstJson;
    
    /**
     * Creates a SplitParsedQuery with pre-parsed QueryAst JSON
     * @param queryAstJson The Quickwit QueryAst serialized as JSON
     */
    public SplitParsedQuery(String queryAstJson) {
        this.queryAstJson = queryAstJson;
    }
    
    /**
     * Returns the QueryAst JSON directly since it's already parsed and serialized
     * @return The Quickwit QueryAst JSON
     */
    @Override
    public String toQueryAstJson() {
        return queryAstJson;
    }
    
    /**
     * Get the QueryAst JSON for debugging/inspection
     * @return The QueryAst JSON string
     */
    public String getQueryAstJson() {
        return queryAstJson;
    }
    
    @Override
    public String toString() {
        return "SplitParsedQuery{queryAst=" + queryAstJson + "}";
    }
}