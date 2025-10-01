package io.indextables.tantivy4java.split;

import java.util.ArrayList;
import java.util.List;

/**
 * A boolean query for split searching that combines multiple subqueries with boolean logic.
 * Equivalent to Tantivy's BooleanQuery but designed for QueryAst conversion.
 */
public class SplitBooleanQuery extends SplitQuery {
    private final List<SplitQuery> mustQueries = new ArrayList<>();
    private final List<SplitQuery> shouldQueries = new ArrayList<>();
    private final List<SplitQuery> mustNotQueries = new ArrayList<>();
    private Integer minimumShouldMatch = null;
    
    /**
     * Create a new empty boolean query.
     */
    public SplitBooleanQuery() {
    }
    
    /**
     * Add a query that MUST match.
     */
    public SplitBooleanQuery addMust(SplitQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        mustQueries.add(query);
        return this;
    }
    
    /**
     * Add a query that SHOULD match (contributes to scoring).
     */
    public SplitBooleanQuery addShould(SplitQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        shouldQueries.add(query);
        return this;
    }
    
    /**
     * Add a query that MUST NOT match (filter out documents).
     */
    public SplitBooleanQuery addMustNot(SplitQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        mustNotQueries.add(query);
        return this;
    }
    
    /**
     * Set the minimum number of should clauses that must match.
     */
    public SplitBooleanQuery setMinimumShouldMatch(int minimum) {
        this.minimumShouldMatch = minimum;
        return this;
    }
    
    /**
     * Get all MUST clauses.
     */
    public List<SplitQuery> getMustQueries() {
        return new ArrayList<>(mustQueries);
    }
    
    /**
     * Get all SHOULD clauses.
     */
    public List<SplitQuery> getShouldQueries() {
        return new ArrayList<>(shouldQueries);
    }
    
    /**
     * Get all MUST NOT clauses.
     */
    public List<SplitQuery> getMustNotQueries() {
        return new ArrayList<>(mustNotQueries);
    }
    
    /**
     * Get the minimum should match value.
     */
    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }
    
    /**
     * Create a boolean query that requires all subqueries to match (intersection).
     */
    public static SplitBooleanQuery intersection(List<SplitQuery> queries) {
        SplitBooleanQuery boolQuery = new SplitBooleanQuery();
        for (SplitQuery query : queries) {
            boolQuery.addMust(query);
        }
        return boolQuery;
    }
    
    /**
     * Create a boolean query where any subquery can match (union).
     */
    public static SplitBooleanQuery union(List<SplitQuery> queries) {
        SplitBooleanQuery boolQuery = new SplitBooleanQuery();
        for (SplitQuery query : queries) {
            boolQuery.addShould(query);
        }
        return boolQuery;
    }
    
    @Override
    public native String toQueryAstJson();
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SplitBooleanQuery(");
        if (!mustQueries.isEmpty()) {
            sb.append("must=").append(mustQueries.size()).append(" ");
        }
        if (!shouldQueries.isEmpty()) {
            sb.append("should=").append(shouldQueries.size()).append(" ");
        }
        if (!mustNotQueries.isEmpty()) {
            sb.append("mustNot=").append(mustNotQueries.size()).append(" ");
        }
        if (minimumShouldMatch != null) {
            sb.append("minShouldMatch=").append(minimumShouldMatch);
        }
        sb.append(")");
        return sb.toString();
    }
}