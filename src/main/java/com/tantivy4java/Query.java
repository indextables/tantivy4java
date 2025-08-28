/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.tantivy4java;

import java.util.List;
import java.util.Map;

/**
 * Represents a query in Tantivy.
 * Provides static factory methods for creating different types of queries.
 */
public class Query implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Query(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Create a term query for exact matches.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param fieldValue Value to search for
     * @param indexOption Index option to use
     * @return New Query instance
     */
    public static Query termQuery(Schema schema, String fieldName, Object fieldValue, String indexOption) {
        long ptr = nativeTermQuery(schema.getNativePtr(), fieldName, fieldValue, indexOption);
        return new Query(ptr);
    }

    /**
     * Create a term query with default index option.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param fieldValue Value to search for
     * @return New Query instance
     */
    public static Query termQuery(Schema schema, String fieldName, Object fieldValue) {
        return termQuery(schema, fieldName, fieldValue, "position");
    }

    /**
     * Create a term set query for matching any of multiple values.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param fieldValues List of values to search for
     * @return New Query instance
     */
    public static Query termSetQuery(Schema schema, String fieldName, List<Object> fieldValues) {
        long ptr = nativeTermSetQuery(schema.getNativePtr(), fieldName, fieldValues);
        return new Query(ptr);
    }

    /**
     * Create a query that matches all documents.
     * @return New Query instance
     */
    public static Query allQuery() {
        long ptr = nativeAllQuery();
        return new Query(ptr);
    }

    /**
     * Create a fuzzy term query for approximate matches.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param text Text to search for
     * @param distance Maximum edit distance
     * @param transpositionCostOne Whether transpositions have cost 1
     * @param prefix Whether to use prefix matching
     * @return New Query instance
     */
    public static Query fuzzyTermQuery(Schema schema, String fieldName, String text, 
                                     int distance, boolean transpositionCostOne, boolean prefix) {
        long ptr = nativeFuzzyTermQuery(schema.getNativePtr(), fieldName, text, 
                                      distance, transpositionCostOne, prefix);
        return new Query(ptr);
    }

    /**
     * Create a fuzzy term query with default options.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param text Text to search for
     * @return New Query instance
     */
    public static Query fuzzyTermQuery(Schema schema, String fieldName, String text) {
        return fuzzyTermQuery(schema, fieldName, text, 1, true, false);
    }

    /**
     * Create a phrase query for matching sequences of words.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param words List of words or (position, word) pairs
     * @param slop Maximum distance between words
     * @return New Query instance
     */
    public static Query phraseQuery(Schema schema, String fieldName, List<Object> words, int slop) {
        long ptr = nativePhraseQuery(schema.getNativePtr(), fieldName, words, slop);
        return new Query(ptr);
    }

    /**
     * Create a phrase query with no slop.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param words List of words
     * @return New Query instance
     */
    public static Query phraseQuery(Schema schema, String fieldName, List<Object> words) {
        return phraseQuery(schema, fieldName, words, 0);
    }

    /**
     * Create a boolean query combining multiple subqueries.
     * @param subqueries List of (occur, query) pairs
     * @return New Query instance
     */
    public static Query booleanQuery(List<OccurQuery> subqueries) {
        long ptr = nativeBooleanQuery(subqueries);
        return new Query(ptr);
    }

    /**
     * Create a disjunction max query.
     * @param subqueries List of subqueries
     * @param tieBreaker Tie breaker value (optional)
     * @return New Query instance
     */
    public static Query disjunctionMaxQuery(List<Query> subqueries, Double tieBreaker) {
        long[] ptrs = subqueries.stream().mapToLong(q -> q.getNativePtr()).toArray();
        long ptr = nativeDisjunctionMaxQuery(ptrs, tieBreaker);
        return new Query(ptr);
    }

    /**
     * Create a disjunction max query without tie breaker.
     * @param subqueries List of subqueries
     * @return New Query instance
     */
    public static Query disjunctionMaxQuery(List<Query> subqueries) {
        return disjunctionMaxQuery(subqueries, null);
    }

    /**
     * Create a boosted query.
     * @param query Query to boost
     * @param boost Boost factor
     * @return New Query instance
     */
    public static Query boostQuery(Query query, double boost) {
        long ptr = nativeBoostQuery(query.getNativePtr(), boost);
        return new Query(ptr);
    }

    /**
     * Create a regex query.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param regexPattern Regular expression pattern
     * @return New Query instance
     */
    public static Query regexQuery(Schema schema, String fieldName, String regexPattern) {
        long ptr = nativeRegexQuery(schema.getNativePtr(), fieldName, regexPattern);
        return new Query(ptr);
    }

    /**
     * Create a wildcard query for pattern matching.
     * Supports * (any sequence) and ? (single character) wildcards.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param pattern Wildcard pattern (e.g., "test*", "?ello", "wo*d")
     * @return New Query instance
     */
    public static Query wildcardQuery(Schema schema, String fieldName, String pattern) {
        long ptr = nativeWildcardQuery(schema.getNativePtr(), fieldName, pattern);
        return new Query(ptr);
    }

    /**
     * Create a wildcard query for pattern matching with lenient option.
     * Supports * (any sequence) and ? (single character) wildcards.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param pattern Wildcard pattern (e.g., "test*", "?ello", "wo*d")
     * @param lenient Whether to ignore missing fields (returns no matches instead of error)
     * @return New Query instance
     */
    public static Query wildcardQuery(Schema schema, String fieldName, String pattern, boolean lenient) {
        long ptr = nativeWildcardQueryLenient(schema.getNativePtr(), fieldName, pattern, lenient);
        return new Query(ptr);
    }


    /**
     * Create a more-like-this query.
     * @param docAddress Document address to use as reference
     * @param minDocFrequency Minimum document frequency
     * @param maxDocFrequency Maximum document frequency
     * @param minTermFrequency Minimum term frequency
     * @param maxQueryTerms Maximum query terms
     * @param minWordLength Minimum word length
     * @param maxWordLength Maximum word length
     * @param boostFactor Boost factor
     * @param stopWords Stop words list
     * @return New Query instance
     */
    public static Query moreLikeThisQuery(DocAddress docAddress, Integer minDocFrequency, 
                                        Integer maxDocFrequency, Integer minTermFrequency,
                                        Integer maxQueryTerms, Integer minWordLength,
                                        Integer maxWordLength, Double boostFactor,
                                        List<String> stopWords) {
        long ptr = nativeMoreLikeThisQuery(docAddress.getNativePtr(), minDocFrequency, 
                                         maxDocFrequency, minTermFrequency, maxQueryTerms,
                                         minWordLength, maxWordLength, boostFactor, stopWords);
        return new Query(ptr);
    }

    /**
     * Create a more-like-this query with default options.
     * @param docAddress Document address to use as reference
     * @return New Query instance
     */
    public static Query moreLikeThisQuery(DocAddress docAddress) {
        return moreLikeThisQuery(docAddress, 5, null, 2, 25, null, null, 1.0, List.of());
    }

    /**
     * Create a constant score query.
     * @param query Query to wrap
     * @param score Constant score
     * @return New Query instance
     */
    public static Query constScoreQuery(Query query, double score) {
        long ptr = nativeConstScoreQuery(query.getNativePtr(), score);
        return new Query(ptr);
    }

    /**
     * Create a range query.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param fieldType Field type
     * @param lowerBound Lower bound value
     * @param upperBound Upper bound value
     * @param includeLower Whether to include lower bound
     * @param includeUpper Whether to include upper bound
     * @return New Query instance
     */
    public static Query rangeQuery(Schema schema, String fieldName, FieldType fieldType,
                                 Object lowerBound, Object upperBound, 
                                 boolean includeLower, boolean includeUpper) {
        long ptr = nativeRangeQuery(schema.getNativePtr(), fieldName, fieldType.getValue(),
                                  lowerBound, upperBound, includeLower, includeUpper);
        return new Query(ptr);
    }

    /**
     * Create a range query with inclusive bounds.
     * @param schema Schema for field validation
     * @param fieldName Field to search in
     * @param fieldType Field type
     * @param lowerBound Lower bound value
     * @param upperBound Upper bound value
     * @return New Query instance
     */
    public static Query rangeQuery(Schema schema, String fieldName, FieldType fieldType,
                                 Object lowerBound, Object upperBound) {
        return rangeQuery(schema, fieldName, fieldType, lowerBound, upperBound, true, true);
    }

    /**
     * Explain how this query matches a specific document.
     * @param searcher Searcher to use for explanation
     * @param docAddress Document address to explain
     * @return Explanation object
     */
    public Explanation explain(Searcher searcher, DocAddress docAddress) {
        if (closed) {
            throw new IllegalStateException("Query has been closed");
        }
        long ptr = nativeExplain(nativePtr, searcher.getNativePtr(), docAddress.getNativePtr());
        return new Explanation(ptr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Query has been closed");
        }
        return nativePtr;
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativePtr);
            closed = true;
            nativePtr = 0;
        }
    }

    @Override
    public String toString() {
        if (closed) {
            return "Query{CLOSED}";
        }
        return nativeToString(nativePtr);
    }

    /**
     * Helper class for boolean query construction.
     */
    public static class OccurQuery {
        private final Occur occur;
        private final Query query;

        public OccurQuery(Occur occur, Query query) {
            this.occur = occur;
            this.query = query;
        }

        public Occur getOccur() { return occur; }
        public Query getQuery() { return query; }
    }

    // Native method declarations
    private static native long nativeTermQuery(long schemaPtr, String fieldName, Object fieldValue, String indexOption);
    private static native long nativeTermSetQuery(long schemaPtr, String fieldName, List<Object> fieldValues);
    private static native long nativeAllQuery();
    private static native long nativeFuzzyTermQuery(long schemaPtr, String fieldName, String text, int distance, boolean transpositionCostOne, boolean prefix);
    private static native long nativePhraseQuery(long schemaPtr, String fieldName, List<Object> words, int slop);
    private static native long nativeBooleanQuery(List<OccurQuery> subqueries);
    private static native long nativeDisjunctionMaxQuery(long[] queryPtrs, Double tieBreaker);
    private static native long nativeBoostQuery(long queryPtr, double boost);
    private static native long nativeRegexQuery(long schemaPtr, String fieldName, String regexPattern);
    private static native long nativeWildcardQuery(long schemaPtr, String fieldName, String pattern);
    private static native long nativeWildcardQueryLenient(long schemaPtr, String fieldName, String pattern, boolean lenient);
    private static native long nativeMoreLikeThisQuery(long docAddressPtr, Integer minDocFrequency, Integer maxDocFrequency, Integer minTermFrequency, Integer maxQueryTerms, Integer minWordLength, Integer maxWordLength, Double boostFactor, List<String> stopWords);
    private static native long nativeConstScoreQuery(long queryPtr, double score);
    private static native long nativeRangeQuery(long schemaPtr, String fieldName, int fieldType, Object lowerBound, Object upperBound, boolean includeLower, boolean includeUpper);
    private static native long nativeExplain(long queryPtr, long searcherPtr, long docAddressPtr);
    private static native String nativeToString(long queryPtr);
    private static native void nativeClose(long ptr);
}
