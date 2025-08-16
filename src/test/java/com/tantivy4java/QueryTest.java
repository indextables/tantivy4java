package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

public class QueryTest {

    private Schema schema;
    private Field titleField;
    private Field bodyField;
    private Field scoreField;

    @BeforeEach
    void setUp() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT)
               .addTextField("body", FieldType.TEXT)
               .addIntField("score", FieldType.INTEGER);
        
        schema = builder.build();
        titleField = schema.getField("title");
        bodyField = schema.getField("body");
        scoreField = schema.getField("score");
    }

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    void testTermQuery() {
        try (TermQuery query = Query.term(titleField, "hello")) {
            assertNotNull(query);
        }
    }

    @Test
    void testRangeQuery() {
        try (RangeQuery query = Query.range(scoreField, "10", "100")) {
            assertNotNull(query);
        }
    }

    @Test
    void testBooleanQuery() {
        try (BooleanQuery boolQuery = Query.bool()) {
            assertNotNull(boolQuery);
            
            try (TermQuery titleQuery = Query.term(titleField, "hello");
                 TermQuery bodyQuery = Query.term(bodyField, "world")) {
                
                boolQuery.must(titleQuery);
                boolQuery.should(bodyQuery);
                
                assertNotNull(boolQuery);
            }
        }
    }

    @Test
    void testBooleanQueryMustNot() {
        try (BooleanQuery boolQuery = Query.bool()) {
            try (TermQuery excludeQuery = Query.term(titleField, "spam")) {
                boolQuery.mustNot(excludeQuery);
                assertNotNull(boolQuery);
            }
        }
    }

    @Test
    void testAllQuery() {
        try (AllQuery allQuery = Query.all()) {
            assertNotNull(allQuery);
        }
    }

    @Test
    void testComplexBooleanQuery() {
        try (BooleanQuery mainQuery = Query.bool()) {
            try (TermQuery titleQuery = Query.term(titleField, "important");
                 BooleanQuery subQuery = Query.bool();
                 TermQuery bodyQuery1 = Query.term(bodyField, "news");
                 TermQuery bodyQuery2 = Query.term(bodyField, "update")) {
                
                subQuery.should(bodyQuery1).should(bodyQuery2);
                mainQuery.must(titleQuery).must(subQuery);
                
                assertNotNull(mainQuery);
            }
        }
    }

    @Test
    void testQueryResourceManagement() {
        TermQuery query1 = Query.term(titleField, "test1");
        TermQuery query2 = Query.term(titleField, "test2");
        BooleanQuery boolQuery = Query.bool();
        
        boolQuery.must(query1).should(query2);
        
        query1.close();
        query2.close();
        boolQuery.close();
    }

    @Test
    void testMultipleTermQueries() {
        try (TermQuery query1 = Query.term(titleField, "first");
             TermQuery query2 = Query.term(titleField, "second");
             TermQuery query3 = Query.term(bodyField, "content")) {
            
            assertNotNull(query1);
            assertNotNull(query2);
            assertNotNull(query3);
        }
    }

    @Test
    void testRangeQueryDifferentFields() {
        try (RangeQuery scoreRange = Query.range(scoreField, "1", "10");
             RangeQuery titleRange = Query.range(titleField, "a", "z")) {
            
            assertNotNull(scoreRange);
            assertNotNull(titleRange);
        }
    }
}