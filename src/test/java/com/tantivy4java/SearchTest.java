package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class SearchTest {

    @TempDir
    Path tempDir;

    private Schema schema;
    private Field titleField;
    private Field bodyField;
    private Field scoreField;
    private Index index;

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
        
        index = Index.create(schema, tempDir);
        populateIndex();
    }

    @AfterEach
    void tearDown() {
        if (index != null) {
            index.close();
        }
        if (schema != null) {
            schema.close();
        }
    }

    private void populateIndex() {
        try (IndexWriter writer = index.writer(50)) {
            
            try (Document doc1 = new Document()) {
                doc1.addText(titleField, "Java Programming");
                doc1.addText(bodyField, "Learn Java programming with examples");
                doc1.addInteger(scoreField, 95L);
                writer.addDocument(doc1);
            }
            
            try (Document doc2 = new Document()) {
                doc2.addText(titleField, "Python Programming");
                doc2.addText(bodyField, "Python is easy to learn and powerful");
                doc2.addInteger(scoreField, 88L);
                writer.addDocument(doc2);
            }
            
            try (Document doc3 = new Document()) {
                doc3.addText(titleField, "Advanced Java");
                doc3.addText(bodyField, "Advanced Java concepts and patterns");
                doc3.addInteger(scoreField, 92L);
                writer.addDocument(doc3);
            }
            
            try (Document doc4 = new Document()) {
                doc4.addText(titleField, "Data Structures");
                doc4.addText(bodyField, "Learn data structures and algorithms");
                doc4.addInteger(scoreField, 90L);
                writer.addDocument(doc4);
            }
            
            writer.commit();
        }
    }

    @Test
    void testBasicTermSearch() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (TermQuery query = Query.term(titleField, "Java");
                 SearchResults results = searcher.search(query, 10)) {
                
                assertEquals(2, results.size());
                
                for (SearchResult result : results) {
                    Document doc = searcher.doc(result.getDocId());
                    String title = doc.getText(titleField);
                    assertTrue(title.contains("Java"));
                    doc.close();
                }
            }
        }
    }

    @Test
    void testTermSearchInBody() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (TermQuery query = Query.term(bodyField, "learn");
                 SearchResults results = searcher.search(query, 10)) {
                
                assertEquals(2, results.size());
                
                for (SearchResult result : results) {
                    Document doc = searcher.doc(result.getDocId());
                    String body = doc.getText(bodyField);
                    assertTrue(body.toLowerCase().contains("learn"));
                    doc.close();
                }
            }
        }
    }

    @Test
    void testRangeSearch() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (RangeQuery query = Query.range(scoreField, "90", "95");
                 SearchResults results = searcher.search(query, 10)) {
                
                assertEquals(2, results.size());
                
                for (SearchResult result : results) {
                    Document doc = searcher.doc(result.getDocId());
                    long score = doc.getInteger(scoreField);
                    assertTrue(score >= 90 && score <= 95);
                    doc.close();
                }
            }
        }
    }

    @Test
    void testBooleanMustSearch() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (BooleanQuery boolQuery = Query.bool();
                 TermQuery titleQuery = Query.term(titleField, "Programming");
                 TermQuery bodyQuery = Query.term(bodyField, "learn")) {
                
                boolQuery.must(titleQuery).must(bodyQuery);
                
                try (SearchResults results = searcher.search(boolQuery, 10)) {
                    assertEquals(1, results.size());
                    
                    SearchResult result = results.get(0);
                    Document doc = searcher.doc(result.getDocId());
                    String title = doc.getText(titleField);
                    assertTrue(title.contains("Programming"));
                    doc.close();
                }
            }
        }
    }

    @Test
    void testBooleanShouldSearch() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (BooleanQuery boolQuery = Query.bool();
                 TermQuery javaQuery = Query.term(titleField, "Java");
                 TermQuery pythonQuery = Query.term(titleField, "Python")) {
                
                boolQuery.should(javaQuery).should(pythonQuery);
                
                try (SearchResults results = searcher.search(boolQuery, 10)) {
                    assertEquals(3, results.size());
                }
            }
        }
    }

    @Test
    void testBooleanMustNotSearch() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (BooleanQuery boolQuery = Query.bool();
                 AllQuery allQuery = Query.all();
                 TermQuery excludeQuery = Query.term(titleField, "Python")) {
                
                boolQuery.must(allQuery).mustNot(excludeQuery);
                
                try (SearchResults results = searcher.search(boolQuery, 10)) {
                    assertEquals(3, results.size());
                    
                    for (SearchResult result : results) {
                        Document doc = searcher.doc(result.getDocId());
                        String title = doc.getText(titleField);
                        assertFalse(title.contains("Python"));
                        doc.close();
                    }
                }
            }
        }
    }

    @Test
    void testAllQuery() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (AllQuery allQuery = Query.all();
                 SearchResults results = searcher.search(allQuery, 10)) {
                
                assertEquals(4, results.size());
            }
        }
    }

    @Test
    void testSearchScoring() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (TermQuery query = Query.term(titleField, "Java");
                 SearchResults results = searcher.search(query, 10)) {
                
                assertEquals(2, results.size());
                
                SearchResult result1 = results.get(0);
                SearchResult result2 = results.get(1);
                
                assertTrue(result1.getScore() > 0.0f);
                assertTrue(result2.getScore() > 0.0f);
            }
        }
    }

    @Test
    void testLimitedResults() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (AllQuery allQuery = Query.all();
                 SearchResults results = searcher.search(allQuery, 2)) {
                
                assertEquals(2, results.size());
            }
        }
    }

    @Test
    void testComplexBooleanQuery() {
        try (IndexReader reader = index.reader()) {
            Searcher searcher = reader.searcher();
            
            try (BooleanQuery mainQuery = Query.bool();
                 BooleanQuery titleQuery = Query.bool();
                 TermQuery javaQuery = Query.term(titleField, "Java");
                 TermQuery programmingQuery = Query.term(titleField, "Programming");
                 TermQuery bodyQuery = Query.term(bodyField, "examples")) {
                
                titleQuery.should(javaQuery).should(programmingQuery);
                mainQuery.must(titleQuery).must(bodyQuery);
                
                try (SearchResults results = searcher.search(mainQuery, 10)) {
                    assertEquals(1, results.size());
                    
                    SearchResult result = results.get(0);
                    Document doc = searcher.doc(result.getDocId());
                    String title = doc.getText(titleField);
                    assertEquals("Java Programming", title);
                    doc.close();
                }
            }
        }
    }
}