package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Iterator;

public class BasicIndexTest {

    @TempDir
    Path tempDir;

    private Schema schema;
    private Field titleField;
    private Field bodyField;

    @BeforeEach
    void setUp() {
        SchemaBuilder schemaBuilder = Schema.builder();
        titleField = schemaBuilder.addTextField("title", FieldType.TEXT).build().getField("title");
        bodyField = schemaBuilder.addTextField("body", FieldType.TEXT).build().getField("body");
        schema = schemaBuilder.build();
    }

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    void testCreateIndex() {
        try (Index index = Index.create(schema, tempDir)) {
            assertNotNull(index);
            
            Schema retrievedSchema = index.schema();
            assertNotNull(retrievedSchema);
            
            Field retrievedTitleField = retrievedSchema.getField("title");
            assertNotNull(retrievedTitleField);
            assertEquals("title", retrievedTitleField.getName());
            
            retrievedSchema.close();
        }
    }

    @Test
    void testIndexWriter() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            assertNotNull(writer);
            
            try (Document doc = new Document()) {
                doc.addText(titleField, "Hello World");
                doc.addText(bodyField, "This is a test document");
                writer.addDocument(doc);
            }
            
            writer.commit();
        }
    }

    @Test
    void testBasicSearch() {
        try (Index index = Index.create(schema, tempDir)) {
            
            try (IndexWriter writer = index.writer(50)) {
                try (Document doc1 = new Document()) {
                    doc1.addText(titleField, "Hello World");
                    doc1.addText(bodyField, "This is a test document");
                    writer.addDocument(doc1);
                }
                
                try (Document doc2 = new Document()) {
                    doc2.addText(titleField, "Goodbye World");
                    doc2.addText(bodyField, "This is another test document");
                    writer.addDocument(doc2);
                }
                
                writer.commit();
            }
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (TermQuery query = Query.term(titleField, "Hello")) {
                    try (SearchResults results = searcher.search(query, 10)) {
                        assertEquals(1, results.size());
                        
                        SearchResult result = results.get(0);
                        assertTrue(result.getScore() > 0.0f);
                        assertTrue(result.getDocId() >= 0);
                    }
                }
            }
        }
    }

    @Test
    void testBooleanQuery() {
        try (Index index = Index.create(schema, tempDir)) {
            
            try (IndexWriter writer = index.writer(50)) {
                try (Document doc1 = new Document()) {
                    doc1.addText(titleField, "Java Programming");
                    doc1.addText(bodyField, "Learn Java programming language");
                    writer.addDocument(doc1);
                }
                
                try (Document doc2 = new Document()) {
                    doc2.addText(titleField, "Python Programming");
                    doc2.addText(bodyField, "Learn Python programming language");
                    writer.addDocument(doc2);
                }
                
                writer.commit();
            }
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (BooleanQuery boolQuery = Query.bool();
                     TermQuery titleQuery = Query.term(titleField, "Programming");
                     TermQuery bodyQuery = Query.term(bodyField, "Learn")) {
                    
                    boolQuery.must(titleQuery).must(bodyQuery);
                    
                    try (SearchResults results = searcher.search(boolQuery, 10)) {
                        assertEquals(2, results.size());
                    }
                }
            }
        }
    }

    @Test
    void testAllQuery() {
        try (Index index = Index.create(schema, tempDir)) {
            
            try (IndexWriter writer = index.writer(50)) {
                try (Document doc1 = new Document()) {
                    doc1.addText(titleField, "Document 1");
                    writer.addDocument(doc1);
                }
                
                try (Document doc2 = new Document()) {
                    doc2.addText(titleField, "Document 2");
                    writer.addDocument(doc2);
                }
                
                writer.commit();
            }
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all()) {
                    try (SearchResults results = searcher.search(allQuery, 10)) {
                        assertEquals(2, results.size());
                    }
                }
            }
        }
    }

    @Test
    void testDocumentRetrieval() {
        try (Index index = Index.create(schema, tempDir)) {
            
            try (IndexWriter writer = index.writer(50)) {
                try (Document doc = new Document()) {
                    doc.addText(titleField, "Test Document");
                    doc.addText(bodyField, "This is the body content");
                    writer.addDocument(doc);
                }
                
                writer.commit();
            }
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all()) {
                    try (SearchResults results = searcher.search(allQuery, 10)) {
                        assertEquals(1, results.size());
                        
                        SearchResult result = results.get(0);
                        Document retrievedDoc = searcher.doc(result.getDocId());
                        
                        assertEquals("Test Document", retrievedDoc.getText(titleField));
                        assertEquals("This is the body content", retrievedDoc.getText(bodyField));
                        
                        retrievedDoc.close();
                    }
                }
            }
        }
    }

    @Test
    void testIterableSearchResults() {
        try (Index index = Index.create(schema, tempDir)) {
            
            try (IndexWriter writer = index.writer(50)) {
                for (int i = 0; i < 5; i++) {
                    try (Document doc = new Document()) {
                        doc.addText(titleField, "Document " + i);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all()) {
                    try (SearchResults results = searcher.search(allQuery, 10)) {
                        assertEquals(5, results.size());
                        
                        int count = 0;
                        for (SearchResult result : results) {
                            assertNotNull(result);
                            assertTrue(result.getScore() > 0.0f);
                            count++;
                        }
                        assertEquals(5, count);
                    }
                }
            }
        }
    }
}