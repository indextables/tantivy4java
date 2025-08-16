package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class SimpleValidationTest {

    @TempDir
    Path tempDir;

    @Test
    void testBasicFunctionality() {
        // Test schema creation
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT);
        
        try (Schema schema = builder.build()) {
            assertNotNull(schema);
            
            Field titleField = schema.getField("title");
            assertNotNull(titleField);
            assertEquals("title", titleField.getName());
            
            // Test index creation
            try (Index index = Index.create(schema, tempDir)) {
                assertNotNull(index);
                
                // Test document creation and storage
                try (Document doc = new Document()) {
                    doc.addText(titleField, "Hello World");
                    assertEquals("Hello World", doc.getText(titleField));
                }
                
                // Test index writer
                try (IndexWriter writer = index.writer(50)) {
                    assertNotNull(writer);
                    
                    try (Document doc = new Document()) {
                        doc.addText(titleField, "Test Document");
                        writer.addDocument(doc);
                    }
                    
                    writer.commit();
                }
                
                // Test search
                try (IndexReader reader = index.reader()) {
                    assertNotNull(reader);
                    
                    Searcher searcher = reader.searcher();
                    assertNotNull(searcher);
                    
                    try (TermQuery query = Query.term(titleField, "Test");
                         SearchResults results = searcher.search(query, 10)) {
                        
                        assertNotNull(results);
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
    void testIsolation() {
        // First index
        SchemaBuilder builder1 = Schema.builder();
        builder1.addTextField("title", FieldType.TEXT);
        
        try (Schema schema1 = builder1.build()) {
            Field titleField1 = schema1.getField("title");
            
            try (Index index1 = Index.create(schema1, tempDir.resolve("index1"))) {
                try (IndexWriter writer1 = index1.writer(50)) {
                    try (Document doc = new Document()) {
                        doc.addText(titleField1, "Doc 1");
                        writer1.addDocument(doc);
                    }
                    writer1.commit();
                }
                
                try (IndexReader reader1 = index1.reader()) {
                    Searcher searcher1 = reader1.searcher();
                    try (AllQuery query1 = Query.all();
                         SearchResults results1 = searcher1.search(query1, 10)) {
                        assertEquals(1, results1.size(), "Index 1 should have 1 document");
                    }
                }
            }
        }
        
        // Second index (should be isolated)
        SchemaBuilder builder2 = Schema.builder();
        builder2.addTextField("title", FieldType.TEXT);
        
        try (Schema schema2 = builder2.build()) {
            Field titleField2 = schema2.getField("title");
            
            try (Index index2 = Index.create(schema2, tempDir.resolve("index2"))) {
                try (IndexWriter writer2 = index2.writer(50)) {
                    try (Document doc1 = new Document()) {
                        doc1.addText(titleField2, "Doc A");
                        writer2.addDocument(doc1);
                    }
                    try (Document doc2 = new Document()) {
                        doc2.addText(titleField2, "Doc B");
                        writer2.addDocument(doc2);
                    }
                    writer2.commit();
                }
                
                try (IndexReader reader2 = index2.reader()) {
                    Searcher searcher2 = reader2.searcher();
                    try (AllQuery query2 = Query.all();
                         SearchResults results2 = searcher2.search(query2, 10)) {
                        assertEquals(2, results2.size(), "Index 2 should have 2 documents");
                    }
                }
            }
        }
    }
}