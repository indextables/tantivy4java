package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class IndexWriterTest {

    @TempDir
    Path tempDir;

    private Schema schema;
    private Field titleField;
    private Field bodyField;

    @BeforeEach
    void setUp() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT)
               .addTextField("body", FieldType.TEXT);
        
        schema = builder.build();
        titleField = schema.getField("title");
        bodyField = schema.getField("body");
    }

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    void testWriterCreation() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            assertNotNull(writer);
        }
    }

    @Test
    void testAddDocument() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            try (Document doc = new Document()) {
                doc.addText(titleField, "Test Title");
                doc.addText(bodyField, "Test Body");
                
                assertDoesNotThrow(() -> writer.addDocument(doc));
            }
        }
    }

    @Test
    void testCommit() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            try (Document doc = new Document()) {
                doc.addText(titleField, "Test Title");
                writer.addDocument(doc);
            }
            
            assertDoesNotThrow(() -> writer.commit());
        }
    }

    @Test
    void testRollback() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            try (Document doc = new Document()) {
                doc.addText(titleField, "Test Title");
                writer.addDocument(doc);
            }
            
            assertDoesNotThrow(() -> writer.rollback());
        }
    }

    @Test
    void testMultipleDocuments() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            for (int i = 0; i < 10; i++) {
                try (Document doc = new Document()) {
                    doc.addText(titleField, "Title " + i);
                    doc.addText(bodyField, "Body content for document " + i);
                    writer.addDocument(doc);
                }
            }
            
            writer.commit();
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all();
                     SearchResults results = searcher.search(allQuery, 20)) {
                    assertEquals(10, results.size());
                }
            }
        }
    }

    @Test
    void testDeleteDocuments() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            try (Document doc1 = new Document()) {
                doc1.addText(titleField, "Keep This");
                doc1.addText(bodyField, "Important document");
                writer.addDocument(doc1);
            }
            
            try (Document doc2 = new Document()) {
                doc2.addText(titleField, "Delete This");
                doc2.addText(bodyField, "Unwanted document");
                writer.addDocument(doc2);
            }
            
            writer.commit();
            
            writer.deleteDocuments("title", "Delete This");
            writer.commit();
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all();
                     SearchResults results = searcher.search(allQuery, 10)) {
                    assertEquals(1, results.size());
                    
                    SearchResult result = results.get(0);
                    Document remainingDoc = searcher.doc(result.getDocId());
                    assertEquals("Keep This", remainingDoc.getText(titleField));
                    remainingDoc.close();
                }
            }
        }
    }

    @Test
    void testCommitAndRollback() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(50)) {
            
            try (Document doc1 = new Document()) {
                doc1.addText(titleField, "Committed Document");
                writer.addDocument(doc1);
            }
            writer.commit();
            
            try (Document doc2 = new Document()) {
                doc2.addText(titleField, "Rolled Back Document");
                writer.addDocument(doc2);
            }
            writer.rollback();
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all();
                     SearchResults results = searcher.search(allQuery, 10)) {
                    assertEquals(1, results.size());
                    
                    SearchResult result = results.get(0);
                    Document remainingDoc = searcher.doc(result.getDocId());
                    assertEquals("Committed Document", remainingDoc.getText(titleField));
                    remainingDoc.close();
                }
            }
        }
    }

    @Test
    void testLargeHeapSize() {
        try (Index index = Index.create(schema, tempDir);
             IndexWriter writer = index.writer(100)) {
            
            for (int i = 0; i < 100; i++) {
                try (Document doc = new Document()) {
                    doc.addText(titleField, "Large batch document " + i);
                    doc.addText(bodyField, "Content for document " + i + " with more text to fill space");
                    writer.addDocument(doc);
                }
            }
            
            writer.commit();
            
            try (IndexReader reader = index.reader()) {
                Searcher searcher = reader.searcher();
                
                try (AllQuery allQuery = Query.all();
                     SearchResults results = searcher.search(allQuery, 150)) {
                    assertEquals(100, results.size());
                }
            }
        }
    }
}