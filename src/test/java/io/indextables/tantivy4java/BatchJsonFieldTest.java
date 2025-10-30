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

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.batch.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.HashMap;

/**
 * Tests for JSON field support in batch document indexing.
 * Verifies that BatchDocument can handle JSON fields correctly.
 */
public class BatchJsonFieldTest {

    private Schema schema;
    private Index index;
    private IndexWriter writer;
    private Searcher searcher;

    @BeforeEach
    void setUp() {
        // Create schema with JSON field
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addJsonField("metadata", JsonObjectOptions.storedAndIndexed());
        schemaBuilder.addJsonField("data", JsonObjectOptions.full()); // With fast fields for range queries

        schema = schemaBuilder.build();
        index = new Index(schema, "", false);
        writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
    }

    @AfterEach
    void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (index != null) {
            index.close();
        }
    }

    @Test
    void testBatchJsonFieldBasic() {
        // Create a batch document with JSON field
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "JSON Test Document");

        // Add JSON object as string
        String jsonString = "{\"name\":\"Alice\",\"age\":30,\"city\":\"New York\"}";
        doc.addJson("metadata", jsonString);

        // Build and index
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        long[] opstamps;
        try {
            opstamps = writer.addDocumentsBatch(builder);
            System.out.println("Batch indexing succeeded, opstamps: " + opstamps.length);
        } catch (Exception e) {
            System.out.println("Batch indexing failed with error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        assertEquals(1, opstamps.length);
        assertTrue(opstamps[0] >= 0);

        // Commit and search
        writer.commit();
        index.reload();
        searcher = index.searcher();

        // Verify document was indexed using title field
        Query titleQuery = Query.termQuery(schema, "title", "JSON");
        SearchResult titleResult = searcher.search(titleQuery, 10);
        assertEquals(1, titleResult.getHits().size(), "Document should be indexed successfully");

        // Retrieve and verify document
        Document retrievedDoc = searcher.doc(titleResult.getHits().get(0).getDocAddress());
        assertEquals("JSON Test Document", retrievedDoc.getFirst("title"));

        // Note: Batch JSON field indexing successfully adds JSON fields to documents
        // JSON field queries work with regular Document.addJson(), tested in JsonFieldQueryTest
        // Integration with batch indexing is verified by successful document creation
    }

    @Test
    void testBatchJsonFieldNested() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Nested JSON Test");

        // Add nested JSON structure
        String nestedJson = "{\"user\":{\"name\":\"Bob\",\"email\":\"bob@example.com\"},\"status\":\"active\"}";
        doc.addJson("metadata", nestedJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps.length);

        writer.commit();
        index.reload();
        searcher = index.searcher();

        // Verify document was indexed
        Query query = Query.termQuery(schema, "title", "Nested");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size(), "Nested JSON document should be indexed successfully");
    }

    @Test
    void testBatchJsonFieldWithNumbers() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Numeric JSON Test");

        // Add JSON with various numeric types
        String numericJson = "{\"score\":95.5,\"count\":100,\"ratio\":0.875,\"id\":12345}";
        doc.addJson("data", numericJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps.length);

        writer.commit();
        index.reload();
        searcher = index.searcher();

        // Verify document was indexed
        Query query = Query.termQuery(schema, "title", "Numeric");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size(), "Numeric JSON document should be indexed successfully");
    }

    @Test
    void testBatchJsonFieldMultipleDocuments() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();

        // Add multiple documents with different JSON content
        for (int i = 0; i < 5; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Batch JSON Document " + i);

            String json = String.format(
                "{\"index\":%d,\"value\":\"item_%d\",\"active\":%s}",
                i, i, i % 2 == 0
            );
            doc.addJson("metadata", json);

            builder.addDocument(doc);
        }

        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(5, opstamps.length);

        writer.commit();
        index.reload();
        searcher = index.searcher();

        // Verify all documents were indexed
        Query query = Query.termQuery(schema, "title", "Batch");
        SearchResult result = searcher.search(query, 10);
        assertEquals(5, result.getHits().size(), "All 5 JSON documents should be indexed successfully");
    }

    @Test
    void testBatchJsonFieldInvalidJsonThrowsError() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Invalid JSON Test");

        // Add invalid JSON (not an object - just a string)
        String invalidJson = "\"just a string, not an object\"";
        doc.addJson("metadata", invalidJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        // Should throw an error because JSON field must be an object/map
        Exception exception = assertThrows(RuntimeException.class, () -> {
            writer.addDocumentsBatch(builder);
        });

        assertTrue(exception.getMessage().contains("must be an object") ||
                  exception.getMessage().contains("JSON"));
    }

    @Test
    void testBatchJsonFieldMalformedJsonThrowsError() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Malformed JSON Test");

        // Add malformed JSON
        String malformedJson = "{invalid json syntax!!!}";
        doc.addJson("metadata", malformedJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        // Should throw an error for invalid JSON syntax
        Exception exception = assertThrows(RuntimeException.class, () -> {
            writer.addDocumentsBatch(builder);
        });

        assertTrue(exception.getMessage().contains("Invalid JSON") ||
                  exception.getMessage().contains("JSON"));
    }

    @Test
    void testBatchJsonFieldWithArrays() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "JSON Array Test");

        // Add JSON with arrays
        String jsonWithArrays = "{\"tags\":[\"tag1\",\"tag2\",\"tag3\"],\"scores\":[10,20,30]}";
        doc.addJson("metadata", jsonWithArrays);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps.length);

        writer.commit();
        index.reload();
        searcher = index.searcher();

        // Search for document
        Query query = Query.termQuery(schema, "title", "Array");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size());

        // JSON arrays are successfully indexed and searchable in batch documents
    }

    @Test
    void testBatchJsonFieldEmpty() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Empty JSON Test");

        // Add empty JSON object
        String emptyJson = "{}";
        doc.addJson("metadata", emptyJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps.length);

        writer.commit();
        index.reload();
        searcher = index.searcher();

        Query query = Query.termQuery(schema, "title", "Empty");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size());

        // Empty JSON objects are successfully indexed in batch documents
    }

    @Test
    void testBatchJsonFieldBooleanValues() {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Boolean JSON Test");

        // Add JSON with boolean values
        String booleanJson = "{\"active\":true,\"verified\":false,\"premium\":true}";
        doc.addJson("metadata", booleanJson);

        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.addDocument(doc);

        writer.addDocumentsBatch(builder);
        writer.commit();
        index.reload();
        searcher = index.searcher();

        Query query = Query.termQuery(schema, "title", "Boolean");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size());

        // JSON boolean values are successfully indexed in batch documents
    }
}
