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

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import io.indextables.tantivy4java.query.Occur;
import java.util.ArrayList;

/**
 * Comprehensive tests for batch document indexing functionality.
 * Tests the BatchDocument, BatchDocumentBuilder, and native batch processing.
 */
public class BatchDocumentIndexingTest {
    
    private Schema schema;
    private Index index;
    private IndexWriter writer;
    private Searcher searcher;
    
    @BeforeEach
    void setUp() {
        // Create a schema with various field types for comprehensive testing
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("count", true, true, false);
        schemaBuilder.addFloatField("score", true, true, false);
        schemaBuilder.addBooleanField("published", true, true, false);
        schemaBuilder.addDateField("timestamp", true, true, false);
        // Note: Skipping bytes and json fields for now as they're not implemented
        schemaBuilder.addTextField("data", true, false, "default", "position"); // Use text instead of bytes
        schemaBuilder.addTextField("metadata", true, false, "default", "position"); // Use text instead of json
        schemaBuilder.addTextField("client_ip", true, false, "default", "position"); // Use text instead of IP
        schemaBuilder.addUnsignedField("id", true, true, false);
        
        schema = schemaBuilder.build();
        index = new Index(schema, "", false);
        writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1); // 50MB heap, 1 thread
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
    void testBatchDocumentBasicCreation() {
        BatchDocument doc = new BatchDocument();
        assertTrue(doc.isEmpty());
        assertEquals(0, doc.getNumFields());
        
        doc.addText("title", "Test Document");
        doc.addInteger("count", 42);
        doc.addFloat("score", 3.14);
        doc.addBoolean("published", true);
        
        assertFalse(doc.isEmpty());
        assertEquals(4, doc.getNumFields());
        assertTrue(doc.getFieldNames().contains("title"));
        assertTrue(doc.getFieldNames().contains("count"));
        assertTrue(doc.getFieldNames().contains("score"));
        assertTrue(doc.getFieldNames().contains("published"));
    }
    
    @Test
    void testBatchDocumentFromMap() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "Test Document");
        fields.put("count", 42L);
        fields.put("score", 3.14);
        fields.put("published", true);
        fields.put("timestamp", LocalDateTime.now());
        
        BatchDocument doc = BatchDocument.fromMap(fields);
        assertEquals(5, doc.getNumFields());
        
        // Test multi-value fields
        fields.put("tags", Arrays.asList("tag1", "tag2", "tag3"));
        BatchDocument docWithMultiValue = BatchDocument.fromMap(fields);
        assertEquals(6, docWithMultiValue.getNumFields());
    }
    
    @Test 
    void testBatchDocumentBuilderBasicFunctionality() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        assertTrue(builder.isEmpty());
        assertEquals(0, builder.getDocumentCount());
        
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Document 1");
        doc1.addInteger("count", 1);
        
        BatchDocument doc2 = new BatchDocument();
        doc2.addText("title", "Document 2");
        doc2.addInteger("count", 2);
        
        builder.addDocument(doc1).addDocument(doc2);
        
        assertFalse(builder.isEmpty());
        assertEquals(2, builder.getDocumentCount());
        assertTrue(builder.getEstimatedSize() > 0);
    }
    
    @Test
    void testBatchDocumentBuilderFromMaps() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        Map<String, Object> doc1Fields = new HashMap<>();
        doc1Fields.put("title", "Document 1");
        doc1Fields.put("count", 1L);
        doc1Fields.put("score", 1.0);
        
        Map<String, Object> doc2Fields = new HashMap<>();
        doc2Fields.put("title", "Document 2");
        doc2Fields.put("count", 2L);
        doc2Fields.put("score", 2.0);
        
        builder.addDocumentFromMap(doc1Fields)
               .addDocumentFromMap(doc2Fields);
               
        assertEquals(2, builder.getDocumentCount());
        
        // Test adding multiple documents from list
        BatchDocument doc3 = BatchDocument.fromMap(Map.of(
            "title", "Document 3",
            "count", 3L,
            "score", 3.0
        ));
        
        builder.addDocuments(Arrays.asList(doc3));
        assertEquals(3, builder.getDocumentCount());
    }
    
    @Test
    void testBatchDocumentBuilderSerialization() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Add documents with various field types
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Test Document 1");
        doc1.addInteger("count", 100);
        doc1.addUnsigned("id", 1001L);
        doc1.addFloat("score", 95.5);
        doc1.addBoolean("published", true);
        doc1.addDate("timestamp", LocalDateTime.of(2023, 1, 1, 12, 0));
        doc1.addText("data", "binary data"); // Use text instead of bytes
        doc1.addText("metadata", "{\"key1\":\"value1\",\"key2\":42}"); // Use text instead of json
        doc1.addText("client_ip", "192.168.1.1"); // Use text instead of IP
        
        BatchDocument doc2 = new BatchDocument();
        doc2.addText("title", "Test Document 2");
        doc2.addInteger("count", 200);
        doc2.addUnsigned("id", 1002L);
        doc2.addFloat("score", 87.3);
        doc2.addBoolean("published", false);
        
        builder.addDocument(doc1).addDocument(doc2);
        
        // Test ByteBuffer serialization
        ByteBuffer buffer = builder.build();
        assertNotNull(buffer);
        assertTrue(buffer.isDirect());
        assertTrue(buffer.remaining() > 0);
        
        // Test byte array serialization
        byte[] array = builder.buildArray();
        assertNotNull(array);
        assertTrue(array.length > 0);
        assertEquals(buffer.remaining(), array.length);
    }
    
    @Test
    void testBatchDocumentBuilderValidation() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Test null document validation
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addDocument(null);
        });
        
        // Test null document list validation
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addDocuments(null);
        });
        
        // Test build with empty builder
        assertThrows(IllegalStateException.class, () -> {
            builder.build();
        });
        
        // Test build with empty builder (array version)
        assertThrows(IllegalStateException.class, () -> {
            builder.buildArray();
        });
    }
    
    @Test
    void testSingleDocumentBatchIndexing() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Single Batch Document");
        doc.addText("content", "This document was added via batch indexing");
        doc.addInteger("count", 42);
        doc.addFloat("score", 3.14);
        doc.addBoolean("published", true);
        doc.addUnsigned("id", 12345L);
        
        builder.addDocument(doc);
        
        // Add documents via batch
        long[] opstamps = writer.addDocumentsBatch(builder);
        assertNotNull(opstamps);
        assertEquals(1, opstamps.length);
        // Note: In-memory indices may return 0 as opstamp, which is acceptable
        assertTrue(opstamps[0] >= 0, "Expected non-negative opstamp but got: " + opstamps[0]);
        
        // Commit and verify
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        Query query = Query.termQuery(schema, "title", "Single");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size());
        
        Document retrievedDoc = searcher.doc(result.getHits().get(0).getDocAddress());
        assertEquals("Single Batch Document", retrievedDoc.getFirst("title"));
        assertEquals(42L, retrievedDoc.getFirst("count"));
        assertEquals(3.14, (Double) retrievedDoc.getFirst("score"), 0.001);
        assertEquals(true, retrievedDoc.getFirst("published"));
        assertEquals(12345L, retrievedDoc.getFirst("id"));
    }
    
    @Test
    void testMultiDocumentBatchIndexing() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Create multiple documents with different content
        for (int i = 0; i < 10; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Batch Document " + i);
            doc.addText("content", "Content for document number " + i);
            doc.addInteger("count", i * 10);
            doc.addFloat("score", i * 1.5);
            doc.addBoolean("published", i % 2 == 0);
            doc.addUnsigned("id", 1000L + i);
            
            builder.addDocument(doc);
        }
        
        // Add documents via batch
        long[] opstamps = writer.addDocumentsBatch(builder);
        assertNotNull(opstamps);
        assertEquals(10, opstamps.length);
        
        // Verify all opstamps are non-negative (in-memory indices may return 0)
        for (long opstamp : opstamps) {
            assertTrue(opstamp >= 0);
        }
        
        // Commit and verify
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        // Search for all batch documents
        Query query = Query.termQuery(schema, "title", "Batch");
        SearchResult result = searcher.search(query, 20);
        assertEquals(10, result.getHits().size());
        
        // Verify specific documents
        Query specificQuery = Query.termQuery(schema, "title", "Document");
        SearchResult specificResult = searcher.search(specificQuery, 20);
        assertEquals(10, specificResult.getHits().size());
    }
    
    @Test
    void testBatchIndexingWithDirectBuffer() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Direct Buffer Document 1");
        doc1.addInteger("count", 100);
        
        BatchDocument doc2 = new BatchDocument();
        doc2.addText("title", "Direct Buffer Document 2");
        doc2.addInteger("count", 200);
        
        builder.addDocument(doc1).addDocument(doc2);
        
        // Get the direct buffer
        ByteBuffer buffer = builder.build();
        
        // Add documents via direct buffer
        long[] opstamps = writer.addDocumentsByBuffer(buffer);
        assertNotNull(opstamps);
        assertEquals(2, opstamps.length);
        
        // Commit and verify
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        Query query = Query.termQuery(schema, "title", "Direct");
        SearchResult result = searcher.search(query, 10);
        assertEquals(2, result.getHits().size());
    }
    
    @Test
    void testBatchIndexingValidation() {
        // Test null buffer validation
        assertThrows(IllegalArgumentException.class, () -> {
            writer.addDocumentsByBuffer(null);
        });
        
        // Test non-direct buffer validation
        ByteBuffer nonDirectBuffer = ByteBuffer.allocate(100);
        assertThrows(IllegalArgumentException.class, () -> {
            writer.addDocumentsByBuffer(nonDirectBuffer);
        });
        
        // Test empty buffer validation
        ByteBuffer emptyBuffer = ByteBuffer.allocateDirect(0);
        assertThrows(IllegalArgumentException.class, () -> {
            writer.addDocumentsByBuffer(emptyBuffer);
        });
        
        // Test null builder validation
        assertThrows(IllegalArgumentException.class, () -> {
            writer.addDocumentsBatch(null);
        });
        
        // Test empty builder validation
        BatchDocumentBuilder emptyBuilder = new BatchDocumentBuilder();
        assertThrows(IllegalArgumentException.class, () -> {
            writer.addDocumentsBatch(emptyBuilder);
        });
    }
    
    @Test
    void testLargeBatchIndexing() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Create a large batch of documents
        int batchSize = 1000;
        for (int i = 0; i < batchSize; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Large Batch Document " + i);
            doc.addText("content", "This is a large batch test document with index " + i);
            doc.addInteger("count", i);
            doc.addFloat("score", i * 0.1);
            doc.addBoolean("published", i % 3 == 0);
            doc.addUnsigned("id", 10000L + i);
            
            builder.addDocument(doc);
        }
        
        // Track estimated size
        int estimatedSize = builder.getEstimatedSize();
        assertTrue(estimatedSize > batchSize * 50); // Should be at least 50 bytes per document
        
        // Add documents via batch
        long[] opstamps = writer.addDocumentsBatch(builder);
        assertNotNull(opstamps);
        assertEquals(batchSize, opstamps.length);
        
        // Commit and verify
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        // Search for all large batch documents
        Query query = Query.termQuery(schema, "title", "Large");
        SearchResult result = searcher.search(query, batchSize + 10);
        assertEquals(batchSize, result.getHits().size());
        
        // Verify specific document in the middle
        Query specificQuery = Query.termQuery(schema, "title", "Document");
        Query rangeQuery = Query.rangeQuery(schema, "count", FieldType.INTEGER, 400L, 600L, true, true);
        List<Query.OccurQuery> mustQueries = new ArrayList<>();
        mustQueries.add(new Query.OccurQuery(Occur.MUST, specificQuery));
        mustQueries.add(new Query.OccurQuery(Occur.MUST, rangeQuery));
        specificQuery = Query.booleanQuery(mustQueries);
        
        SearchResult specificResult = searcher.search(specificQuery, 300);
        assertEquals(201, specificResult.getHits().size()); // 400 to 600 inclusive
    }
    
    @Test
    void testBatchIndexingWithComplexTypes() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "Complex Types Document");
        doc.addDate("timestamp", LocalDateTime.of(2023, 6, 15, 14, 30, 45));
        doc.addText("data", "complex binary data with special chars: ñáéíóú"); // Use text instead of bytes
        doc.addText("metadata", "{\"nested\":{\"key\":\"value\"},\"array\":[1,2,3],\"string\":\"test value\"}"); // Use text instead of json
        doc.addText("client_ip", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"); // Use text instead of IP
        
        builder.addDocument(doc);
        
        // Add documents via batch
        long[] opstamps = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps.length);
        
        // Commit and verify
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        Query query = Query.termQuery(schema, "title", "Complex");
        SearchResult result = searcher.search(query, 10);
        assertEquals(1, result.getHits().size());
        
        Document retrievedDoc = searcher.doc(result.getHits().get(0).getDocAddress());
        assertNotNull(retrievedDoc.getFirst("timestamp"));
        assertNotNull(retrievedDoc.getFirst("data"));
        assertNotNull(retrievedDoc.getFirst("metadata"));
        assertNotNull(retrievedDoc.getFirst("client_ip"));
    }
    
    @Test
    void testBatchBuilderReuse() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // First batch
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Reuse Test 1");
        builder.addDocument(doc1);
        
        long[] opstamps1 = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps1.length);
        
        // Clear and reuse builder
        builder.clear();
        assertTrue(builder.isEmpty());
        assertEquals(0, builder.getDocumentCount());
        
        // Second batch
        BatchDocument doc2 = new BatchDocument();
        doc2.addText("title", "Reuse Test 2");
        builder.addDocument(doc2);
        
        long[] opstamps2 = writer.addDocumentsBatch(builder);
        assertEquals(1, opstamps2.length);
        
        // Verify both documents were indexed
        writer.commit();
        index.reload();
        searcher = index.searcher();
        
        Query query = Query.termQuery(schema, "title", "Reuse");
        SearchResult result = searcher.search(query, 10);
        assertEquals(2, result.getHits().size());
    }
    
    @Test
    void testBatchIndexingPerformanceCharacteristics() {
        // This test verifies that batch indexing maintains expected performance characteristics
        int smallBatchSize = 10;
        int largeBatchSize = 100;
        
        // Small batch test
        BatchDocumentBuilder smallBuilder = new BatchDocumentBuilder();
        for (int i = 0; i < smallBatchSize; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Performance Test Small " + i);
            doc.addInteger("count", i);
            smallBuilder.addDocument(doc);
        }
        
        long startTime = System.currentTimeMillis();
        long[] smallOps = writer.addDocumentsBatch(smallBuilder);
        long smallBatchTime = System.currentTimeMillis() - startTime;
        
        assertEquals(smallBatchSize, smallOps.length);
        
        // Large batch test
        BatchDocumentBuilder largeBuilder = new BatchDocumentBuilder();
        for (int i = 0; i < largeBatchSize; i++) {
            BatchDocument doc = new BatchDocument();
            doc.addText("title", "Performance Test Large " + i);
            doc.addInteger("count", i);
            largeBuilder.addDocument(doc);
        }
        
        startTime = System.currentTimeMillis();
        long[] largeOps = writer.addDocumentsBatch(largeBuilder);
        long largeBatchTime = System.currentTimeMillis() - startTime;
        
        assertEquals(largeBatchSize, largeOps.length);
        
        // Verify that larger batches aren't drastically slower (allowing for some overhead)
        // The key benefit is fewer JNI calls, so per-document time should improve
        double smallPerDoc = (double) smallBatchTime / smallBatchSize;
        double largePerDoc = (double) largeBatchTime / largeBatchSize;
        
        // Handle cases where timing is too small to measure accurately
        if (smallBatchTime < 5) { // Less than 5ms - measurement is unreliable
            // For very fast operations, just verify large batch completes reasonably
            assertTrue(largePerDoc <= 1.0, // Should be under 1ms per document
                String.format("Large batch per-doc time (%.3f) should be reasonable", largePerDoc));
        } else {
            // Large batch should be at least as efficient as small batch (or better)
            // Allow some margin for measurement variance
            assertTrue(largePerDoc <= smallPerDoc * 2.0, 
                String.format("Large batch per-doc time (%.3f) should be similar or better than small batch (%.3f)", 
                             largePerDoc, smallPerDoc));
        }
    }
    
    @Test
    void testAutoFlushOnClose() {
        // Test automatic flushing with try-with-resources
        int documentCount = 0;
        try (BatchDocumentBuilder builder = new BatchDocumentBuilder(writer)) {
            // Add some documents
            for (int i = 0; i < 5; i++) {
                BatchDocument doc = new BatchDocument();
                doc.addText("title", "Auto-flush Document " + i);
                doc.addText("content", "This document should be auto-flushed on close");
                doc.addInteger("count", i);
                builder.addDocument(doc);
                documentCount++;
            }
            
            // Check builder state
            assertEquals(5, builder.getDocumentCount());
            assertFalse(builder.isClosed());
            assertTrue(builder.hasAssociatedWriter());
        } // close() is called here automatically
        
        // Verify documents were flushed
        writer.commit();
        index.reload();
        
        try (Searcher searcher = index.searcher()) {
            Query query = Query.termQuery(schema, "title", "Document");
            SearchResult result = searcher.search(query, 10);
            assertEquals(documentCount, result.getHits().size());
        }
    }
    
    @Test
    void testManualFlush() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.associateWith(writer);
        
        // Add documents
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Manual Flush Test");
        doc1.addText("content", "Test manual flushing");
        builder.addDocument(doc1);
        
        assertEquals(1, builder.getDocumentCount());
        assertTrue(builder.hasAssociatedWriter());
        
        // Manual flush
        long[] opstamps = builder.flush();
        assertEquals(1, opstamps.length);
        assertTrue(opstamps[0] >= 0);
        
        // Should be empty after flush
        assertEquals(0, builder.getDocumentCount());
        
        builder.close(); // Should be safe to call after manual flush
    }
    
    @Test
    void testClosedBuilderThrowsException() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        builder.close();
        
        assertTrue(builder.isClosed());
        
        // Should throw exception when trying to add documents to closed builder
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "This should fail");
        
        assertThrows(IllegalStateException.class, () -> {
            builder.addDocument(doc);
        });
    }
    
    @Test
    void testFlushWithoutAssociatedWriter() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        BatchDocument doc = new BatchDocument();
        doc.addText("title", "No Writer Test");
        builder.addDocument(doc);
        
        assertFalse(builder.hasAssociatedWriter());
        
        // Flush should return empty array when no writer associated
        long[] result = builder.flush();
        assertEquals(0, result.length);
        
        // Document should still be there since flush did nothing
        assertEquals(1, builder.getDocumentCount());
        
        builder.close();
    }

    @Test
    void testBatchSizeTracking() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Initially empty
        assertTrue(builder.isEmpty());
        assertEquals(0, builder.getDocumentCount());
        
        // Test size tracking with first document
        BatchDocument doc1 = new BatchDocument();
        doc1.addText("title", "Size Tracking Test 1");
        doc1.addInteger("count", 100);
        
        builder.addDocument(doc1);
        
        int sizeAfterOne = builder.getEstimatedSize();
        assertTrue(sizeAfterOne > 0);
        assertFalse(builder.wouldExceedSize(sizeAfterOne + 1000, doc1)); // Plenty of room
        
        // Add second document
        BatchDocument doc2 = new BatchDocument();
        doc2.addText("title", "Size Tracking Test 2");
        doc2.addInteger("count", 200);
        
        builder.addDocument(doc2);
        
        int sizeAfterTwo = builder.getEstimatedSize();
        assertTrue(sizeAfterTwo > sizeAfterOne);
        
        // Test current size (actual serialized size)
        int currentSize = builder.getCurrentSize();
        assertTrue(currentSize > 0);
        
        // Test formatted size
        String formattedSize = builder.getFormattedSize();
        assertNotNull(formattedSize);
        assertTrue(formattedSize.contains("bytes") || formattedSize.contains("KB"));
        
        // Test memory stats
        BatchDocumentBuilder.BatchMemoryStats stats = builder.getMemoryStats();
        assertEquals(2, stats.getDocumentCount());
        assertTrue(stats.getEstimatedSize() > 0);
        assertTrue(stats.getAverageDocumentSize() > 0);
        assertTrue(stats.getMaxDocumentSize() >= stats.getMinDocumentSize());
        assertTrue(stats.getDocumentsPerMB() > 0);
        
        // Test formatted stats
        assertNotNull(stats.getFormattedSize());
        assertNotNull(stats.getFormattedAverageDocumentSize());
        assertNotNull(stats.getSummary());
        assertTrue(stats.getSummary().contains("Documents: 2"));
    }
    
    @Test
    void testBatchSizeLimiting() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Create a moderately sized document
        BatchDocument testDoc = new BatchDocument();
        testDoc.addText("title", "Size Limiting Test Document");
        testDoc.addText("content", "This document is used for testing size limiting functionality. ".repeat(10));
        testDoc.addInteger("count", 42);
        testDoc.addFloat("score", 3.14);
        
        builder.addDocument(testDoc);
        
        // Get size with one document
        int sizeWithOne = builder.getEstimatedSize();
        
        // Test size limiting
        BatchDocument anotherDoc = new BatchDocument();
        anotherDoc.addText("title", "Another Test Document");
        anotherDoc.addText("content", "Additional content for size testing. ".repeat(20));
        
        // With a very small limit, it should exceed
        assertTrue(builder.wouldExceedSize(sizeWithOne / 2, anotherDoc));
        
        // With a generous limit, it shouldn't exceed  
        assertFalse(builder.wouldExceedSize(sizeWithOne * 10, anotherDoc));
        
        // Test that first document is always allowed
        BatchDocumentBuilder newBuilder = new BatchDocumentBuilder();
        assertFalse(newBuilder.wouldExceedSize(1, testDoc)); // Even tiny limit allows first doc
    }
    
    @Test
    void testMemoryStatsAccuracy() {
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Add documents of varying sizes
        BatchDocument smallDoc = new BatchDocument();
        smallDoc.addText("title", "Small");
        smallDoc.addInteger("count", 1);
        
        BatchDocument mediumDoc = new BatchDocument();
        mediumDoc.addText("title", "Medium Document");
        mediumDoc.addText("content", "Some content here");
        mediumDoc.addInteger("count", 100);
        mediumDoc.addFloat("score", 2.5);
        
        BatchDocument largeDoc = new BatchDocument();
        largeDoc.addText("title", "Large Document with Extensive Content");
        largeDoc.addText("content", "Very long content that takes up more space. ".repeat(50));
        largeDoc.addText("description", "Additional field with more text. ".repeat(25));
        largeDoc.addInteger("count", 1000);
        largeDoc.addFloat("score", 9.99);
        largeDoc.addBoolean("published", true);
        
        builder.addDocument(smallDoc)
               .addDocument(mediumDoc)
               .addDocument(largeDoc);
               
        BatchDocumentBuilder.BatchMemoryStats stats = builder.getMemoryStats();
        
        // Verify statistics make sense
        assertEquals(3, stats.getDocumentCount());
        assertTrue(stats.getEstimatedSize() > 0);
        assertTrue(stats.getAverageDocumentSize() > 0);
        assertTrue(stats.getMaxDocumentSize() > stats.getAverageDocumentSize());
        assertTrue(stats.getMinDocumentSize() < stats.getAverageDocumentSize());
        assertTrue(stats.getMaxDocumentSize() > stats.getMinDocumentSize());
        
        // Verify documents per MB calculation
        double docsPerMB = stats.getDocumentsPerMB();
        assertTrue(docsPerMB > 0);
        assertTrue(docsPerMB < 1_000_000); // Sanity check - shouldn't be crazy high
        
        // Test summary contains expected information
        String summary = stats.getSummary();
        assertTrue(summary.contains("Documents: 3"));
        assertTrue(summary.contains("Total Size:"));
        assertTrue(summary.contains("Avg Doc Size:"));
        assertTrue(summary.contains("Min Doc Size:"));
        assertTrue(summary.contains("Max Doc Size:"));
        assertTrue(summary.contains("Docs per MB:"));
    }
}
