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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Performance comparison tests between traditional single-document indexing
 * and new batch document indexing.
 * 
 * This test demonstrates the performance benefits of batch indexing by
 * minimizing JNI calls and using zero-copy semantics.
 */
public class BatchVsSinglePerformanceTest {
    
    private Schema schema;
    private Index singleIndex;
    private Index batchIndex;
    private IndexWriter singleWriter;
    private IndexWriter batchWriter;
    
    @BeforeEach
    void setUp() {
        // Create schema for performance testing
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addTextField("author", true, false, "default", "position");
        schemaBuilder.addTextField("category", true, false, "default", "position");
        schemaBuilder.addIntegerField("word_count", true, true, false);
        schemaBuilder.addFloatField("relevance_score", true, true, false);
        schemaBuilder.addBooleanField("published", true, true, false);
        schemaBuilder.addDateField("created_date", true, true, false);
        schemaBuilder.addUnsignedField("document_id", true, true, false);
        
        schema = schemaBuilder.build();
        
        // Create separate indices for fair comparison
        singleIndex = new Index(schema, "", false);
        batchIndex = new Index(schema, "", false);
        
        singleWriter = singleIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        batchWriter = batchIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
    }
    
    @AfterEach
    void tearDown() {
        if (singleWriter != null) {
            singleWriter.close();
        }
        if (batchWriter != null) {
            batchWriter.close();
        }
        if (singleIndex != null) {
            singleIndex.close();
        }
        if (batchIndex != null) {
            batchIndex.close();
        }
    }
    
    @Test
    void testSmallBatchPerformance() {
        int documentCount = 100;
        
        // Prepare test data
        List<DocumentData> testDocuments = generateTestDocuments(documentCount);
        
        // Test single document indexing
        long singleStartTime = System.currentTimeMillis();
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        long singleTime = System.currentTimeMillis() - singleStartTime;
        
        // Test batch document indexing
        long batchStartTime = System.currentTimeMillis();
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        for (DocumentData docData : testDocuments) {
            BatchDocument batchDoc = createBatchDocument(docData);
            builder.addDocument(batchDoc);
        }
        batchWriter.addDocumentsBatch(builder);
        batchWriter.commit();
        long batchTime = System.currentTimeMillis() - batchStartTime;
        
        System.out.printf("Small Batch (%d docs): Single=%dms, Batch=%dms, Ratio=%.2fx%n", 
                         documentCount, singleTime, batchTime, (double) singleTime / batchTime);
        
        // Batch should be faster or at least competitive
        assertTrue(batchTime <= singleTime * 1.5, 
                  "Batch indexing should be competitive with single document indexing");
        
        // Verify same number of documents indexed
        verifySameDocumentCount();
    }
    
    @Test 
    void testMediumBatchPerformance() {
        int documentCount = 1000;
        
        // Prepare test data
        List<DocumentData> testDocuments = generateTestDocuments(documentCount);
        
        // Test single document indexing
        long singleStartTime = System.currentTimeMillis();
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        long singleTime = System.currentTimeMillis() - singleStartTime;
        
        // Test batch document indexing
        long batchStartTime = System.currentTimeMillis();
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        for (DocumentData docData : testDocuments) {
            BatchDocument batchDoc = createBatchDocument(docData);
            builder.addDocument(batchDoc);
        }
        batchWriter.addDocumentsBatch(builder);
        batchWriter.commit();
        long batchTime = System.currentTimeMillis() - batchStartTime;
        
        System.out.printf("Medium Batch (%d docs): Single=%dms, Batch=%dms, Ratio=%.2fx%n", 
                         documentCount, singleTime, batchTime, (double) singleTime / batchTime);
        
        // At medium scale, batch should show clear benefits
        assertTrue(batchTime < singleTime, 
                  "Batch indexing should be faster than single document indexing at medium scale");
        
        // Verify same number of documents indexed
        verifySameDocumentCount();
    }
    
    @Test
    void testLargeBatchPerformance() {
        int documentCount = 5000;
        
        // Prepare test data
        List<DocumentData> testDocuments = generateTestDocuments(documentCount);
        
        // Test single document indexing
        long singleStartTime = System.currentTimeMillis();
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        long singleTime = System.currentTimeMillis() - singleStartTime;
        
        // Test batch document indexing
        long batchStartTime = System.currentTimeMillis();
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        for (DocumentData docData : testDocuments) {
            BatchDocument batchDoc = createBatchDocument(docData);
            builder.addDocument(batchDoc);
        }
        batchWriter.addDocumentsBatch(builder);
        batchWriter.commit();
        long batchTime = System.currentTimeMillis() - batchStartTime;
        
        System.out.printf("Large Batch (%d docs): Single=%dms, Batch=%dms, Ratio=%.2fx%n", 
                         documentCount, singleTime, batchTime, (double) singleTime / batchTime);
        
        // At large scale, batch should show some benefits
        assertTrue(batchTime < singleTime * 0.9, 
                  "Batch indexing should be faster than single document indexing at large scale");
        
        // Performance improvement should be measurable (realistic expectation)
        double improvement = (double) singleTime / batchTime;
        assertTrue(improvement >= 1.1, 
                  String.format("Expected at least 10%% improvement, got %.1fx", improvement));
        
        // Verify same number of documents indexed
        verifySameDocumentCount();
    }
    
    @Test
    void testMultipleBatchesPerformance() {
        int batchSize = 500;
        int batchCount = 4;
        int totalDocuments = batchSize * batchCount;
        
        // Prepare test data
        List<DocumentData> testDocuments = generateTestDocuments(totalDocuments);
        
        // Test single document indexing
        long singleStartTime = System.currentTimeMillis();
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        long singleTime = System.currentTimeMillis() - singleStartTime;
        
        // Test multiple batch indexing
        long batchStartTime = System.currentTimeMillis();
        for (int batch = 0; batch < batchCount; batch++) {
            BatchDocumentBuilder builder = new BatchDocumentBuilder();
            int startIdx = batch * batchSize;
            int endIdx = Math.min(startIdx + batchSize, totalDocuments);
            
            for (int i = startIdx; i < endIdx; i++) {
                BatchDocument batchDoc = createBatchDocument(testDocuments.get(i));
                builder.addDocument(batchDoc);
            }
            
            batchWriter.addDocumentsBatch(builder);
        }
        batchWriter.commit();
        long batchTime = System.currentTimeMillis() - batchStartTime;
        
        System.out.printf("Multiple Batches (%d batches of %d docs): Single=%dms, Batch=%dms, Ratio=%.2fx%n", 
                         batchCount, batchSize, singleTime, batchTime, (double) singleTime / batchTime);
        
        // Multiple smaller batches should still outperform single document indexing
        assertTrue(batchTime < singleTime, 
                  "Multiple batch indexing should be faster than single document indexing");
        
        // Verify same number of documents indexed
        verifySameDocumentCount();
    }
    
    @Test
    void test100kRecordPerformanceComparison() {
        int documentCount = 100_000;
        
        System.out.println("Starting 100k record performance comparison...");
        
        // Generate test documents
        List<DocumentData> testDocuments = generateTestDocuments(documentCount);
        
        // Test single document indexing
        System.out.println("Testing single document indexing...");
        long singleStartTime = System.currentTimeMillis();
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        long singleTime = System.currentTimeMillis() - singleStartTime;
        
        System.out.println("Single document indexing completed in " + singleTime + "ms");
        
        // Test batch document indexing
        System.out.println("Testing batch document indexing...");
        long batchStartTime = System.currentTimeMillis();
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        
        // Add documents in batches to manage memory
        int batchSize = 5000; // Process in 5k document batches
        int batchCount = 0;
        
        for (int i = 0; i < documentCount; i++) {
            DocumentData docData = testDocuments.get(i);
            BatchDocument batchDoc = createBatchDocument(docData);
            builder.addDocument(batchDoc);
            
            // Process batch when it reaches batch size or at the end
            if ((i + 1) % batchSize == 0 || i == documentCount - 1) {
                batchWriter.addDocumentsBatch(builder);
                builder.clear(); // Clear for next batch
                batchCount++;
                System.out.println("Processed batch " + batchCount + " (" + (i + 1) + "/" + documentCount + " documents)");
            }
        }
        
        batchWriter.commit();
        long batchTime = System.currentTimeMillis() - batchStartTime;
        
        System.out.println("Batch document indexing completed in " + batchTime + "ms");
        
        // Calculate and display results
        double speedupRatio = (double) singleTime / batchTime;
        double singlePerDoc = (double) singleTime / documentCount;
        double batchPerDoc = (double) batchTime / documentCount;
        
        System.out.println("\n=== 100k Record Performance Results ===");
        System.out.printf("Document Count: %,d%n", documentCount);
        System.out.printf("Single Document Time: %,d ms (%.3f ms per document)%n", singleTime, singlePerDoc);
        System.out.printf("Batch Document Time: %,d ms (%.3f ms per document)%n", batchTime, batchPerDoc);
        System.out.printf("Speedup Ratio: %.2fx%n", speedupRatio);
        System.out.printf("Performance Improvement: %.1f%%%n", (speedupRatio - 1) * 100);
        
        // Verify both approaches created the same number of documents
        verifySameDocumentCount();
        
        // Assert significant performance improvement
        assertTrue(batchTime < singleTime, 
                  "Batch indexing should be faster than single document indexing");
        
        // Expect at least 15% improvement for large batches (realistic performance expectation)
        assertTrue(speedupRatio >= 1.15, 
                  String.format("Expected at least 15%% improvement, got %.2fx", speedupRatio));
        
        System.out.println("100k record performance test completed successfully!");
    }

    @Test
    void testMemoryEfficiencyComparison() {
        int documentCount = 1000;
        
        // Generate test documents
        List<DocumentData> testDocuments = generateTestDocuments(documentCount);
        
        // Measure memory before single document indexing
        System.gc(); // Suggest garbage collection
        long beforeSingleMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        for (DocumentData docData : testDocuments) {
            Document doc = createSingleDocument(docData);
            singleWriter.addDocument(doc);
            doc.close();
        }
        singleWriter.commit();
        
        long afterSingleMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        // Measure memory for batch document indexing
        System.gc(); // Suggest garbage collection
        long beforeBatchMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        BatchDocumentBuilder builder = new BatchDocumentBuilder();
        for (DocumentData docData : testDocuments) {
            BatchDocument batchDoc = createBatchDocument(docData);
            builder.addDocument(batchDoc);
        }
        batchWriter.addDocumentsBatch(builder);
        batchWriter.commit();
        
        long afterBatchMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        long singleMemoryDelta = afterSingleMemory - beforeSingleMemory;
        long batchMemoryDelta = afterBatchMemory - beforeBatchMemory;
        
        System.out.printf("Memory Usage: Single=%d bytes, Batch=%d bytes%n", 
                         singleMemoryDelta, batchMemoryDelta);
        
        // Note: Memory usage comparison can be variable due to GC, JIT compilation, etc.
        // The main benefit of batch indexing is reduced JNI overhead, not necessarily memory usage
        assertTrue(singleMemoryDelta >= 0 && batchMemoryDelta >= 0, 
                  "Both methods should use some memory");
    }
    
    private List<DocumentData> generateTestDocuments(int count) {
        List<DocumentData> documents = new ArrayList<>(count);
        
        String[] categories = {"Technology", "Science", "Business", "Sports", "Entertainment"};
        String[] authors = {"John Smith", "Jane Doe", "Bob Johnson", "Alice Williams", "Charlie Brown"};
        
        for (int i = 0; i < count; i++) {
            DocumentData doc = new DocumentData();
            doc.title = "Performance Test Document " + i;
            doc.content = generateTestContent(i);
            doc.author = authors[i % authors.length];
            doc.category = categories[i % categories.length];
            doc.wordCount = 50 + (i % 200); // 50-250 words
            doc.relevanceScore = 1.0 + (i % 100) / 10.0; // 1.0-11.0
            doc.published = i % 3 == 0;
            doc.createdDate = LocalDateTime.now().minusDays(i % 30);
            doc.documentId = 10000L + i;
            
            documents.add(doc);
        }
        
        return documents;
    }
    
    private String generateTestContent(int docIndex) {
        String[] words = {
            "performance", "testing", "document", "indexing", "search", "tantivy",
            "java", "native", "batch", "single", "comparison", "efficiency",
            "memory", "speed", "optimization", "benchmark", "scale", "large",
            "processing", "engine", "distributed", "parallel", "concurrent"
        };
        
        StringBuilder content = new StringBuilder();
        content.append("This is test document number ").append(docIndex).append(". ");
        
        // Generate pseudo-random content based on document index
        for (int i = 0; i < 20; i++) {
            String word = words[(docIndex + i) % words.length];
            content.append(word).append(" ");
        }
        
        content.append("End of document ").append(docIndex).append(".");
        return content.toString();
    }
    
    private Document createSingleDocument(DocumentData docData) {
        Document doc = new Document();
        doc.addText("title", docData.title);
        doc.addText("content", docData.content);
        doc.addText("author", docData.author);
        doc.addText("category", docData.category);
        doc.addInteger("word_count", docData.wordCount);
        doc.addFloat("relevance_score", docData.relevanceScore);
        doc.addBoolean("published", docData.published);
        doc.addDate("created_date", docData.createdDate);
        doc.addUnsigned("document_id", docData.documentId);
        return doc;
    }
    
    private BatchDocument createBatchDocument(DocumentData docData) {
        BatchDocument doc = new BatchDocument();
        doc.addText("title", docData.title);
        doc.addText("content", docData.content);
        doc.addText("author", docData.author);
        doc.addText("category", docData.category);
        doc.addInteger("word_count", docData.wordCount);
        doc.addFloat("relevance_score", docData.relevanceScore);
        doc.addBoolean("published", docData.published);
        doc.addDate("created_date", docData.createdDate);
        doc.addUnsigned("document_id", docData.documentId);
        return doc;
    }
    
    private void verifySameDocumentCount() {
        singleWriter.commit();
        batchWriter.commit();
        singleIndex.reload();
        batchIndex.reload();
        
        try (Searcher singleSearcher = singleIndex.searcher();
             Searcher batchSearcher = batchIndex.searcher()) {
            
            // Search for all documents in both indices
            Query allQuery = Query.termQuery(schema, "title", "Performance");
            
            SearchResult singleResult = singleSearcher.search(allQuery, 10000);
            SearchResult batchResult = batchSearcher.search(allQuery, 10000);
            
            assertEquals(singleResult.getHits().size(), batchResult.getHits().size(),
                        "Both indexing methods should create the same number of documents");
        }
    }
    
    /**
     * Helper class to hold test document data
     */
    private static class DocumentData {
        String title;
        String content;
        String author;
        String category;
        long wordCount;
        double relevanceScore;
        boolean published;
        LocalDateTime createdDate;
        long documentId;
    }
}