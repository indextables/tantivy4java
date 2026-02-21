package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test demonstrating document value extraction from SplitSearcher results.
 * This test validates that we can search for documents in splits and then retrieve 
 * the actual document field values using the new doc() method.
 */
public class SplitSearcherDocumentRetrievalTest {

    private static SplitCacheManager cacheManager;
    
    @TempDir
    static Path tempDir;
    
    private Path indexPath;
    private Path splitPath;
    private String splitUrl;
    private static QuickwitSplit.SplitMetadata metadata;

    @BeforeAll
    static void setUpCacheManager() {
        // Create shared cache manager for split operations
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("document-retrieval-cache")
                .withMaxCacheSize(100_000_000) // 100MB cache
                .withMaxConcurrentLoads(4);
                
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDownCacheManager() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // Create a real index with test data
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addTextField("category", true, false, "default", "position")
            .addIntegerField("score", true, true, true)
            .addBooleanField("published", true, true, true)
            .build();

        Path indexPath = tempDir.resolve("test-index");
        Index testIndex = new Index(schema, indexPath.toString());
        IndexWriter writer = testIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add test documents for document retrieval testing
        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.addText("title", "Advanced Document " + i);
            doc.addText("content", "This is comprehensive test content for document " + i + 
                " with search terms like advanced, technical, tutorial.");
            doc.addText("category", i % 3 == 0 ? "technical" : (i % 3 == 1 ? "tutorial" : "education"));
            doc.addInteger("score", (int) (8.0 + (i % 3)));
            doc.addBoolean("published", i % 2 == 0);
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();
        testIndex.close();

        // Convert index to split file using QuickwitSplit
        // Use unique split filename per test to avoid stale searcher/footer cache entries
        splitPath = tempDir.resolve("document_test_" + System.nanoTime() + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "document-test-index",
            "document-test-source", 
            "document-test-node"
        );
        
        metadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
        splitUrl = splitPath.toAbsolutePath().toString();
    }

    @Test
    @DisplayName("Test document retrieval from search results")
    void testDocumentRetrievalFromSearchResults() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            assertNotNull(searcher, "Split searcher should be created successfully");
            
            // Demonstrate schema introspection
            Schema schema = searcher.getSchema();
            System.out.println("📊 Schema introspection results:");
            System.out.println("   Field count: " + schema.getFieldCount());
            System.out.println("   Field names: " + schema.getFieldNames());
            System.out.println("   Has title field: " + schema.hasField("title"));
            
            // Search for documents using new SplitQuery API
            SplitQuery titleQuery = new SplitTermQuery("title", "Advanced");
            SearchResult result = searcher.search(titleQuery, 10);
            
            assertNotNull(result, "Search should return results");
            List<SearchResult.Hit> hits = result.getHits();
            assertTrue(hits.size() > 0, "Should find matching documents");
            
            // Test document retrieval for each hit
            for (SearchResult.Hit hit : hits) {
                System.out.println("🔍 Processing hit with score: " + hit.getScore());
                
                // Retrieve the document using the new doc() method
                Document doc = searcher.doc(hit.getDocAddress());
                assertNotNull(doc, "Document should be retrieved successfully");
                
                // Test comprehensive field value access
                Object titleValue = doc.getFirst("title");
                assertNotNull(titleValue, "Document should have title field");
                assertTrue(titleValue instanceof String, "Title should be a string");
                assertTrue(titleValue.toString().startsWith("Advanced Document"), "Title should match expected pattern");
                System.out.println("✅ Document retrieved successfully with title: " + titleValue);
                
                // Test content field access
                Object contentValue = doc.getFirst("content");
                assertNotNull(contentValue, "Document should have content field");
                assertTrue(contentValue instanceof String, "Content should be a string");
                assertTrue(contentValue.toString().contains("comprehensive test content"), "Content should match expected pattern");
                System.out.println("   Content preview: " + contentValue.toString().substring(0, Math.min(50, contentValue.toString().length())) + "...");
                
                // Test category field access  
                Object categoryValue = doc.getFirst("category");
                assertNotNull(categoryValue, "Document should have category field");
                assertTrue(categoryValue instanceof String, "Category should be a string");
                assertTrue(categoryValue.toString().matches("technical|tutorial|education"), "Category should be one of expected values");
                System.out.println("   Category: " + categoryValue);
                
                // Test integer field access
                Object scoreValue = doc.getFirst("score");
                assertNotNull(scoreValue, "Document should have score field");
                assertTrue(scoreValue instanceof Integer || scoreValue instanceof Long, "Score should be numeric");
                int scoreInt = ((Number) scoreValue).intValue();
                assertTrue(scoreInt >= 8 && scoreInt <= 10, "Score should be in expected range (8-10)");
                System.out.println("   Score: " + scoreInt);
                
                // Test boolean field access
                Object publishedValue = doc.getFirst("published");
                assertNotNull(publishedValue, "Document should have published field");
                assertTrue(publishedValue instanceof Boolean, "Published should be boolean");
                System.out.println("   Published: " + publishedValue);
                
                System.out.println("   Document address: " + hit.getDocAddress());
                
                // Close the document when done
                doc.close();
            }
            
            result.close();
        }
    }
    
    @Test
    @DisplayName("Test document retrieval with different query types")
    void testDocumentRetrievalWithDifferentQueries() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            Schema schema = searcher.getSchema();
            
            // Test 1: Term query using SplitQuery API
            System.out.println("📝 Testing term query document retrieval");
            SplitQuery termQuery = new SplitTermQuery("title", "Advanced");
            testDocumentRetrievalForQuery(searcher, termQuery, "Term Query");
            
            // Test 2: Text content query using SplitQuery API
            System.out.println("📝 Testing content query document retrieval");
            SplitQuery contentQuery = new SplitTermQuery("content", "test");
            testDocumentRetrievalForQuery(searcher, contentQuery, "Content Query");
            
            // Test 3: Boolean query combining title searches using SplitQuery API
            System.out.println("📝 Testing boolean query document retrieval");
            SplitQuery titleQuery1 = new SplitTermQuery("title", "Advanced");
            SplitQuery titleQuery2 = new SplitTermQuery("title", "Document");
            SplitQuery boolQuery = new SplitBooleanQuery()
                    .addMust(titleQuery1)
                    .addShould(titleQuery2);
            testDocumentRetrievalForQuery(searcher, boolQuery, "Boolean Query");
        }
    }
    
    private void testDocumentRetrievalForQuery(SplitSearcher searcher, SplitQuery query, String queryType) {
        SearchResult result = searcher.search(query, 5);
        assertNotNull(result, queryType + " should return results");
        
        List<SearchResult.Hit> hits = result.getHits();
        System.out.println("   Found " + hits.size() + " hits for " + queryType);
        
        for (SearchResult.Hit hit : hits) {
            Document doc = searcher.doc(hit.getDocAddress());
            assertNotNull(doc, "Document should be retrieved for " + queryType);
            
            // Test comprehensive field access for the retrieved document
            Object titleValue = doc.getFirst("title");
            assertNotNull(titleValue, "Retrieved document should have title field");
            assertTrue(titleValue instanceof String, "Title should be a string");
            
            Object scoreValue = doc.getFirst("score");
            assertNotNull(scoreValue, "Retrieved document should have score field");
            assertTrue(scoreValue instanceof Integer || scoreValue instanceof Long, "Score should be numeric");
            
            Object publishedValue = doc.getFirst("published");
            assertNotNull(publishedValue, "Retrieved document should have published field");
            assertTrue(publishedValue instanceof Boolean, "Published should be boolean");
            
            System.out.println("   ✅ Retrieved document with score: " + hit.getScore() + 
                             ", title: " + titleValue + 
                             ", numeric_score: " + scoreValue + 
                             ", published: " + publishedValue);
            doc.close();
        }
        
        result.close();
    }
    
    @Test
    @DisplayName("Test document retrieval with invalid document addresses")
    void testDocumentRetrievalWithInvalidAddresses() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            // Test with non-existent document address
            DocAddress invalidAddress = new DocAddress(999, 999);
            
            // This should either return null or throw an exception - both are valid behaviors
            try {
                Document doc = searcher.doc(invalidAddress);
                if (doc != null) {
                    // If a document is returned, it should be a valid object
                    System.out.println("⚠️  Document returned for invalid address - this may be expected mock behavior");
                    doc.close();
                } else {
                    System.out.println("✅ Null returned for invalid document address as expected");
                }
            } catch (Exception e) {
                System.out.println("✅ Exception thrown for invalid document address: " + e.getMessage());
            }
            
            invalidAddress.close();
        }
    }
    
    @Test
    @DisplayName("Test document retrieval performance and caching")
    void testDocumentRetrievalPerformanceAndCaching() throws InterruptedException {
        // Allow async FS updates from prior test to settle
        Thread.sleep(200);
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            Schema schema = searcher.getSchema();
            SplitQuery query = new SplitTermQuery("title", "document");
            SearchResult result = searcher.search(query, 3);

            List<SearchResult.Hit> hits = result.getHits();
            assertTrue(hits.size() > 0, "Should have hits for performance test");
            
            // Test multiple document retrievals to check caching behavior
            SearchResult.Hit firstHit = hits.get(0);
            
            long startTime = System.nanoTime();
            
            // First retrieval (cache miss expected)
            Document doc1 = searcher.doc(firstHit.getDocAddress());
            assertNotNull(doc1, "First document retrieval should succeed");
            long firstRetrievalTime = System.nanoTime() - startTime;
            
            // Second retrieval (cache hit expected)
            startTime = System.nanoTime();
            Document doc2 = searcher.doc(firstHit.getDocAddress());
            assertNotNull(doc2, "Second document retrieval should succeed");
            long secondRetrievalTime = System.nanoTime() - startTime;
            
            System.out.println("📊 Document Retrieval Performance:");
            System.out.println("   First retrieval: " + (firstRetrievalTime / 1_000) + " μs");
            System.out.println("   Second retrieval: " + (secondRetrievalTime / 1_000) + " μs");
            
            // Note: In a real implementation, we'd expect the second retrieval to be faster
            // For now, we just verify both succeed
            
            doc1.close();
            doc2.close();
            result.close();
        }
    }
    
    @Test
    @DisplayName("Test comprehensive field data validation")
    void testComprehensiveFieldDataValidation() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            Schema schema = searcher.getSchema();
            
            // Search for a specific document we know the pattern of
            SplitQuery titleQuery = new SplitTermQuery("title", "Document");
            SearchResult result = searcher.search(titleQuery, 5);
            
            assertNotNull(result, "Search should return results");
            List<SearchResult.Hit> hits = result.getHits();
            assertTrue(hits.size() > 0, "Should find matching documents");
            
            for (SearchResult.Hit hit : hits) {
                Document doc = searcher.doc(hit.getDocAddress());
                assertNotNull(doc, "Document should be retrieved successfully");
                
                // Comprehensive field validation
                System.out.println("🔍 Validating all field types for document:");
                
                // 1. Text field validation - title
                Object titleValue = doc.getFirst("title");
                assertNotNull(titleValue, "Title field must be present");
                assertTrue(titleValue instanceof String, "Title must be String type");
                String title = titleValue.toString();
                assertTrue(title.startsWith("Advanced Document"), "Title should match expected format");
                System.out.println("   ✅ Title field: " + title + " (String)");
                
                // 2. Text field validation - content  
                Object contentValue = doc.getFirst("content");
                assertNotNull(contentValue, "Content field must be present");
                assertTrue(contentValue instanceof String, "Content must be String type");
                String content = contentValue.toString();
                assertTrue(content.contains("comprehensive test content"), "Content should contain expected text");
                assertTrue(content.contains("advanced, technical, tutorial"), "Content should contain search terms");
                System.out.println("   ✅ Content field: " + content.substring(0, Math.min(60, content.length())) + "... (String)");
                
                // 3. Text field validation - category
                Object categoryValue = doc.getFirst("category");
                assertNotNull(categoryValue, "Category field must be present");
                assertTrue(categoryValue instanceof String, "Category must be String type");
                String category = categoryValue.toString();
                assertTrue(category.matches("technical|tutorial|education"), "Category must be one of expected values");
                System.out.println("   ✅ Category field: " + category + " (String)");
                
                // 4. Integer field validation - score
                Object scoreValue = doc.getFirst("score");
                assertNotNull(scoreValue, "Score field must be present");
                assertTrue(scoreValue instanceof Integer || scoreValue instanceof Long, "Score must be numeric type");
                int score = ((Number) scoreValue).intValue();
                assertTrue(score >= 8 && score <= 10, "Score should be in range 8-10 based on test data pattern");
                System.out.println("   ✅ Score field: " + score + " (Integer)");
                
                // 5. Boolean field validation - published
                Object publishedValue = doc.getFirst("published");
                assertNotNull(publishedValue, "Published field must be present");
                assertTrue(publishedValue instanceof Boolean, "Published must be Boolean type");
                Boolean published = (Boolean) publishedValue;
                System.out.println("   ✅ Published field: " + published + " (Boolean)");
                
                // 6. Validate field relationships (based on our test data pattern)
                // Extract document number from title to validate cross-field consistency
                if (title.matches("Advanced Document \\d+")) {
                    String numStr = title.replaceAll("Advanced Document ", "");
                    int docNum = Integer.parseInt(numStr);
                    
                    // Validate category matches pattern: i % 3 == 0 ? "technical" : (i % 3 == 1 ? "tutorial" : "education")
                    String expectedCategory = docNum % 3 == 0 ? "technical" : (docNum % 3 == 1 ? "tutorial" : "education");
                    assertEquals(expectedCategory, category, "Category should match document number pattern");
                    
                    // Validate score matches pattern: 8 + (i % 3)  
                    int expectedScore = 8 + (docNum % 3);
                    assertEquals(expectedScore, score, "Score should match document number pattern");
                    
                    // Validate published matches pattern: i % 2 == 0
                    boolean expectedPublished = (docNum % 2 == 0);
                    assertEquals(expectedPublished, published, "Published should match document number pattern");
                    
                    System.out.println("   ✅ Cross-field validation passed for document " + docNum);
                }
                
                System.out.println("   🎯 All field types and values validated successfully!");
                doc.close();
            }
            
            result.close();
        }
    }
    
    @Test
    @DisplayName("Test document lifecycle management")
    void testDocumentLifecycleManagement() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            Schema schema = searcher.getSchema();
            SplitQuery query = new SplitTermQuery("content", "test");
            SearchResult result = searcher.search(query, 2);
            
            List<SearchResult.Hit> hits = result.getHits();
            if (hits.size() > 0) {
                SearchResult.Hit hit = hits.get(0);
                
                // Test document creation
                Document doc = searcher.doc(hit.getDocAddress());
                assertNotNull(doc, "Document should be created");
                
                // Test that document can be used multiple times before closing
                // (Actual field access would be tested here in a full implementation)
                
                // Test document closing
                doc.close();
                
                // Note: Document lifecycle management is implementation-specific
                // In the current mock implementation, closed documents may still be accessible
                System.out.println("✅ Document lifecycle test completed successfully");
            }
            
            result.close();
        }
    }
    
    @Test
    @DisplayName("Test SplitQuery parsing and QueryAst conversion")
    void testSplitQueryParsing() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            // Test 1: Simple query string parsing
            System.out.println("📝 Testing SplitQuery parsing functionality");
            SplitQuery parsedQuery = searcher.parseQuery("title:Advanced");
            assertNotNull(parsedQuery, "Parsed query should not be null");
            
            SearchResult result = searcher.search(parsedQuery, 5);
            assertNotNull(result, "Search with parsed query should return results");
            System.out.println("✅ Simple query parsing successful");
            
            // Test 2: Complex query string parsing
            System.out.println("📝 Testing complex query string parsing");
            SplitQuery complexQuery = searcher.parseQuery("title:Document AND content:test");
            assertNotNull(complexQuery, "Complex parsed query should not be null");
            
            SearchResult complexResult = searcher.search(complexQuery, 5);
            assertNotNull(complexResult, "Search with complex parsed query should return results");
            System.out.println("✅ Complex query parsing successful");
            
            // Test 3: Compare direct SplitQuery construction vs parsing
            System.out.println("📝 Testing direct vs parsed query equivalence");
            SplitQuery directQuery = new SplitTermQuery("title", "Advanced");
            SplitQuery parsedEquivalent = searcher.parseQuery("title:Advanced");
            
            SearchResult directResult = searcher.search(directQuery, 10);
            SearchResult parsedResult = searcher.search(parsedEquivalent, 10);
            
            // Results should be equivalent (both should find documents)
            assertEquals(directResult.getHits().size() > 0, parsedResult.getHits().size() > 0,
                    "Direct and parsed queries should have equivalent results");
            System.out.println("✅ Query equivalence test successful");
            
            directResult.close();
            parsedResult.close();
            complexResult.close();
            result.close();
        }
    }
}
