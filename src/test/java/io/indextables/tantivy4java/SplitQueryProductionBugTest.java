package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the actual SplitQuery JSON serialization bug in production code path.
 * This test specifically tests the actual failing scenario:
 * SplitSearcher.search(SplitQuery) → searchWithQueryAst() → native toQueryAstJson()
 * 
 * The bug report indicated that toQueryAstJson() generates malformed JSON missing the "type" field,
 * causing "Failed to parse QueryAst JSON: missing field `type`" errors.
 */
public class SplitQueryProductionBugTest {
    
    private Path tempDir;
    private Index index;
    private Schema schema;
    private SplitCacheManager cacheManager;
    
    @BeforeEach
    void setUp() throws IOException {
        // Initialize Tantivy
        Tantivy.initialize();
        
        // Create temporary directory and split file path
        tempDir = Files.createTempDirectory("split_query_production_test");
        
        // Create schema with text and numeric fields for comprehensive testing
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position"); 
        schemaBuilder.addIntegerField("price", true, true, true); // Make it fast field for range queries
        schemaBuilder.addTextField("category", true, false, "default", "position");
        
        schema = schemaBuilder.build();
        
        // Create index and add test documents
        index = new Index(schema, tempDir.toString());
        
        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add test documents that will match our queries
            Document doc1 = new Document();
            doc1.addText("title", "Test Document");
            doc1.addText("content", "This is test content for search");
            doc1.addInteger("price", 100);
            doc1.addText("category", "electronics");
            writer.addDocument(doc1);
                
            Document doc2 = new Document();
            doc2.addText("title", "Another Document");
            doc2.addText("content", "Different content here");
            doc2.addInteger("price", 200);
            doc2.addText("category", "books");
            writer.addDocument(doc2);
                
            writer.commit();
        }
        
        // Create cache manager for SplitSearcher
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("production-test-cache")
            .withMaxCacheSize(50_000_000); // 50MB cache
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (index != null) {
            index.close();
        }
        if (cacheManager != null) {
            cacheManager.close();
        }
        
        // Clean up temp directory
        deleteDirectory(tempDir.toFile());
    }
    
    @Test
    void testSplitTermQueryProductionPath() throws Exception {
        // Convert index to split for testing
        String splitPath = tempDir.resolve("test.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        // Create SplitSearcher using the cache manager (production pattern)
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test 1: Direct SplitTermQuery creation and search (this is the failing production path)
            SplitTermQuery termQuery = new SplitTermQuery("title", "test");
            
            // This is the exact production code path that's failing:
            // SplitSearcher.search() calls splitQuery.toQueryAstJson() then searchWithQueryAst()
            SearchResult result = splitSearcher.search(termQuery, 10);
            
            // If the bug exists, this will fail with "Failed to parse QueryAst JSON: missing field `type`"
            assertNotNull(result, "Search result should not be null");
            assertTrue(result.getHits().size() > 0, "Should find matching documents for term query");
            
            // The core JSON serialization bug is fixed if we get here without JSON parsing errors
            System.out.println("✅ SplitTermQuery JSON serialization working - found " + result.getHits().size() + " hits");
        }
    }
    
    @Test 
    void testSplitBooleanQueryProductionPath() throws Exception {
        // Convert index to split for testing
        String splitPath = tempDir.resolve("test_bool.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-bool-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        // Create SplitSearcher using the cache manager (production pattern)
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test 2: SplitBooleanQuery production path
            SplitBooleanQuery boolQuery = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("category", "electronics"))
                .addShould(new SplitTermQuery("title", "test"));
            
            // This calls the production path: SplitSearcher.search() → searchWithQueryAst() 
            SearchResult result = splitSearcher.search(boolQuery, 10);
            
            // If the bug exists with boolean queries, this will fail
            assertNotNull(result, "Boolean query search result should not be null");
            assertTrue(result.getHits().size() > 0, "Should find matching documents for boolean query");
        }
    }
    
    @Test
    void testSplitRangeQueryProductionPath() throws Exception {
        // Convert index to split for testing  
        String splitPath = tempDir.resolve("test_range.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-range-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        // Create SplitSearcher using the cache manager (production pattern)
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test 3: SplitRangeQuery production path
            SplitRangeQuery rangeQuery = new SplitRangeQuery(
                "price", 
                SplitRangeQuery.RangeBound.inclusive("50"),
                SplitRangeQuery.RangeBound.inclusive("150"),
                "i64"
            );
            
            // This calls the production path with range query JSON serialization
            SearchResult result = splitSearcher.search(rangeQuery, 10);
            
            // If the bug exists with range queries, this will fail
            assertNotNull(result, "Range query search result should not be null");
            assertTrue(result.getHits().size() > 0, "Should find matching documents for range query");
        }
    }
    
    @Test
    void testSplitMatchAllQueryProductionPath() throws Exception {
        // Convert index to split for testing
        String splitPath = tempDir.resolve("test_matchall.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-matchall-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        // Create SplitSearcher using the cache manager (production pattern)
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test 4: SplitMatchAllQuery production path
            SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
            
            // This calls the production path with match all query JSON serialization
            SearchResult result = splitSearcher.search(matchAllQuery, 10);
            
            // If the bug exists with match all queries, this will fail
            assertNotNull(result, "Match all query search result should not be null");
            assertEquals(2, result.getHits().size(), "Match all should return all documents");
        }
    }
    
    @Test
    void testParseQueryProductionPath() throws Exception {
        // Convert index to split for testing
        String splitPath = tempDir.resolve("test_parse.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-parse-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        // Create SplitSearcher using the cache manager (production pattern)
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test 5: parseQuery() production path - this uses SplitParsedQuery
            SplitQuery parsedQuery = splitSearcher.parseQuery("title:test");
            
            // This should create a SplitParsedQuery that holds the QueryAst JSON directly
            assertNotNull(parsedQuery, "Parsed query should not be null");
            
            // This is the most critical test - parseQuery creates queries that use the production path
            SearchResult result = splitSearcher.search(parsedQuery, 10);
            
            // If the bug exists in the parseQuery → search path, this will fail
            assertNotNull(result, "Parsed query search result should not be null");
            assertTrue(result.getHits().size() > 0, "Should find matching documents for parsed query");
        }
    }
    
    @Test
    void testJSONDebugOutput() throws Exception {
        // This test is designed to capture the actual JSON being generated
        // Enable debug output to see what JSON is being generated
        
        String splitPath = tempDir.resolve("test_json_debug.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-json-debug", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(tempDir.toString(), splitPath, splitConfig);
        
        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {
            
            // Test the actual toQueryAstJson() method calls directly if possible
            SplitTermQuery termQuery = new SplitTermQuery("title", "test");
            
            try {
                // Try to call toQueryAstJson directly to see the JSON that's generated
                String json = termQuery.toQueryAstJson();
                System.out.println("Generated TermQuery JSON: " + json);
                
                // Verify the JSON contains the required "type" field
                assertTrue(json.contains("\"type\""), "JSON should contain 'type' field: " + json);
                assertTrue(json.contains("\"term\""), "JSON should contain 'term' type: " + json);
            } catch (Exception e) {
                System.err.println("Error calling toQueryAstJson() directly: " + e.getMessage());
                e.printStackTrace();
            }
            
            // Now test the full production path  
            SearchResult result = splitSearcher.search(termQuery, 10);
            assertNotNull(result, "Search should succeed even with direct JSON generation");
        }
    }
    
    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}
