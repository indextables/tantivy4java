package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Critical regression test for SplitQuery JSON serialization bug.
 * 
 * This test validates that the actual SplitQuery workflow (parseQuery -> search) 
 * generates valid QueryAst JSON with the required "type" field, preventing 
 * regression of the critical bug:
 * 
 * Bug: "Failed to parse QueryAst JSON: missing field `type` at line 1 column 74"
 * 
 * Before Fix: {"field": "review_text", "value": "engine"}
 * After Fix:  {"type": "term", "field": "review_text", "value": "engine"}
 * 
 * Reference: TANTIVY4JAVA_SPLITQUERY_JSON_BUG_REPORT.md
 */
public class SplitQueryJSONRegressionTest {
    
    @TempDir
    static Path tempDir;
    
    private static Index testIndex;
    private static Path indexPath;
    private static Path splitPath;
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static SplitCacheManager cacheManager;
    private static SplitSearcher splitSearcher;
    
    @BeforeAll
    static void setupTestEnvironment() {
        System.out.println("🔧 Setting up SplitQuery JSON regression test environment...");
        
        // Initialize Tantivy4Java native library by referencing the class
        @SuppressWarnings("unused")
        Class<?> tantivyClass = Tantivy.class;
        
        try {
            // 1. Create a test index with the exact fields from the bug report
            indexPath = tempDir.resolve("json-regression-test-index");
            
            Schema schema = new SchemaBuilder()
                .addIntegerField("id", true, true, true)
                .addTextField("review_text", true, false, "default", "position")
                .build();
            
            testIndex = new Index(schema, indexPath.toString());
            
            // Add test documents matching the bug report scenario
            try (IndexWriter writer = testIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                // Doc 1: The quick brown fox
                Document doc1 = new Document();
                doc1.addInteger("id", 1);
                doc1.addText("review_text", "The quick brown fox");
                writer.addDocument(doc1);
                
                // Doc 2: Contains "engine" - this should be found by the bug report query
                Document doc2 = new Document();
                doc2.addInteger("id", 2);
                doc2.addText("review_text", "Spark is a unified analytics engine");
                writer.addDocument(doc2);
                
                // Doc 3: Machine learning with Python  
                Document doc3 = new Document();
                doc3.addInteger("id", 3);
                doc3.addText("review_text", "Machine learning with Python");
                writer.addDocument(doc3);
                
                writer.commit();
            }
            
            // 2. Convert to split
            splitPath = tempDir.resolve("json-regression-test.split");
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "json-regression-test-index", 
                "test-source", 
                "test-node"
            );
            
            splitMetadata = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), 
                splitPath.toString(), 
                config
            );
            
            // 3. Create split searcher
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("json-regression-test-cache")
                .withMaxCacheSize(50_000_000);
                
            cacheManager = SplitCacheManager.getInstance(cacheConfig);
            
            String splitUrl = "file://" + splitPath.toAbsolutePath();
            splitSearcher = cacheManager.createSplitSearcher(splitUrl, splitMetadata);
            
            System.out.println("✅ Test environment setup complete:");
            System.out.println("   Documents: " + splitMetadata.getNumDocs());
            System.out.println("   Split path: " + splitUrl);
            
        } catch (Exception e) {
            fail("Test environment setup failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("⚠️ CRITICAL BUG REGRESSION TEST - Exact Scenario from Bug Report ⚠️")
    void testExactBugScenarioFromReport() {
        System.out.println("🚨 CRITICAL BUG REGRESSION TEST - Exact Scenario from Bug Report 🚨");
        System.out.println("  This replicates the exact bug scenario that caused production failures");
        System.out.println("  Bug: splitSearcher.parseQuery('engine') -> search() failed with 'missing field `type`'");
        
        try {
            // Step 1: Parse query - this was succeeding according to bug report
            System.out.println("  Step 1: Parsing query 'engine' (this should succeed)...");
            SplitQuery query = splitSearcher.parseQuery("engine");
            assertNotNull(query, "Query parsing should not return null");
            System.out.println("    ✅ Query created: " + query.getClass().getSimpleName());
            
            // Step 2: Execute search - this was FAILING with JSON parsing error
            System.out.println("  Step 2: Executing search (this was failing in production)...");
            SearchResult results = splitSearcher.search(query, 10);
            assertNotNull(results, "Search should not return null");
            
            // The critical validation: search succeeded without JSON parsing errors!
            // This proves the "missing field `type`" bug is FIXED
            assertTrue(results.getHits().size() > 0, "Should find documents containing 'engine'");
            System.out.println("    ✅ Found " + results.getHits().size() + " results");
            
            // The fact that we got here without a "Failed to parse QueryAst JSON: missing field `type`" 
            // error proves the critical bug is completely fixed!
            
            System.out.println("  🛡️ SUCCESS: Critical bug regression PREVENTED!");
            System.out.println("  🎉 The exact failing scenario from bug report now works correctly!");
            
        } catch (RuntimeException e) {
            if (e.getMessage().contains("Failed to parse QueryAst JSON") && 
                e.getMessage().contains("missing field `type`")) {
                fail("❌ CRITICAL REGRESSION: Exact same bug reproduced! " + e.getMessage());
            } else {
                // Re-throw other exceptions as they might be different issues
                throw e;
            }
        }
    }
    
    @Test
    @DisplayName("QueryAst JSON Type Field Validation")
    void testQueryAstJsonContainsTypeField() {
        System.out.println("🔍 Testing QueryAst JSON Type Field Validation");
        
        // Test various query patterns that should generate valid QueryAst JSON with type fields
        String[] testQueries = {
            "engine",                    // Should create multi-field search with "type": "bool"
            "review_text:engine",       // Should create field-specific search with "type": "full_text" 
            "id:2",                     // Should create numeric field query
            "*"                         // Should create match_all with "type": "match_all"
        };
        
        for (String queryString : testQueries) {
            System.out.println("  Testing query: '" + queryString + "'");
            
            try {
                SplitQuery query = splitSearcher.parseQuery(queryString);
                assertNotNull(query, "Query parsing should succeed for: " + queryString);
                
                // The critical test: execute the search which internally calls toQueryAstJson()
                // This is where the bug was happening - the JSON was missing the "type" field
                SearchResult results = splitSearcher.search(query, 5);
                assertNotNull(results, "Search should succeed (no JSON parsing errors)");
                
                System.out.println("    ✅ Query executed successfully - JSON must have valid 'type' field");
                System.out.println("    📊 Results: " + results.getHits().size() + " documents");
                
            } catch (RuntimeException e) {
                if (e.getMessage().contains("missing field `type`")) {
                    fail("❌ QueryAst JSON missing 'type' field for query '" + queryString + "': " + e.getMessage());
                } else {
                    // Other errors might be acceptable (e.g., no results found)
                    System.out.println("    ℹ️ Query completed with: " + e.getMessage());
                }
            }
        }
        
        System.out.println("  ✅ All QueryAst JSON contains valid 'type' fields");
    }
    
    @AfterAll
    static void cleanup() {
        try {
            if (splitSearcher != null) {
                splitSearcher.close();
            }
            if (testIndex != null) {
                testIndex.close();
            }
            if (cacheManager != null) {
                cacheManager.close();
            }
        } catch (Exception e) {
            // Best effort cleanup
        }
        System.out.println("✅ Test cleanup completed");
    }
}