package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.io.IOException;

/**
 * Simple test to isolate and debug range query issues with SplitSearcher.
 * This test creates a simple index with numeric fields and tests range queries.
 */
public class SimpleSplitRangeQueryTest {
    
    private static Path tempDir;
    private static String splitPath;
    
    @BeforeAll
    public static void setup() throws Exception {
        System.out.println("=== SimpleSplitRangeQueryTest Setup ===");
        
        // Create a temporary directory for our test
        tempDir = Files.createTempDirectory("split_range_test_");
        System.out.println("Created temp directory: " + tempDir);
        
        // Create a simple index with numeric fields
        createTestIndexWithNumericFields();
    }
    
    private static void createTestIndexWithNumericFields() throws Exception {
        System.out.println("\n📝 Creating test index with numeric fields...");
        
        // Create schema with text and numeric fields
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("category", true, false, "default", "position");
        schemaBuilder.addIntegerField("price", true, true, true);  // stored, indexed, FAST (required for range queries)
        schemaBuilder.addIntegerField("quantity", true, true, true);  // stored, indexed, FAST
        schemaBuilder.addTextField("description", true, false, "default", "position");
        
        Schema schema = schemaBuilder.build();
        System.out.println("Schema created with fields: " + schema.getFieldNames());
        
        // Create index
        Path indexPath = tempDir.resolve("test_index");
        Index index = new Index(schema, indexPath.toString());
        
        // Add documents with various price ranges
        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add products with different prices
            for (int i = 1; i <= 20; i++) {
                Document doc = new Document();
                doc.addText("title", "Product " + i);
                doc.addText("category", i <= 10 ? "electronics" : "books");
                doc.addInteger("price", i * 10L);  // Prices: 10, 20, 30, ..., 200
                doc.addInteger("quantity", i * 5L);
                doc.addText("description", "This is product number " + i + " with price " + (i * 10));
                writer.addDocument(doc);
            }
            
            writer.commit();
            System.out.println("✅ Added 20 documents with prices from 10 to 200");
        }
        
        // Ensure index is properly closed before conversion
        index.close();
        
        // Convert to Quickwit split
        System.out.println("\n🔄 Converting index to Quickwit split...");
        
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "test-index-" + UUID.randomUUID().toString().substring(0, 8),
            "test-source",
            "test-node"
        );
        
        splitPath = tempDir.resolve("test.split").toString();
        
        // Debug: Check if index path exists before conversion
        System.out.println("Index path exists: " + Files.exists(Path.of(indexPath.toString())));
        System.out.println("Index path is directory: " + Files.isDirectory(Path.of(indexPath.toString())));
        
        splitMetadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(),
            splitPath,
            splitConfig
        );
        
        System.out.println("✅ Created split: " + splitPath);
        System.out.println("   Split file exists: " + Files.exists(Path.of(splitPath)));
        System.out.println("   Split file is regular file: " + Files.isRegularFile(Path.of(splitPath)));
        System.out.println("   Split ID: " + splitMetadata.getSplitId());
        System.out.println("   Documents: " + splitMetadata.getNumDocs());
        System.out.println("   Size: " + splitMetadata.getUncompressedSizeBytes() + " bytes");
    }
    
    private static QuickwitSplit.SplitMetadata splitMetadata;
    
    @Test
    public void testSimpleRangeQueryOnSplit() throws Exception {
        System.out.println("\n=== Test: Simple Range Query on Split ===");
        
        assertNotNull(splitPath, "Split path should be set");
        assertTrue(Files.exists(Path.of(splitPath)), "Split file should exist");
        
        // Create cache manager with minimal configuration
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache")
            .withMaxCacheSize(10_000_000);  // 10MB cache
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        // Open the split searcher
        String splitUri = "file://" + splitPath;
        System.out.println("\n🔍 Opening SplitSearcher for: " + splitUri);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, splitMetadata)) {
            
            System.out.println("✅ SplitSearcher created successfully");
            
            // Get schema to verify fields
            Schema schema = searcher.getSchema();
            System.out.println("Schema fields: " + schema.getFieldNames());
            assertTrue(schema.hasField("price"), "Schema should have price field");
            
            // Test 1: Simple term query first (sanity check)
            System.out.println("\n📌 Test 1: Term query for category='electronics'");
            SplitQuery termQuery = new SplitTermQuery("category", "electronics");
            SearchResult termResults = searcher.search(termQuery, 100);
            System.out.println("   Found " + termResults.getHits().size() + " electronics items");
            assertEquals(10, termResults.getHits().size(), "Should find 10 electronics items");
            
            // Test 2: Range query for prices between 50 and 150
            System.out.println("\n📌 Test 2: Range query for price between 50 and 150");
            SplitQuery rangeQuery = new SplitRangeQuery(
                "price", 
                SplitRangeQuery.RangeBound.inclusive("50"),
                SplitRangeQuery.RangeBound.inclusive("150"),
                "i64"  // Specify that price is an integer field
            );
            
            System.out.println("   Executing range query...");
            SearchResult rangeResults = searcher.search(rangeQuery, 100);
            
            System.out.println("   Found " + rangeResults.getHits().size() + " items in price range");
            
            // Verify results
            // Prices 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150 = 11 items
            assertEquals(11, rangeResults.getHits().size(), "Should find 11 items in price range 50-150");
            
            // Test 3: Different range query
            System.out.println("\n📌 Test 3: Range query for price between 100 and 200");
            SplitQuery rangeQuery2 = new SplitRangeQuery(
                "price",
                SplitRangeQuery.RangeBound.inclusive("100"),
                SplitRangeQuery.RangeBound.inclusive("200"),
                "i64"  // Specify that price is an integer field
            );
            
            SearchResult rangeResults2 = searcher.search(rangeQuery2, 100);
            System.out.println("   Found " + rangeResults2.getHits().size() + " items in price range");
            
            // Prices 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200 = 11 items
            assertEquals(11, rangeResults2.getHits().size(), "Should find 11 items in price range 100-200");
            
            // Test 4: Exclusive bounds
            System.out.println("\n📌 Test 4: Range query with exclusive bounds");
            SplitQuery rangeQuery3 = new SplitRangeQuery(
                "price",
                SplitRangeQuery.RangeBound.exclusive("50"),
                SplitRangeQuery.RangeBound.exclusive("100"),
                "i64"  // Specify that price is an integer field
            );
            
            SearchResult rangeResults3 = searcher.search(rangeQuery3, 100);
            System.out.println("   Found " + rangeResults3.getHits().size() + " items in price range (50, 100)");
            
            // Prices 60, 70, 80, 90 = 4 items
            assertEquals(4, rangeResults3.getHits().size(), "Should find 4 items in exclusive range (50, 100)");
            
            // Test 5: Unbounded range
            System.out.println("\n📌 Test 5: Range query with unbounded upper limit");
            SplitQuery rangeQuery4 = new SplitRangeQuery(
                "price",
                SplitRangeQuery.RangeBound.inclusive("150"),
                SplitRangeQuery.RangeBound.unbounded(),
                "i64"  // Specify that price is an integer field
            );
            
            SearchResult rangeResults4 = searcher.search(rangeQuery4, 100);
            System.out.println("   Found " + rangeResults4.getHits().size() + " items with price >= 150");
            
            // Prices 150, 160, 170, 180, 190, 200 = 6 items
            assertEquals(6, rangeResults4.getHits().size(), "Should find 6 items with price >= 150");
            
            System.out.println("\n✅ All range query tests passed!");
            
        } catch (Exception e) {
            System.err.println("\n❌ Test failed with exception:");
            e.printStackTrace();
            throw e;
        }
    }
    
    @Test
    public void testRangeQueryWithDocumentRetrieval() throws Exception {
        System.out.println("\n=== Test: Range Query with Document Retrieval ===");
        
        // This test specifically checks if document retrieval affects the searcher
        // (addressing the issue seen in RealS3EndToEndTest)
        
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache-2")
            .withMaxCacheSize(10_000_000);
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        String splitUri = "file://" + splitPath;
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, splitMetadata)) {
            
            System.out.println("✅ SplitSearcher created");
            
            // First do a term query and retrieve a document
            System.out.println("\n📌 Step 1: Term query and document retrieval");
            SplitQuery termQuery = new SplitTermQuery("category", "electronics");
            SearchResult termResults = searcher.search(termQuery, 5);
            
            assertTrue(termResults.getHits().size() > 0, "Should find some results");
            
            // Retrieve and examine a document (similar to what RealS3EndToEndTest does)
            if (termResults.getHits().size() > 0) {
                try (Document doc = searcher.doc(termResults.getHits().get(0).getDocAddress())) {
                    String title = (String) doc.getFirst("title");
                    Integer price = (Integer) doc.getFirst("price");
                    System.out.println("   Retrieved document: " + title + " with price: " + price);
                    assertNotNull(title, "Document should have title");
                    assertNotNull(price, "Document should have price");
                }
            }
            
            // Now try a range query (this is where RealS3EndToEndTest fails)
            System.out.println("\n📌 Step 2: Range query after document retrieval");
            SplitQuery rangeQuery = new SplitRangeQuery(
                "price",
                SplitRangeQuery.RangeBound.inclusive("10"),
                SplitRangeQuery.RangeBound.inclusive("100"),
                "i64"  // Specify that price is an integer field
            );
            
            System.out.println("   Executing range query...");
            SearchResult rangeResults = searcher.search(rangeQuery, 100);
            
            System.out.println("   Found " + rangeResults.getHits().size() + " items in price range");
            
            // Prices 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 = 10 items
            assertEquals(10, rangeResults.getHits().size(), "Should find 10 items in price range 10-100");
            
            // Try another document retrieval to ensure searcher is still valid
            System.out.println("\n📌 Step 3: Another document retrieval after range query");
            if (rangeResults.getHits().size() > 0) {
                try (Document doc = searcher.doc(rangeResults.getHits().get(0).getDocAddress())) {
                    Integer price = (Integer) doc.getFirst("price");
                    System.out.println("   Retrieved document with price: " + price);
                    assertNotNull(price, "Document should have price");
                    assertTrue(price >= 10 && price <= 100, "Price should be in range");
                }
            }
            
            System.out.println("\n✅ Range query with document retrieval test passed!");
            
        } catch (Exception e) {
            System.err.println("\n❌ Test failed with exception:");
            e.printStackTrace();
            throw e;
        }
    }
    
    @Test
    public void testRangeQueryAsFirstSearch() throws Exception {
        System.out.println("\n=== Test: Range Query as First Search (no prior queries) ===");
        
        assertNotNull(splitPath, "Split path should be set");
        assertTrue(Files.exists(Path.of(splitPath)), "Split file should exist");
        
        // Create cache manager with minimal configuration
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache-first")
            .withMaxCacheSize(10_000_000);  // 10MB cache
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        // Open the split searcher
        String splitUri = "file://" + splitPath;
        System.out.println("\n🔍 Opening SplitSearcher for: " + splitUri);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, splitMetadata)) {
            
            System.out.println("✅ SplitSearcher created successfully");
            
            // IMMEDIATELY run a range query as the first operation
            System.out.println("\n📌 Running range query as FIRST search operation");
            SplitQuery rangeQuery = new SplitRangeQuery(
                "price",
                SplitRangeQuery.RangeBound.inclusive("30"),
                SplitRangeQuery.RangeBound.inclusive("80"),
                "i64"  // Specify that price is an integer field
            );
            
            System.out.println("   Executing range query for prices 30-80...");
            SearchResult rangeResults = searcher.search(rangeQuery, 100);
            
            System.out.println("   Found " + rangeResults.getHits().size() + " items in price range");
            
            // Prices 30, 40, 50, 60, 70, 80 = 6 items
            assertEquals(6, rangeResults.getHits().size(), "Should find 6 items in price range 30-80");
            
            System.out.println("\n✅ Range query as first search operation succeeded!");
            
        } catch (Exception e) {
            System.err.println("\n❌ Test failed with exception:");
            e.printStackTrace();
            throw e;
        }
    }
    
    @AfterAll
    public static void cleanup() throws Exception {
        // Clean up temporary files
        if (tempDir != null && Files.exists(tempDir)) {
            try {
                // Delete all files in the temp directory
                Files.walk(tempDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + path);
                        }
                    });
                System.out.println("✅ Cleaned up temp directory: " + tempDir);
            } catch (Exception e) {
                System.err.println("Warning: Failed to clean up temp directory: " + e.getMessage());
            }
        }
    }
}