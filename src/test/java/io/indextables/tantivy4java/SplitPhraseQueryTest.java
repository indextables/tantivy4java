package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive test suite for SplitPhraseQuery functionality.
 *
 * This test validates that phrase queries work correctly in the split query API,
 * including exact phrase matching, sloppy phrase matching, and edge cases.
 */
@DisplayName("SplitPhraseQuery Comprehensive Tests")
public class SplitPhraseQueryTest {

    private String tempDir;
    private String splitPath;
    private SplitSearcher splitSearcher;
    private SplitCacheManager cacheManager;
    private Schema schema;

    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("=== Setting up SplitPhraseQuery Test ===");

        // Create temp directory
        tempDir = "/tmp/split_phrase_query_test_" + System.currentTimeMillis();
        File splitDir = new File(tempDir);
        splitDir.mkdirs();
        splitPath = tempDir + "/phrase_query_test.split";

        // Create test schema with text fields for phrase queries
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addTextField("description", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);

            schema = builder.build();
        }

        // Create test index with sample documents
        String indexPath = tempDir + "/test_index";
        try (Index index = new Index(schema, indexPath, false)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                // Document 1: Machine learning tutorial
                try (Document doc1 = new Document()) {
                    doc1.addInteger("id", 1);
                    doc1.addText("title", "Machine Learning Tutorial");
                    doc1.addText("content", "This is a comprehensive machine learning tutorial that covers advanced techniques and algorithms for data science applications");
                    doc1.addText("description", "Learn machine learning fundamentals with practical examples");
                    writer.addDocument(doc1);
                }

                // Document 2: Deep learning introduction
                try (Document doc2 = new Document()) {
                    doc2.addInteger("id", 2);
                    doc2.addText("title", "Deep Learning Introduction");
                    doc2.addText("content", "An introduction to deep learning networks and neural network architectures used in modern AI systems");
                    doc2.addText("description", "Introduction to deep learning concepts and neural networks");
                    writer.addDocument(doc2);
                }

                // Document 3: Data science methods
                try (Document doc3 = new Document()) {
                    doc3.addInteger("id", 3);
                    doc3.addText("title", "Data Science Methods");
                    doc3.addText("content", "Statistical methods and data analysis techniques for data science practitioners and researchers");
                    doc3.addText("description", "Comprehensive guide to data science statistical methods");
                    writer.addDocument(doc3);
                }

                // Document 4: Advanced algorithms
                try (Document doc4 = new Document()) {
                    doc4.addInteger("id", 4);
                    doc4.addText("title", "Advanced Machine Learning Algorithms");
                    doc4.addText("content", "Explore advanced algorithms in machine learning including ensemble methods and optimization techniques");
                    doc4.addText("description", "Advanced machine learning algorithm implementations");
                    writer.addDocument(doc4);
                }

                writer.commit();
            }

            index.reload();
        }

        // Convert to split file
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "phrase-test-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
        System.out.println("Created test split with " + splitMetadata.getNumDocs() + " documents");

        // Create split searcher with cache manager
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("phrase-test-cache")
            .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata);

        System.out.println("✅ SplitPhraseQuery test setup complete");
    }

    @AfterEach
    public void tearDown() {
        if (splitSearcher != null) {
            try {
                splitSearcher.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitSearcher: " + e.getMessage());
            }
        }

        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitCacheManager: " + e.getMessage());
            }
        }

        // Clean up temp directory
        try {
            if (tempDir != null) {
                File tempDirFile = new File(tempDir);
                deleteRecursively(tempDirFile);
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to clean up temp directory: " + e.getMessage());
        }
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteRecursively(f);
                }
            }
        }
        file.delete();
    }

    @Test
    @DisplayName("Basic phrase query construction and validation")
    public void testBasicPhraseQueryConstruction() {
        System.out.println("\n🔎 Test: Basic phrase query construction");

        // Test basic construction
        List<String> terms = Arrays.asList("machine", "learning");
        SplitPhraseQuery query = new SplitPhraseQuery("title", terms);

        assertEquals("title", query.getField());
        assertEquals(terms, query.getTerms());
        assertEquals(0, query.getSlop());
        assertTrue(query.isExactPhrase());
        assertEquals(2, query.getTermCount());

        System.out.println("  ✅ Basic construction working: " + query);

        // Test construction with slop
        SplitPhraseQuery sloppyQuery = new SplitPhraseQuery("content", terms, 2);
        assertEquals(2, sloppyQuery.getSlop());
        assertFalse(sloppyQuery.isExactPhrase());

        System.out.println("  ✅ Sloppy construction working: " + sloppyQuery);

        // Test static factory methods
        SplitPhraseQuery exactPhrase = SplitPhraseQuery.exactPhrase("title", "machine", "learning");
        assertTrue(exactPhrase.isExactPhrase());

        SplitPhraseQuery sloppyPhrase = SplitPhraseQuery.sloppyPhrase("content", 1, "deep", "learning");
        assertEquals(1, sloppyPhrase.getSlop());

        System.out.println("  ✅ Factory methods working");
    }

    @Test
    @DisplayName("Phrase query validation and error handling")
    public void testPhraseQueryValidation() {
        System.out.println("\n🔎 Test: Phrase query validation");

        // Test null field
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery(null, Arrays.asList("term1", "term2"));
        });

        // Test empty field
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("  ", Arrays.asList("term1", "term2"));
        });

        // Test null terms
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("field", (List<String>) null);
        });

        // Test empty terms
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("field", Arrays.asList());
        });

        // Test null individual term
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("field", Arrays.asList("term1", null, "term3"));
        });

        // Test empty individual term
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("field", Arrays.asList("term1", "  ", "term3"));
        });

        // Test negative slop
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitPhraseQuery("field", Arrays.asList("term1", "term2"), -1);
        });

        System.out.println("  ✅ All validation tests passed");
    }

    @Test
    @DisplayName("Phrase query equals and hashCode")
    public void testPhraseQueryEquality() {
        System.out.println("\n🔎 Test: Phrase query equality");

        List<String> terms1 = Arrays.asList("machine", "learning");
        List<String> terms2 = Arrays.asList("machine", "learning");
        List<String> terms3 = Arrays.asList("deep", "learning");

        SplitPhraseQuery query1 = new SplitPhraseQuery("title", terms1, 0);
        SplitPhraseQuery query2 = new SplitPhraseQuery("title", terms2, 0);
        SplitPhraseQuery query3 = new SplitPhraseQuery("title", terms3, 0);
        SplitPhraseQuery query4 = new SplitPhraseQuery("content", terms1, 0);
        SplitPhraseQuery query5 = new SplitPhraseQuery("title", terms1, 1);

        // Test equality
        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());

        // Test inequality
        assertNotEquals(query1, query3);  // Different terms
        assertNotEquals(query1, query4);  // Different field
        assertNotEquals(query1, query5);  // Different slop
        assertNotEquals(query1, null);
        assertNotEquals(query1, "not a query");

        System.out.println("  ✅ Equality tests passed");
    }

    @Test
    @DisplayName("Phrase query toString formatting")
    public void testPhraseQueryToString() {
        System.out.println("\n🔎 Test: Phrase query toString");

        // Test exact phrase
        SplitPhraseQuery exactQuery = new SplitPhraseQuery("title", Arrays.asList("machine", "learning"));
        String exactString = exactQuery.toString();
        assertTrue(exactString.contains("SplitPhraseQuery"));
        assertTrue(exactString.contains("field=title"));
        assertTrue(exactString.contains("\"machine\""));
        assertTrue(exactString.contains("\"learning\""));
        assertFalse(exactString.contains("slop="));

        System.out.println("  Exact phrase: " + exactString);

        // Test sloppy phrase
        SplitPhraseQuery sloppyQuery = new SplitPhraseQuery("content", Arrays.asList("deep", "learning"), 2);
        String sloppyString = sloppyQuery.toString();
        assertTrue(sloppyString.contains("slop=2"));

        System.out.println("  Sloppy phrase: " + sloppyString);

        System.out.println("  ✅ ToString formatting tests passed");
    }

    @Test
    @DisplayName("Phrase query native method and JSON generation")
    public void testPhraseQueryNativeMethod() {
        System.out.println("\n🔎 Test: Phrase query native method and JSON generation");

        // Create a phrase query
        SplitPhraseQuery query = new SplitPhraseQuery("title", Arrays.asList("machine", "learning"));

        // Test that native method works and generates correct QueryAst JSON
        String queryAst = query.toQueryAstJson();
        System.out.println("  QueryAst JSON: " + queryAst);

        assertNotNull(queryAst);
        assertTrue(queryAst.contains("\"type\":\"full_text\""));
        assertTrue(queryAst.contains("\"field\":\"title\""));
        assertTrue(queryAst.contains("\"text\":\"machine learning\""));
        assertTrue(queryAst.contains("\"mode\":{\"type\":\"phrase\"}"));

        System.out.println("  ✅ Native method executed successfully and generated correct JSON");
    }

    @Test
    @DisplayName("Complete phrase query search integration")
    public void testPhraseQuerySearchIntegration() {
        System.out.println("\n🔎 Test: Complete phrase query search integration");

        // Test exact phrase matching
        SplitPhraseQuery exactQuery = new SplitPhraseQuery("title", Arrays.asList("machine", "learning"));

        try {
            SearchResult result = splitSearcher.search(exactQuery, 10);
            System.out.println("  📊 Exact phrase 'machine learning' found " + result.getHits().size() + " results");

            // Should find at least one document with "Machine Learning" phrase
            assertTrue(result.getHits().size() > 0, "Should find documents with 'machine learning' phrase");

            // Verify search results are valid (has hits with scores)
            for (var hit : result.getHits()) {
                assertTrue(hit.getScore() > 0.0, "Hit should have positive score");
                assertNotNull(hit.getDocAddress(), "Hit should have valid document address");
                System.out.println("    Found document at: " + hit.getDocAddress() + " (score: " + hit.getScore() + ")");
            }

        } catch (Exception e) {
            fail("Phrase query search failed: " + e.getMessage());
        }

        // Test sloppy phrase matching
        System.out.println("\n  🔍 Testing sloppy phrase query (slop=1)");
        SplitPhraseQuery sloppyQuery = new SplitPhraseQuery("content", Arrays.asList("deep", "learning"), 1);

        try {
            SearchResult sloppyResult = splitSearcher.search(sloppyQuery, 10);
            System.out.println("  📊 Sloppy phrase 'deep learning' (slop=1) found " + sloppyResult.getHits().size() + " results");

            // Should find documents even with words in between
            assertTrue(sloppyResult.getHits().size() > 0, "Should find documents with 'deep learning' allowing slop");

        } catch (Exception e) {
            fail("Sloppy phrase query search failed: " + e.getMessage());
        }

        System.out.println("  ✅ Complete phrase query search integration working");
    }

    @Test
    @DisplayName("Additional phrase query factory method integration")
    public void testPhraseQueryFactoryIntegration() {
        System.out.println("\n🔎 Test: Phrase query factory method integration");

        try {
            // Create phrase queries using factory methods
            SplitPhraseQuery exactPhrase = SplitPhraseQuery.exactPhrase("title", "machine", "learning");
            SplitPhraseQuery sloppyPhrase = SplitPhraseQuery.sloppyPhrase("content", 1, "machine", "learning");

            System.out.println("  Created phrase queries:");
            System.out.println("    Exact: " + exactPhrase);
            System.out.println("    Sloppy: " + sloppyPhrase);

            // Test factory method correctness
            assertEquals("title", exactPhrase.getField());
            assertEquals(0, exactPhrase.getSlop());
            assertEquals(Arrays.asList("machine", "learning"), exactPhrase.getTerms());

            assertEquals("content", sloppyPhrase.getField());
            assertEquals(1, sloppyPhrase.getSlop());
            assertEquals(Arrays.asList("machine", "learning"), sloppyPhrase.getTerms());

            System.out.println("  ✅ Factory method integration working correctly");

        } catch (Exception e) {
            fail("Factory method integration test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Comprehensive phrase query factory methods")
    public void testPhraseQueryFactoryMethods() {
        System.out.println("\n🔎 Test: Phrase query factory methods");

        // Test exactPhrase with List
        List<String> terms = Arrays.asList("data", "science");
        SplitPhraseQuery exact1 = SplitPhraseQuery.exactPhrase("title", terms);
        assertEquals(0, exact1.getSlop());
        assertEquals(terms, exact1.getTerms());

        // Test exactPhrase with varargs
        SplitPhraseQuery exact2 = SplitPhraseQuery.exactPhrase("content", "deep", "learning", "tutorial");
        assertEquals(0, exact2.getSlop());
        assertEquals(Arrays.asList("deep", "learning", "tutorial"), exact2.getTerms());

        // Test sloppyPhrase with List
        SplitPhraseQuery sloppy1 = SplitPhraseQuery.sloppyPhrase("description", 2, terms);
        assertEquals(2, sloppy1.getSlop());
        assertEquals(terms, sloppy1.getTerms());

        // Test sloppyPhrase with varargs
        SplitPhraseQuery sloppy2 = SplitPhraseQuery.sloppyPhrase("title", 1, "advanced", "algorithms");
        assertEquals(1, sloppy2.getSlop());
        assertEquals(Arrays.asList("advanced", "algorithms"), sloppy2.getTerms());

        System.out.println("  ✅ All factory methods working correctly");
    }
}
