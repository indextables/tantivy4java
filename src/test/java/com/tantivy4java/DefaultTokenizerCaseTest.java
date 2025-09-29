package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

/**
 * Test to verify that term queries work correctly with the default tokenizer
 * after removing automatic lowercasing in native code.
 */
public class DefaultTokenizerCaseTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "default_tokenizer_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Use DEFAULT tokenizer which lowercases text during indexing
            builder.addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add document with uppercase text
                        // Default tokenizer will lowercase "HELLO World" to "hello world" during indexing
                        try (Document doc = new Document()) {
                            doc.addText("title", "HELLO World");
                            writer.addDocument(doc);
                        }

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                    this.metadata = metadata;
                }
            }
        }

        String uniqueCacheName = uniqueId + "-cache";
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    public void testDefaultTokenizerCaseSensitivity() {
        System.out.println("🧪 Testing case sensitivity with default tokenizer...");

        // Test 1: parseQuery handles case correctly (uses Quickwit's native parser)
        SplitQuery parsedQueryUpper = searcher.parseQuery("title:HELLO");
        SearchResult parsedResultUpper = searcher.search(parsedQueryUpper, 10);
        System.out.println("📊 parseQuery('title:HELLO'): " + parsedResultUpper.getHits().size() + " hits");

        SplitQuery parsedQueryLower = searcher.parseQuery("title:hello");
        SearchResult parsedResultLower = searcher.search(parsedQueryLower, 10);
        System.out.println("📊 parseQuery('title:hello'): " + parsedResultLower.getHits().size() + " hits");

        // Test 2: SplitTermQuery with uppercase (this might fail after removing auto-lowercase)
        SplitTermQuery termQueryUpper = new SplitTermQuery("title", "HELLO");
        SearchResult termResultUpper = searcher.search(termQueryUpper, 10);
        System.out.println("📊 SplitTermQuery('title', 'HELLO'): " + termResultUpper.getHits().size() + " hits");

        // Test 3: SplitTermQuery with lowercase (should work)
        SplitTermQuery termQueryLower = new SplitTermQuery("title", "hello");
        SearchResult termResultLower = searcher.search(termQueryLower, 10);
        System.out.println("📊 SplitTermQuery('title', 'hello'): " + termResultLower.getHits().size() + " hits");

        // Analysis of results
        System.out.println("\n🔍 Analysis:");
        System.out.println("The default tokenizer lowercases 'HELLO World' to 'hello world' during indexing.");
        System.out.println("- parseQuery() should handle case automatically (both upper and lower should work)");
        System.out.println("- SplitTermQuery('title', 'hello') should work (matches indexed lowercase text)");
        System.out.println("- SplitTermQuery('title', 'HELLO') should fail (uppercase doesn't match lowercase index)");

        // Expected behavior after fix:
        // - parseQuery should work for both cases (Quickwit handles tokenization)
        // - SplitTermQuery with lowercase should work (matches index)
        // - SplitTermQuery with uppercase should fail (doesn't match index)

        if (termResultUpper.getHits().size() == 0 && termResultLower.getHits().size() > 0) {
            System.out.println("✅ Expected behavior: SplitTermQuery respects case, lowercase works, uppercase fails");
        } else if (termResultUpper.getHits().size() > 0 && termResultLower.getHits().size() > 0) {
            System.out.println("❌ Issue: Both uppercase and lowercase SplitTermQuery work (unexpected)");
        } else {
            System.out.println("❌ Issue: Neither uppercase nor lowercase SplitTermQuery works");
        }
    }
}