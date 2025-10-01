package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Test class for text, JSON, and string field types with SplitSearcher integration.
 * Demonstrates how all field types work seamlessly with Quickwit split functionality.
 */
public class SplitSearcherFieldTypesTest {

    @Test
    @DisplayName("SplitSearcher String vs Text Field Behavior")
    public void testSplitSearcherStringVsTextBehavior(@TempDir Path tempDir) {
        System.out.println("🚀 === SPLIT SEARCHER STRING VS TEXT BEHAVIOR TEST ===");
        System.out.println("Validating string exact matching vs text tokenization with SplitSearcher");

        String indexPath = tempDir.resolve("behavior_index").toString();
        String splitPath = tempDir.resolve("behavior_split.split").toString();

        try {
            // Create index with both text and string fields
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("text_content", true, false, "default", "position")
                    .addStringField("string_content", true, true, false)
                    .addIntegerField("id", true, true, true);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            try (Document doc = new Document()) {
                                doc.addInteger("id", 1);
                                doc.addText("text_content", "Advanced Machine Learning Techniques");
                                doc.addString("string_content", "Advanced Machine Learning Techniques");
                                writer.addDocument(doc);
                            }

                            writer.commit();
                        }

                        // Convert to split
                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "behavior-test", "test-source", "test-node");
                        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                        // Test with SplitSearcher
                        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("behavior-cache")
                            .withMaxCacheSize(20_000_000);
                        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

                        try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {

                            // Test text field tokenization
                            System.out.println("\n🔎 Testing text field tokenization with SplitSearcher");
                            SplitQuery textTokenQuery = splitSearcher.parseQuery("text_content:machine");
                            SearchResult textResult = splitSearcher.search(textTokenQuery, 10);

                            assertTrue(textResult.getHits().size() >= 1,
                                "Text field should match token 'machine'");
                            System.out.println("  ✅ Text field matched token: " + textResult.getHits().size() + " documents");

                            // Test string field exact matching requirement
                            System.out.println("\n🔎 Testing string field exact matching with SplitSearcher");
                            SplitQuery stringTokenQuery = splitSearcher.parseQuery("string_content:machine");
                            SearchResult stringTokenResult = splitSearcher.search(stringTokenQuery, 10);

                            assertEquals(0, stringTokenResult.getHits().size(),
                                "String field should NOT match partial token 'machine'");
                            System.out.println("  ✅ String field rejected token: " + stringTokenResult.getHits().size() + " documents");

                            SplitQuery stringExactQuery = splitSearcher.parseQuery("string_content:\"Advanced Machine Learning Techniques\"");
                            SearchResult stringExactResult = splitSearcher.search(stringExactQuery, 10);

                            assertEquals(1, stringExactResult.getHits().size(),
                                "String field should match exact phrase");
                            System.out.println("  ✅ String field matched exact phrase: " + stringExactResult.getHits().size() + " documents");
                        }
                    }
                }
            }

            System.out.println("\n🎉 === SPLIT SEARCHER BEHAVIOR TEST COMPLETED ===");
            System.out.println("✨ Validated string vs text field behavior with SplitSearcher integration");

        } catch (Exception e) {
            fail("Split searcher behavior test failed: " + e.getMessage());
        }
    }
}
