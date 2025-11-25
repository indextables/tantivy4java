package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Minimal test to isolate date field split file issue
 */
public class MinimalSplitDateTest {

    @Test
    public void testSingleSplitWithDateField() throws Exception {
        String tempDir = "/tmp/minimal_split_date_test_" + System.currentTimeMillis();
        File splitDir = new File(tempDir);
        splitDir.mkdirs();
        String splitPath = tempDir + "/test.split";

        try {
            // Create simple schema with date field
            SchemaBuilder builder = new SchemaBuilder();
            builder.addTextField("title", true, false, "default", "position");
            builder.addDateField("published_date", true, true, false);
            Schema schema = builder.build();

            // Create index
            Path indexPath = Paths.get(tempDir).resolve("test_index");
            Index index = new Index(schema, indexPath.toString());
            IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

            // Add ONE document with date
            writer.addJson("{\"title\": \"Test Document\", \"published_date\": \"2023-01-15T00:00:00Z\"}");

            writer.commit();
            writer.close();
            index.close();

            System.out.println("✅ Created index with 1 document");

            // Convert to split
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig("test-index", "test-source", "test-node");
            QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath, config);

            System.out.println("✅ Converted to split: " + splitPath);
            System.out.println("   Documents in split: " + splitMetadata.getNumDocs());

            // Search the split
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("minimal-test-cache");
            SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
            SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata);

            System.out.println("✅ Created searcher");

            // Try a simple search
            SplitQuery query = searcher.parseQuery("title:Test");
            System.out.println("✅ Parsed query");

            SearchResult results = searcher.search(query, 10);
            System.out.println("✅ Search completed");
            System.out.println("   Results: " + results.getHits().size() + " documents");

            assertEquals(1, results.getHits().size(), "Should find 1 document");

            searcher.close();
            System.out.println("✅ Test passed");

        } finally {
            // Cleanup
            if (Files.exists(Paths.get(tempDir))) {
                Files.walk(Paths.get(tempDir))
                        .map(Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);
            }
        }
    }
}
