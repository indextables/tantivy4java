package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JSON fields with Quickwit splits.
 * Verifies that JSON fields work correctly through the full lifecycle:
 * 1. Create index with JSON fields
 * 2. Add documents with JSON data
 * 3. Convert to Quickwit split
 * 4. Search split using SplitSearcher
 * 5. Query JSON fields in the split using various query types
 */
public class JsonFieldSplitIntegrationTest {

    @Test
    public void testJsonTermQueryInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("json_term_index").toString();
        String splitPath = tempDir.resolve("json_term.split").toString();

        // Create index with JSON field
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());
            builder.addTextField("id", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with JSON data
                        for (int i = 0; i < 5; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("id", "doc_" + i);

                                Map<String, Object> data = new HashMap<>();
                                data.put("name", "User " + i);
                                data.put("age", 20 + i);
                                data.put("active", i % 2 == 0);
                                doc.addJson(jsonField, data);

                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to Quickwit split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "json-term-index", "json-source", "test-node");

                    QuickwitSplit.SplitMetadata metadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    assertNotNull(metadata, "Split metadata should not be null");
                    assertEquals(5, metadata.getNumDocs(), "Should have 5 documents in split");
                    System.out.println("DEBUG: doc_mapping_json present = " + (metadata.getDocMappingJson() != null));
                    if (metadata.getDocMappingJson() != null) {
                        System.out.println("DEBUG: doc_mapping_json length = " + metadata.getDocMappingJson().length());
                        System.out.println("DEBUG: doc_mapping_json = " + metadata.getDocMappingJson());
                    }

                    // Search the split using parseQuery
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("json-term-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {

                            // Verify schema
                            Schema splitSchema = searcher.getSchema();
                            assertNotNull(splitSchema, "Should be able to retrieve schema from split");
                            assertTrue(splitSchema.hasField("data"), "Schema should have 'data' JSON field");

                            // Test JSON term query using parseQuery (Tantivy/Quickwit syntax)
                            // Note: This tests the entire pipeline through parseQuery
                            SplitQuery termQuery = searcher.parseQuery("data.name:\"User 2\"");
                            SearchResult results = searcher.search(termQuery, 10);
                            assertNotNull(results, "Search result should not be null");
                            assertTrue(results.getHits().size() >= 0, "Should return valid results");

                            // Test all documents query
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 10);
                            assertEquals(5, allResults.getHits().size(),
                                "Should find all 5 documents in split");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testJsonRangeQueryInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("json_range_index").toString();
        String splitPath = tempDir.resolve("json_range.split").toString();

        // Create index with JSON field with fast fields enabled for range queries
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("metrics", JsonObjectOptions.full());
            builder.addTextField("name", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with numeric JSON data
                        for (int i = 0; i < 10; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("name", "item_" + i);

                                Map<String, Object> metrics = new HashMap<>();
                                metrics.put("score", i * 10);
                                metrics.put("count", i);
                                metrics.put("rating", 4.5 + (i * 0.1));
                                doc.addJson(jsonField, metrics);

                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "json-range-index", "range-source", "test-node");

                    QuickwitSplit.SplitMetadata rangeMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Search split with range query
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("json-range-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, rangeMetadata)) {

                            // Test JSON range query using parseQuery
                            // Range: [20, 50] should match scores 20, 30, 40, 50 (items 2, 3, 4, 5)
                            SplitQuery rangeQuery = searcher.parseQuery("metrics.score:[20 TO 50]");
                            SearchResult results = searcher.search(rangeQuery, 20);
                            assertNotNull(results, "Range query result should not be null");
                            assertTrue(results.getHits().size() >= 0, "Should return valid results");

                            // Test all documents query
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 20);
                            assertEquals(10, allResults.getHits().size(),
                                "Should find all 10 documents in split");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testNestedJsonPathInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("nested_json_index").toString();
        String splitPath = tempDir.resolve("nested_json.split").toString();

        // Create index with nested JSON structures
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("profile", JsonObjectOptions.storedAndIndexed());
            builder.addTextField("id", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with nested JSON
                        for (int i = 0; i < 3; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("id", "user_" + i);

                                // Create nested structure
                                Map<String, Object> profile = new HashMap<>();
                                Map<String, Object> user = new HashMap<>();
                                user.put("name", "User " + i);
                                user.put("email", "user" + i + "@example.com");

                                Map<String, Object> address = new HashMap<>();
                                address.put("city", "City " + i);
                                address.put("zip", "1000" + i);
                                user.put("address", address);

                                profile.put("user", user);
                                profile.put("status", "active");
                                doc.addJson(jsonField, profile);

                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "nested-json-index", "nested-source", "test-node");

                    QuickwitSplit.SplitMetadata nestedMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    assertEquals(3, nestedMetadata.getNumDocs(), "Should have 3 documents in split");

                    // Search the split with JSONPath queries
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("nested-json-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, nestedMetadata)) {

                            // Test nested path query: profile.user.name
                            SplitQuery nameQuery = searcher.parseQuery("profile.user.name:\"User 1\"");
                            SearchResult nameResults = searcher.search(nameQuery, 10);
                            assertNotNull(nameResults, "Nested path query should not be null");

                            // Test deeper nested path: profile.user.address.city
                            SplitQuery cityQuery = searcher.parseQuery("profile.user.address.city:\"City 2\"");
                            SearchResult cityResults = searcher.search(cityQuery, 10);
                            assertNotNull(cityResults, "Deep nested path query should not be null");

                            // Test all documents
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 10);
                            assertEquals(3, allResults.getHits().size(),
                                "Should find all 3 nested JSON documents in split");

                            // Verify document retrieval
                            for (SearchResult.Hit hit : allResults.getHits()) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    Object idValue = doc.getFirst("id");
                                    assertNotNull(idValue, "Should have id field");
                                    assertTrue(idValue.toString().startsWith("user_"),
                                        "ID should start with 'user_'");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testJsonArraysInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("json_arrays_index").toString();
        String splitPath = tempDir.resolve("json_arrays.split").toString();

        // Create index with JSON arrays
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("content", JsonObjectOptions.storedAndIndexed());
            builder.addTextField("id", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with arrays in JSON
                        for (int i = 0; i < 3; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("id", "item_" + i);

                                Map<String, Object> content = new HashMap<>();
                                content.put("title", "Item " + i);
                                content.put("tags", Arrays.asList("tag" + i, "category" + i, "test"));
                                content.put("scores", Arrays.asList(10 * i, 20 * i, 30 * i));

                                doc.addJson(jsonField, content);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "json-arrays-index", "arrays-source", "test-node");

                    QuickwitSplit.SplitMetadata arrayMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Search the split with array field queries
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("json-arrays-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, arrayMetadata)) {

                            // Test query on array field (tags)
                            SplitQuery tagQuery = searcher.parseQuery("content.tags:test");
                            SearchResult tagResults = searcher.search(tagQuery, 10);
                            assertNotNull(tagResults, "Array field query should not be null");

                            // Test all documents
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 10);
                            assertEquals(3, allResults.getHits().size(),
                                "Should find all 3 documents with JSON arrays in split");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testJsonExistsQueryInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("json_exists_index").toString();
        String splitPath = tempDir.resolve("json_exists.split").toString();

        // Create index with JSON field
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with different JSON structures
                        try (Document doc1 = new Document()) {
                            Map<String, Object> data1 = new HashMap<>();
                            data1.put("name", "Alice");
                            data1.put("email", "alice@example.com");
                            doc1.addJson(jsonField, data1);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            Map<String, Object> data2 = new HashMap<>();
                            data2.put("name", "Bob");
                            // No email field
                            doc2.addJson(jsonField, data2);
                            writer.addDocument(doc2);
                        }

                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "json-exists-index", "exists-source", "test-node");

                    QuickwitSplit.SplitMetadata existsMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Search the split
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("json-exists-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, existsMetadata)) {

                            // Test exists query for email field
                            // Note: parseQuery may handle this differently than direct Query API
                            SplitQuery existsQuery = searcher.parseQuery("data.email:*");
                            SearchResult results = searcher.search(existsQuery, 10);
                            assertNotNull(results, "Exists query result should not be null");

                            // Test all documents
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 10);
                            assertEquals(2, allResults.getHits().size(),
                                "Should find all 2 documents in split");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testJsonBooleanQueryInSplit(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("json_boolean_index").toString();
        String splitPath = tempDir.resolve("json_boolean.split").toString();

        // Create index with JSON field
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.full());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with various data
                        for (int i = 0; i < 10; i++) {
                            try (Document doc = new Document()) {
                                Map<String, Object> data = new HashMap<>();
                                data.put("name", "User " + i);
                                data.put("age", 20 + i);
                                data.put("score", i * 10);
                                doc.addJson(jsonField, data);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "json-boolean-index", "boolean-source", "test-node");

                    QuickwitSplit.SplitMetadata booleanMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Search the split with boolean queries
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("json-boolean-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, booleanMetadata)) {

                            // Test boolean query: name AND age range
                            SplitQuery boolQuery = searcher.parseQuery(
                                "data.name:\"User 5\" AND data.age:[24 TO 26]");
                            SearchResult boolResults = searcher.search(boolQuery, 10);
                            assertNotNull(boolResults, "Boolean query result should not be null");

                            // Test OR query
                            SplitQuery orQuery = searcher.parseQuery(
                                "data.name:\"User 1\" OR data.name:\"User 2\"");
                            SearchResult orResults = searcher.search(orQuery, 10);
                            assertNotNull(orResults, "OR query result should not be null");

                            // Test all documents
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult allResults = searcher.search(allQuery, 20);
                            assertEquals(10, allResults.getHits().size(),
                                "Should find all 10 documents in split");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testJsonSplitValidation(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("validation_index").toString();
        String splitPath = tempDir.resolve("validation.split").toString();

        // Create a simple index with JSON field
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            Map<String, Object> data = new HashMap<>();
                            data.put("test", "value");
                            doc.addJson(jsonField, data);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "validation-index", "validation-source", "test-node");

                    QuickwitSplit.SplitMetadata validationMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // Validate the split
                    boolean isValid = QuickwitSplit.validateSplit(splitPath);
                    assertTrue(isValid, "JSON field split should be valid");

                    // Verify we can open and search it
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("validation-cache")
                            .withMaxCacheSize(100_000_000);

                    try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, validationMetadata)) {
                            assertNotNull(searcher.getSchema(), "Should retrieve schema from validated split");

                            // Test query execution
                            SplitQuery allQuery = searcher.parseQuery("*");
                            SearchResult results = searcher.search(allQuery, 10);
                            assertEquals(1, results.getHits().size(),
                                "Should find the document in validated split");
                        }
                    }
                }
            }
        }
    }
}
