package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

/**
 * Test for ExistsQuery support - IS NOT NULL / IS NULL query functionality.
 *
 * ExistsQuery allows checking if a field has any non-null value in documents.
 * This is essential for filtering documents based on field presence.
 *
 * IMPORTANT: ExistsQuery requires FAST fields. Fields without the FAST flag
 * will result in an error when used with existsQuery().
 */
public class ExistsQueryTest {

    @Test
    @DisplayName("ExistsQuery matches documents with field present")
    public void testExistsQueryMatchesDocs() {
        System.out.println("=== ExistsQuery Matches Docs Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create schema with FAST fields (required for ExistsQuery)
            builder.addTextField("title", true, true, "raw", "position")  // stored, fast
                   .addIntegerField("count", true, true, true)            // stored, indexed, fast
                   .addIntegerField("id", true, true, true);              // stored, indexed, fast

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Document 1: Has all fields
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Complete Document");
                            doc1.addInteger("count", 10);
                            doc1.addInteger("id", 1);
                            writer.addDocument(doc1);
                        }

                        // Document 2: Has all fields
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Another Complete");
                            doc2.addInteger("count", 20);
                            doc2.addInteger("id", 2);
                            writer.addDocument(doc2);
                        }

                        // Document 3: Missing 'count' field
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Missing Count");
                            // Intentionally skip 'count' field
                            doc3.addInteger("id", 3);
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                        System.out.println("Indexed 3 documents (2 with count, 1 without)");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");

                        // Test existsQuery on 'count' field - should find only 2 documents
                        try (Query existsQuery = Query.existsQuery(schema, "count")) {
                            try (SearchResult result = searcher.search(existsQuery, 10)) {
                                List<SearchResult.Hit> hits = result.getHits();
                                assertEquals(2, hits.size(), "ExistsQuery on 'count' should find 2 documents");

                                System.out.println("ExistsQuery('count') found " + hits.size() + " documents:");
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String title = doc.get("title").get(0).toString();
                                        System.out.println("  - " + title);
                                        assertNotEquals("Missing Count", title, "Should not find document with missing count");
                                    }
                                }
                            }
                        }

                        // Test existsQuery on 'id' field - should find all 3 documents
                        try (Query existsId = Query.existsQuery(schema, "id")) {
                            try (SearchResult result = searcher.search(existsId, 10)) {
                                assertEquals(3, result.getHits().size(), "ExistsQuery on 'id' should find all 3 documents");
                                System.out.println("ExistsQuery('id') found all 3 documents");
                            }
                        }
                    }
                }
            }

            System.out.println("ExistsQuery matches test PASSED");

        } catch (Exception e) {
            fail("ExistsQuery matches test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("ExistsQuery excludes documents without field")
    public void testExistsQueryExcludesDocs() {
        System.out.println("=== ExistsQuery Excludes Docs Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("name", true, true, "raw", "position")  // stored, fast
                   .addTextField("email", true, true, "raw", "position") // stored, fast - optional field
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Users with email
                        try (Document doc = new Document()) {
                            doc.addText("name", "Alice");
                            doc.addText("email", "alice@example.com");
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }

                        try (Document doc = new Document()) {
                            doc.addText("name", "Bob");
                            doc.addText("email", "bob@example.com");
                            doc.addInteger("id", 2);
                            writer.addDocument(doc);
                        }

                        // Users without email
                        try (Document doc = new Document()) {
                            doc.addText("name", "Charlie");
                            // No email
                            doc.addInteger("id", 3);
                            writer.addDocument(doc);
                        }

                        try (Document doc = new Document()) {
                            doc.addText("name", "Diana");
                            // No email
                            doc.addInteger("id", 4);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                        System.out.println("Indexed 4 users (2 with email, 2 without)");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Find users WITH email
                        try (Query hasEmail = Query.existsQuery(schema, "email")) {
                            try (SearchResult result = searcher.search(hasEmail, 10)) {
                                assertEquals(2, result.getHits().size(), "Should find 2 users with email");

                                System.out.println("Users with email:");
                                for (SearchResult.Hit hit : result.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String name = doc.get("name").get(0).toString();
                                        String email = doc.get("email").get(0).toString();
                                        System.out.println("  - " + name + " (" + email + ")");
                                        assertTrue(name.equals("Alice") || name.equals("Bob"),
                                            "Only Alice and Bob have email");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("ExistsQuery excludes test PASSED");

        } catch (Exception e) {
            fail("ExistsQuery excludes test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("ExistsQuery requires FAST field")
    public void testExistsQueryRequiresFastField() {
        System.out.println("=== ExistsQuery Requires Fast Field Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create a field WITHOUT the fast flag
            builder.addTextField("title", true, false, "default", "position")  // stored=true, fast=FALSE
                   .addIntegerField("id", true, true, false);  // stored=true, indexed=true, fast=FALSE

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Test Document");
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Attempting to use existsQuery on non-fast field should fail
                        Exception exception = assertThrows(RuntimeException.class, () -> {
                            try (Query query = Query.existsQuery(schema, "title")) {
                                searcher.search(query, 10);
                            }
                        });

                        String message = exception.getMessage();
                        System.out.println("Expected error: " + message);
                        assertTrue(
                            message.contains("fast") || message.contains("Fast") || message.contains("FAST"),
                            "Error should mention fast field requirement"
                        );
                    }
                }
            }

            System.out.println("ExistsQuery fast field requirement test PASSED");

        } catch (Exception e) {
            // This is expected behavior - the exception proves the validation works
            String message = e.getMessage();
            if (message != null && (message.contains("fast") || message.contains("Fast") || message.contains("FAST"))) {
                System.out.println("Correctly rejected non-fast field: " + message);
                System.out.println("ExistsQuery fast field requirement test PASSED");
            } else {
                fail("Unexpected error: " + e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("IS NULL query via boolean MUST_NOT negation")
    public void testNotExistsQuery() {
        System.out.println("=== IS NULL (NOT EXISTS) Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("name", true, true, "raw", "position")
                   .addTextField("phone", true, true, "raw", "position")  // optional field with fast
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Contacts with phone
                        try (Document doc = new Document()) {
                            doc.addText("name", "Alice");
                            doc.addText("phone", "555-1234");
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }

                        // Contacts without phone (IS NULL)
                        try (Document doc = new Document()) {
                            doc.addText("name", "Bob");
                            // No phone
                            doc.addInteger("id", 2);
                            writer.addDocument(doc);
                        }

                        try (Document doc = new Document()) {
                            doc.addText("name", "Charlie");
                            // No phone
                            doc.addInteger("id", 3);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                        System.out.println("Indexed 3 contacts (1 with phone, 2 without)");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // IS NULL query: Find contacts WITHOUT phone
                        // This is done using: MUST(allQuery) AND MUST_NOT(existsQuery)
                        try (Query allQuery = Query.allQuery();
                             Query hasPhone = Query.existsQuery(schema, "phone")) {

                            Query isNull = Query.booleanQuery(Arrays.asList(
                                new Query.OccurQuery(Occur.MUST, allQuery),
                                new Query.OccurQuery(Occur.MUST_NOT, hasPhone)
                            ));

                            try (isNull; SearchResult result = searcher.search(isNull, 10)) {
                                List<SearchResult.Hit> hits = result.getHits();
                                assertEquals(2, hits.size(), "Should find 2 contacts without phone");

                                System.out.println("Contacts without phone (IS NULL):");
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String name = doc.get("name").get(0).toString();
                                        System.out.println("  - " + name);
                                        assertTrue(name.equals("Bob") || name.equals("Charlie"),
                                            "Only Bob and Charlie should be missing phone");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("IS NULL (NOT EXISTS) query test PASSED");

        } catch (Exception e) {
            fail("IS NULL query test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("ExistsQuery works with all fast field types")
    public void testExistsQueryAllFieldTypes() {
        System.out.println("=== ExistsQuery All Field Types Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Various fast field types
            builder.addTextField("text_field", true, true, "raw", "position")   // text fast
                   .addIntegerField("int_field", true, true, true)              // int fast
                   .addFloatField("float_field", true, true, true)              // float fast
                   .addBooleanField("bool_field", true, true, true)             // bool fast
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Document with all fields
                        try (Document doc = new Document()) {
                            doc.addText("text_field", "hello");
                            doc.addInteger("int_field", 42);
                            doc.addFloat("float_field", 3.14);
                            doc.addBoolean("bool_field", true);
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }

                        // Document with only some fields
                        try (Document doc = new Document()) {
                            doc.addText("text_field", "world");
                            // Missing int_field
                            doc.addFloat("float_field", 2.71);
                            // Missing bool_field
                            doc.addInteger("id", 2);
                            writer.addDocument(doc);
                        }

                        // Document with only id
                        try (Document doc = new Document()) {
                            // Missing all optional fields
                            doc.addInteger("id", 3);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                        System.out.println("Indexed 3 documents with various field combinations");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Test each field type
                        String[] fields = {"text_field", "int_field", "float_field", "bool_field"};
                        int[] expectedCounts = {2, 1, 2, 1};

                        for (int i = 0; i < fields.length; i++) {
                            try (Query existsQuery = Query.existsQuery(schema, fields[i])) {
                                try (SearchResult result = searcher.search(existsQuery, 10)) {
                                    int count = result.getHits().size();
                                    System.out.println("ExistsQuery('" + fields[i] + "') found " + count + " docs");
                                    assertEquals(expectedCounts[i], count,
                                        "ExistsQuery on " + fields[i] + " should find " + expectedCounts[i] + " docs");
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("ExistsQuery all field types test PASSED");

        } catch (Exception e) {
            fail("ExistsQuery all field types test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("JSON ExistsQuery checks JSON path existence")
    public void testJsonExistsQuery() {
        System.out.println("=== JSON ExistsQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // JSON field with FAST (using full() which includes fast)
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.full());
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Document with nested user.email
                        try (Document doc = new Document()) {
                            java.util.Map<String, Object> data = new java.util.HashMap<>();
                            java.util.Map<String, Object> user = new java.util.HashMap<>();
                            user.put("name", "Alice");
                            user.put("email", "alice@example.com");
                            data.put("user", user);
                            doc.addJson(jsonField, data);
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }

                        // Document with user.name but NO email
                        try (Document doc = new Document()) {
                            java.util.Map<String, Object> data = new java.util.HashMap<>();
                            java.util.Map<String, Object> user = new java.util.HashMap<>();
                            user.put("name", "Bob");
                            // No email
                            data.put("user", user);
                            doc.addJson(jsonField, data);
                            doc.addInteger("id", 2);
                            writer.addDocument(doc);
                        }

                        // Document with completely different structure
                        try (Document doc = new Document()) {
                            java.util.Map<String, Object> data = new java.util.HashMap<>();
                            data.put("status", "active");
                            // No user at all
                            doc.addJson(jsonField, data);
                            doc.addInteger("id", 3);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                        System.out.println("Indexed 3 documents with varying JSON structures");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Check for data.user.email existence
                        try (Query existsEmail = Query.jsonExistsQuery(schema, "data", "user.email")) {
                            try (SearchResult result = searcher.search(existsEmail, 10)) {
                                int count = result.getHits().size();
                                System.out.println("jsonExistsQuery('data', 'user.email') found " + count + " docs");
                                assertEquals(1, count, "Only Alice should have user.email");
                            }
                        }

                        // Check for data.user.name existence (should find Alice and Bob)
                        try (Query existsName = Query.jsonExistsQuery(schema, "data", "user.name")) {
                            try (SearchResult result = searcher.search(existsName, 10)) {
                                int count = result.getHits().size();
                                System.out.println("jsonExistsQuery('data', 'user.name') found " + count + " docs");
                                assertEquals(2, count, "Alice and Bob should have user.name");
                            }
                        }

                        // Check for data.user existence with subpaths (should find Alice and Bob)
                        try (Query existsUser = Query.jsonExistsQuery(schema, "data", "user", true)) {
                            try (SearchResult result = searcher.search(existsUser, 10)) {
                                int count = result.getHits().size();
                                System.out.println("jsonExistsQuery('data', 'user', checkSubpaths=true) found " + count + " docs");
                                assertEquals(2, count, "Documents with any user.* field should match");
                            }
                        }
                    }
                }
            }

            System.out.println("JSON ExistsQuery test PASSED");

        } catch (Exception e) {
            fail("JSON ExistsQuery test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("JSON ExistsQuery with subpaths parameter")
    public void testJsonExistsQueryWithSubpaths() {
        System.out.println("=== JSON ExistsQuery With Subpaths Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("attributes", JsonObjectOptions.full());
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Document with nested structure
                        try (Document doc = new Document()) {
                            java.util.Map<String, Object> attrs = new java.util.HashMap<>();
                            java.util.Map<String, Object> nested = new java.util.HashMap<>();
                            nested.put("value", 123);
                            attrs.put("nested", nested);
                            doc.addJson(jsonField, attrs);
                            doc.addInteger("id", 1);
                            writer.addDocument(doc);
                        }

                        // Document with top-level only
                        try (Document doc = new Document()) {
                            java.util.Map<String, Object> attrs = new java.util.HashMap<>();
                            attrs.put("top_level", "value");
                            doc.addJson(jsonField, attrs);
                            doc.addInteger("id", 2);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // With checkSubpaths=true, query on "attributes" should match any subfield
                        try (Query withSubpaths = Query.jsonExistsQuery(schema, "attributes", "", true)) {
                            try (SearchResult result = searcher.search(withSubpaths, 10)) {
                                System.out.println("checkSubpaths=true on root found " + result.getHits().size() + " docs");
                                assertEquals(2, result.getHits().size(), "Should find all docs with any attributes");
                            }
                        }

                        // Query on specific subpath
                        try (Query nestedQuery = Query.jsonExistsQuery(schema, "attributes", "nested.value", false)) {
                            try (SearchResult result = searcher.search(nestedQuery, 10)) {
                                System.out.println("Exact path 'nested.value' found " + result.getHits().size() + " docs");
                                assertEquals(1, result.getHits().size(), "Only doc 1 has nested.value");
                            }
                        }
                    }
                }
            }

            System.out.println("JSON ExistsQuery with subpaths test PASSED");

        } catch (Exception e) {
            fail("JSON ExistsQuery with subpaths test failed: " + e.getMessage());
        }
    }

    // ========================================================================
    // Split API Tests
    // ========================================================================

    @Test
    @DisplayName("SplitExistsQuery converts to correct QueryAst JSON")
    public void testSplitExistsQueryToJson() {
        System.out.println("=== SplitExistsQuery to JSON Test ===");

        SplitExistsQuery existsQuery = new SplitExistsQuery("email");
        String json = existsQuery.toQueryAstJson();

        System.out.println("SplitExistsQuery JSON: " + json);

        // Should contain FieldPresence type
        assertTrue(json.contains("FieldPresence") || json.contains("field_presence"),
            "JSON should indicate FieldPresence query type");
        assertTrue(json.contains("email"), "JSON should contain field name 'email'");

        System.out.println("SplitExistsQuery to JSON test PASSED");
    }

    @Test
    @DisplayName("SplitExistsQuery in boolean query for IS NULL pattern")
    public void testSplitExistsQueryInBooleanQuery() {
        System.out.println("=== SplitExistsQuery Boolean Query Test ===");

        // IS NULL pattern: MUST(matchAll) AND MUST_NOT(exists)
        SplitBooleanQuery isNullQuery = new SplitBooleanQuery()
            .addMust(new SplitMatchAllQuery())
            .addMustNot(new SplitExistsQuery("optional_field"));

        String json = isNullQuery.toQueryAstJson();
        System.out.println("IS NULL Boolean Query JSON: " + json);

        // Should be a boolean query with must_not containing field presence
        assertTrue(json.contains("Bool") || json.contains("bool"),
            "Should be a boolean query");
        assertTrue(json.contains("optional_field"),
            "Should contain the field name");

        System.out.println("SplitExistsQuery boolean query test PASSED");
    }

    @Test
    @DisplayName("SplitExistsQuery field validation")
    public void testSplitExistsQueryFieldValidation() {
        System.out.println("=== SplitExistsQuery Field Validation Test ===");

        // Test null field throws exception
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitExistsQuery(null);
        }, "Should throw exception for null field");

        // Test valid field
        SplitExistsQuery query = new SplitExistsQuery("valid_field");
        assertEquals("valid_field", query.getField(), "Field should match");
        assertEquals("SplitExistsQuery(field=valid_field)", query.toString(), "toString should be correct");

        System.out.println("SplitExistsQuery field validation test PASSED");
    }

    @Test
    @DisplayName("SplitExistsQuery equals and hashCode")
    public void testSplitExistsQueryEquality() {
        System.out.println("=== SplitExistsQuery Equality Test ===");

        SplitExistsQuery query1 = new SplitExistsQuery("email");
        SplitExistsQuery query2 = new SplitExistsQuery("email");
        SplitExistsQuery query3 = new SplitExistsQuery("phone");

        // Test equals
        assertEquals(query1, query2, "Same field should be equal");
        assertNotEquals(query1, query3, "Different fields should not be equal");
        assertNotEquals(query1, null, "Should not equal null");
        assertNotEquals(query1, "email", "Should not equal different type");

        // Test hashCode
        assertEquals(query1.hashCode(), query2.hashCode(), "Same field should have same hashCode");

        System.out.println("SplitExistsQuery equality test PASSED");
    }
}
