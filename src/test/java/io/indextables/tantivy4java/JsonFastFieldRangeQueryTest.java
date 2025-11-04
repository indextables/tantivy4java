/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.HashMap;

/**
 * Test for JSON field fast field support with range queries.
 *
 * This test verifies whether Tantivy's JsonObjectOptions.setFast(true)
 * enables fast fields on nested JSON sub-fields, allowing range queries
 * on numeric fields within JSON objects.
 *
 * Related to bug report: TANTIVY4JAVA_JSON_FAST_FIELD_BUG_REPORT.md
 */
public class JsonFastFieldRangeQueryTest {

    @Test
    void testRangeQueryOnNestedJsonIntegerField() {
        // This tests the core issue: can we execute range queries on JSON sub-fields?
        // Expected: If Tantivy's setFast(true) enables fast fields on sub-fields, this should work
        // Bug report says: This currently fails with "Field 'user.age' is not configured as fast field"

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");

            // Create JSON field with fast fields enabled
            // According to documentation, setFast(true) should enable fast field access on sub-fields
            Field userField = builder.addJsonField("user", JsonObjectOptions.full());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Index test documents with nested user data

                        // Document 1: Alice, age 30
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "User Alice");

                            Map<String, Object> user1 = new HashMap<>();
                            user1.put("name", "Alice");
                            user1.put("age", 30);
                            user1.put("city", "NYC");

                            doc1.addJson(userField, user1);
                            writer.addDocument(doc1);
                        }

                        // Document 2: Bob, age 25
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "User Bob");

                            Map<String, Object> user2 = new HashMap<>();
                            user2.put("name", "Bob");
                            user2.put("age", 25);
                            user2.put("city", "SF");

                            doc2.addJson(userField, user2);
                            writer.addDocument(doc2);
                        }

                        // Document 3: Charlie, age 35
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "User Charlie");

                            Map<String, Object> user3 = new HashMap<>();
                            user3.put("name", "Charlie");
                            user3.put("age", 35);
                            user3.put("city", "NYC");

                            doc3.addJson(userField, user3);
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                        index.reload();

                        try (Searcher searcher = index.searcher()) {
                            // Attempt range query on user.age field (nested integer)
                            // Using Query.jsonRangeQuery which is designed for JSON sub-fields
                            try (Query query = Query.jsonRangeQuery(
                                schema,
                                "user",          // JSON field name
                                "age",           // Sub-field path
                                29L,             // Lower bound (exclusive - >29, not >=29)
                                100L,            // Upper bound
                                false,           // Lower bound NOT inclusive
                                true             // Upper bound inclusive
                            )) {

                                SearchResult result = searcher.search(query, 10);

                                // If fast fields work on JSON sub-fields, we should get 2 results:
                                // Alice (30) and Charlie (35)
                                System.out.println("✅ Range query succeeded! Found " + result.getHits().size() + " results");
                                assertEquals(2, result.getHits().size(), "Should find Alice (30) and Charlie (35)");

                                // Verify document retrieval
                                for (var hit : result.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String jsonStr = (String) doc.getFirst("user");
                                        assertNotNull(jsonStr, "Should be able to retrieve JSON field");
                                        System.out.println("  Found document: " + jsonStr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            // If fast fields don't work on JSON sub-fields, we'll get an error
            String errorMsg = e.getMessage();
            System.out.println("❌ Range query failed with error: " + errorMsg);

            // Check if this is the expected fast field error
            if (errorMsg != null && (errorMsg.contains("not configured as fast field") ||
                errorMsg.contains("user.age") || errorMsg.contains("fast field"))) {
                fail("BUG CONFIRMED: Range queries on JSON sub-fields fail. " +
                     "Tantivy's JsonObjectOptions.setFast(true) does not enable fast fields " +
                     "for nested sub-fields. Error: " + errorMsg);
            } else {
                // Some other error - rethrow
                throw e;
            }
        }
    }

    @Test
    void testEqualityQueryOnNestedJsonField() {
        // Equality queries should work on JSON fields even without fast fields
        // This establishes baseline functionality

        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field userField = builder.addJsonField("user", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            Map<String, Object> userData = new HashMap<>();
                            userData.put("name", "Alice");
                            userData.put("age", 30);

                            doc.addJson(userField, userData);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                        index.reload();

                        try (Searcher searcher = index.searcher()) {
                            // Use JSON term query for equality
                            try (Query query = Query.jsonTermQuery(schema, "user", "name", "Alice")) {
                                SearchResult result = searcher.search(query, 10);

                                System.out.println("✅ Equality query on user.name succeeded! Found " + result.getHits().size() + " results");
                                assertTrue(result.getHits().size() > 0, "Should find Alice");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Equality query on JSON sub-field should work: " + e.getMessage());
        }
    }

    @Test
    void testRangeQueryWithDifferentNumericValues() {
        // Test with more varied numeric values to ensure range logic works correctly

        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field dataField = builder.addJsonField("data", JsonObjectOptions.full());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with different scores
                        int[] scores = {10, 20, 30, 40, 50};
                        for (int score : scores) {
                            try (Document doc = new Document()) {
                                Map<String, Object> data = new HashMap<>();
                                data.put("score", score);
                                doc.addJson(dataField, data);
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                        index.reload();

                        try (Searcher searcher = index.searcher()) {
                            // Query for scores between 20 and 40 (inclusive)
                            try (Query query = Query.jsonRangeQuery(
                                schema,
                                "data",
                                "score",
                                20L,
                                40L,
                                true,   // >= 20
                                true    // <= 40
                            )) {

                                SearchResult result = searcher.search(query, 10);

                                System.out.println("✅ Range query on scores succeeded! Found " + result.getHits().size() + " results");
                                // Should find scores: 20, 30, 40 = 3 documents
                                assertEquals(3, result.getHits().size(), "Should find documents with scores 20, 30, 40");
                            }
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            String errorMsg = e.getMessage();
            System.out.println("❌ Range query with varied scores failed: " + errorMsg);

            if (errorMsg != null && errorMsg.contains("not configured as fast field")) {
                fail("BUG CONFIRMED: Range queries fail on JSON numeric sub-fields. Error: " + errorMsg);
            } else {
                throw e;
            }
        }
    }
}
