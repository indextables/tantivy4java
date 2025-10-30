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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Test for JSON field retrieval bug fix (TANTIVY4JAVA_JSON_RETRIEVAL_BUG.md).
 *
 * Expected: JSON fields return valid JSON strings like {"name":"Alice","age":30}
 * Bug: Was returning Rust debug format like Object([("age", U64(30)), ("name", Str("Alice"))])
 */
public class JsonRetrievalBugFixTest {

    @Test
    void testJsonObjectRetrievalReturnsValidJson() throws Exception {
        // Setup
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            Field userField = builder.addJsonField("user", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Write JSON document
                        try (Document doc = new Document()) {
                            doc.addText("title", "Test Document");

                            Map<String, Object> userData = new HashMap<>();
                            userData.put("name", "Alice");
                            userData.put("age", 30);
                            doc.addJson(userField, userData);

                            writer.addDocument(doc);
                        }
                        writer.commit();

                        // Read and verify
                        index.reload();
                        try (Searcher searcher = index.searcher()) {
                            Query query = Query.termQuery(schema, "title", "Test");
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size());

                            try (Document retrieved = searcher.doc(result.getHits().get(0).getDocAddress())) {
                                String jsonString = (String) retrieved.getFirst("user");

                                // CRITICAL: Should NOT contain Rust debug format
                                assertFalse(jsonString.contains("Object(["),
                                    "Should not contain Rust debug format 'Object(['");
                                assertFalse(jsonString.contains("U64("),
                                    "Should not contain Rust type annotation 'U64('");
                                assertFalse(jsonString.contains("Str("),
                                    "Should not contain Rust type annotation 'Str('");

                                // Should be valid JSON parseable by Jackson
                                ObjectMapper mapper = new ObjectMapper();
                                Map<String, Object> parsed = mapper.readValue(
                                    jsonString,
                                    new TypeReference<Map<String, Object>>() {}
                                );

                                assertEquals("Alice", parsed.get("name"));
                                assertEquals(30, ((Number) parsed.get("age")).intValue());
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void testJsonArrayRetrievalReturnsValidJson() throws Exception {
        // Setup
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            Field tagsField = builder.addJsonField("tags", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Write JSON document with array inside object
                        try (Document doc = new Document()) {
                            doc.addText("title", "Array Test");

                            List<String> tagList = new ArrayList<>();
                            tagList.add("java");
                            tagList.add("tantivy");
                            tagList.add("search");

                            // JSON fields must be objects, not arrays at root level
                            Map<String, Object> tagsData = new HashMap<>();
                            tagsData.put("tags", tagList);
                            doc.addJson(tagsField, tagsData);

                            writer.addDocument(doc);
                        }
                        writer.commit();

                        // Read and verify
                        index.reload();
                        try (Searcher searcher = index.searcher()) {
                            Query query = Query.termQuery(schema, "title", "Array");
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size());

                            try (Document retrieved = searcher.doc(result.getHits().get(0).getDocAddress())) {
                                String jsonString = (String) retrieved.getFirst("tags");

                                // Should NOT contain Rust debug format
                                assertFalse(jsonString.contains("Array(["),
                                    "Should not contain Rust debug format 'Array(['");

                                // Should be valid JSON object parseable by Jackson
                                ObjectMapper mapper = new ObjectMapper();
                                Map<String, Object> parsed = mapper.readValue(
                                    jsonString,
                                    new TypeReference<Map<String, Object>>() {}
                                );

                                @SuppressWarnings("unchecked")
                                List<String> tags = (List<String>) parsed.get("tags");
                                assertNotNull(tags);
                                assertEquals(3, tags.size());
                                assertTrue(tags.contains("java"));
                                assertTrue(tags.contains("tantivy"));
                                assertTrue(tags.contains("search"));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void testNestedJsonRetrievalReturnsValidJson() throws Exception {
        // Setup
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            Field dataField = builder.addJsonField("data", JsonObjectOptions.full());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Write nested JSON document
                        try (Document doc = new Document()) {
                            doc.addText("title", "Nested Test");

                            Map<String, Object> address = new HashMap<>();
                            address.put("city", "NYC");
                            address.put("zip", 10001);

                            Map<String, Object> userData = new HashMap<>();
                            userData.put("name", "Bob");
                            userData.put("address", address);

                            doc.addJson(dataField, userData);
                            writer.addDocument(doc);
                        }
                        writer.commit();

                        // Read and verify
                        index.reload();
                        try (Searcher searcher = index.searcher()) {
                            Query query = Query.termQuery(schema, "title", "Nested");
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size());

                            try (Document retrieved = searcher.doc(result.getHits().get(0).getDocAddress())) {
                                String jsonString = (String) retrieved.getFirst("data");

                                // Should be valid nested JSON
                                ObjectMapper mapper = new ObjectMapper();
                                Map<String, Object> parsed = mapper.readValue(
                                    jsonString,
                                    new TypeReference<Map<String, Object>>() {}
                                );

                                assertEquals("Bob", parsed.get("name"));

                                @SuppressWarnings("unchecked")
                                Map<String, Object> parsedAddress = (Map<String, Object>) parsed.get("address");
                                assertNotNull(parsedAddress);
                                assertEquals("NYC", parsedAddress.get("city"));
                                assertEquals(10001, ((Number) parsedAddress.get("zip")).intValue());
                            }
                        }
                    }
                }
            }
        }
    }
}
