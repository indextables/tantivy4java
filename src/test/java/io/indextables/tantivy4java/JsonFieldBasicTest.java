package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import org.junit.jupiter.api.Test;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic test to verify JSON field functionality works.
 */
public class JsonFieldBasicTest {

    @Test
    public void testBasicJsonFieldCreation() {
        // Create schema with JSON field
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                assertNotNull(jsonField, "JSON field should be created");
                assertNotNull(schema, "Schema should be created");
                assertTrue(schema.hasField("data"), "Schema should contain 'data' field");
            }
        }
    }

    @Test
    public void testJsonFieldIndexingFromString() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                // Create RAM index
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add document with JSON string
                        try (Document doc = new Document()) {
                            doc.addJson(jsonField, "{\"name\": \"John\", \"age\": 30}");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    // Verify document was indexed
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document indexed");
                    }
                }
            }
        }
    }

    @Test
    public void testJsonFieldIndexingFromObject() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                // Create RAM index
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add document with Java object
                        try (Document doc = new Document()) {
                            Map<String, Object> data = new HashMap<>();
                            data.put("name", "Jane");
                            data.put("age", 25);
                            data.put("active", true);
                            doc.addJson(jsonField, data);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    // Verify document was indexed
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document indexed");
                    }
                }
            }
        }
    }

    @Test
    public void testNestedJsonObject() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                // Create RAM index
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add document with nested structure
                        try (Document doc = new Document()) {
                            Map<String, Object> data = new HashMap<>();
                            Map<String, Object> user = new HashMap<>();
                            user.put("name", "Alice");
                            user.put("age", 28);
                            data.put("user", user);
                            data.put("status", "active");
                            doc.addJson(jsonField, data);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    // Verify document was indexed
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document indexed");
                    }
                }
            }
        }
    }

    @Test
    public void testJsonFieldWithArrays() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                // Create RAM index
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add document with arrays
                        try (Document doc = new Document()) {
                            Map<String, Object> data = new HashMap<>();
                            data.put("tags", Arrays.asList("java", "search", "tantivy"));
                            data.put("scores", Arrays.asList(10, 20, 30));
                            doc.addJson(jsonField, data);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    // Verify document was indexed
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document indexed");
                    }
                }
            }
        }
    }

    @Test
    public void testMultipleDocumentsWithJson() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                // Create RAM index
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add multiple documents
                        for (int i = 0; i < 10; i++) {
                            try (Document doc = new Document()) {
                                Map<String, Object> data = new HashMap<>();
                                data.put("id", i);
                                data.put("name", "Item " + i);
                                doc.addJson(jsonField, data);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    // Verify all documents were indexed
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(10, searcher.getNumDocs(), "Should have 10 documents indexed");
                    }
                }
            }
        }
    }

    @Test
    public void testJsonFieldWithDifferentOptions() {
        // Test different JsonObjectOptions configurations
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Stored only
            Field storedField = builder.addJsonField("stored_only", JsonObjectOptions.stored());
            assertNotNull(storedField);

            // Indexed only
            Field indexedField = builder.addJsonField("indexed_only", JsonObjectOptions.indexed());
            assertNotNull(indexedField);

            // Stored and indexed
            Field storedIndexedField = builder.addJsonField("stored_indexed",
                JsonObjectOptions.storedAndIndexed());
            assertNotNull(storedIndexedField);

            // Full options
            Field fullField = builder.addJsonField("full", JsonObjectOptions.full());
            assertNotNull(fullField);

            try (Schema schema = builder.build()) {
                assertNotNull(schema);
            }
        }
    }

    @Test
    public void testMixedTypeValues() {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            Map<String, Object> data = new HashMap<>();
                            data.put("string_field", "text value");
                            data.put("int_field", 42);
                            data.put("float_field", 3.14);
                            data.put("bool_field", true);
                            data.put("null_field", null);
                            doc.addJson(jsonField, data);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Reload index to see committed documents
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document with mixed types");
                    }
                }
            }
        }
    }
}
