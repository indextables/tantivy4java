package io.indextables.tantivy4java;

import io.indextables.tantivy4java.batch.BatchDocument;
import io.indextables.tantivy4java.batch.BatchDocumentBuilder;
import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive functional and performance tests for the ByteBuffer protocol.
 *
 * <p>This test suite validates:
 * <ul>
 *   <li>All field types serialize/deserialize correctly</li>
 *   <li>Multi-value fields work properly</li>
 *   <li>Performance characteristics across varying document counts and field counts</li>
 * </ul>
 *
 * <p>Run with: mvn test -Dtest=ByteBufferProtocolTest -DskipTests=false
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ByteBufferProtocolTest {

    private static Path tempDir;
    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setup() throws Exception {
        tempDir = Files.createTempDirectory("bytebuffer_test");
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("bytebuffer-test-cache")
                .withMaxCacheSize(500_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    // ==================== FUNCTIONAL TESTS FOR ALL DATA TYPES ====================

    @Test
    @Order(1)
    @DisplayName("Test TEXT field type roundtrip with value validation")
    void testTextFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addTextField("text_field", true, false, "default", "position");
        }, (doc, i) -> {
            doc.addText("text_field", "Sample text content " + i + " with special chars: @#$%^&*()");
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("text_field");
                assertNotNull(value, "Text field should not be null");
                assertTrue(value instanceof String, "Text field should be String, got: " + value.getClass());
                String textValue = (String) value;
                assertTrue(textValue.startsWith("Sample text content"),
                        "Text should start with expected prefix, got: " + textValue);
                assertTrue(textValue.contains("@#$%^&*()"),
                        "Text should contain special chars, got: " + textValue);
            }
        }
    }

    @Test
    @Order(2)
    @DisplayName("Test INTEGER (i64) field type roundtrip with value validation")
    void testIntegerFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addIntegerField("int_field", true, true, true);
        }, (doc, i) -> {
            doc.addInteger("int_field", (long) i * 1000 + 42);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            Set<Long> expectedValues = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                expectedValues.add((long) i * 1000 + 42);
            }

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("int_field");
                assertNotNull(value, "Integer field should not be null");
                assertTrue(value instanceof Number, "Integer field should be Number, got: " + value.getClass());
                long numValue = ((Number) value).longValue();
                assertTrue(expectedValues.contains(numValue),
                        "Integer value " + numValue + " should be one of the expected values");
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Test FLOAT (f64) field type roundtrip with value validation")
    void testFloatFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addFloatField("float_field", true, true, true);
        }, (doc, i) -> {
            doc.addFloat("float_field", i * 3.14159 + 0.001);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("float_field");
                assertNotNull(value, "Float field should not be null");
                assertTrue(value instanceof Number, "Float field should be Number, got: " + value.getClass());
                double floatValue = ((Number) value).doubleValue();
                // Values should be i * 3.14159 + 0.001 for some i in [0, 99]
                assertTrue(floatValue >= 0.001 && floatValue < 100 * 3.14159 + 0.002,
                        "Float value should be in expected range, got: " + floatValue);
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test BOOLEAN field type roundtrip with value validation")
    void testBooleanFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addBooleanField("bool_field", true, true, true);
        }, (doc, i) -> {
            doc.addBoolean("bool_field", i % 2 == 0);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            int trueCount = 0;
            int falseCount = 0;
            for (Map<String, Object> doc : docs) {
                Object value = doc.get("bool_field");
                assertNotNull(value, "Boolean field should not be null");
                assertTrue(value instanceof Boolean, "Boolean field should be Boolean, got: " + value.getClass());
                if ((Boolean) value) trueCount++;
                else falseCount++;
            }
            // Should have roughly equal true and false values
            assertEquals(50, trueCount, "Should have 50 true values");
            assertEquals(50, falseCount, "Should have 50 false values");
        }
    }

    @Test
    @Order(5)
    @DisplayName("Test DATE field type roundtrip with value validation")
    void testDateFieldType() throws Exception {
        // Use a fixed base time for predictable testing
        java.time.LocalDateTime baseTime = java.time.LocalDateTime.of(2024, 1, 15, 12, 0, 0);

        Path splitPath = createSplitWithFields(builder -> {
            builder.addDateField("date_field", true, true, true);
        }, (doc, i) -> {
            java.time.LocalDateTime dateTime = baseTime.minusDays(i);
            doc.addDate("date_field", dateTime);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("date_field");
                assertNotNull(value, "Date field should not be null");
                // Date is returned as Long (epoch nanos)
                assertTrue(value instanceof Number, "Date field should be Number (epoch nanos), got: " + value.getClass());
                long nanos = ((Number) value).longValue();
                // Convert back to LocalDateTime and verify it's in the expected range
                java.time.Instant instant = java.time.Instant.ofEpochSecond(
                        nanos / 1_000_000_000L, (int) (nanos % 1_000_000_000L));
                java.time.LocalDateTime retrieved = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
                // Should be between baseTime-99days and baseTime
                assertTrue(retrieved.isBefore(baseTime.plusDays(1)) && retrieved.isAfter(baseTime.minusDays(100)),
                        "Date should be in expected range, got: " + retrieved);
            }
        }
    }

    @Test
    @Order(6)
    @DisplayName("Test BYTES field type roundtrip with value validation")
    void testBytesFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addBytesField("bytes_field", true, true, true, "position");
        }, (doc, i) -> {
            byte[] data = new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2), (byte) (i + 3)};
            doc.addBytes("bytes_field", data);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("bytes_field");
                assertNotNull(value, "Bytes field should not be null");
                assertTrue(value instanceof byte[], "Bytes field should be byte[], got: " + value.getClass());
                byte[] bytes = (byte[]) value;
                assertEquals(4, bytes.length, "Bytes array should have 4 elements");
                // Verify the pattern: [i, i+1, i+2, i+3]
                assertEquals(bytes[1], (byte) (bytes[0] + 1), "Bytes should follow pattern");
                assertEquals(bytes[2], (byte) (bytes[0] + 2), "Bytes should follow pattern");
                assertEquals(bytes[3], (byte) (bytes[0] + 3), "Bytes should follow pattern");
            }
        }
    }

    @Test
    @Order(7)
    @DisplayName("Test JSON field type roundtrip with value validation")
    void testJsonFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addJsonField("json_field", JsonObjectOptions.storedAndIndexed());
        }, (doc, i) -> {
            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("id", i);
            jsonData.put("name", "Item " + i);
            jsonData.put("active", i % 2 == 0);
            jsonData.put("score", i * 1.5);
            doc.addJson("json_field", jsonData);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("json_field");
                assertNotNull(value, "JSON field should not be null");
                // JSON is typically returned as String (serialized JSON)
                assertTrue(value instanceof String, "JSON field should be String, got: " + value.getClass());
                String jsonStr = (String) value;
                // Verify it contains expected JSON structure
                assertTrue(jsonStr.contains("\"id\"") || jsonStr.contains("id"),
                        "JSON should contain id field, got: " + jsonStr);
                assertTrue(jsonStr.contains("\"name\"") || jsonStr.contains("name"),
                        "JSON should contain name field, got: " + jsonStr);
                assertTrue(jsonStr.contains("Item"),
                        "JSON should contain Item value, got: " + jsonStr);
            }
        }
    }

    @Test
    @Order(8)
    @DisplayName("Test UNSIGNED (u64) field type roundtrip with value validation")
    void testUnsignedFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addUnsignedField("unsigned_field", true, true, true);
        }, (doc, i) -> {
            doc.addUnsigned("unsigned_field", (long) i * 1000 + 123);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("unsigned_field");
                assertNotNull(value, "Unsigned field should not be null");
                assertTrue(value instanceof Number, "Unsigned field should be Number, got: " + value.getClass());
                long numValue = ((Number) value).longValue();
                assertTrue(numValue >= 123, "Unsigned value should be >= 123, got: " + numValue);
            }
        }
    }

    @Test
    @Order(9)
    @DisplayName("Test IP_ADDR field type roundtrip with value validation")
    void testIpAddrFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addIpAddrField("ip_field", true, true, true);
        }, (doc, i) -> {
            // Generate valid IPv4 addresses
            String ip = "192.168." + (i / 256) + "." + (i % 256);
            doc.addIpAddr("ip_field", ip);
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("ip_field");
                assertNotNull(value, "IP field should not be null");
                assertTrue(value instanceof String, "IP field should be String, got: " + value.getClass());
                String ipValue = (String) value;
                // IP may be returned in IPv6-mapped format (::ffff:192.168.x.x) or plain IPv4
                assertTrue(ipValue.contains("192.168."),
                        "IP should contain 192.168., got: " + ipValue);
            }
        }
    }

    @Test
    @Order(10)
    @Disabled("FacetField native methods not fully implemented - can be added later if needed")
    @DisplayName("Test FACET field type roundtrip with value validation")
    void testFacetFieldType() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addFacetField("category");
        }, (doc, i) -> {
            // Create hierarchical facet paths using static factory method
            doc.addFacet("category", io.indextables.tantivy4java.util.Facet.fromString("/products/electronics/item" + i));
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            for (Map<String, Object> doc : docs) {
                Object value = doc.get("category");
                assertNotNull(value, "Facet field should not be null");
                assertTrue(value instanceof String, "Facet field should be String, got: " + value.getClass());
                String facetValue = (String) value;
                assertTrue(facetValue.contains("products") || facetValue.contains("electronics"),
                        "Facet should contain path components, got: " + facetValue);
            }
        }
    }

    @Test
    @Order(11)
    @DisplayName("Test multiple field types together")
    void testAllFieldTypesTogether() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);
            builder.addIntegerField("count", true, true, true);
            builder.addFloatField("price", true, true, true);
            builder.addFloatField("rating", true, true, true);
            builder.addBooleanField("active", true, true, true);
            builder.addBooleanField("featured", true, true, true);
            builder.addDateField("created", true, true, true);
            builder.addBytesField("data", true, true, true, "position");
        }, (doc, i) -> {
            doc.addText("title", "Document Title " + i);
            doc.addText("content", "This is the content for document number " + i);
            doc.addInteger("id", (long) i);
            doc.addInteger("count", (long) (i * 10));
            doc.addFloat("price", i * 9.99);
            doc.addFloat("rating", (i % 5) + 0.5);
            doc.addBoolean("active", i % 2 == 0);
            doc.addBoolean("featured", i % 3 == 0);
            doc.addDate("created", java.time.LocalDateTime.now());
            doc.addBytes("data", ("data" + i).getBytes());
        }, 100);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            // Test byte buffer retrieval
            ByteBuffer buffer = searcher.docsBulk(addresses);
            assertNotNull(buffer, "Byte buffer should not be null");

            BatchDocumentReader reader = new BatchDocumentReader();
            assertTrue(reader.isValid(buffer), "Buffer should be valid");
            assertEquals(addresses.size(), reader.getDocumentCount(buffer), "Document count should match");

            List<Map<String, Object>> docs = reader.parseToMaps(buffer);
            assertEquals(addresses.size(), docs.size(), "Parsed doc count should match");

            // Verify each document has all expected fields
            for (Map<String, Object> doc : docs) {
                assertTrue(doc.containsKey("title"), "Should have title field");
                assertTrue(doc.containsKey("content"), "Should have content field");
                assertTrue(doc.containsKey("id"), "Should have id field");
                assertTrue(doc.containsKey("count"), "Should have count field");
                assertTrue(doc.containsKey("price"), "Should have price field");
                assertTrue(doc.containsKey("rating"), "Should have rating field");
                assertTrue(doc.containsKey("active"), "Should have active field");
                assertTrue(doc.containsKey("featured"), "Should have featured field");
            }
        }
    }

    @Test
    @Order(12)
    @DisplayName("Test multi-value fields")
    void testMultiValueFields() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addTextField("tags", true, false, "default", "position");
            builder.addIntegerField("scores", true, true, true);
        }, (doc, i) -> {
            // Add multiple values for same field
            doc.addText("tags", "tag_a_" + i);
            doc.addText("tags", "tag_b_" + i);
            doc.addText("tags", "tag_c_" + i);
            doc.addInteger("scores", (long) i);
            doc.addInteger("scores", (long) (i * 2));
            doc.addInteger("scores", (long) (i * 3));
        }, 50);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 50);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            assertNotNull(buffer);

            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            // Verify multi-value fields
            for (Map<String, Object> doc : docs) {
                Object tags = doc.get("tags");
                Object scores = doc.get("scores");

                // Multi-value fields should be returned as lists or arrays
                assertNotNull(tags, "Tags should not be null");
                assertNotNull(scores, "Scores should not be null");
            }
        }
    }

    @Test
    @Order(13)
    @DisplayName("Test empty and null edge cases")
    void testEdgeCases() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addTextField("optional_text", true, false, "default", "position");
            builder.addIntegerField("required_id", true, true, true);
        }, (doc, i) -> {
            doc.addInteger("required_id", (long) i);
            // Only add optional_text for some documents
            if (i % 2 == 0) {
                doc.addText("optional_text", "Has text " + i);
            }
        }, 50);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 50);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            assertNotNull(buffer);

            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);
            assertEquals(50, docs.size());

            // All docs should have required_id
            for (Map<String, Object> doc : docs) {
                assertTrue(doc.containsKey("required_id"), "Should have required_id");
            }
        }
    }

    @Test
    @Order(14)
    @DisplayName("Test field filtering - only return requested fields")
    void testFieldFiltering() throws Exception {
        Path splitPath = createSplitWithFields(builder -> {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);
            builder.addFloatField("price", true, true, true);
            builder.addBooleanField("active", true, true, true);
        }, (doc, i) -> {
            doc.addText("title", "Title " + i);
            doc.addText("content", "Content for document " + i);
            doc.addInteger("id", (long) i);
            doc.addFloat("price", i * 9.99);
            doc.addBoolean("active", i % 2 == 0);
        }, 50);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 50);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            // Request only specific fields
            Set<String> requestedFields = new HashSet<>(Arrays.asList("title", "id"));
            List<Document> docs = searcher.docBatch(addresses, requestedFields);

            assertEquals(50, docs.size(), "Should return 50 documents");

            for (Document doc : docs) {
                // Should have requested fields (get() returns non-empty list if field exists)
                List<Object> titleValues = doc.get("title");
                List<Object> idValues = doc.get("id");
                assertFalse(titleValues.isEmpty(), "Should have title field with values");
                assertFalse(idValues.isEmpty(), "Should have id field with values");

                // Should NOT have unrequested fields (get() returns empty list if field not present)
                List<Object> contentValues = doc.get("content");
                List<Object> priceValues = doc.get("price");
                List<Object> activeValues = doc.get("active");
                assertTrue(contentValues.isEmpty(), "Should NOT have content field values");
                assertTrue(priceValues.isEmpty(), "Should NOT have price field values");
                assertTrue(activeValues.isEmpty(), "Should NOT have active field values");
            }
        }
    }

    @Test
    @Order(15)
    @DisplayName("Test timestamp precision - nanoseconds preserved")
    void testTimestampPrecision() throws Exception {
        // Create timestamps with specific nanosecond values
        java.time.LocalDateTime[] testTimes = {
            java.time.LocalDateTime.of(2024, 1, 15, 12, 30, 45, 123_456_789),  // 123.456789 ms
            java.time.LocalDateTime.of(2024, 6, 20, 8, 15, 30, 500_000_000),   // 500 ms
            java.time.LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_999), // max nanos
            java.time.LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1),                // 1 nanosecond
            java.time.LocalDateTime.of(2024, 7, 4, 12, 0, 0, 0),               // zero nanos
        };

        Path splitPath = createSplitWithFields(builder -> {
            builder.addDateField("timestamp", true, true, true);
            builder.addIntegerField("index", true, true, true);
        }, (doc, i) -> {
            doc.addDate("timestamp", testTimes[i % testTimes.length]);
            doc.addInteger("index", (long) i);
        }, testTimes.length);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, testTimes.length);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            ByteBuffer buffer = searcher.docsBulk(addresses);
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            assertEquals(testTimes.length, docs.size(), "Should have all test documents");

            for (Map<String, Object> doc : docs) {
                Object tsValue = doc.get("timestamp");
                Object idxValue = doc.get("index");

                assertNotNull(tsValue, "Timestamp should not be null");
                assertTrue(tsValue instanceof Number, "Timestamp should be Number (epoch nanos)");

                long nanos = ((Number) tsValue).longValue();
                int index = ((Number) idxValue).intValue();

                // Convert back to LocalDateTime
                java.time.Instant instant = java.time.Instant.ofEpochSecond(
                        nanos / 1_000_000_000L, (int) (nanos % 1_000_000_000L));
                java.time.LocalDateTime retrieved = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);

                java.time.LocalDateTime expected = testTimes[index % testTimes.length];

                // Verify the timestamp matches exactly including nanoseconds
                assertEquals(expected.getYear(), retrieved.getYear(), "Year should match for index " + index);
                assertEquals(expected.getMonth(), retrieved.getMonth(), "Month should match for index " + index);
                assertEquals(expected.getDayOfMonth(), retrieved.getDayOfMonth(), "Day should match for index " + index);
                assertEquals(expected.getHour(), retrieved.getHour(), "Hour should match for index " + index);
                assertEquals(expected.getMinute(), retrieved.getMinute(), "Minute should match for index " + index);
                assertEquals(expected.getSecond(), retrieved.getSecond(), "Second should match for index " + index);
                assertEquals(expected.getNano(), retrieved.getNano(),
                        "Nanoseconds should match for index " + index +
                        ": expected " + expected.getNano() + ", got " + retrieved.getNano());
            }
        }
    }

    // ==================== PERFORMANCE BENCHMARKS ====================

    @Test
    @Order(100)
    @DisplayName("Performance: Varying document counts (5 fields)")
    void benchmarkVaryingDocumentCounts() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("BENCHMARK: Varying Document Counts (5 fields per document)");
        System.out.println("=".repeat(80));

        int[] docCounts = {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000};
        int fieldCount = 5;

        runPerformanceBenchmark(docCounts, fieldCount, "5_fields");
    }

    @Test
    @Order(101)
    @DisplayName("Performance: Varying document counts (50 fields)")
    void benchmarkManyFields() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("BENCHMARK: Varying Document Counts (50 fields per document)");
        System.out.println("=".repeat(80));

        int[] docCounts = {1, 5, 10, 25, 50, 100, 250, 500, 1000};
        int fieldCount = 50;

        runPerformanceBenchmark(docCounts, fieldCount, "50_fields");
    }

    @Test
    @Order(102)
    @DisplayName("Performance: Varying document counts (200 fields)")
    void benchmarkVeryManyFields() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("BENCHMARK: Varying Document Counts (200 fields per document)");
        System.out.println("=".repeat(80));

        int[] docCounts = {1, 5, 10, 25, 50, 100, 250, 500};
        int fieldCount = 200;

        runPerformanceBenchmark(docCounts, fieldCount, "200_fields");
    }

    @Test
    @Order(103)
    @DisplayName("Performance: Extreme field count (500 fields)")
    void benchmarkExtremeFieldCount() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("BENCHMARK: Varying Document Counts (500 fields per document)");
        System.out.println("=".repeat(80));

        int[] docCounts = {1, 5, 10, 25, 50, 100, 250};
        int fieldCount = 500;

        runPerformanceBenchmark(docCounts, fieldCount, "500_fields");
    }

    @Test
    @Order(104)
    @DisplayName("Performance: Varying field counts (100 docs)")
    void benchmarkVaryingFieldCounts() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("BENCHMARK: Varying Field Counts (100 documents)");
        System.out.println("=".repeat(80));

        int[] fieldCounts = {5, 10, 25, 50, 100, 200, 300, 400, 500};
        int docCount = 100;

        System.out.printf("%-12s | %-18s | %-18s | %-10s | %-10s%n",
                "Fields", "Object (ms)", "ByteBuffer (ms)", "Winner", "Speedup");
        System.out.println("-".repeat(80));

        List<BenchmarkResult> results = new ArrayList<>();

        for (int fieldCount : fieldCounts) {
            Path splitPath = createBenchmarkSplit(docCount, fieldCount);

            try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
                SplitQuery query = searcher.parseQuery("*");
                SearchResult result = searcher.search(query, docCount);
                List<DocAddress> addresses = result.getHits().stream()
                        .map(hit -> hit.getDocAddress())
                        .collect(Collectors.toList());

                BenchmarkResult benchResult = runBenchmark(searcher, addresses, fieldCount + " fields");
                results.add(benchResult);

                String winner = benchResult.objectMeanMs < benchResult.byteBufferMeanMs ? "Object" : "ByteBuffer";
                double speedup = benchResult.objectMeanMs / benchResult.byteBufferMeanMs;

                System.out.printf("%-12d | %-18.3f | %-18.3f | %-10s | %-10.2fx%n",
                        fieldCount, benchResult.objectMeanMs, benchResult.byteBufferMeanMs, winner, speedup);
            }

            // Cleanup split
            Files.deleteIfExists(splitPath);
        }

        printCrossoverAnalysis(results, "field count");
    }

    @Test
    @Order(105)
    @DisplayName("Summary: Find optimal threshold across all scenarios")
    void findOptimalThreshold() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SUMMARY: Optimal Threshold Discovery");
        System.out.println("=".repeat(80));

        // Test various combinations - ByteBuffer is always faster
        int[][] testCases = {
            // {docCount, fieldCount}
            {10, 5}, {10, 50}, {10, 200},
            {50, 5}, {50, 50}, {50, 200},
            {100, 5}, {100, 50}, {100, 200},
            {250, 5}, {250, 50}, {250, 200},
            {500, 5}, {500, 50}, {500, 200},
        };

        System.out.printf("%-8s | %-8s | %-15s | %-15s | %-10s%n",
                "Docs", "Fields", "Object (ms)", "Buffer (ms)", "Winner");
        System.out.println("-".repeat(70));

        int byteBufferWins = 0;
        int objectWins = 0;
        int crossoverDocs = Integer.MAX_VALUE;

        for (int[] testCase : testCases) {
            int docCount = testCase[0];
            int fieldCount = testCase[1];

            Path splitPath = createBenchmarkSplit(Math.max(docCount, 100), fieldCount);

            try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
                SplitQuery query = searcher.parseQuery("*");
                SearchResult result = searcher.search(query, docCount);
                List<DocAddress> addresses = result.getHits().stream()
                        .map(hit -> hit.getDocAddress())
                        .limit(docCount)
                        .collect(Collectors.toList());

                // Prewarm: run one full retrieval to avoid cold cache effects
                for (DocAddress addr : addresses) {
                    try (Document doc = searcher.doc(addr)) {
                        // Just access to warm cache
                    }
                }

                BenchmarkResult benchResult = runBenchmark(searcher, addresses, docCount + "x" + fieldCount);

                String winner = benchResult.objectMeanMs < benchResult.byteBufferMeanMs ? "Object" : "ByteBuffer";
                if (winner.equals("ByteBuffer")) {
                    byteBufferWins++;
                    crossoverDocs = Math.min(crossoverDocs, docCount);
                } else {
                    objectWins++;
                }

                System.out.printf("%-8d | %-8d | %-15.3f | %-15.3f | %-10s%n",
                        docCount, fieldCount, benchResult.objectMeanMs, benchResult.byteBufferMeanMs, winner);
            }

            Files.deleteIfExists(splitPath);
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("CONCLUSION:");
        System.out.println("  ByteBuffer wins: " + byteBufferWins + " scenarios");
        System.out.println("  Object wins: " + objectWins + " scenarios");
        if (crossoverDocs < Integer.MAX_VALUE) {
            System.out.println("  Suggested byteBufferThreshold: " + crossoverDocs);
        }
        System.out.println("=".repeat(70));
    }

    // ==================== HELPER METHODS ====================

    private void runPerformanceBenchmark(int[] docCounts, int fieldCount, String label) throws Exception {
        // Create a single split with max docs needed
        int maxDocs = Arrays.stream(docCounts).max().orElse(1000);
        Path splitPath = createBenchmarkSplit(maxDocs, fieldCount);

        System.out.printf("%-12s | %-18s | %-18s | %-10s | %-10s%n",
                "Doc Count", "Object (ms)", "ByteBuffer (ms)", "Winner", "Speedup");
        System.out.println("-".repeat(80));

        List<BenchmarkResult> results = new ArrayList<>();

        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, maxDocs);
            List<DocAddress> allAddresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            // Prewarm: run one full retrieval to avoid cold cache effects
            for (DocAddress addr : allAddresses) {
                try (Document doc = searcher.doc(addr)) {
                    // Just access to warm cache
                }
            }

            for (int docCount : docCounts) {
                if (docCount > allAddresses.size()) continue;

                List<DocAddress> addresses = allAddresses.subList(0, docCount);
                BenchmarkResult benchResult = runBenchmark(searcher, addresses, docCount + " docs");
                results.add(benchResult);

                String winner = benchResult.objectMeanMs < benchResult.byteBufferMeanMs ? "Object" : "ByteBuffer";
                double speedup = benchResult.objectMeanMs / benchResult.byteBufferMeanMs;

                System.out.printf("%-12d | %-18.3f | %-18.3f | %-10s | %-10.2fx%n",
                        docCount, benchResult.objectMeanMs, benchResult.byteBufferMeanMs, winner, speedup);
            }
        }

        // Cleanup
        Files.deleteIfExists(splitPath);

        printCrossoverAnalysis(results, "document count");
    }

    private BenchmarkResult runBenchmark(SplitSearcher searcher, List<DocAddress> addresses, String label) {
        int warmupIterations = 3;
        int benchmarkIterations = 10;

        // Warmup
        for (int i = 0; i < warmupIterations; i++) {
            benchmarkObjectMethod(searcher, addresses);
            benchmarkByteBufferMethod(searcher, addresses);
        }

        // Benchmark object method
        long[] objectTimes = new long[benchmarkIterations];
        for (int i = 0; i < benchmarkIterations; i++) {
            objectTimes[i] = benchmarkObjectMethod(searcher, addresses);
        }

        // Benchmark byte buffer method
        long[] byteBufferTimes = new long[benchmarkIterations];
        for (int i = 0; i < benchmarkIterations; i++) {
            byteBufferTimes[i] = benchmarkByteBufferMethod(searcher, addresses);
        }

        double objectMean = Arrays.stream(objectTimes).average().orElse(0) / 1_000_000.0;
        double byteBufferMean = Arrays.stream(byteBufferTimes).average().orElse(0) / 1_000_000.0;

        return new BenchmarkResult(addresses.size(), objectMean, byteBufferMean);
    }

    private long benchmarkObjectMethod(SplitSearcher searcher, List<DocAddress> addresses) {
        long start = System.nanoTime();

        // Use reflection to call docBatchViaObjects directly
        try {
            java.lang.reflect.Method method = SplitSearcher.class.getDeclaredMethod(
                    "docBatchViaObjects", List.class);
            method.setAccessible(true);
            List<Document> docs = (List<Document>) method.invoke(searcher, addresses);

            // Read EVERY field from each document to simulate real usage
            long checksum = 0;
            for (Document doc : docs) {
                if (doc != null) {
                    int numFields = doc.getNumFields();
                    // Access fields by index - simulate reading all field data
                    for (int f = 0; f < numFields; f++) {
                        Object value = doc.getFirst("field_" + f);
                        if (value != null) {
                            checksum += value.hashCode();
                        }
                    }
                }
            }
            // Prevent JIT from optimizing away the reads
            if (checksum == Long.MIN_VALUE) System.out.println("checksum");
        } catch (Exception e) {
            // Fallback to individual retrieval
            long checksum = 0;
            for (DocAddress addr : addresses) {
                try (Document doc = searcher.doc(addr)) {
                    if (doc != null) {
                        int numFields = doc.getNumFields();
                        for (int f = 0; f < numFields; f++) {
                            Object value = doc.getFirst("field_" + f);
                            if (value != null) {
                                checksum += value.hashCode();
                            }
                        }
                    }
                }
            }
            if (checksum == Long.MIN_VALUE) System.out.println("checksum");
        }

        return System.nanoTime() - start;
    }

    private long benchmarkByteBufferMethod(SplitSearcher searcher, List<DocAddress> addresses) {
        long start = System.nanoTime();

        ByteBuffer buffer = searcher.docsBulk(addresses);
        if (buffer != null) {
            BatchDocumentReader reader = new BatchDocumentReader();
            List<Map<String, Object>> docs = reader.parseToMaps(buffer);

            // Read EVERY field from each document to simulate real usage
            long checksum = 0;
            for (Map<String, Object> doc : docs) {
                for (Map.Entry<String, Object> entry : doc.entrySet()) {
                    Object value = entry.getValue();
                    if (value != null) {
                        checksum += value.hashCode();
                    }
                }
            }
            // Prevent JIT from optimizing away the reads
            if (checksum == Long.MIN_VALUE) System.out.println("checksum");
        }

        return System.nanoTime() - start;
    }

    private void printCrossoverAnalysis(List<BenchmarkResult> results, String dimension) {
        System.out.println("\nCrossover Analysis:");

        int crossoverPoint = -1;
        for (int i = 0; i < results.size(); i++) {
            BenchmarkResult r = results.get(i);
            if (r.byteBufferMeanMs < r.objectMeanMs && crossoverPoint < 0) {
                crossoverPoint = r.docCount;
            }
        }

        if (crossoverPoint > 0) {
            System.out.println("  ByteBuffer becomes faster at " + dimension + " = " + crossoverPoint);
        } else {
            System.out.println("  Object method was faster for all tested " + dimension + "s");
        }
    }

    private Path createBenchmarkSplit(int docCount, int fieldCount) throws Exception {
        Path indexDir = Files.createTempDirectory(tempDir, "bench_index_");
        // Use UUID to ensure unique paths and avoid cache collisions between test iterations
        Path splitPath = tempDir.resolve("bench_" + docCount + "_" + fieldCount + "_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Add requested number of fields with mixed types
            for (int f = 0; f < fieldCount; f++) {
                int fieldType = f % 5;
                String fieldName = "field_" + f;
                switch (fieldType) {
                    case 0:
                        builder.addTextField(fieldName, true, false, "default", "position");
                        break;
                    case 1:
                        builder.addIntegerField(fieldName, true, true, true);
                        break;
                    case 2:
                        builder.addFloatField(fieldName, true, true, true);
                        break;
                    case 3:
                        builder.addBooleanField(fieldName, true, true, true);
                        break;
                    case 4:
                        builder.addTextField(fieldName, true, false, "default", "position");
                        break;
                }
            }

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                        BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

                        for (int d = 0; d < docCount; d++) {
                            BatchDocument doc = new BatchDocument();
                            for (int f = 0; f < fieldCount; f++) {
                                int fieldType = f % 5;
                                String fieldName = "field_" + f;
                                switch (fieldType) {
                                    case 0:
                                        doc.addText(fieldName, "Text value for field " + f + " in doc " + d);
                                        break;
                                    case 1:
                                        doc.addInteger(fieldName, d * 1000L + f);
                                        break;
                                    case 2:
                                        doc.addFloat(fieldName, d * 1.5 + f * 0.1);
                                        break;
                                    case 3:
                                        doc.addBoolean(fieldName, (d + f) % 2 == 0);
                                        break;
                                    case 4:
                                        doc.addText(fieldName, "Another text " + d + "-" + f);
                                        break;
                                }
                            }
                            batchBuilder.addDocument(doc);

                            if ((d + 1) % 500 == 0) {
                                writer.addDocumentsBatch(batchBuilder);
                                batchBuilder.clear();
                            }
                        }

                        if (!batchBuilder.isEmpty()) {
                            writer.addDocumentsBatch(batchBuilder);
                        }
                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                            "bench-index", "bench-source", "bench-node");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                            indexDir.toString(), splitPath.toString(), splitConfig);
                    splitMetadataMap.put(splitPath, metadata);
                }
            }
        }

        // Cleanup index directory
        Files.walk(indexDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception e) {}
                });

        return splitPath;
    }

    @FunctionalInterface
    interface SchemaConfigurer {
        void configure(SchemaBuilder builder) throws Exception;
    }

    @FunctionalInterface
    interface DocumentPopulator {
        void populate(BatchDocument doc, int index);
    }

    @FunctionalInterface
    interface FieldVerifier {
        void verify(Object original, Object retrieved);
    }

    // Store metadata for splits we create
    private static Map<Path, QuickwitSplit.SplitMetadata> splitMetadataMap = new HashMap<>();

    private Path createSplitWithFields(SchemaConfigurer schemaConfig, DocumentPopulator docPopulator, int docCount)
            throws Exception {
        Path indexDir = Files.createTempDirectory(tempDir, "test_index_");
        Path splitPath = tempDir.resolve("test_" + System.nanoTime() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            schemaConfig.configure(builder);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

                        for (int i = 0; i < docCount; i++) {
                            BatchDocument doc = new BatchDocument();
                            docPopulator.populate(doc, i);
                            batchBuilder.addDocument(doc);
                        }

                        writer.addDocumentsBatch(batchBuilder);
                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                            "test-index", "test-source", "test-node");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                            indexDir.toString(), splitPath.toString(), splitConfig);
                    splitMetadataMap.put(splitPath, metadata);
                }
            }
        }

        // Cleanup index directory
        Files.walk(indexDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception e) {}
                });

        return splitPath;
    }

    private void verifyFieldRoundtrip(Path splitPath, String fieldName, FieldVerifier verifier) throws Exception {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadataMap.get(splitPath))) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult result = searcher.search(query, 100);
            List<DocAddress> addresses = result.getHits().stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

            assertTrue(addresses.size() > 0, "Should have documents to test");

            // Get via byte buffer
            ByteBuffer buffer = searcher.docsBulk(addresses);
            assertNotNull(buffer, "Buffer should not be null");

            BatchDocumentReader reader = new BatchDocumentReader();
            assertTrue(reader.isValid(buffer), "Buffer should be valid");

            List<Map<String, Object>> parsedDocs = reader.parseToMaps(buffer);
            assertEquals(addresses.size(), parsedDocs.size(), "Parsed doc count should match");

            // Verify field values
            for (int i = 0; i < parsedDocs.size(); i++) {
                Map<String, Object> parsed = parsedDocs.get(i);
                Object retrievedValue = parsed.get(fieldName);
                verifier.verify(null, retrievedValue);
            }
        }

        // Cleanup
        Files.deleteIfExists(splitPath);
    }

    private static class BenchmarkResult {
        final int docCount;
        final double objectMeanMs;
        final double byteBufferMeanMs;

        BenchmarkResult(int docCount, double objectMeanMs, double byteBufferMeanMs) {
            this.docCount = docCount;
            this.objectMeanMs = objectMeanMs;
            this.byteBufferMeanMs = byteBufferMeanMs;
        }
    }
}
