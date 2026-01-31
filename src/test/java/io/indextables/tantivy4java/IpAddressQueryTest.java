package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Comprehensive test for IP Address query support in Tantivy4Java.
 * Tests term queries, range queries, and error handling for IP address fields.
 */
public class IpAddressQueryTest {

    @Test
    @DisplayName("IPv4 exact term query")
    public void testIpv4ExactTermQuery() {
        System.out.println("=== IPv4 Exact Term Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with IPv4 addresses
                        addDocumentWithIp(writer, "Server 1", "192.168.1.1", 1);
                        addDocumentWithIp(writer, "Server 2", "192.168.1.2", 2);
                        addDocumentWithIp(writer, "Server 3", "10.0.0.1", 3);
                        addDocumentWithIp(writer, "Server 4", "127.0.0.1", 4);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Test exact match for 192.168.1.1
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.1")) {
                            System.out.println("Query: " + query.toString());
                            try (SearchResult result = searcher.search(query, 10)) {
                                var hits = result.getHits();
                                assertEquals(1, hits.size(), "Should find exactly one document with 192.168.1.1");

                                try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                    assertEquals("Server 1", doc.getFirst("title"));
                                    System.out.println("Found: " + doc.getFirst("title") + " at " + doc.getFirst("ip_addr"));
                                }
                            }
                        }

                        // Test exact match for localhost
                        try (Query query = Query.termQuery(schema, "ip_addr", "127.0.0.1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                var hits = result.getHits();
                                assertEquals(1, hits.size(), "Should find exactly one document with 127.0.0.1");

                                try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                    assertEquals("Server 4", doc.getFirst("title"));
                                    System.out.println("Found localhost: " + doc.getFirst("title"));
                                }
                            }
                        }

                        // Test non-existent IP
                        try (Query query = Query.termQuery(schema, "ip_addr", "8.8.8.8")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                var hits = result.getHits();
                                assertEquals(0, hits.size(), "Should not find any document with 8.8.8.8");
                                System.out.println("Correctly found 0 results for non-existent IP");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 exact term query test PASSED\n");
    }

    @Test
    @DisplayName("IPv6 exact term query")
    public void testIpv6ExactTermQuery() {
        System.out.println("=== IPv6 Exact Term Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with IPv6 addresses
                        addDocumentWithIp(writer, "IPv6 Localhost", "::1", 1);
                        addDocumentWithIp(writer, "IPv6 Server 1", "2001:db8::1", 2);
                        addDocumentWithIp(writer, "IPv6 Server 2", "fe80::1", 3);
                        addDocumentWithIp(writer, "IPv6 Full", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", 4);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Test IPv6 localhost
                        try (Query query = Query.termQuery(schema, "ip_addr", "::1")) {
                            System.out.println("Query for ::1: " + query.toString());
                            try (SearchResult result = searcher.search(query, 10)) {
                                var hits = result.getHits();
                                assertEquals(1, hits.size(), "Should find exactly one document with ::1");

                                try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                    assertEquals("IPv6 Localhost", doc.getFirst("title"));
                                    System.out.println("Found IPv6 localhost: " + doc.getFirst("ip_addr"));
                                }
                            }
                        }

                        // Test shortened IPv6 address
                        try (Query query = Query.termQuery(schema, "ip_addr", "2001:db8::1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                var hits = result.getHits();
                                assertEquals(1, hits.size(), "Should find exactly one document with 2001:db8::1");
                                System.out.println("Found 2001:db8::1");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv6 exact term query test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 range query")
    public void testIpv4RangeQuery() {
        System.out.println("=== IPv4 Range Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents in 192.168.1.x subnet
                        for (int i = 1; i <= 10; i++) {
                            addDocumentWithIp(writer, "Server " + i, "192.168.1." + i, i);
                        }
                        // Add some documents outside the range
                        addDocumentWithIp(writer, "External 1", "10.0.0.1", 11);
                        addDocumentWithIp(writer, "External 2", "172.16.0.1", 12);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Range query: 192.168.1.3 to 192.168.1.7 (inclusive)
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "192.168.1.3", "192.168.1.7", true, true)) {
                            System.out.println("Range query [192.168.1.3 TO 192.168.1.7]: " + query.toString());
                            try (SearchResult result = searcher.search(query, 20)) {
                                var hits = result.getHits();
                                assertEquals(5, hits.size(), "Should find 5 documents in range [192.168.1.3, 192.168.1.7]");

                                System.out.println("Found " + hits.size() + " documents in range:");
                                for (var hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String ip = (String) doc.getFirst("ip_addr");
                                        System.out.println("  - " + doc.getFirst("title") + " at " + ip);
                                    }
                                }
                            }
                        }

                        // Range query: exclusive bounds
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "192.168.1.3", "192.168.1.7", false, false)) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find 3 documents in range (192.168.1.3, 192.168.1.7) exclusive");
                                System.out.println("Exclusive range found " + hits.size() + " documents");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 range query test PASSED\n");
    }

    @Test
    @DisplayName("IPv6 range query")
    public void testIpv6RangeQuery() {
        System.out.println("=== IPv6 Range Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add IPv6 addresses
                        addDocumentWithIp(writer, "IPv6-1", "2001:db8::1", 1);
                        addDocumentWithIp(writer, "IPv6-2", "2001:db8::2", 2);
                        addDocumentWithIp(writer, "IPv6-3", "2001:db8::3", 3);
                        addDocumentWithIp(writer, "IPv6-4", "2001:db8::10", 4);
                        addDocumentWithIp(writer, "IPv6-5", "2001:db8::ff", 5);
                        addDocumentWithIp(writer, "Outside", "fe80::1", 6);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Range query on IPv6 addresses
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "2001:db8::1", "2001:db8::10", true, true)) {
                            System.out.println("IPv6 range query: " + query.toString());
                            try (SearchResult result = searcher.search(query, 20)) {
                                var hits = result.getHits();
                                assertTrue(hits.size() >= 4, "Should find at least 4 documents in IPv6 range");
                                System.out.println("Found " + hits.size() + " IPv6 addresses in range");

                                for (var hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        System.out.println("  - " + doc.getFirst("ip_addr"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv6 range query test PASSED\n");
    }

    @Test
    @DisplayName("Mixed IPv4/IPv6 in same index")
    public void testMixedIpVersions() {
        System.out.println("=== Mixed IPv4/IPv6 Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position")
                   .addTextField("ip_version", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add both IPv4 and IPv6 addresses
                        try (Document doc = new Document()) {
                            doc.addText("title", "IPv4 Server");
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            doc.addText("ip_version", "v4");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("title", "IPv6 Server");
                            doc.addIpAddr("ip_addr", "2001:db8::1");
                            doc.addText("ip_version", "v6");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("title", "Localhost v4");
                            doc.addIpAddr("ip_addr", "127.0.0.1");
                            doc.addText("ip_version", "v4");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("title", "Localhost v6");
                            doc.addIpAddr("ip_addr", "::1");
                            doc.addText("ip_version", "v6");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(4, searcher.getNumDocs(), "Should have 4 documents");

                        // Query IPv4
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size());
                                System.out.println("Found IPv4 server");
                            }
                        }

                        // Query IPv6
                        try (Query query = Query.termQuery(schema, "ip_addr", "2001:db8::1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size());
                                System.out.println("Found IPv6 server");
                            }
                        }

                        // Query both localhosts
                        try (Query ipv4Query = Query.termQuery(schema, "ip_addr", "127.0.0.1");
                             Query ipv6Query = Query.termQuery(schema, "ip_addr", "::1");
                             Query boolQuery = Query.booleanQuery(List.of(
                                 new Query.OccurQuery(Occur.SHOULD, ipv4Query),
                                 new Query.OccurQuery(Occur.SHOULD, ipv6Query)
                             ))) {
                            try (SearchResult result = searcher.search(boolQuery, 10)) {
                                assertEquals(2, result.getHits().size(), "Should find both localhost versions");
                                System.out.println("Found both localhost versions (v4 and v6)");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Mixed IPv4/IPv6 test PASSED\n");
    }

    @Test
    @DisplayName("Open-ended range queries (lower only, upper only)")
    public void testOpenEndedRangeQueries() {
        System.out.println("=== Open-ended Range Queries Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add a range of IP addresses
                        String[] ips = {
                            "10.0.0.1", "10.0.0.100", "10.0.0.200",
                            "172.16.0.1", "172.16.0.100",
                            "192.168.1.1", "192.168.1.100", "192.168.255.255"
                        };
                        for (int i = 0; i < ips.length; i++) {
                            addDocumentWithIp(writer, "Server " + i, ips[i], i);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Lower bound only: >= 172.16.0.1
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "172.16.0.1", null, true, true)) {
                            System.out.println("Query >= 172.16.0.1: " + query.toString());
                            try (SearchResult result = searcher.search(query, 20)) {
                                var hits = result.getHits();
                                assertTrue(hits.size() >= 5, "Should find IPs >= 172.16.0.1");
                                System.out.println("Found " + hits.size() + " documents >= 172.16.0.1");
                            }
                        }

                        // Upper bound only: <= 172.16.0.1
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                null, "172.16.0.1", true, true)) {
                            System.out.println("Query <= 172.16.0.1: " + query.toString());
                            try (SearchResult result = searcher.search(query, 20)) {
                                var hits = result.getHits();
                                assertTrue(hits.size() >= 4, "Should find IPs <= 172.16.0.1");
                                System.out.println("Found " + hits.size() + " documents <= 172.16.0.1");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Open-ended range queries test PASSED\n");
    }

    @Test
    @DisplayName("Invalid IP format error handling")
    public void testInvalidIpFormatErrorHandling() {
        System.out.println("=== Invalid IP Format Error Handling Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Test invalid IP format in term query
                    Exception termException = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.termQuery(schema, "ip_addr", "not.an.ip.address")) {
                            // Should throw before we get here
                        }
                    });
                    System.out.println("Term query correctly rejected invalid IP: " + termException.getMessage());
                    assertTrue(termException.getMessage().contains("Invalid IP address"),
                        "Error message should mention invalid IP address");

                    // Test invalid IP format in range query
                    Exception rangeException = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "invalid", "192.168.1.1", true, true)) {
                            // Should throw before we get here
                        }
                    });
                    System.out.println("Range query correctly rejected invalid IP: " + rangeException.getMessage());
                    assertTrue(rangeException.getMessage().contains("Invalid IP address"),
                        "Error message should mention invalid IP address");
                }
            }
        }

        System.out.println("Invalid IP format error handling test PASSED\n");
    }

    @Test
    @DisplayName("Special IP addresses (zero, broadcast, loopback)")
    public void testSpecialIpAddresses() {
        System.out.println("=== Special IP Addresses Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("description", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Special IPv4 addresses
                        try (Document doc = new Document()) {
                            doc.addText("description", "Zero Address v4");
                            doc.addIpAddr("ip_addr", "0.0.0.0");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("description", "Broadcast v4");
                            doc.addIpAddr("ip_addr", "255.255.255.255");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("description", "Loopback v4");
                            doc.addIpAddr("ip_addr", "127.0.0.1");
                            writer.addDocument(doc);
                        }

                        // Special IPv6 addresses
                        try (Document doc = new Document()) {
                            doc.addText("description", "Zero Address v6");
                            doc.addIpAddr("ip_addr", "::");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("description", "Loopback v6");
                            doc.addIpAddr("ip_addr", "::1");
                            writer.addDocument(doc);
                        }

                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(5, searcher.getNumDocs(), "Should have 5 special IP documents");

                        // Query each special address
                        String[] specialIps = {"0.0.0.0", "255.255.255.255", "127.0.0.1", "::", "::1"};
                        for (String ip : specialIps) {
                            try (Query query = Query.termQuery(schema, "ip_addr", ip)) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    assertEquals(1, result.getHits().size(),
                                        "Should find document with special IP: " + ip);
                                    System.out.println("Found special IP: " + ip);
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Special IP addresses test PASSED\n");
    }

    @Test
    @DisplayName("IPv4-mapped IPv6 addresses")
    public void testIpv4MappedIpv6() {
        System.out.println("=== IPv4-mapped IPv6 Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add an IPv4 address
                        try (Document doc = new Document()) {
                            doc.addText("title", "IPv4 format");
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Query using IPv4 format
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size());
                                System.out.println("Found via IPv4 format query");
                            }
                        }

                        // Query using IPv4-mapped IPv6 format (::ffff:192.168.1.1)
                        try (Query query = Query.termQuery(schema, "ip_addr", "::ffff:192.168.1.1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size(),
                                    "IPv4-mapped IPv6 format should match same address");
                                System.out.println("Found via IPv4-mapped IPv6 format query");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4-mapped IPv6 test PASSED\n");
    }

    @Test
    @DisplayName("Null value in term query should error")
    public void testNullValueInTermQuery() {
        System.out.println("=== Null Value Term Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Test null value in term query
                    Exception exception = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.termQuery(schema, "ip_addr", null)) {
                            // Should throw before we get here
                        }
                    });
                    System.out.println("Null value correctly rejected: " + exception.getMessage());
                    assertTrue(exception.getMessage().toLowerCase().contains("null"),
                        "Error message should mention null");
                }
            }
        }

        System.out.println("Null value term query test PASSED\n");
    }

    @Test
    @DisplayName("Empty string IP should error")
    public void testEmptyStringIp() {
        System.out.println("=== Empty String IP Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Test empty string in term query
                    Exception exception = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.termQuery(schema, "ip_addr", "")) {
                            // Should throw before we get here
                        }
                    });
                    System.out.println("Empty string correctly rejected: " + exception.getMessage());
                    assertTrue(exception.getMessage().contains("Invalid IP address"),
                        "Error message should mention invalid IP address");
                }
            }
        }

        System.out.println("Empty string IP test PASSED\n");
    }

    @Test
    @DisplayName("IPv6 case insensitivity")
    public void testIpv6CaseInsensitivity() {
        System.out.println("=== IPv6 Case Insensitivity Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Index with lowercase IPv6
                        try (Document doc = new Document()) {
                            doc.addText("title", "Lowercase IPv6");
                            doc.addIpAddr("ip_addr", "2001:db8::1");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Query with uppercase - should match
                        try (Query query = Query.termQuery(schema, "ip_addr", "2001:DB8::1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size(),
                                    "Uppercase IPv6 query should match lowercase indexed address");
                                System.out.println("Uppercase query matched lowercase indexed address");
                            }
                        }

                        // Query with mixed case - should match
                        try (Query query = Query.termQuery(schema, "ip_addr", "2001:Db8::1")) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(1, result.getHits().size(),
                                    "Mixed case IPv6 query should match");
                                System.out.println("Mixed case query matched");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv6 case insensitivity test PASSED\n");
    }

    @Test
    @DisplayName("Leading zeros in IPv4")
    public void testLeadingZerosInIpv4() {
        System.out.println("=== Leading Zeros IPv4 Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Index with standard format
                        try (Document doc = new Document()) {
                            doc.addText("title", "Standard IP");
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Note: Leading zeros behavior depends on the parser
                        // Some parsers treat "010" as octal (8), others as decimal (10)
                        // We test what actually happens
                        try {
                            try (Query query = Query.termQuery(schema, "ip_addr", "192.168.001.001")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    // If it parses successfully and matches, that's fine
                                    if (result.getHits().size() == 1) {
                                        System.out.println("Leading zeros query matched (parser treats as decimal)");
                                    } else {
                                        System.out.println("Leading zeros query did not match (parsed differently)");
                                    }
                                }
                            }
                        } catch (RuntimeException e) {
                            // If it throws, that's also acceptable behavior
                            System.out.println("Leading zeros rejected: " + e.getMessage());
                        }
                    }
                }
            }
        }

        System.out.println("Leading zeros IPv4 test PASSED\n");
    }

    @Test
    @DisplayName("CIDR notation should fail (not supported)")
    public void testCidrNotationNotSupported() {
        System.out.println("=== CIDR Notation Test (Negative) ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // CIDR notation is NOT a valid IP address for term queries
                    Exception exception = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.0/24")) {
                            // Should throw - CIDR is not a single IP
                        }
                    });
                    System.out.println("CIDR notation correctly rejected: " + exception.getMessage());
                    assertTrue(exception.getMessage().contains("Invalid IP address"),
                        "Error message should mention invalid IP address");
                }
            }
        }

        System.out.println("CIDR notation test PASSED\n");
    }

    @Test
    @DisplayName("Boolean MUST_NOT with IP addresses")
    public void testBooleanMustNotWithIp() {
        System.out.println("=== Boolean MUST_NOT IP Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Server A");
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("title", "Server B");
                            doc.addIpAddr("ip_addr", "192.168.1.2");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addText("title", "Server C");
                            doc.addIpAddr("ip_addr", "192.168.1.3");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(3, searcher.getNumDocs());

                        // Find all servers EXCEPT 192.168.1.2
                        try (Query allQuery = Query.allQuery();
                             Query excludeQuery = Query.termQuery(schema, "ip_addr", "192.168.1.2");
                             Query boolQuery = Query.booleanQuery(List.of(
                                 new Query.OccurQuery(Occur.MUST, allQuery),
                                 new Query.OccurQuery(Occur.MUST_NOT, excludeQuery)
                             ))) {
                            try (SearchResult result = searcher.search(boolQuery, 10)) {
                                assertEquals(2, result.getHits().size(),
                                    "Should find 2 servers (excluding 192.168.1.2)");

                                for (var hit : result.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String ip = (String) doc.getFirst("ip_addr");
                                        assertNotEquals("192.168.1.2", ip,
                                            "Should not include excluded IP");
                                        System.out.println("Found: " + doc.getFirst("title") + " at " + ip);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Boolean MUST_NOT IP test PASSED\n");
    }

    @Test
    @DisplayName("Non-indexed IP field term query behavior")
    public void testNonIndexedIpFieldTermQuery() {
        System.out.println("=== Non-indexed IP Field Term Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create IP field with indexed=false
            builder.addIpAddrField("ip_addr", true, false, true);  // stored=true, indexed=false, fast=true

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Term query on non-indexed field throws an error at search time
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.1")) {
                            Exception exception = assertThrows(RuntimeException.class, () -> {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    // Should throw before we get here
                                }
                            });
                            System.out.println("Non-indexed field query correctly failed: " + exception.getMessage());
                            assertTrue(exception.getMessage().contains("not indexed"),
                                "Error message should mention field is not indexed");
                        }
                    }
                }
            }
        }

        System.out.println("Non-indexed IP field term query test PASSED\n");
    }

    @Test
    @DisplayName("Non-fast IP field range query behavior")
    public void testNonFastIpFieldRangeQuery() {
        System.out.println("=== Non-fast IP Field Range Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create IP field with fast=false
            builder.addIpAddrField("ip_addr", true, true, false);  // stored=true, indexed=true, fast=false

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "192.168.1.10");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Range query should still work on non-fast field (uses term dictionary)
                        // but may be slower for large datasets
                        try (Query query = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                "192.168.1.0", "192.168.1.255", true, true)) {
                            try (SearchResult result = searcher.search(query, 10)) {
                                assertEquals(2, result.getHits().size(),
                                    "Range query should work on non-fast field");
                                System.out.println("Non-fast field range query found " +
                                    result.getHits().size() + " results");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Non-fast IP field range query test PASSED\n");
    }

    @Test
    @DisplayName("IP address with whitespace should error")
    public void testIpWithWhitespace() {
        System.out.println("=== IP with Whitespace Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Test IP with leading/trailing whitespace
                    Exception exception = assertThrows(RuntimeException.class, () -> {
                        try (Query query = Query.termQuery(schema, "ip_addr", " 192.168.1.1 ")) {
                            // Should throw - whitespace makes it invalid
                        }
                    });
                    System.out.println("IP with whitespace correctly rejected: " + exception.getMessage());
                }
            }
        }

        System.out.println("IP with whitespace test PASSED\n");
    }

    @Test
    @DisplayName("Complex boolean query with multiple IP conditions")
    public void testComplexBooleanIpQuery() {
        System.out.println("=== Complex Boolean IP Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip_addr", true, true, true)
                   .addTextField("datacenter", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // US-East servers (10.x.x.x)
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "10.0.0.1");
                            doc.addText("datacenter", "us-east");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "10.0.0.2");
                            doc.addText("datacenter", "us-east");
                            writer.addDocument(doc);
                        }
                        // US-West servers (172.x.x.x)
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "172.16.0.1");
                            doc.addText("datacenter", "us-west");
                            writer.addDocument(doc);
                        }
                        // EU servers (192.x.x.x)
                        try (Document doc = new Document()) {
                            doc.addIpAddr("ip_addr", "192.168.1.1");
                            doc.addText("datacenter", "eu-west");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // Find servers in US (10.x OR 172.x range) but exclude 10.0.0.2
                        try (Query range10 = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                     "10.0.0.0", "10.255.255.255", true, true);
                             Query range172 = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                     "172.0.0.0", "172.255.255.255", true, true);
                             Query usRanges = Query.booleanQuery(List.of(
                                 new Query.OccurQuery(Occur.SHOULD, range10),
                                 new Query.OccurQuery(Occur.SHOULD, range172)
                             ));
                             Query excludeIp = Query.termQuery(schema, "ip_addr", "10.0.0.2");
                             Query finalQuery = Query.booleanQuery(List.of(
                                 new Query.OccurQuery(Occur.MUST, usRanges),
                                 new Query.OccurQuery(Occur.MUST_NOT, excludeIp)
                             ))) {

                            try (SearchResult result = searcher.search(finalQuery, 10)) {
                                assertEquals(2, result.getHits().size(),
                                    "Should find 2 US servers (excluding 10.0.0.2)");

                                System.out.println("Found US servers (excluding 10.0.0.2):");
                                for (var hit : result.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        System.out.println("  - " + doc.getFirst("ip_addr") +
                                            " (" + doc.getFirst("datacenter") + ")");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Complex boolean IP query test PASSED\n");
    }

    // Helper method to add a document with IP address
    private void addDocumentWithIp(IndexWriter writer, String title, String ipAddr, int id) {
        try (Document doc = new Document()) {
            doc.addText("title", title);
            doc.addIpAddr("ip_addr", ipAddr);
            doc.addInteger("id", id);
            writer.addDocument(doc);
        }
    }
}
