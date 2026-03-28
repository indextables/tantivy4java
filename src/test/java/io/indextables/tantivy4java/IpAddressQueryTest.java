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
    @DisplayName("IPv4 CIDR /24 via termQuery returns matching subnet documents")
    public void testIpv4CidrViaTermQuery() {
        System.out.println("=== IPv4 CIDR /24 via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // IPs inside 192.168.1.0/24
                        addDocumentWithIp(writer, "Inside 1", "192.168.1.0", 1);   // network addr — included
                        addDocumentWithIp(writer, "Inside 2", "192.168.1.1", 2);
                        addDocumentWithIp(writer, "Inside 3", "192.168.1.128", 3);
                        addDocumentWithIp(writer, "Inside 4", "192.168.1.255", 4); // broadcast addr — included
                        // IPs outside 192.168.1.0/24
                        addDocumentWithIp(writer, "Outside 1", "192.168.0.255", 5); // just below
                        addDocumentWithIp(writer, "Outside 2", "192.168.2.0", 6);   // just above
                        addDocumentWithIp(writer, "Outside 3", "10.0.0.1", 7);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.0/24")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(4, result.getHits().size(),
                                    "CIDR /24 should match exactly the 4 IPs in 192.168.1.0–255");
                                System.out.println("CIDR /24 found " + result.getHits().size() + " documents");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /24 via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR /32 is equivalent to exact match")
    public void testIpv4CidrSlash32ExactMatch() {
        System.out.println("=== IPv4 CIDR /32 Exact Match Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "Target", "192.168.1.1", 1);
                        addDocumentWithIp(writer, "Other",  "192.168.1.2", 2);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // CIDR /32 must return the same result as exact term query
                        try (Query cidrQuery  = Query.termQuery(schema, "ip_addr", "192.168.1.1/32");
                             Query exactQuery = Query.termQuery(schema, "ip_addr", "192.168.1.1")) {
                            try (SearchResult cidrResult  = searcher.search(cidrQuery,  10);
                                 SearchResult exactResult = searcher.search(exactQuery, 10)) {
                                assertEquals(exactResult.getHits().size(), cidrResult.getHits().size(),
                                    "/32 CIDR must match the same document count as exact term query");
                                assertEquals(1, cidrResult.getHits().size(),
                                    "/32 CIDR should match exactly one document");
                                System.out.println("/32 CIDR matches " + cidrResult.getHits().size() + " doc (same as exact)");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /32 exact match test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR /0 matches all documents (match-all fast path)")
    public void testIpv4CidrSlash0MatchAll() {
        System.out.println("=== IPv4 CIDR /0 Match-All Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "A", "10.0.0.1",    1);
                        addDocumentWithIp(writer, "B", "172.16.0.1",  2);
                        addDocumentWithIp(writer, "C", "192.168.1.1", 3);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "0.0.0.0/0")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "/0 CIDR should match all IP documents");
                                System.out.println("/0 CIDR matched all " + result.getHits().size() + " documents");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /0 match-all test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR non-byte-aligned /25 splits subnet correctly")
    public void testIpv4CidrNonByteAligned() {
        System.out.println("=== IPv4 CIDR Non-Byte-Aligned /25 Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // 192.168.1.0/25 → 192.168.1.0–127
                        addDocumentWithIp(writer, "First half boundary", "192.168.1.0",   1);
                        addDocumentWithIp(writer, "First half mid",      "192.168.1.64",  2);
                        addDocumentWithIp(writer, "First half end",      "192.168.1.127", 3);
                        // Should be excluded (second half)
                        addDocumentWithIp(writer, "Second half start",   "192.168.1.128", 4);
                        addDocumentWithIp(writer, "Second half end",     "192.168.1.255", 5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.0/25")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "/25 should match IPs 0–127, not 128–255");
                                System.out.println("/25 CIDR matched " + result.getHits().size() + " IPs (0–127 range)");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR non-byte-aligned /25 test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 wildcard last-octet via termQuery returns same results as CIDR /24")
    public void testIpv4WildcardViaTermQuery() {
        System.out.println("=== IPv4 Wildcard via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "Inside 1", "192.168.1.0",   1);
                        addDocumentWithIp(writer, "Inside 2", "192.168.1.1",   2);
                        addDocumentWithIp(writer, "Inside 3", "192.168.1.255", 3);
                        addDocumentWithIp(writer, "Outside",  "192.168.2.1",   4);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query wildcardQuery = Query.termQuery(schema, "ip_addr", "192.168.1.*");
                             Query cidrQuery     = Query.termQuery(schema, "ip_addr", "192.168.1.0/24")) {
                            try (SearchResult wildcardResult = searcher.search(wildcardQuery, 20);
                                 SearchResult cidrResult     = searcher.search(cidrQuery, 20)) {
                                assertEquals(cidrResult.getHits().size(), wildcardResult.getHits().size(),
                                    "Wildcard 192.168.1.* must return same count as CIDR 192.168.1.0/24");
                                assertEquals(3, wildcardResult.getHits().size(),
                                    "Wildcard should match the 3 IPs in the subnet");
                                System.out.println("Wildcard and CIDR both matched " + wildcardResult.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 wildcard via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 wildcard *.*.*.*  matches all documents (match-all fast path)")
    public void testIpv4WildcardAllOctets() {
        System.out.println("=== IPv4 Wildcard *.*.*.* Match-All Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "A", "10.0.0.1",    1);
                        addDocumentWithIp(writer, "B", "172.16.0.1",  2);
                        addDocumentWithIp(writer, "C", "192.168.1.1", 3);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "*.*.*.*")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "*.*.*.* should match all IP documents via the match-all fast path");
                                System.out.println("*.*.*.* matched all " + result.getHits().size() + " documents");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 wildcard *.*.*.* match-all test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 wildcard multi-octet 10.0.*.* matches correct subnet")
    public void testIpv4WildcardMultiOctet() {
        System.out.println("=== IPv4 Multi-Octet Wildcard Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "Match 1",  "10.0.0.1",    1);
                        addDocumentWithIp(writer, "Match 2",  "10.0.255.254", 2);
                        addDocumentWithIp(writer, "No match", "10.1.0.1",    3);
                        addDocumentWithIp(writer, "No match", "192.168.1.1", 4);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "10.0.*.*")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "10.0.*.* should match only IPs in 10.0.0.0–10.0.255.255");
                                System.out.println("10.0.*.* matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 multi-octet wildcard test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 non-contiguous wildcard 10.*.1.* is rejected (not silently over-matched)")
    public void testIpv4WildcardMixedPositions() {
        System.out.println("=== IPv4 Non-Contiguous Wildcard Rejection Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                // Non-contiguous wildcard "10.*.1.*" cannot be expressed as a single IP range
                // without producing false positives (e.g. 10.0.2.5 would incorrectly match).
                // The expansion is intentionally rejected so the caller receives an explicit
                // error rather than silently wrong results.
                assertThrows(RuntimeException.class,
                    () -> Query.termQuery(schema, "ip_addr", "10.*.1.*"),
                    "Non-contiguous wildcard 10.*.1.* must be rejected, not silently over-matched");
            }
        }

        System.out.println("IPv4 non-contiguous wildcard rejection test PASSED\n");
    }

    @Test
    @DisplayName("Boundary IPs: network and broadcast addresses are included, neighbors excluded")
    public void testCidrBoundaryIps() {
        System.out.println("=== CIDR Boundary IPs Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "Just below",  "192.168.0.255", 1);
                        addDocumentWithIp(writer, "Network",     "192.168.1.0",   2);  // ← must be included
                        addDocumentWithIp(writer, "Mid",         "192.168.1.128", 3);
                        addDocumentWithIp(writer, "Broadcast",   "192.168.1.255", 4);  // ← must be included
                        addDocumentWithIp(writer, "Just above",  "192.168.2.0",   5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.0/24")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "Should include .1.0 and .1.255 (both inclusive), exclude .0.255 and .2.0");
                                System.out.println("Boundary test matched " + result.getHits().size() + " docs (network, mid, broadcast)");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("CIDR boundary IPs test PASSED\n");
    }

    @Test
    @DisplayName("CIDR result set equals explicit range query result set")
    public void testCidrEquivalentToExplicitRange() {
        System.out.println("=== CIDR Equivalent to Explicit Range Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "A", "192.168.1.0",   1);
                        addDocumentWithIp(writer, "B", "192.168.1.100", 2);
                        addDocumentWithIp(writer, "C", "192.168.1.255", 3);
                        addDocumentWithIp(writer, "D", "192.168.2.0",   4);
                        addDocumentWithIp(writer, "E", "10.0.0.1",      5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query cidrQuery  = Query.termQuery(schema, "ip_addr", "192.168.1.0/24");
                             Query rangeQuery = Query.rangeQuery(schema, "ip_addr", FieldType.IP_ADDR,
                                                 "192.168.1.0", "192.168.1.255", true, true)) {
                            try (SearchResult cidrResult  = searcher.search(cidrQuery,  20);
                                 SearchResult rangeResult = searcher.search(rangeQuery, 20)) {
                                assertEquals(rangeResult.getHits().size(), cidrResult.getHits().size(),
                                    "CIDR /24 must return same count as explicit range [.1.0 TO .1.255]");
                                System.out.println("CIDR and range both returned " + cidrResult.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("CIDR equivalent to explicit range test PASSED\n");
    }

    @Test
    @DisplayName("Boolean OR of CIDR terms returns union of subnets")
    public void testBooleanOrOfCidrTerms() {
        System.out.println("=== Boolean OR of CIDR Terms Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "10 net", "10.0.0.1",      1);
                        addDocumentWithIp(writer, "192 net", "192.168.1.50", 2);
                        addDocumentWithIp(writer, "172 net", "172.16.0.1",   3);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query q1 = Query.termQuery(schema, "ip_addr", "10.0.0.0/8");
                             Query q2 = Query.termQuery(schema, "ip_addr", "192.168.1.0/24");
                             Query boolQuery = Query.booleanQuery(java.util.List.of(
                                 new Query.OccurQuery(Occur.SHOULD, q1),
                                 new Query.OccurQuery(Occur.SHOULD, q2)
                             ))) {
                            try (SearchResult result = searcher.search(boolQuery, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "OR of /8 and /24 CIDR terms should union to 2 matching docs");
                                System.out.println("Boolean OR of CIDR terms matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Boolean OR of CIDR terms test PASSED\n");
    }

    @Test
    @DisplayName("Boolean OR with mixed CIDR term and literal IP")
    public void testMixedCidrAndLiteralInBooleanOr() {
        System.out.println("=== Mixed CIDR and Literal IP in Boolean OR Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "In subnet", "10.0.0.5",    1);
                        addDocumentWithIp(writer, "Exact match", "192.168.1.1", 2);
                        addDocumentWithIp(writer, "No match",   "172.16.0.1",  3);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query cidrQuery  = Query.termQuery(schema, "ip_addr", "10.0.0.0/8");
                             Query exactQuery = Query.termQuery(schema, "ip_addr", "192.168.1.1");
                             Query boolQuery  = Query.booleanQuery(java.util.List.of(
                                 new Query.OccurQuery(Occur.SHOULD, cidrQuery),
                                 new Query.OccurQuery(Occur.SHOULD, exactQuery)
                             ))) {
                            try (SearchResult result = searcher.search(boolQuery, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "OR of CIDR range and exact IP should match both");
                                System.out.println("Mixed CIDR + literal matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Mixed CIDR and literal in Boolean OR test PASSED\n");
    }

    @Test
    @DisplayName("IPv4-mapped CIDR correctness despite ::ffff: internal representation")
    public void testIpv4MappedCidrCorrectness() {
        System.out.println("=== IPv4-Mapped CIDR Correctness Test ===");

        // Tantivy stores IPv4 addresses as IPv4-mapped IPv6 (::ffff:x.x.x.x) internally.
        // This test verifies that CIDR expansion on IPv4 addresses produces correct results
        // even given that internal representation.
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Index as plain IPv4 — Tantivy stores as ::ffff: internally
                        addDocumentWithIp(writer, "IPv4 in subnet",  "192.168.1.50",  1);
                        addDocumentWithIp(writer, "IPv4 in subnet",  "192.168.1.200", 2);
                        addDocumentWithIp(writer, "IPv4 out subnet", "192.168.2.1",   3);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // CIDR expansion must work correctly despite internal ::ffff: representation
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.1.0/24")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "IPv4 CIDR expansion must work correctly given ::ffff: internal storage");
                                System.out.println("IPv4-mapped CIDR matched " + result.getHits().size() + " docs correctly");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4-mapped CIDR correctness test PASSED\n");
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

    @Test
    @DisplayName("parseQuery path: CIDR and wildcard expansion via SplitSearcher.parseQuery()")
    public void testCidrAndWildcardViaParseQuery(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir) throws Exception {
        System.out.println("=== CIDR and Wildcard via parseQuery() Test ===");

        String uniqueId = "ip_parse_query_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        String splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "In /24",       "192.168.1.1",   1);
                        addDocumentWithIp(writer, "In /24",       "192.168.1.100", 2);
                        addDocumentWithIp(writer, "Outside /24",  "192.168.2.1",   3);
                        addDocumentWithIp(writer, "10.x subnet",  "10.0.0.1",      4);
                        writer.commit();
                    }

                    io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitConfig config =
                        new io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitConfig(
                            uniqueId, "test-source", "test-node");
                    io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata metadata =
                        io.indextables.tantivy4java.split.merge.QuickwitSplit.convertIndexFromPath(
                            indexPath, splitPath, config);

                    String cacheName = uniqueId + "-cache";
                    io.indextables.tantivy4java.split.SplitCacheManager.CacheConfig cacheConfig =
                        new io.indextables.tantivy4java.split.SplitCacheManager.CacheConfig(cacheName);

                    try (io.indextables.tantivy4java.split.SplitCacheManager cacheManager =
                             io.indextables.tantivy4java.split.SplitCacheManager.getInstance(cacheConfig);
                         io.indextables.tantivy4java.split.SplitSearcher splitSearcher =
                             cacheManager.createSplitSearcher("file://" + splitPath, metadata)) {

                        // CIDR via parseQuery: ip_addr:192.168.1.0/24
                        io.indextables.tantivy4java.split.SplitQuery cidrQuery =
                            splitSearcher.parseQuery("ip_addr:192.168.1.0/24");
                        try (SearchResult result = splitSearcher.search(cidrQuery, 20)) {
                            assertEquals(2, result.getHits().size(),
                                "parseQuery CIDR '192.168.1.0/24' should match 2 docs in /24 subnet");
                            System.out.println("parseQuery CIDR matched " + result.getHits().size() + " docs");
                        }

                        // Wildcard via parseQuery: ip_addr:192.168.1.*
                        io.indextables.tantivy4java.split.SplitQuery wildcardQuery =
                            splitSearcher.parseQuery("ip_addr:192.168.1.*");
                        try (SearchResult result = splitSearcher.search(wildcardQuery, 20)) {
                            assertEquals(2, result.getHits().size(),
                                "parseQuery wildcard '192.168.1.*' should match same 2 docs as /24 CIDR");
                            System.out.println("parseQuery wildcard matched " + result.getHits().size() + " docs");
                        }

                        // CIDR OR another subnet via parseQuery
                        io.indextables.tantivy4java.split.SplitQuery orQuery =
                            splitSearcher.parseQuery("ip_addr:192.168.1.0/24 OR ip_addr:10.0.0.0/8");
                        try (SearchResult result = splitSearcher.search(orQuery, 20)) {
                            assertEquals(3, result.getHits().size(),
                                "parseQuery OR of two CIDRs should union both subnets");
                            System.out.println("parseQuery OR of CIDRs matched " + result.getHits().size() + " docs");
                        }
                    }
                }
            }
        }

        System.out.println("parseQuery CIDR and wildcard test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR /16 byte-aligned via termQuery")
    public void testIpv4CidrSlash16ViaTermQuery() {
        System.out.println("=== IPv4 CIDR /16 via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Inside 10.0.0.0/16 (10.0.0.0 – 10.0.255.255)
                        addDocumentWithIp(writer, "In /16 A", "10.0.0.1",   1);
                        addDocumentWithIp(writer, "In /16 B", "10.0.255.254", 2);
                        // Outside /16
                        addDocumentWithIp(writer, "Out /16",  "10.1.0.1",   3);
                        addDocumentWithIp(writer, "Out /16",  "192.168.1.1", 4);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "10.0.0.0/16")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "/16 should match exactly the 2 IPs inside 10.0.0.0–10.0.255.255");
                                System.out.println("/16 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /16 via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR non-byte-aligned /17 via termQuery")
    public void testIpv4CidrSlash17ViaTermQuery() {
        System.out.println("=== IPv4 CIDR /17 via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // 172.16.0.0/17 → 172.16.0.0 – 172.16.127.255
                        addDocumentWithIp(writer, "In /17 low",  "172.16.0.1",   1);
                        addDocumentWithIp(writer, "In /17 high", "172.16.127.254", 2);
                        // Second half: 172.16.128.0 – 172.16.255.255 — must be excluded
                        addDocumentWithIp(writer, "Out /17",     "172.16.128.0",  3);
                        addDocumentWithIp(writer, "Out /17",     "172.16.255.255", 4);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "172.16.0.0/17")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "/17 should match 172.16.0.0–127.255, exclude 172.16.128.0–255.255");
                                System.out.println("/17 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /17 via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR non-byte-aligned /23 via termQuery")
    public void testIpv4CidrSlash23ViaTermQuery() {
        System.out.println("=== IPv4 CIDR /23 via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // 192.168.2.0/23 → 192.168.2.0 – 192.168.3.255
                        addDocumentWithIp(writer, "In /23 A", "192.168.2.0",   1);
                        addDocumentWithIp(writer, "In /23 B", "192.168.2.128", 2);
                        addDocumentWithIp(writer, "In /23 C", "192.168.3.255", 3);
                        // Outside — 192.168.1.x and 192.168.4.x must be excluded
                        addDocumentWithIp(writer, "Out low",  "192.168.1.255", 4);
                        addDocumentWithIp(writer, "Out high", "192.168.4.0",   5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "192.168.2.0/23")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "/23 should match 192.168.2.0–192.168.3.255, exclude .1.255 and .4.0");
                                System.out.println("/23 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /23 via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 CIDR non-byte-aligned /27 via termQuery")
    public void testIpv4CidrSlash27ViaTermQuery() {
        System.out.println("=== IPv4 CIDR /27 via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // 10.0.0.0/27 → 10.0.0.0 – 10.0.0.31
                        addDocumentWithIp(writer, "In /27 A", "10.0.0.0",  1);
                        addDocumentWithIp(writer, "In /27 B", "10.0.0.16", 2);
                        addDocumentWithIp(writer, "In /27 C", "10.0.0.31", 3);
                        // 10.0.0.32 starts the next /27 block — must be excluded
                        addDocumentWithIp(writer, "Out",      "10.0.0.32", 4);
                        addDocumentWithIp(writer, "Out",      "10.0.0.255", 5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "10.0.0.0/27")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "/27 should match 10.0.0.0–10.0.0.31, exclude .32 and above");
                                System.out.println("/27 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 CIDR /27 via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv4 wildcard three octets 10.*.*.* matches full 10.x.x.x space")
    public void testIpv4WildcardThreeOctets() {
        System.out.println("=== IPv4 Wildcard Three Octets Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        addDocumentWithIp(writer, "10 A", "10.0.0.1",     1);
                        addDocumentWithIp(writer, "10 B", "10.128.64.32",  2);
                        addDocumentWithIp(writer, "10 C", "10.255.255.254", 3);
                        addDocumentWithIp(writer, "Out",  "11.0.0.1",      4);
                        addDocumentWithIp(writer, "Out",  "192.168.1.1",   5);
                        writer.commit();
                    }
                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "10.*.*.*")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "10.*.*.* should match all 3 IPs in the 10.x.x.x space");
                                System.out.println("10.*.*.* matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv4 wildcard three octets test PASSED\n");
    }

    @Test
    @DisplayName("IPv6 fe80::/10 CIDR (non-byte-aligned link-local range) via termQuery")
    public void testIpv6Fe80CidrViaTermQuery() {
        System.out.println("=== IPv6 fe80::/10 CIDR via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Inside fe80::/10 (link-local: fe80:: – febf:ffff:...)
                        addDocumentWithIp(writer, "Link-local 1", "fe80::1",    1);
                        addDocumentWithIp(writer, "Link-local 2", "fe80::2",    2);
                        addDocumentWithIp(writer, "Link-local 3", "fea0::1",    3);  // also inside /10
                        // Outside fe80::/10
                        addDocumentWithIp(writer, "Outside 1",    "fec0::1",    4);  // fec0 > febf — outside
                        addDocumentWithIp(writer, "Outside 2",    "2001:db8::1",5);
                        addDocumentWithIp(writer, "Outside 3",    "::1",        6);
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        try (Query query = Query.termQuery(schema, "ip_addr", "fe80::/10")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "fe80::/10 should match only the 3 link-local addresses inside the /10 block");
                                System.out.println("fe80::/10 matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv6 fe80::/10 CIDR via termQuery test PASSED\n");
    }

    @Test
    @DisplayName("IPv6 CIDR via termQuery returns correct subnet documents")
    public void testIpv6CidrViaTermQuery() {
        System.out.println("=== IPv6 CIDR via termQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)
                   .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // IPs inside 2001:db8::/32
                        addDocumentWithIp(writer, "db8 network",    "2001:db8::",          1);  // base address — included
                        addDocumentWithIp(writer, "db8 server 1",   "2001:db8::1",         2);
                        addDocumentWithIp(writer, "db8 server 2",   "2001:db8:cafe::1",    3);
                        // IPs outside 2001:db8::/32
                        addDocumentWithIp(writer, "outside 1",      "2001:db9::1",         4);  // different /32 block
                        addDocumentWithIp(writer, "outside 2",      "fe80::1",             5);  // link-local
                        addDocumentWithIp(writer, "outside 3",      "::1",                 6);  // loopback
                        // ::1/128 exact-match CIDR
                        addDocumentWithIp(writer, "loopback exact", "::1",                 7);  // duplicate for count check
                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        // 2001:db8::/32 — matches all addresses in the 2001:db8:0:0::/32 space
                        try (Query query = Query.termQuery(schema, "ip_addr", "2001:db8::/32")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(3, result.getHits().size(),
                                    "2001:db8::/32 should match exactly the 3 IPs in that /32 block");
                                System.out.println("IPv6 /32 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }

                        // ::1/128 — exact-match CIDR; should match both ::1 docs
                        try (Query query = Query.termQuery(schema, "ip_addr", "::1/128")) {
                            try (SearchResult result = searcher.search(query, 20)) {
                                assertEquals(2, result.getHits().size(),
                                    "::1/128 is an exact-match CIDR; should match both ::1 documents");
                                System.out.println("IPv6 /128 CIDR matched " + result.getHits().size() + " docs");
                            }
                        }
                    }
                }
            }
        }

        System.out.println("IPv6 CIDR via termQuery test PASSED\n");
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
