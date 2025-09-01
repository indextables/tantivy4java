package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test for IP Address field support following Python tantivy library patterns
 */
public class IpAddressFieldTest {
    
    @Test
    @DisplayName("IP Address field creation, indexing, and retrieval")
    public void testIpAddressFieldSupport() {
        System.out.println("=== IP Address Field Support Test ===");
        System.out.println("Testing IP address field creation, indexing, and retrieval");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with IP address field
            builder.addTextField("title", true, false, "default", "position")
                   .addIpAddrField("ip_addr", true, true, true)  // stored, indexed, fast
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Index documents with IP address fields
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Document 1: IPv4 address
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Server 1");
                            doc1.addIpAddr("ip_addr", "10.0.0.1");
                            doc1.addInteger("id", 1);
                            writer.addDocument(doc1);
                        }
                        
                        // Document 2: IPv4 localhost
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Server 2");
                            doc2.addIpAddr("ip_addr", "127.0.0.1");
                            doc2.addInteger("id", 2);
                            writer.addDocument(doc2);
                        }
                        
                        // Document 3: IPv6 address
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Server 3");
                            doc3.addIpAddr("ip_addr", "::1");
                            doc3.addInteger("id", 3);
                            writer.addDocument(doc3);
                        }
                        
                        writer.commit();
                        System.out.println("✓ Indexed 3 documents with IP address fields");
                    }
                    
                    // Reload to see committed documents
                    index.reload();
                    System.out.println("✓ Reloaded index");
                    
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("✓ Created searcher");
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                        
                        // Test basic search functionality with IP address fields
                        System.out.println("\n=== Testing Document Retrieval with IP Address Fields ===");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 documents");
                                
                                for (int i = 0; i < hits.size(); i++) {
                                    SearchResult.Hit hit = hits.get(i);
                                    System.out.println("\n--- Document " + (i+1) + " ---");
                                    System.out.println("Score: " + hit.getScore());
                                    
                                    // Retrieve document with IP address field
                                    try (Document retrievedDoc = searcher.doc(hit.getDocAddress())) {
                                        System.out.println("Document field extraction:");
                                        
                                        // Extract text field
                                        List<Object> titleValues = retrievedDoc.get("title");
                                        assertFalse(titleValues.isEmpty(), "Title field should not be empty");
                                        System.out.println("  📖 Title: " + titleValues.get(0));
                                        
                                        // Extract IP address field
                                        List<Object> ipValues = retrievedDoc.get("ip_addr");
                                        assertFalse(ipValues.isEmpty(), "IP address field should not be empty");
                                        Object ipValue = ipValues.get(0);
                                        assertInstanceOf(String.class, ipValue, "IP address value should be String");
                                        System.out.println("  🌐 IP Address: " + ipValue + " (type: " + ipValue.getClass().getSimpleName() + ")");
                                        
                                        // Validate IP address format
                                        String ipString = ipValue.toString();
                                        assertTrue(isValidIpAddress(ipString), "Should be a valid IP address: " + ipString);
                                        
                                        // Extract integer field
                                        List<Object> idValues = retrievedDoc.get("id");
                                        assertFalse(idValues.isEmpty(), "ID field should not be empty");
                                        System.out.println("  🔢 ID: " + idValues.get(0));
                                    }
                                }
                            }
                        }
                        
                        // Test search by title (to verify basic search still works)
                        System.out.println("\n=== Testing Search Functionality ===");
                        try (Query titleQuery = index.parseQuery("Server", Arrays.asList("title"))) {
                            try (SearchResult result = searcher.search(titleQuery, 5)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 servers");
                                System.out.println("Server search found " + hits.size() + " documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> titles = doc.get("title");
                                        String title = titles.isEmpty() ? "No title" : titles.get(0).toString();
                                        assertTrue(title.contains("Server"), "Title should contain 'Server'");
                                        
                                        List<Object> ipAddrs = doc.get("ip_addr");
                                        String ipAddr = ipAddrs.isEmpty() ? "No IP" : ipAddrs.get(0).toString();
                                        
                                        System.out.println("  🖥️  Found: \"" + title + "\" at " + ipAddr + 
                                                         " (score: " + String.format("%.3f", hit.getScore()) + ")");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 IP Address Field Test COMPLETED!");
            System.out.println("\n✅ Successfully Demonstrated:");
            System.out.println("  🌐 IP address field creation in schema (addIpAddrField)");
            System.out.println("  📝 IP address value addition to documents (addIpAddr)");
            System.out.println("  🔍 Document indexing and retrieval with IP address fields");
            System.out.println("  🏷️  IP address field value extraction as String objects");
            System.out.println("  📊 Support for both IPv4 and IPv6 addresses");
            System.out.println("  🔧 Python tantivy library compatibility for IP address fields");
            
        } catch (Exception e) {
            fail("IP address field test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("IP Address field validation and edge cases")
    public void testIpAddressValidation() {
        System.out.println("\n=== IP Address Validation Test ===");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("ip", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Test various valid IP address formats
                        String[] validIps = {
                            "0.0.0.0",           // Network address
                            "255.255.255.255",   // Broadcast address
                            "192.168.1.1",       // Private IPv4
                            "::1",               // IPv6 localhost
                            "2001:db8::1",       // IPv6 address
                            "fe80::1%lo0"        // IPv6 link-local with zone (if supported)
                        };
                        
                        for (int i = 0; i < validIps.length - 1; i++) { // Skip the zone ID one for now
                            try (Document doc = new Document()) {
                                doc.addIpAddr("ip", validIps[i]);
                                writer.addDocument(doc);
                            }
                        }
                        
                        writer.commit();
                        System.out.println("✓ Indexed 5 documents with valid IP addresses");
                    }
                    
                    index.reload();
                    
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(5, searcher.getNumDocs(), "Should have documents with valid IPs");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(5, hits.size(), "Should find all documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> ipValues = doc.get("ip");
                                        assertFalse(ipValues.isEmpty(), "IP field should not be empty");
                                        String ipString = ipValues.get(0).toString();
                                        assertTrue(isValidIpAddress(ipString), "Should be valid IP: " + ipString);
                                        System.out.println("Valid IP address: " + ipString);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("✅ IP address validation test passed");
            
        } catch (Exception e) {
            fail("IP address validation test failed: " + e.getMessage());
        }
    }
    
    /**
     * Simple IP address validation helper
     */
    private boolean isValidIpAddress(String ip) {
        if (ip == null || ip.trim().isEmpty()) {
            return false;
        }
        
        try {
            // Try to parse as an IP address
            java.net.InetAddress.getByName(ip);
            return true;
        } catch (java.net.UnknownHostException e) {
            return false;
        }
    }
}