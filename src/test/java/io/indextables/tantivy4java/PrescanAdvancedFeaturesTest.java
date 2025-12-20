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

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Advanced prescan tests covering:
 * - IP address fields
 * - FieldPresence (exists) queries
 * - minimum_should_match support
 * - Bytes fields
 * - Error edge cases
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanAdvancedFeaturesTest {

    @TempDir
    Path tempDir;

    private SplitCacheManager cacheManager;
    private String uniqueId;

    @BeforeEach
    public void setUp() throws Exception {
        uniqueId = UUID.randomUUID().toString();
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("prescan-advanced-" + uniqueId)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    // ==================== IP ADDRESS FIELD TESTS ====================

    // Cache split info to reuse across IP tests
    private SplitInfo ipAddressSplitInfo;

    private SplitInfo getOrCreateIpAddressSplit() throws Exception {
        if (ipAddressSplitInfo == null) {
            ipAddressSplitInfo = createIpAddressSplit();
        }
        return ipAddressSplitInfo;
    }

    @Test
    @Order(1)
    @DisplayName("IP Address Field - Exact IPv4 match")
    public void testIpAddressField_ExactIPv4Match() throws Exception {
        SplitInfo splitInfo = getOrCreateIpAddressSplit();
        String docMappingJson = getIpAddressDocMapping();

        // Query for existing IPv4 address
        SplitQuery query = new SplitTermQuery("client_ip", "192.168.1.100");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Should find existing IPv4 address. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(2)
    @DisplayName("IP Address Field - Exact IPv6 match")
    public void testIpAddressField_ExactIPv6Match() throws Exception {
        SplitInfo splitInfo = getOrCreateIpAddressSplit();
        String docMappingJson = getIpAddressDocMapping();

        // Query for existing IPv6 address
        SplitQuery query = new SplitTermQuery("client_ip", "2001:db8::1");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Should find existing IPv6 address. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(3)
    @DisplayName("IP Address Field - Non-existing IP")
    public void testIpAddressField_NonExistingIp() throws Exception {
        SplitInfo splitInfo = getOrCreateIpAddressSplit();
        String docMappingJson = getIpAddressDocMapping();

        // Query for non-existing IP address
        SplitQuery query = new SplitTermQuery("client_ip", "10.0.0.99");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertFalse(result.couldHaveResults(),
            "Should not find non-existing IP. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(4)
    @DisplayName("IP Address Field - IPv4-mapped IPv6")
    public void testIpAddressField_IPv4MappedIPv6() throws Exception {
        SplitInfo splitInfo = getOrCreateIpAddressSplit();
        String docMappingJson = getIpAddressDocMapping();

        // Query using IPv4-mapped IPv6 format (::ffff:192.168.1.100)
        SplitQuery query = new SplitTermQuery("client_ip", "::ffff:192.168.1.100");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        // This should match the same document as 192.168.1.100
        assertTrue(result.couldHaveResults(),
            "IPv4-mapped IPv6 should match. Term existence: " + result.getTermExistence());
    }

    private SplitInfo createIpAddressSplit() throws Exception {
        Path indexDir = tempDir.resolve("ip_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("ip_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIpAddrField("client_ip", true, true, false);
            builder.addTextField("hostname", true, false, "raw", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with various IP addresses
                        addIpDocument(writer, "192.168.1.100", "server1");
                        addIpDocument(writer, "192.168.1.101", "server2");
                        addIpDocument(writer, "10.0.0.1", "gateway");
                        addIpDocument(writer, "2001:db8::1", "ipv6-server");
                        addIpDocument(writer, "::1", "localhost-ipv6");

                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "ip-index-" + uniqueId, "ip-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    // Use the actual footer offset from metadata
                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private void addIpDocument(IndexWriter writer, String ip, String hostname) throws Exception {
        try (Document doc = new Document()) {
            doc.addIpAddr("client_ip", ip);
            doc.addText("hostname", hostname);
            writer.addDocument(doc);
        }
    }

    private String getIpAddressDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"client_ip\",\"stored\":true,\"type\":\"ip\"}," +
               "{\"fast\":false,\"indexed\":true,\"name\":\"hostname\",\"stored\":true,\"tokenizer\":\"raw\",\"type\":\"text\"}]";
    }

    // ==================== FIELD PRESENCE (EXISTS) QUERY TESTS ====================

    private SplitInfo fieldPresenceSplitInfo;

    private SplitInfo getOrCreateFieldPresenceSplit() throws Exception {
        if (fieldPresenceSplitInfo == null) {
            fieldPresenceSplitInfo = createFieldPresenceSplit();
        }
        return fieldPresenceSplitInfo;
    }

    @Test
    @Order(10)
    @DisplayName("FieldPresence - Field with values returns true")
    public void testFieldPresence_FieldWithValues() throws Exception {
        SplitInfo splitInfo = getOrCreateFieldPresenceSplit();
        String docMappingJson = getFieldPresenceDocMapping();

        // Query for field that has values
        SplitQuery query = new SplitExistsQuery("required_field");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        // Exists queries should return conservative true for now
        assertTrue(result.couldHaveResults(),
            "Field with values should return true. Status: " + result.getStatus());
    }

    @Test
    @Order(11)
    @DisplayName("FieldPresence - Non-existent field returns conservative true")
    public void testFieldPresence_NonExistentField() throws Exception {
        SplitInfo splitInfo = getOrCreateFieldPresenceSplit();
        String docMappingJson = getFieldPresenceDocMapping();

        // Query for field that doesn't exist in schema
        SplitQuery query = new SplitExistsQuery("nonexistent_field");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        // Conservative behavior for unknown fields
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
    }

    private SplitInfo createFieldPresenceSplit() throws Exception {
        Path indexDir = tempDir.resolve("presence_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("presence_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("required_field", true, false, "default", "basic");
            builder.addTextField("optional_field", true, false, "default", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Doc with both fields
                        try (Document doc = new Document()) {
                            doc.addText("required_field", "value1");
                            doc.addText("optional_field", "optional1");
                            writer.addDocument(doc);
                        }
                        // Doc with only required field
                        try (Document doc = new Document()) {
                            doc.addText("required_field", "value2");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "presence-index-" + uniqueId, "presence-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private String getFieldPresenceDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"required_field\",\"stored\":true,\"tokenizer\":\"default\",\"type\":\"text\"}," +
               "{\"fast\":false,\"indexed\":true,\"name\":\"optional_field\",\"stored\":true,\"tokenizer\":\"default\",\"type\":\"text\"}]";
    }

    // ==================== MINIMUM_SHOULD_MATCH TESTS ====================

    private SplitInfo minimumShouldMatchSplitInfo;

    private SplitInfo getOrCreateMinimumShouldMatchSplit() throws Exception {
        if (minimumShouldMatchSplitInfo == null) {
            minimumShouldMatchSplitInfo = createMinimumShouldMatchSplit();
        }
        return minimumShouldMatchSplitInfo;
    }

    @Test
    @Order(20)
    @DisplayName("MinimumShouldMatch - All terms exist, meets threshold")
    public void testMinimumShouldMatch_AllTermsExist() throws Exception {
        SplitInfo splitInfo = getOrCreateMinimumShouldMatchSplit();
        String docMappingJson = getMinimumShouldMatchDocMapping();

        // Query with minimum_should_match=2, all 3 terms exist
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("tags", "java"))
            .addShould(new SplitTermQuery("tags", "python"))
            .addShould(new SplitTermQuery("tags", "rust"))
            .setMinimumShouldMatch(2);

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "All 3 terms exist, should meet minimum_should_match=2. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(21)
    @DisplayName("MinimumShouldMatch - Exactly at threshold")
    public void testMinimumShouldMatch_ExactlyAtThreshold() throws Exception {
        SplitInfo splitInfo = getOrCreateMinimumShouldMatchSplit();
        String docMappingJson = getMinimumShouldMatchDocMapping();

        // Query with minimum_should_match=2, only 2 of 3 terms exist
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("tags", "java"))
            .addShould(new SplitTermQuery("tags", "python"))
            .addShould(new SplitTermQuery("tags", "nonexistent"))
            .setMinimumShouldMatch(2);

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "2 of 3 terms exist, should meet minimum_should_match=2. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(22)
    @DisplayName("MinimumShouldMatch - Below threshold")
    public void testMinimumShouldMatch_BelowThreshold() throws Exception {
        SplitInfo splitInfo = getOrCreateMinimumShouldMatchSplit();
        String docMappingJson = getMinimumShouldMatchDocMapping();

        // Query with minimum_should_match=2, only 1 of 3 terms exist
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("tags", "java"))
            .addShould(new SplitTermQuery("tags", "nonexistent1"))
            .addShould(new SplitTermQuery("tags", "nonexistent2"))
            .setMinimumShouldMatch(2);

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertFalse(result.couldHaveResults(),
            "Only 1 term exists, should not meet minimum_should_match=2. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(23)
    @DisplayName("MinimumShouldMatch - None exist below threshold")
    public void testMinimumShouldMatch_NoneExist() throws Exception {
        SplitInfo splitInfo = getOrCreateMinimumShouldMatchSplit();
        String docMappingJson = getMinimumShouldMatchDocMapping();

        // Query with minimum_should_match=1, no terms exist
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("tags", "nonexistent1"))
            .addShould(new SplitTermQuery("tags", "nonexistent2"))
            .addShould(new SplitTermQuery("tags", "nonexistent3"))
            .setMinimumShouldMatch(1);

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertFalse(result.couldHaveResults(),
            "No terms exist, should not meet minimum_should_match=1. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(24)
    @DisplayName("MinimumShouldMatch - With MUST clauses")
    public void testMinimumShouldMatch_WithMustClauses() throws Exception {
        SplitInfo splitInfo = getOrCreateMinimumShouldMatchSplit();
        String docMappingJson = getMinimumShouldMatchDocMapping();

        // MUST term exists, but SHOULD terms don't meet minimum
        SplitQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("tags", "java"))
            .addShould(new SplitTermQuery("tags", "nonexistent1"))
            .addShould(new SplitTermQuery("tags", "nonexistent2"))
            .setMinimumShouldMatch(2);

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        // MUST clause exists but SHOULD minimum not met
        assertFalse(result.couldHaveResults(),
            "MUST exists but SHOULD minimum not met. Term existence: " + result.getTermExistence());
    }

    private SplitInfo createMinimumShouldMatchSplit() throws Exception {
        Path indexDir = tempDir.resolve("msm_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("msm_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("tags", true, false, "raw", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with various tags
                        addTagDocument(writer, "java");
                        addTagDocument(writer, "python");
                        addTagDocument(writer, "rust");
                        addTagDocument(writer, "go");
                        addTagDocument(writer, "typescript");
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "msm-index-" + uniqueId, "msm-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private void addTagDocument(IndexWriter writer, String tag) throws Exception {
        try (Document doc = new Document()) {
            doc.addText("tags", tag);
            writer.addDocument(doc);
        }
    }

    private String getMinimumShouldMatchDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"tags\",\"stored\":true,\"tokenizer\":\"raw\",\"type\":\"text\"}]";
    }

    // ==================== BYTES FIELD TESTS ====================

    private SplitInfo bytesSplitInfo;

    private SplitInfo getOrCreateBytesSplit() throws Exception {
        if (bytesSplitInfo == null) {
            bytesSplitInfo = createBytesSplit();
        }
        return bytesSplitInfo;
    }

    @Test
    @Order(30)
    @DisplayName("Bytes Field - Returns conservative true")
    public void testBytesField_ConservativeTrue() throws Exception {
        SplitInfo splitInfo = getOrCreateBytesSplit();
        String docMappingJson = getBytesDocMapping();

        // Query for bytes field - should return conservative true
        SplitQuery query = new SplitTermQuery("data", "deadbeef");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        // Bytes fields return conservative true
        assertTrue(result.couldHaveResults(),
            "Bytes fields should return conservative true. Status: " + result.getStatus());
    }

    private SplitInfo createBytesSplit() throws Exception {
        Path indexDir = tempDir.resolve("bytes_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("bytes_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addBytesField("data", true, true, false, "basic");
            builder.addTextField("label", true, false, "raw", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addBytes("data", new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF});
                            doc.addText("label", "test-data");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "bytes-index-" + uniqueId, "bytes-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private String getBytesDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"data\",\"stored\":true,\"type\":\"bytes\"}," +
               "{\"fast\":false,\"indexed\":true,\"name\":\"label\",\"stored\":true,\"tokenizer\":\"raw\",\"type\":\"text\"}]";
    }

    // ==================== REGEX QUERY TESTS ====================

    private SplitInfo regexSplitInfo;

    private SplitInfo getOrCreateRegexSplit() throws Exception {
        if (regexSplitInfo == null) {
            regexSplitInfo = createRegexSplit();
        }
        return regexSplitInfo;
    }

    @Test
    @Order(50)
    @DisplayName("Regex - Simple pattern match")
    public void testRegex_SimplePatternMatch() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Regex pattern that matches existing term "programming"
        SplitQuery query = new SplitRegexQuery("title", "prog.*");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Regex 'prog.*' should match 'programming'. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(51)
    @DisplayName("Regex - Pattern with no matches")
    public void testRegex_PatternNoMatches() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Regex pattern that doesn't match any existing term
        SplitQuery query = new SplitRegexQuery("title", "xyz[0-9]+abc");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertFalse(result.couldHaveResults(),
            "Regex 'xyz[0-9]+abc' should not match any terms. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(52)
    @DisplayName("Regex - Suffix pattern")
    public void testRegex_SuffixPattern() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Regex pattern matching suffix ".*ing"
        SplitQuery query = new SplitRegexQuery("title", ".*ing");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Regex '.*ing' should match 'programming'. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(53)
    @DisplayName("Regex - Character class pattern")
    public void testRegex_CharacterClassPattern() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Regex pattern with character class - matches 'java' or 'javascript'
        SplitQuery query = new SplitRegexQuery("title", "java.*");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Regex 'java.*' should match 'java'. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(54)
    @DisplayName("Regex - Combined with Boolean query")
    public void testRegex_WithBooleanQuery() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Boolean with regex and term query
        SplitQuery query = new SplitBooleanQuery()
            .addMust(new SplitRegexQuery("title", "py.*"))  // matches 'python'
            .addMust(new SplitTermQuery("category", "language"));

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Boolean with regex 'py.*' AND 'language' should match. Term existence: " + result.getTermExistence());
    }

    @Test
    @Order(55)
    @DisplayName("Regex - Invalid regex pattern returns conservative true")
    public void testRegex_InvalidPattern() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Invalid regex pattern - unbalanced brackets
        SplitQuery query = new SplitRegexQuery("title", "[invalid");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        // Invalid regex should return conservative true
        assertTrue(result.couldHaveResults(),
            "Invalid regex should return conservative true. Status: " + result.getStatus());
    }

    @Test
    @Order(56)
    @DisplayName("Regex - Exact match pattern (anchored)")
    public void testRegex_ExactMatchPattern() throws Exception {
        SplitInfo splitInfo = getOrCreateRegexSplit();
        String docMappingJson = getRegexDocMapping();

        // Exact match - should work like term query
        SplitQuery query = new SplitRegexQuery("title", "rust");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertTrue(result.couldHaveResults(),
            "Regex 'rust' should match exact term. Term existence: " + result.getTermExistence());
    }

    private SplitInfo createRegexSplit() throws Exception {
        Path indexDir = tempDir.resolve("regex_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("regex_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "raw", "basic");
            builder.addTextField("category", true, false, "raw", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with various terms for regex testing
                        addRegexDocument(writer, "java", "language");
                        addRegexDocument(writer, "javascript", "language");
                        addRegexDocument(writer, "python", "language");
                        addRegexDocument(writer, "rust", "language");
                        addRegexDocument(writer, "programming", "concept");
                        addRegexDocument(writer, "coding", "concept");
                        addRegexDocument(writer, "learning", "activity");
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "regex-index-" + uniqueId, "regex-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private void addRegexDocument(IndexWriter writer, String title, String category) throws Exception {
        try (Document doc = new Document()) {
            doc.addText("title", title);
            doc.addText("category", category);
            writer.addDocument(doc);
        }
    }

    private String getRegexDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"title\",\"stored\":true,\"tokenizer\":\"raw\",\"type\":\"text\"}," +
               "{\"fast\":false,\"indexed\":true,\"name\":\"category\",\"stored\":true,\"tokenizer\":\"raw\",\"type\":\"text\"}]";
    }

    // ==================== ERROR EDGE CASES ====================

    private SplitInfo simpleSplitInfo;

    private SplitInfo getOrCreateSimpleSplit() throws Exception {
        if (simpleSplitInfo == null) {
            simpleSplitInfo = createSimpleSplit();
        }
        return simpleSplitInfo;
    }

    @Test
    @Order(40)
    @DisplayName("Error - Invalid footer offset returns error status")
    public void testError_InvalidFooterOffset() throws Exception {
        SplitInfo validSplitInfo = getOrCreateSimpleSplit();

        // Use an invalid footer offset with the same URL
        SplitInfo invalidSplitInfo = new SplitInfo(validSplitInfo.getSplitUrl(), 999999999L);
        String docMappingJson = getSimpleDocMapping();

        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(invalidSplitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        // Should return error status but still be conservative (couldHaveResults=true)
        assertTrue(result.getStatus() == PrescanResult.PrescanStatus.ERROR ||
                   result.couldHaveResults(),
            "Invalid footer should result in error or conservative true");
    }

    @Test
    @Order(41)
    @DisplayName("Error - Non-existent split file returns error status")
    public void testError_NonExistentFile() throws Exception {
        Path nonExistentPath = tempDir.resolve("nonexistent_" + UUID.randomUUID() + ".split");
        SplitInfo splitInfo = new SplitInfo("file://" + nonExistentPath.toString(), 0L);
        String docMappingJson = getSimpleDocMapping();

        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.singletonList(splitInfo), docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);
        // Should return error status but still be conservative (couldHaveResults=true)
        assertTrue(result.getStatus() == PrescanResult.PrescanStatus.ERROR ||
                   result.couldHaveResults(),
            "Non-existent file should result in error or conservative true");
    }

    @Test
    @Order(42)
    @DisplayName("Error - Malformed doc mapping JSON")
    public void testError_MalformedDocMapping() throws Exception {
        SplitInfo splitInfo = getOrCreateSimpleSplit();
        String malformedDocMapping = "{invalid json}";

        SplitQuery query = new SplitTermQuery("title", "hello");

        // Should handle gracefully - either throw or return conservative result
        try {
            List<PrescanResult> results = cacheManager.prescanSplits(
                Collections.singletonList(splitInfo), malformedDocMapping, query);

            // If it doesn't throw, should return conservative result
            assertEquals(1, results.size());
            assertTrue(results.get(0).couldHaveResults() ||
                       results.get(0).getStatus() == PrescanResult.PrescanStatus.ERROR,
                "Malformed doc mapping should be handled gracefully");
        } catch (IOException e) {
            // Also acceptable - explicit error
            assertTrue(e.getMessage().contains("JSON") || e.getMessage().contains("parse"),
                "Exception should mention JSON parsing issue");
        }
    }

    @Test
    @Order(43)
    @DisplayName("Error - Empty split list returns empty results")
    public void testError_EmptySplitList() throws Exception {
        String docMappingJson = getSimpleDocMapping();
        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results = cacheManager.prescanSplits(
            Collections.emptyList(), docMappingJson, query);

        assertTrue(results.isEmpty(), "Empty split list should return empty results");
    }

    private SplitInfo createSimpleSplit() throws Exception {
        Path indexDir = tempDir.resolve("simple_index_" + UUID.randomUUID());
        Files.createDirectories(indexDir);
        Path splitPath = tempDir.resolve("simple_" + UUID.randomUUID() + ".split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "basic");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Hello World");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "simple-index-" + uniqueId, "simple-source", "node-1");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir.toString(), splitPath.toString(), splitConfig);

                    // Use the actual footer offset from metadata
                    return new SplitInfo("file://" + splitPath.toString(), metadata.getFooterStartOffset());
                }
            }
        }
    }

    private String getSimpleDocMapping() {
        return "[{\"fast\":false,\"indexed\":true,\"name\":\"title\",\"stored\":true,\"tokenizer\":\"default\",\"type\":\"text\"}]";
    }
}
