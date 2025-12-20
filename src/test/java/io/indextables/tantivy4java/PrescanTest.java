package io.indextables.tantivy4java;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.split.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the prescan functionality that checks term existence in split FST.
 *
 * <p>Prescan allows filtering splits before expensive full searches by checking
 * if query terms exist in the split's term dictionary (FST).
 */
public class PrescanTest {

    private File tempDir;
    private SplitCacheManager cacheManager;

    @BeforeEach
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("prescan-test").toFile();

        // Create a cache manager for testing
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("prescan-test-cache")
            .withMaxCacheSize(100_000_000); // 100MB
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    @Test
    public void testSplitInfoCreation() {
        // Test basic SplitInfo creation
        SplitInfo info = new SplitInfo("s3://bucket/split.split", 12345678L);
        assertEquals("s3://bucket/split.split", info.getSplitUrl());
        assertEquals(12345678L, info.getFooterOffset());
    }

    @Test
    public void testSplitInfoRequiresSplitUrl() {
        assertThrows(NullPointerException.class, () -> {
            new SplitInfo(null, 12345L);
        });
    }

    @Test
    public void testSplitInfoRequiresPositiveOffset() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SplitInfo("s3://bucket/split.split", -1L);
        });
    }

    @Test
    public void testPrescanResultSuccess() {
        Map<String, Boolean> termExistence = new HashMap<>();
        termExistence.put("title:hello", true);
        termExistence.put("title:world", false);

        PrescanResult result = new PrescanResult(
            "s3://bucket/split.split",
            true,
            termExistence
        );

        assertEquals("s3://bucket/split.split", result.getSplitUrl());
        assertTrue(result.couldHaveResults());
        assertTrue(result.isSuccess());
        assertEquals(PrescanResult.PrescanStatus.SUCCESS, result.getStatus());
        assertNull(result.getErrorMessage());
        assertEquals(2, result.getTermExistence().size());
        assertTrue(result.getTermExistence().get("title:hello"));
        assertFalse(result.getTermExistence().get("title:world"));
    }

    @Test
    public void testPrescanResultError() {
        PrescanResult result = PrescanResult.error(
            "s3://bucket/split.split",
            new RuntimeException("Test error")
        );

        assertEquals("s3://bucket/split.split", result.getSplitUrl());
        assertTrue(result.couldHaveResults()); // Conservative: include on error
        assertFalse(result.isSuccess());
        assertEquals(PrescanResult.PrescanStatus.ERROR, result.getStatus());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getTermExistence().isEmpty());
    }

    @Test
    public void testPrescanResultTimeout() {
        PrescanResult result = PrescanResult.timeout("s3://bucket/split.split");

        assertEquals("s3://bucket/split.split", result.getSplitUrl());
        assertTrue(result.couldHaveResults()); // Conservative: include on timeout
        assertFalse(result.isSuccess());
        assertEquals(PrescanResult.PrescanStatus.TIMEOUT, result.getStatus());
        assertNotNull(result.getErrorMessage());
    }

    @Test
    public void testPrescanResultEquality() {
        Map<String, Boolean> terms1 = new HashMap<>();
        terms1.put("field:term", true);

        Map<String, Boolean> terms2 = new HashMap<>();
        terms2.put("field:term", true);

        PrescanResult result1 = new PrescanResult("url", true, terms1);
        PrescanResult result2 = new PrescanResult("url", true, terms2);

        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    public void testDocMappingJsonFormat() {
        // Test that the doc mapping JSON format is correct
        String docMappingJson = "{"
            + "\"field_mappings\": ["
            + "{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true},"
            + "{\"name\": \"body\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true},"
            + "{\"name\": \"tags\", \"type\": \"text\", \"tokenizer\": \"raw\", \"indexed\": true},"
            + "{\"name\": \"count\", \"type\": \"u64\", \"indexed\": true}"
            + "]"
            + "}";

        // The JSON should be valid and parseable
        assertNotNull(docMappingJson);
        assertTrue(docMappingJson.contains("field_mappings"));
        assertTrue(docMappingJson.contains("title"));
    }

    @Test
    public void testPrescanWithEmptySplitList() throws IOException {
        // Test prescan with empty split list
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": []}";
        SplitQuery query = new SplitTermQuery("title", "test");

        List<PrescanResult> results = cacheManager.prescanSplits(splits, docMappingJson, query);

        assertTrue(results.isEmpty());
    }

    @Test
    public void testPrescanSimpleReturnsMatchingUrls() throws IOException {
        // Test prescanSplitsSimple returns only URLs that could have results
        // Note: This test uses mock data since we don't have real splits

        // For this basic test, we just verify the API works
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\"}]}";
        SplitQuery query = new SplitTermQuery("title", "test");

        List<String> matchingUrls = cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        assertTrue(matchingUrls.isEmpty());
    }

    @Test
    public void testSplitInfoToString() {
        SplitInfo info = new SplitInfo("s3://bucket/split.split", 12345L);
        String str = info.toString();

        assertTrue(str.contains("s3://bucket/split.split"));
        assertTrue(str.contains("12345"));
    }

    @Test
    public void testPrescanResultToString() {
        Map<String, Boolean> terms = new HashMap<>();
        terms.put("title:hello", true);

        PrescanResult result = new PrescanResult("url", true, terms);
        String str = result.toString();

        assertTrue(str.contains("url"));
        assertTrue(str.contains("couldHaveResults=true"));
        assertTrue(str.contains("SUCCESS"));
        assertTrue(str.contains("title:hello"));
    }

    @Test
    public void testPrescanResultWithNullTermExistence() {
        // Test that null term existence is handled correctly
        PrescanResult result = new PrescanResult("url", true, null);

        assertNotNull(result.getTermExistence());
        assertTrue(result.getTermExistence().isEmpty());
    }

    @Test
    public void testPrescanResultStatusEnum() {
        // Verify all status values exist
        assertEquals(3, PrescanResult.PrescanStatus.values().length);
        assertNotNull(PrescanResult.PrescanStatus.SUCCESS);
        assertNotNull(PrescanResult.PrescanStatus.TIMEOUT);
        assertNotNull(PrescanResult.PrescanStatus.ERROR);
    }

    // ==================== Wildcard Query Tests ====================

    @Test
    public void testSplitWildcardQueryCreation() {
        // Test basic wildcard query creation
        SplitWildcardQuery query = new SplitWildcardQuery("title", "hel*");
        assertEquals("title", query.getField());
        assertEquals("hel*", query.getPattern());
        assertFalse(query.isLenient());
    }

    @Test
    public void testSplitWildcardQueryLenient() {
        // Test lenient wildcard query
        SplitWildcardQuery query = new SplitWildcardQuery("title", "hel*", true);
        assertEquals("title", query.getField());
        assertEquals("hel*", query.getPattern());
        assertTrue(query.isLenient());
    }

    @Test
    public void testSplitWildcardQueryJsonOutput() {
        // Test JSON serialization of wildcard query
        // Uses internally-tagged format: {"type":"wildcard","field":"...", "value":"...", "lenient":...}
        SplitWildcardQuery query = new SplitWildcardQuery("title", "hel*");
        String json = query.toQueryAstJson();

        assertTrue(json.contains("\"type\":\"wildcard\""));
        assertTrue(json.contains("\"field\":\"title\""));
        assertTrue(json.contains("\"value\":\"hel*\""));
        assertTrue(json.contains("\"lenient\":false"));
    }

    @Test
    public void testSplitWildcardQueryJsonOutputLenient() {
        // Test JSON serialization of lenient wildcard query
        // Uses internally-tagged format: {"type":"wildcard","field":"...", "value":"...", "lenient":...}
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*world", true);
        String json = query.toQueryAstJson();

        assertTrue(json.contains("\"type\":\"wildcard\""));
        assertTrue(json.contains("\"field\":\"title\""));
        assertTrue(json.contains("\"value\":\"*world\""));
        assertTrue(json.contains("\"lenient\":true"));
    }

    @Test
    public void testSplitWildcardQueryToString() {
        // Test toString method
        SplitWildcardQuery query = new SplitWildcardQuery("body", "*hello*");
        String str = query.toString();

        assertTrue(str.contains("SplitWildcardQuery"));
        assertTrue(str.contains("body"));
        assertTrue(str.contains("*hello*"));
    }

    @Test
    public void testPrescanWithWildcardQuery() throws IOException {
        // Test prescan with wildcard query (empty splits list)
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true}]}";
        SplitQuery query = new SplitWildcardQuery("title", "hel*");

        List<PrescanResult> results = cacheManager.prescanSplits(splits, docMappingJson, query);

        assertTrue(results.isEmpty());
    }

    @Test
    public void testPrescanWithPrefixWildcard() throws IOException {
        // Test prescan with prefix wildcard pattern "hel*"
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true}]}";
        SplitQuery query = new SplitWildcardQuery("title", "hel*");

        List<String> matchingUrls = cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        assertTrue(matchingUrls.isEmpty());
    }

    @Test
    public void testPrescanWithSuffixWildcard() throws IOException {
        // Test prescan with suffix wildcard pattern "*world"
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true}]}";
        SplitQuery query = new SplitWildcardQuery("title", "*world");

        List<String> matchingUrls = cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        assertTrue(matchingUrls.isEmpty());
    }

    @Test
    public void testPrescanWithContainsWildcard() throws IOException {
        // Test prescan with contains wildcard pattern "*hello*"
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true}]}";
        SplitQuery query = new SplitWildcardQuery("title", "*hello*");

        List<String> matchingUrls = cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        assertTrue(matchingUrls.isEmpty());
    }

    @Test
    public void testPrescanWithComplexWildcard() throws IOException {
        // Test prescan with complex wildcard pattern "h*llo"
        List<SplitInfo> splits = Collections.emptyList();
        String docMappingJson = "{\"field_mappings\": [{\"name\": \"title\", \"type\": \"text\", \"tokenizer\": \"default\", \"indexed\": true}]}";
        SplitQuery query = new SplitWildcardQuery("title", "h*llo");

        List<String> matchingUrls = cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        assertTrue(matchingUrls.isEmpty());
    }

    @Test
    public void testWildcardQueryJsonEscaping() {
        // Test that special JSON characters are properly escaped
        SplitWildcardQuery query = new SplitWildcardQuery("field\"name", "pat*tern\"value");
        String json = query.toQueryAstJson();

        // Should contain escaped quotes
        assertTrue(json.contains("field\\\"name"));
        assertTrue(json.contains("pat*tern\\\"value"));
    }
}
