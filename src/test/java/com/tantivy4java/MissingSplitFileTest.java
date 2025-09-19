package com.tantivy4java;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Test to verify that missing split files result in proper Java exceptions
 * rather than JVM crashes. This validates the native code resilience improvements.
 */
public class MissingSplitFileTest {

    @Test 
    @DisplayName("Test 1: Local missing file should throw exception, not crash JVM")
    void test1_localMissingFile() {
        // Create cache manager for this test
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("missing-file-test")
            .withMaxCacheSize(50_000_000); // 50MB
            
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // Create metadata with footer offsets for the missing file (API requirement)
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
            "missing-split", 1000L, 0L,
            java.time.Instant.now(), java.time.Instant.now(),
            new java.util.HashSet<>(), 0L, 0,
            100L, 200L, 50L, 50L, null,  // Add footer and hotcache offsets
            new java.util.ArrayList<>()  // Skipped splits (empty for test)
        );
        
        // Try to access a split file that doesn't exist locally
        String nonExistentPath = "/tmp/does-not-exist.split";
        
        // This should throw a proper Java exception, not crash the JVM  
        Exception exception = assertThrows(RuntimeException.class, () -> {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(nonExistentPath, metadata)) {
                // Try to perform an operation that requires file access
                SplitTermQuery query = new SplitTermQuery("test_field", "test_value");
                searcher.search(query, 10);  // This should fail because file doesn't exist
            }
        });
        
        // Verify we get a proper exception with reasonable error message
        assertNotNull(exception);
        System.out.println("🔍 DEBUG: Actual exception message: '" + exception.getMessage() + "'");
        System.out.println("🔍 DEBUG: Exception class: " + exception.getClass().getName());
        if (exception.getCause() != null) {
            System.out.println("🔍 DEBUG: Cause message: '" + exception.getCause().getMessage() + "'");
        }
        
        assertTrue(exception.getMessage().contains("does-not-exist") || 
                  exception.getMessage().contains("not found") ||
                  exception.getMessage().contains("No such file") ||
                  exception.getMessage().contains("does not exist"));
        
        System.out.println("✅ Local missing file test passed - got expected exception: " + exception.getMessage());
    }

    @Test
    @DisplayName("Test 2: S3 missing file should throw exception, not crash JVM")
    void test2_s3MissingFile() {
        // Create cache manager with S3 credentials
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("s3-missing-file-test")
            .withMaxCacheSize(50_000_000) // 50MB
            .withAwsCredentials("test-access-key", "test-secret-key")
            .withAwsRegion("us-east-1");
            
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // Create metadata with footer offsets for the missing file (API requirement)
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
            "missing-s3-split", 1000L, 0L,
            java.time.Instant.now(), java.time.Instant.now(),
            new java.util.HashSet<>(), 0L, 0,
            100L, 200L, 50L, 50L, null,  // Add footer and hotcache offsets
            new java.util.ArrayList<>()  // Skipped splits (empty for test)
        );
        
        // Try to access a split file that doesn't exist in S3
        String nonExistentS3Path = "s3://tantivy4java-testing/test-splits/does-not-exist.split";
        
        // This should throw a proper Java exception, not crash the JVM
        Exception exception = assertThrows(RuntimeException.class, () -> {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(nonExistentS3Path, metadata)) {
                // Try to perform an operation that requires file access
                SplitTermQuery query = new SplitTermQuery("test_field", "test_value");
                searcher.search(query, 10);  // This should fail because file doesn't exist
            }
        });
        
        // Verify we get a proper exception with reasonable error message
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("does-not-exist") || 
                  exception.getMessage().contains("not found") ||
                  exception.getMessage().contains("404") ||
                  exception.getMessage().contains("NoSuchKey") ||
                  exception.getMessage().contains("does not exist"));
        
        System.out.println("✅ S3 missing file test passed - got expected exception: " + exception.getMessage());
    }

    @Test
    @DisplayName("Test 3: Invalid S3 path should throw exception, not crash JVM")
    void test3_invalidS3Path() {
        // Create cache manager with S3 credentials
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("invalid-s3-path-test")
            .withMaxCacheSize(50_000_000) // 50MB
            .withAwsCredentials("test-access-key", "test-secret-key")
            .withAwsRegion("us-east-1");
            
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // Create metadata with footer offsets for the invalid path (API requirement)
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
            "invalid-s3-split", 1000L, 0L,
            java.time.Instant.now(), java.time.Instant.now(),
            new java.util.HashSet<>(), 0L, 0,
            100L, 200L, 50L, 50L, null,  // Add footer and hotcache offsets
            new java.util.ArrayList<>()  // Skipped splits (empty for test)
        );
        
        // Try to access an invalid S3 path
        String invalidS3Path = "s3://non-existent-bucket-12345/invalid/path.split";
        
        // This should throw a proper Java exception, not crash the JVM
        Exception exception = assertThrows(RuntimeException.class, () -> {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(invalidS3Path, metadata)) {
                // Try to perform an operation that requires file access
                SplitTermQuery query = new SplitTermQuery("test_field", "test_value");
                searcher.search(query, 10);  // This should fail because file doesn't exist
            }
        });
        
        // Verify we get a proper exception with reasonable error message
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("non-existent-bucket") || 
                  exception.getMessage().contains("not found") ||
                  exception.getMessage().contains("404") ||
                  exception.getMessage().contains("NoSuchBucket") ||
                  exception.getMessage().contains("does not exist"));
        
        System.out.println("✅ Invalid S3 path test passed - got expected exception: " + exception.getMessage());
    }
}