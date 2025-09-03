package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashSet;

/**
 * Test for the defensive offset validation in SplitCacheManager
 */
public class OffsetValidationTest {
    
    @Test
    @DisplayName("Test valid offset validation passes")
    public void testValidOffsets() throws Exception {
        System.out.println("=== Testing Valid Offset Validation ===");
        
        // Create valid metadata
        QuickwitSplit.SplitMetadata validMetadata = new QuickwitSplit.SplitMetadata(
            "valid-split", 100L, 1000000L,
            Instant.now(), Instant.now(), new HashSet<>(),
            1L, 1,
            1000L, 5000L,  // Valid footer range: 1000 to 5000
            500L, 200L,    // Valid hotcache: start=500, length=200
            null           // No doc mapping for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should NOT throw an exception - validation should pass
        try {
            // We expect this to fail at the native layer since we don't have an actual split file,
            // but the Java validation should pass
            cacheManager.createSplitSearcher("/tmp/nonexistent.split", validMetadata);
            fail("Expected native layer error for nonexistent file, but got none");
        } catch (Exception e) {
            // This is expected - we should get a native error about the file not existing
            // But the exception should NOT be about invalid offsets
            assertFalse(e.getMessage().contains("Invalid split metadata"), 
                       "Should not get offset validation error for valid offsets");
            System.out.println("✅ Valid offsets passed validation, got expected native error: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Test invalid footer end offset is caught")
    public void testInvalidFooterEndOffset() {
        System.out.println("=== Testing Invalid Footer End Offset ===");
        
        // Create metadata with invalid footer end offset (0)
        QuickwitSplit.SplitMetadata invalidMetadata = new QuickwitSplit.SplitMetadata(
            "invalid-split", 100L, 1000000L,
            Instant.now(), Instant.now(), new HashSet<>(),
            1L, 1,
            1000L, 0L,     // INVALID: footer end = 0
            500L, 200L,
            null           // No doc mapping for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should throw IllegalArgumentException about invalid footer_end_offset
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            cacheManager.createSplitSearcher("/tmp/test.split", invalidMetadata);
        });
        
        assertTrue(exception.getMessage().contains("footer_end_offset must be > 0"), 
                  "Should get specific error about footer_end_offset");
        assertTrue(exception.getMessage().contains("splitId='invalid-split'"), 
                  "Should include split metadata in error message");
        System.out.println("✅ Caught invalid footer end offset: " + exception.getMessage());
    }
    
    @Test
    @DisplayName("Test invalid footer range is caught")
    public void testInvalidFooterRange() {
        System.out.println("=== Testing Invalid Footer Range ===");
        
        // Create metadata with invalid footer range (end <= start)
        QuickwitSplit.SplitMetadata invalidMetadata = new QuickwitSplit.SplitMetadata(
            "invalid-range-split", 100L, 1000000L,
            Instant.now(), Instant.now(), new HashSet<>(),
            1L, 1,
            5000L, 3000L,  // INVALID: footer start > footer end
            500L, 200L,
            null           // No doc mapping for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should throw IllegalArgumentException about invalid footer range
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            cacheManager.createSplitSearcher("/tmp/test.split", invalidMetadata);
        });
        
        assertTrue(exception.getMessage().contains("footer_end_offset") && 
                  exception.getMessage().contains("must be > footer_start_offset"), 
                  "Should get specific error about footer range");
        assertTrue(exception.getMessage().contains("footerStart=5000, footerEnd=3000"), 
                  "Should include actual offset values in error message");
        System.out.println("✅ Caught invalid footer range: " + exception.getMessage());
    }
    
    @Test
    @DisplayName("Test unreasonably large offsets are caught")
    public void testUnreasonablyLargeOffsets() {
        System.out.println("=== Testing Unreasonably Large Offsets ===");
        
        // Create metadata with unreasonably large footer offset (> 100GB)
        long tooLarge = 200L * 1024 * 1024 * 1024; // 200GB
        QuickwitSplit.SplitMetadata invalidMetadata = new QuickwitSplit.SplitMetadata(
            "huge-split", 100L, 1000000L,
            Instant.now(), Instant.now(), new HashSet<>(),
            1L, 1,
            1000L, tooLarge,  // INVALID: 200GB footer end offset
            500L, 200L,
            null              // No doc mapping for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should throw IllegalArgumentException about unreasonably large offset
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            cacheManager.createSplitSearcher("/tmp/test.split", invalidMetadata);
        });
        
        assertTrue(exception.getMessage().contains("unreasonably large") && 
                  exception.getMessage().contains("100GB"), 
                  "Should get specific error about unreasonably large offset");
        System.out.println("✅ Caught unreasonably large offset: " + exception.getMessage());
    }
    
    @Test
    @DisplayName("Test hotcache overflow is caught")
    public void testHotcacheOverflow() {
        System.out.println("=== Testing Hotcache Overflow ===");
        
        // Create metadata with hotcache that would overflow
        QuickwitSplit.SplitMetadata invalidMetadata = new QuickwitSplit.SplitMetadata(
            "overflow-split", 100L, 1000000L,
            Instant.now(), Instant.now(), new HashSet<>(),
            1L, 1,
            1000L, 5000L,
            Long.MAX_VALUE - 100L, 200L,  // INVALID: would overflow when added
            null                          // No doc mapping for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should throw IllegalArgumentException about overflow
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            cacheManager.createSplitSearcher("/tmp/test.split", invalidMetadata);
        });
        
        // Print the actual message to see what we got
        System.out.println("Actual exception message: " + exception.getMessage());
        
        assertTrue(exception.getMessage().contains("would overflow") || 
                  exception.getMessage().contains("unreasonably large"), 
                  "Should get specific error about overflow or unreasonably large offset");
        System.out.println("✅ Caught hotcache overflow: " + exception.getMessage());
    }
}