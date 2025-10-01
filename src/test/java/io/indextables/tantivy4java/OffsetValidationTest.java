package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashSet;
import java.util.ArrayList;

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
            null,          // No doc mapping for test
            new ArrayList<>()  // No skipped splits for test
        );
        
        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
        
        // This should NOT throw an exception - validation should pass
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("/tmp/nonexistent.split", validMetadata)) {
            // Validation passed - the SplitSearcher was created successfully
            // The file existence error will only occur when we try to actually use the searcher
            assertNotNull(searcher, "SplitSearcher should be created with valid metadata");
            System.out.println("✅ Valid offsets passed validation and SplitSearcher created successfully");
            
            // Now try to use the searcher - this should fail with file not found
            try {
                SplitQuery query = new SplitTermQuery("test", "value");
                searcher.search(query, 1);
                fail("Expected native layer error when searching nonexistent file, but got none");
            } catch (Exception searchException) {
                // This is expected - should get a native error about the file not existing
                assertFalse(searchException.getMessage().contains("Invalid split metadata"), 
                           "Should not get offset validation error for valid offsets");
                System.out.println("✅ Got expected native error when searching: " + searchException.getMessage());
            }
        } catch (Exception e) {
            // If creation failed, ensure it's not due to offset validation issues
            assertFalse(e.getMessage().contains("Invalid split metadata"), 
                       "Should not get offset validation error for valid offsets");
            System.out.println("✅ Got exception during creation (acceptable): " + e.getMessage());
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
            null,          // No doc mapping for test
            new ArrayList<>()  // No skipped splits for test
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
            "invalid-range-split",              // splitId
            "test-index",                       // indexUid
            0L,                                 // partitionId
            "test-source",                      // sourceId
            "test-node",                        // nodeId
            100L,                               // numDocs
            1000000L,                           // uncompressedSizeBytes
            Instant.now(),                      // timeRangeStart
            Instant.now(),                      // timeRangeEnd
            System.currentTimeMillis() / 1000,  // createTimestamp
            "Mature",                           // maturity
            new HashSet<>(),                    // tags
            5000L,                              // footerStartOffset
            3000L,                              // footerEndOffset - INVALID: end < start
            1L,                                 // deleteOpstamp
            1,                                  // numMergeOps
            "test-doc-mapping",                 // docMappingUid
            null,                               // docMappingJson
            new ArrayList<>()                   // skippedSplits
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
        assertTrue(exception.getMessage().contains("footerOffsets=5000-3000"),
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
            null,             // No doc mapping for test
            new ArrayList<>() // No skipped splits for test
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
    @DisplayName("Test footer overflow is caught")
    public void testFooterOverflow() {
        System.out.println("=== Testing Footer Overflow ===");

        // Create metadata with footer offsets that are unreasonably large (> 100GB)
        QuickwitSplit.SplitMetadata invalidMetadata = new QuickwitSplit.SplitMetadata(
            "overflow-split",                    // splitId
            "test-index",                       // indexUid
            0L,                                 // partitionId
            "test-source",                      // sourceId
            "test-node",                        // nodeId
            100L,                               // numDocs
            1000000L,                           // uncompressedSizeBytes
            Instant.now(),                      // timeRangeStart
            Instant.now(),                      // timeRangeEnd
            System.currentTimeMillis() / 1000,  // createTimestamp
            "Mature",                           // maturity
            new HashSet<>(),                    // tags
            1000L,                              // footerStartOffset
            Long.MAX_VALUE - 100L,              // footerEndOffset - unreasonably large (> 100GB)
            1L,                                 // deleteOpstamp
            1,                                  // numMergeOps
            "test-doc-mapping",                 // docMappingUid
            null,                               // docMappingJson
            new ArrayList<>()                   // skippedSplits
        );

        // Create cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("validation-test")
            .withMaxCacheSize(10_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);

        // This should throw IllegalArgumentException about unreasonably large offset
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            cacheManager.createSplitSearcher("/tmp/test.split", invalidMetadata);
        });

        // Print the actual message to see what we got
        System.out.println("Actual exception message: " + exception.getMessage());

        assertTrue(exception.getMessage().contains("unreasonably large"),
                  "Should get specific error about unreasonably large footer offset");
        System.out.println("✅ Caught footer overflow: " + exception.getMessage());
    }
}
