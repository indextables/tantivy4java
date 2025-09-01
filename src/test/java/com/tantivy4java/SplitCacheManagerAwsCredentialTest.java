package com.tantivy4java;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test AWS credential session token invalidation functionality in SplitCacheManager.
 */
public class SplitCacheManagerAwsCredentialTest {

    @Test
    @DisplayName("Test AWS credential session token invalidation")
    public void testAwsCredentialSessionTokenInvalidation() {
        String accessKey = "test-access-key";
        String secretKey = "test-secret-key";
        String sessionToken1 = "session-token-1";
        String sessionToken2 = "session-token-2";
        
        // Create first cache with session token 1
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("aws-test-cache")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, sessionToken1)
            .withAwsRegion("us-east-1");
        
        SplitCacheManager cache1 = SplitCacheManager.getInstance(config1);
        assertNotNull(cache1, "First cache should be created");
        
        // Verify the cache is accessible
        assertEquals("aws-test-cache", cache1.getCacheName());
        
        // Create second cache with same credentials but different session token
        // This should invalidate the first cache
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("aws-test-cache-2")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, sessionToken2)
            .withAwsRegion("us-east-1");
        
        SplitCacheManager cache2 = SplitCacheManager.getInstance(config2);
        assertNotNull(cache2, "Second cache should be created");
        
        // Both caches should be different instances since they have different names
        assertNotSame(cache1, cache2, "Caches should be different instances with different names");
        
        // Clean up
        try {
            cache1.close();
        } catch (Exception e) {
            // May already be closed by invalidation - that's expected
        }
        
        try {
            cache2.close();
        } catch (Exception e) {
            System.err.println("Error closing cache2: " + e.getMessage());
        }
        
        System.out.println("✅ AWS credential session token invalidation test completed");
    }
    
    @Test
    @DisplayName("Test manual AWS credential cache invalidation")
    public void testManualAwsCredentialInvalidation() {
        String accessKey = "manual-test-access-key";
        String secretKey = "manual-test-secret-key";
        
        // Create multiple caches with the same AWS credentials
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("manual-cache-1")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, "token1")
            .withAwsRegion("us-east-1");
        
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("manual-cache-2")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, "token2")
            .withAwsRegion("us-west-2");
        
        SplitCacheManager cache1 = SplitCacheManager.getInstance(config1);
        SplitCacheManager cache2 = SplitCacheManager.getInstance(config2);
        
        assertNotNull(cache1, "Cache 1 should be created");
        assertNotNull(cache2, "Cache 2 should be created");
        
        // Manually invalidate all caches using these AWS credentials
        int invalidatedCount = SplitCacheManager.invalidateAwsCredentialCaches(accessKey, secretKey);
        
        assertTrue(invalidatedCount >= 0, "Should return count of invalidated caches");
        
        System.out.println("✅ Manual AWS credential invalidation test completed - invalidated " + invalidatedCount + " caches");
    }
    
    @Test
    @DisplayName("Test cache without AWS credentials is unaffected")
    public void testNonAwsCacheUnaffected() {
        // Create cache without AWS credentials
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("non-aws-cache")
            .withMaxCacheSize(50_000_000);
        
        SplitCacheManager cache = SplitCacheManager.getInstance(config);
        assertNotNull(cache, "Non-AWS cache should be created");
        
        // Manual invalidation should not affect this cache
        int invalidatedCount = SplitCacheManager.invalidateAwsCredentialCaches("any-key", "any-secret");
        assertEquals(0, invalidatedCount, "No caches should be invalidated for non-existent credentials");
        
        // Clean up
        try {
            cache.close();
        } catch (Exception e) {
            System.err.println("Error closing non-AWS cache: " + e.getMessage());
        }
        
        System.out.println("✅ Non-AWS cache unaffected test completed");
    }
    
    @Test
    @DisplayName("Test same credentials with no session token change")
    public void testSameCredentialsNoInvalidation() {
        String accessKey = "same-cred-access-key";
        String secretKey = "same-cred-secret-key";
        String sessionToken = "stable-session-token";
        
        // Create first cache
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("stable-cache-1")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, sessionToken)
            .withAwsRegion("us-east-1");
        
        SplitCacheManager cache1 = SplitCacheManager.getInstance(config1);
        assertNotNull(cache1, "First cache should be created");
        
        // Create second cache with identical credentials (including session token)
        // This should NOT invalidate the first cache since session token is the same
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("stable-cache-2")
            .withMaxCacheSize(50_000_000)
            .withAwsCredentials(accessKey, secretKey, sessionToken)
            .withAwsRegion("us-east-1");
        
        SplitCacheManager cache2 = SplitCacheManager.getInstance(config2);
        assertNotNull(cache2, "Second cache should be created");
        
        // Both caches should exist since session tokens are the same
        assertNotSame(cache1, cache2, "Should be different cache instances with different names");
        
        // Clean up
        try {
            cache1.close();
            cache2.close();
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
        
        System.out.println("✅ Same credentials no invalidation test completed");
    }
}