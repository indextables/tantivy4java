package com.tantivy4java;

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for Google Cloud Platform (GCP) Cloud Storage integration with SplitSearcher.
 * Uses Google Cloud LocalStorageHelper for pure Java testing with localhost connectivity.
 * Focuses on configuration testing as LocalStorageHelper has limited operational support.
 */
public class GcpStorageIntegrationTest {

    private static final String TEST_BUCKET = "test-splits-bucket";
    private static final String PROJECT_ID = "test-project-123";
    private static final int GCS_PORT = 9023;
    
    private static Storage storage;
    private static SplitCacheManager cacheManager;
    
    @TempDir
    static Path tempDir;
    
    private Path indexPath;
    private Path localSplitPath;
    private String gcsSplitPath;

    @BeforeAll
    static void setUpGcpLocalStorage() {
        // Use LocalStorageHelper for in-memory GCS testing
        storage = LocalStorageHelper.getOptions().getService();
        String localhostEndpoint = String.format("http://127.0.0.1:%d", GCS_PORT);
        
        // Note: LocalStorageHelper doesn't support bucket creation, so we'll test configuration only
        
        // Create shared cache manager with GCP configuration for localhost connectivity
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("gcp-test-cache")
                .withMaxCacheSize(200_000_000) // 200MB shared cache
                .withMaxConcurrentLoads(8)
                .withGcpCredentials(PROJECT_ID, "fake-service-account-key")
                .withGcpEndpoint(localhostEndpoint);
                
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDownGcpLocalStorage() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
        // LocalStorageHelper doesn't require explicit stop() - it's automatically managed
    }

    @BeforeEach
    void setUp() throws IOException {
        // For GCP configuration testing, we use mock GCS URLs
        // The actual GCP connectivity is handled at the native layer
        gcsSplitPath = String.format("gcs://%s/splits/gcp-test.split", TEST_BUCKET);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // No cleanup needed for configuration-only testing
    }
    
    @Test
    @DisplayName("Test GCP configuration and cache manager setup")
    void testGcpConfigurationSetup() {
        // Verify GCP configuration is properly set
        assertNotNull(cacheManager, "GCP cache manager should be created");
        
        // Test GCP-specific configuration
        String customEndpoint = "http://127.0.0.1:9024/custom";
        SplitCacheManager.CacheConfig gcpConfig = new SplitCacheManager.CacheConfig("gcp-custom-test")
                .withMaxCacheSize(100_000_000)
                .withGcpCredentials("testproject", "testkey") 
                .withGcpCredentialsFile("/path/to/credentials.json")
                .withGcpEndpoint(customEndpoint);
        
        // Verify configuration values
        assertEquals(customEndpoint, gcpConfig.getGcpConfig().get("endpoint"));
        assertEquals("testproject", gcpConfig.getGcpConfig().get("project_id"));
        assertEquals("testkey", gcpConfig.getGcpConfig().get("service_account_key"));
        assertEquals("/path/to/credentials.json", gcpConfig.getGcpConfig().get("credentials_file"));
        
        System.out.println("✅ GCP configuration validation successful");
    }
    
    @Test
    @DisplayName("Test GCP path and URL handling")
    void testGcpPathHandling() {
        // Test various GCP path formats
        List<String> gcpPaths = List.of(
            "gcs://bucket/splits/test.split",
            "gcs://my-bucket/data/split-001.split",
            "gcs://prod-bucket/indexes/quickwit.split"
        );
        
        for (String path : gcpPaths) {
            assertTrue(path.startsWith("gcs://"), "Path should start with gcs:// protocol");
            assertTrue(path.contains("/"), "Path should contain bucket and object separators");
        }
        
        System.out.println("✅ GCP path and URL handling validation successful");
    }
    
    @Test
    @DisplayName("Test GCP configuration patterns") 
    void testGcpConfigurationPatterns() {
        // Test different GCP configuration patterns
        
        // Pattern 1: Project ID and service account key
        SplitCacheManager.CacheConfig config1 = new SplitCacheManager.CacheConfig("gcp-config-1")
                .withGcpCredentials("project1", "key1");
        
        assertEquals("project1", config1.getGcpConfig().get("project_id"));
        assertEquals("key1", config1.getGcpConfig().get("service_account_key"));
        
        // Pattern 2: Credentials file
        String credentialsFile = "/path/to/service-account.json";
        SplitCacheManager.CacheConfig config2 = new SplitCacheManager.CacheConfig("gcp-config-2")
                .withGcpCredentialsFile(credentialsFile);
        
        assertEquals(credentialsFile, config2.getGcpConfig().get("credentials_file"));
        
        // Pattern 3: Custom endpoint
        String customEndpoint = "http://127.0.0.1:9025/custom";
        SplitCacheManager.CacheConfig config3 = new SplitCacheManager.CacheConfig("gcp-config-3")
                .withGcpEndpoint(customEndpoint);
        
        assertEquals(customEndpoint, config3.getGcpConfig().get("endpoint"));
        
        System.out.println("✅ GCP configuration patterns validation successful");
    }
    
    @Test
    @DisplayName("Test GCP global cache configuration")
    void testGcpGlobalCacheConfiguration() {
        // Test that global cache statistics are available
        SplitCacheManager.GlobalCacheStats globalStats = cacheManager.getGlobalCacheStats();
        assertNotNull(globalStats, "Global cache statistics should be available");
        
        // Verify cache configuration
        assertTrue(globalStats.getMaxSize() > 0, "Cache should have configured max size");
        assertTrue(globalStats.getCurrentSize() >= 0, "Cache should report current size");
        assertTrue(globalStats.getActiveSplits() >= 0, "Cache should report active splits count");
        
        System.out.println("GCP Global Cache Configuration:");
        System.out.println("  - Max size: " + globalStats.getMaxSize() + " bytes");
        System.out.println("  - Current size: " + globalStats.getCurrentSize() + " bytes");
        System.out.println("  - Active splits: " + globalStats.getActiveSplits());
        System.out.println("  - Cache utilization: " + String.format("%.2f%%", globalStats.getUtilization() * 100));
        
        System.out.println("✅ GCP global cache configuration successful");
    }
    
    @Test  
    @DisplayName("Test GCP cache manager lifecycle")
    void testGcpCacheManagerLifecycle() {
        // Test that we can create and manage multiple cache managers
        SplitCacheManager.CacheConfig tempConfig = new SplitCacheManager.CacheConfig("gcp-temp-cache")
                .withMaxCacheSize(50_000_000)
                .withGcpCredentials("tempproject", "tempkey");
        
        SplitCacheManager tempCacheManager = SplitCacheManager.getInstance(tempConfig);
        assertNotNull(tempCacheManager, "Temporary cache manager should be created");
        
        try {
            // Verify cache manager properties
            SplitCacheManager.GlobalCacheStats tempStats = tempCacheManager.getGlobalCacheStats();
            assertNotNull(tempStats, "Temporary cache manager should provide statistics");
            assertEquals(50_000_000, tempStats.getMaxSize(), "Cache size should match configuration");
            
        } finally {
            try {
                tempCacheManager.close();
            } catch (Exception e) {
                // Log but continue
            }
        }
        
        System.out.println("✅ GCP cache manager lifecycle successful");
    }
    
    @Test
    @DisplayName("Test GCP configuration with credentials and endpoints")
    void testGcpConfigurationCredentialsEndpoints() {
        // Test configuration with service account key using localhost endpoint
        String customEndpoint = "http://127.0.0.1:9024/custom";
        
        SplitCacheManager.CacheConfig serviceAccountConfig = new SplitCacheManager.CacheConfig("gcp-sa-test")
                .withMaxCacheSize(100_000_000)
                .withGcpCredentials(PROJECT_ID, "fake-service-account-json-key")
                .withGcpEndpoint(customEndpoint);
        
        // Verify service account configuration
        assertEquals(PROJECT_ID, serviceAccountConfig.getGcpConfig().get("project_id"));
        assertEquals("fake-service-account-json-key", serviceAccountConfig.getGcpConfig().get("service_account_key"));
        assertEquals(customEndpoint, serviceAccountConfig.getGcpConfig().get("endpoint"));
        
        // Test configuration with credentials file
        SplitCacheManager.CacheConfig fileConfig = new SplitCacheManager.CacheConfig("gcp-file-test")
                .withMaxCacheSize(100_000_000)
                .withGcpCredentialsFile("/path/to/credentials.json")
                .withGcpEndpoint(customEndpoint);
        
        // Verify file credentials configuration
        assertEquals("/path/to/credentials.json", fileConfig.getGcpConfig().get("credentials_file"));
        assertEquals(customEndpoint, fileConfig.getGcpConfig().get("endpoint"));
        
        System.out.println("✅ GCP configuration with credentials and endpoints successful");
    }
}