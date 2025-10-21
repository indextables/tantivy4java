package io.indextables.tantivy4java;

import io.indextables.tantivy4java.split.*;


import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive multi-cloud integration test demonstrating AWS S3, Azure Blob Storage, 
 * and GCP Cloud Storage working together with shared cache architecture using localhost endpoints.
 * Focuses on configuration validation rather than actual cloud operations.
 */
public class MultiCloudStorageIntegrationTest {

    // AWS S3 Configuration
    private static final String S3_BUCKET = "s3-test-bucket";
    private static final String S3_ACCESS_KEY = "test-access-key";
    private static final String S3_SECRET_KEY = "test-secret-key";
    private static final int S3_MOCK_PORT = 8003; // Unique port to avoid conflicts with SplitSearcherTest (8001) and S3MergeMockTest (8002)
    
    // Azure Configuration
    private static final String AZURE_CONTAINER = "azure-test-container";
    private static final String AZURE_ACCOUNT_NAME = "devstoreaccount1";
    private static final String AZURE_ACCOUNT_KEY = "test-key";
    
    // GCP Configuration
    private static final String GCP_BUCKET = "gcp-test-bucket";
    private static final String GCP_PROJECT_ID = "multi-cloud-test-project";
    
    // Cloud service instances for localhost connectivity
    private static S3Mock s3Mock;
    private static S3Client s3Client;
    private static Storage gcpStorage;
    private static SplitCacheManager multiCloudCacheManager;
    
    @TempDir
    static Path tempDir;
    
    @BeforeAll
    static void setUpMultiCloudEnvironment() throws IOException {
        // Setup AWS S3Mock
        setupS3Mock();
        
        // Setup Azure configuration
        setupAzureEmulator();
        
        // Setup GCP configuration
        setupGcpEmulator();
        
        // Create unified multi-cloud cache manager
        setupMultiCloudCacheManager();
    }
    
    private static void setupS3Mock() {
        s3Mock = new S3Mock.Builder()
                .withPort(S3_MOCK_PORT)
                .withInMemoryBackend()
                .build();
        s3Mock.start();
        
        s3Client = S3Client.builder()
                .endpointOverride(java.net.URI.create("http://localhost:" + S3_MOCK_PORT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3_ACCESS_KEY, S3_SECRET_KEY)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
        
        s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET).build());
    }
    
    private static void setupAzureEmulator() {
        // Configuration-only testing for Azure - no actual connectivity required
        // The actual Azure connectivity is handled at the native layer
        System.out.println("Azure configuration ready for localhost testing");
    }
    
    private static void setupGcpEmulator() {
        // Use Google's LocalStorageHelper for in-memory GCS testing
        gcpStorage = LocalStorageHelper.getOptions().getService();
        
        // Note: Bucket creation is limited in LocalStorageHelper, focusing on configuration
        System.out.println("GCP configuration ready for localhost testing");
    }
    
    private static void setupMultiCloudCacheManager() {
        String azureEndpoint = "http://127.0.0.1:10000/" + AZURE_ACCOUNT_NAME;
        String gcpEndpoint = "http://127.0.0.1:9023";
        
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("multi-cloud-cache")
                .withMaxCacheSize(500_000_000) // 500MB shared cache for all clouds
                .withMaxConcurrentLoads(16)
                // AWS Configuration
                .withAwsCredentials(S3_ACCESS_KEY, S3_SECRET_KEY)
                .withAwsRegion("us-east-1")
                .withAwsEndpoint("http://127.0.0.1:" + S3_MOCK_PORT)
                // Azure Configuration
                .withAzureCredentials(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY)
                // GCP Configuration
                .withGcpCredentials(GCP_PROJECT_ID, "fake-service-account-key");
        
        multiCloudCacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDownMultiCloudEnvironment() {
        if (multiCloudCacheManager != null) {
            try {
                multiCloudCacheManager.close();
            } catch (Exception e) {
                System.err.println("Error closing multi-cloud cache manager: " + e.getMessage());
            }
        }
        
        if (s3Client != null) s3Client.close();
        if (s3Mock != null) s3Mock.shutdown();
        
        // LocalStorageHelper doesn't require explicit stop() - it's automatically managed
    }
    
    @Test
    @DisplayName("Test multi-cloud configuration validation")
    void testMultiCloudConfigurationValidation() {
        // Verify multi-cloud cache manager was created successfully
        assertNotNull(multiCloudCacheManager, "Multi-cloud cache manager should be created");
        
        // Test accessing configuration for all cloud providers
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("multi-cloud-validation")
                .withMaxCacheSize(300_000_000)
                // AWS Configuration
                .withAwsCredentials("aws-test-key", "aws-test-secret")
                .withAwsRegion("us-west-2")
                .withAwsEndpoint("http://127.0.0.1:8002")
                // Azure Configuration
                .withAzureCredentials("azureaccount", "azurekey")
                .withAzureConnectionString("DefaultEndpointsProtocol=http;AccountName=test;AccountKey=testkey;BlobEndpoint=http://127.0.0.1:10001/test;")
                // GCP Configuration
                .withGcpCredentials("test-project", "test-service-key")
                .withGcpCredentialsFile("/path/to/service-account.json");
        
        // Validate AWS configuration
        assertEquals("aws-test-key", config.getAwsConfig().get("access_key"));
        assertEquals("aws-test-secret", config.getAwsConfig().get("secret_key"));
        assertEquals("us-west-2", config.getAwsConfig().get("region"));
        assertEquals("http://127.0.0.1:8002", config.getAwsConfig().get("endpoint"));
        
        // Validate Azure configuration
        assertEquals("azureaccount", config.getAzureConfig().get("account_name"));
        assertEquals("azurekey", config.getAzureConfig().get("access_key"));
        assertNotNull(config.getAzureConfig().get("connection_string"));

        // Validate GCP configuration
        assertEquals("test-project", config.getGcpConfig().get("project_id"));
        assertEquals("test-service-key", config.getGcpConfig().get("service_account_key"));
        assertEquals("/path/to/service-account.json", config.getGcpConfig().get("credentials_file"));
        
        System.out.println("✅ Multi-cloud configuration validation successful");
    }
    
    @Test
    @DisplayName("Test multi-cloud path patterns")
    void testMultiCloudPathPatterns() {
        // Test path patterns for all three cloud providers
        List<String> cloudPaths = List.of(
            "s3://s3-bucket/splits/test.split",
            "azure://azure-container/splits/test.split", 
            "gcs://gcp-bucket/splits/test.split"
        );
        
        for (String path : cloudPaths) {
            String provider = getCloudProviderFromPath(path);
            assertNotEquals("Unknown", provider, "Should recognize cloud provider from path: " + path);
            assertTrue(path.contains("splits/test.split"), "Path should contain expected split location");
        }
        
        System.out.println("✅ Multi-cloud path patterns validation successful");
    }
    
    @Test
    @DisplayName("Test multi-cloud shared cache statistics")
    void testMultiCloudSharedCacheStatistics() {
        // Verify shared cache statistics across all clouds
        SplitCacheManager.GlobalCacheStats globalStats = multiCloudCacheManager.getGlobalCacheStats();
        assertNotNull(globalStats, "Global cache statistics should be available");
        
        System.out.println("Multi-Cloud Shared Cache Statistics:");
        System.out.println("  - Max size: " + globalStats.getMaxSize() + " bytes");
        System.out.println("  - Current size: " + globalStats.getCurrentSize() + " bytes");
        System.out.println("  - Active splits: " + globalStats.getActiveSplits());
        System.out.println("  - Total hits: " + globalStats.getTotalHits());
        System.out.println("  - Total misses: " + globalStats.getTotalMisses());
        System.out.println("  - Cache utilization: " + String.format("%.2f%%", globalStats.getUtilization() * 100));
        
        // Verify cache configuration
        assertTrue(globalStats.getMaxSize() > 0, "Should have configured max cache size");
        assertTrue(globalStats.getCurrentSize() >= 0, "Should report current cache size");
        assertTrue(globalStats.getActiveSplits() >= 0, "Should report active splits count");
        
        System.out.println("✅ Multi-cloud shared cache statistics successful");
    }
    
    @Test
    @DisplayName("Test multi-cloud cache manager lifecycle")
    void testMultiCloudCacheManagerLifecycle() {
        // Test creating multiple cache managers with different configurations
        SplitCacheManager.CacheConfig tempConfig = new SplitCacheManager.CacheConfig("multi-cloud-temp")
                .withMaxCacheSize(100_000_000)
                .withAwsCredentials("temp-key", "temp-secret")
                .withAwsRegion("eu-west-1")
                .withAzureCredentials("tempazure", "tempkey")
                .withGcpCredentials("temp-project", "temp-service-key");
        
        SplitCacheManager tempCacheManager = SplitCacheManager.getInstance(tempConfig);
        assertNotNull(tempCacheManager, "Temporary multi-cloud cache manager should be created");
        
        try {
            SplitCacheManager.GlobalCacheStats tempStats = tempCacheManager.getGlobalCacheStats();
            assertNotNull(tempStats, "Temporary cache manager should provide statistics");
            assertEquals(100_000_000, tempStats.getMaxSize(), "Cache size should match configuration");
            
        } finally {
            try {
                tempCacheManager.close();
            } catch (Exception e) {
                System.err.println("Error closing temporary cache manager: " + e.getMessage());
            }
        }
        
        System.out.println("✅ Multi-cloud cache manager lifecycle successful");
    }
    
    @Test
    @DisplayName("Test multi-cloud error handling patterns")
    void testMultiCloudErrorHandlingPatterns() {
        // Test configuration validation patterns
        List<String> invalidPaths = List.of(
            "invalid://bucket/test.split",
            "s3://", 
            "azure://",
            "gcs://"
        );
        
        for (String invalidPath : invalidPaths) {
            String provider = getCloudProviderFromPath(invalidPath);
            if (invalidPath.startsWith("invalid://")) {
                assertEquals("Unknown", provider, "Should not recognize invalid protocol");
            } else {
                assertNotEquals("Unknown", provider, "Should recognize valid protocols even with incomplete paths");
            }
        }
        
        System.out.println("✅ Multi-cloud error handling patterns successful");
    }
    
    // Note: Endpoint configuration test removed as endpoint override functionality
    // was removed from the codebase. Cloud storage services now use their default
    // endpoints, and custom endpoints (for testing with Azurite, LocalStack, etc.)
    // should be configured at the native/Rust layer level.

    private String getCloudProviderFromPath(String splitPath) {
        if (splitPath.startsWith("s3://")) return "AWS";
        if (splitPath.startsWith("azure://")) return "Azure";
        if (splitPath.startsWith("gcs://")) return "GCP";
        return "Unknown";
    }
}
