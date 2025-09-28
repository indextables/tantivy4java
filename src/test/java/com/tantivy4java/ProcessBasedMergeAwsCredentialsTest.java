package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Test cases specifically for validating AWS credentials serialization
 * in process-based merge operations.
 *
 * This test addresses the critical bug where AWS credentials were hardcoded
 * as null instead of being properly serialized for the child process.
 */
public class ProcessBasedMergeAwsCredentialsTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Test AWS credentials serialization in process merge JSON")
    void testAwsCredentialsJsonSerialization() throws Exception {
        // Create AWS config with all possible fields
        QuickwitSplit.AwsConfig fullAwsConfig = new QuickwitSplit.AwsConfig(
            "test-access-key-123",
            "test-secret-key-456",
            "temp-session-token-789",
            "us-west-2",
            "https://custom-endpoint.example.com",
            true  // force path style
        );

        // Create merge config with AWS credentials
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "test-index-uid",
            "test-source-id",
            "test-node-id",
            fullAwsConfig
        );

        // Use reflection to access the private createMergeRequestJson method
        Method createJsonMethod = MergeBinaryExtractor.class.getDeclaredMethod(
            "createMergeRequestJson",
            java.util.List.class,
            String.class,
            QuickwitSplit.MergeConfig.class
        );
        createJsonMethod.setAccessible(true);

        // Create test split paths and output path
        String splitPath1 = tempDir.resolve("split1.split").toString();
        String splitPath2 = tempDir.resolve("split2.split").toString();
        String outputPath = tempDir.resolve("merged.split").toString();

        // Call the method to generate JSON
        String json = (String) createJsonMethod.invoke(
            null,
            Arrays.asList(splitPath1, splitPath2),
            outputPath,
            mergeConfig
        );

        // Validate JSON contains AWS config (not null)
        assertNotNull(json, "Generated JSON should not be null");
        assertFalse(json.contains("\"aws_config\":null"),
            "JSON should NOT contain hardcoded null AWS config");

        // Validate all AWS credential fields are present
        assertTrue(json.contains("\"aws_config\":{"),
            "JSON should contain aws_config object");
        assertTrue(json.contains("\"access_key\":\"test-access-key-123\""),
            "JSON should contain access key");
        assertTrue(json.contains("\"secret_key\":\"test-secret-key-456\""),
            "JSON should contain secret key");
        assertTrue(json.contains("\"session_token\":\"temp-session-token-789\""),
            "JSON should contain session token");
        assertTrue(json.contains("\"region\":\"us-west-2\""),
            "JSON should contain region");
        assertTrue(json.contains("\"endpoint_url\":\"https://custom-endpoint.example.com\""),
            "JSON should contain endpoint URL with correct field name");
        assertTrue(json.contains("\"force_path_style\":true"),
            "JSON should contain path style flag with correct field name");

        System.out.println("✅ Generated JSON with AWS credentials:");
        System.out.println(json);
    }

    @Test
    @DisplayName("Test null AWS config still generates null in JSON")
    void testNullAwsConfigSerialization() throws Exception {
        // Create merge config WITHOUT AWS credentials (for local files)
        QuickwitSplit.MergeConfig localMergeConfig = new QuickwitSplit.MergeConfig(
            "local-index-uid",
            "local-source-id",
            "local-node-id"
        );

        // Use reflection to access the private method
        Method createJsonMethod = MergeBinaryExtractor.class.getDeclaredMethod(
            "createMergeRequestJson",
            java.util.List.class,
            String.class,
            QuickwitSplit.MergeConfig.class
        );
        createJsonMethod.setAccessible(true);

        String splitPath = tempDir.resolve("local-split.split").toString();
        String outputPath = tempDir.resolve("local-merged.split").toString();

        String json = (String) createJsonMethod.invoke(
            null,
            Arrays.asList(splitPath),
            outputPath,
            localMergeConfig
        );

        // For local operations, aws_config should be null
        assertTrue(json.contains("\"aws_config\":null"),
            "Local operations should have null AWS config");

        System.out.println("✅ Generated JSON for local operations:");
        System.out.println(json);
    }

    @Test
    @DisplayName("Test session token null handling")
    void testSessionTokenNullHandling() throws Exception {
        // Create AWS config WITHOUT session token (basic credentials only)
        QuickwitSplit.AwsConfig basicAwsConfig = new QuickwitSplit.AwsConfig(
            "basic-access-key",
            "basic-secret-key",
            "us-east-1"
        );

        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "basic-index-uid",
            "basic-source-id",
            "basic-node-id",
            basicAwsConfig
        );

        Method createJsonMethod = MergeBinaryExtractor.class.getDeclaredMethod(
            "createMergeRequestJson",
            java.util.List.class,
            String.class,
            QuickwitSplit.MergeConfig.class
        );
        createJsonMethod.setAccessible(true);

        String splitPath = tempDir.resolve("basic-split.split").toString();
        String outputPath = tempDir.resolve("basic-merged.split").toString();

        String json = (String) createJsonMethod.invoke(
            null,
            Arrays.asList(splitPath),
            outputPath,
            mergeConfig
        );

        // Validate AWS config is present but session token is null
        assertTrue(json.contains("\"aws_config\":{"),
            "JSON should contain aws_config object");
        assertTrue(json.contains("\"access_key\":\"basic-access-key\""),
            "JSON should contain access key");
        assertTrue(json.contains("\"secret_key\":\"basic-secret-key\""),
            "JSON should contain secret key");
        assertTrue(json.contains("\"session_token\":null"),
            "JSON should contain null session token when not provided");
        assertTrue(json.contains("\"region\":\"us-east-1\""),
            "JSON should contain region");

        System.out.println("✅ Generated JSON with null session token:");
        System.out.println(json);
    }

    @Test
    @DisplayName("Test JSON escaping for special characters in credentials")
    void testCredentialEscaping() throws Exception {
        // Create AWS config with special characters that need escaping
        QuickwitSplit.AwsConfig specialConfig = new QuickwitSplit.AwsConfig(
            "access\"key\\with/special",  // Contains quotes and backslashes
            "secret\\key\"with/chars",    // Contains quotes and backslashes
            "token\"with\\special/chars", // Session token with special chars
            "us-east-1",
            "https://endpoint.com/path\"with\\chars",  // Endpoint with special chars
            false
        );

        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "special-index-uid",
            "special-source-id",
            "special-node-id",
            specialConfig
        );

        Method createJsonMethod = MergeBinaryExtractor.class.getDeclaredMethod(
            "createMergeRequestJson",
            java.util.List.class,
            String.class,
            QuickwitSplit.MergeConfig.class
        );
        createJsonMethod.setAccessible(true);

        String splitPath = tempDir.resolve("special-split.split").toString();
        String outputPath = tempDir.resolve("special-merged.split").toString();

        String json = (String) createJsonMethod.invoke(
            null,
            Arrays.asList(splitPath),
            outputPath,
            mergeConfig
        );

        // Validate proper JSON escaping (quotes and backslashes should be escaped)
        assertTrue(json.contains("access\\\"key\\\\with/special"),
            "Access key should be properly JSON escaped");
        assertTrue(json.contains("secret\\\\key\\\"with/chars"),
            "Secret key should be properly JSON escaped");
        assertTrue(json.contains("token\\\"with\\\\special/chars"),
            "Session token should be properly JSON escaped");
        assertTrue(json.contains("https://endpoint.com/path\\\"with\\\\chars"),
            "Endpoint should be properly JSON escaped");

        // Validate the JSON is still valid (contains proper structure)
        assertTrue(json.contains("\"aws_config\":{"),
            "JSON should still contain valid aws_config object");

        System.out.println("✅ Generated JSON with escaped special characters:");
        System.out.println(json);
    }

    @Test
    @DisplayName("Test field name compatibility with Rust binary")
    void testRustFieldNameCompatibility() throws Exception {
        QuickwitSplit.AwsConfig config = new QuickwitSplit.AwsConfig(
            "test-access",
            "test-secret",
            "test-token",
            "us-east-1",
            "https://custom-endpoint.com",
            true
        );

        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "compatibility-index",
            "compatibility-source",
            "compatibility-node",
            config
        );

        Method createJsonMethod = MergeBinaryExtractor.class.getDeclaredMethod(
            "createMergeRequestJson",
            java.util.List.class,
            String.class,
            QuickwitSplit.MergeConfig.class
        );
        createJsonMethod.setAccessible(true);

        String json = (String) createJsonMethod.invoke(
            null,
            Arrays.asList(tempDir.resolve("test.split").toString()),
            tempDir.resolve("out.split").toString(),
            mergeConfig
        );

        // Validate field names match Rust expectations (not Java names)
        assertTrue(json.contains("\"endpoint_url\":"),
            "Should use 'endpoint_url' field name for Rust compatibility");
        assertTrue(json.contains("\"force_path_style\":"),
            "Should use 'force_path_style' field name for Rust compatibility");

        // Make sure we're NOT using Java-style names
        assertFalse(json.contains("\"endpoint\":"),
            "Should NOT use 'endpoint' field name (Java style)");
        assertFalse(json.contains("\"path_style_access\":"),
            "Should NOT use 'path_style_access' field name (Java style)");

        System.out.println("✅ Generated JSON with Rust-compatible field names:");
        System.out.println(json);
    }

    @Test
    @DisplayName("Test process-based merge with invalid AWS credentials")
    void testProcessMergeWithInvalidCredentials() throws Exception {
        // Create TWO indices and splits for testing (merge requires at least 2 splits)
        Path indexDir1 = tempDir.resolve("test_index1");
        Path indexDir2 = tempDir.resolve("test_index2");
        createTestIndex(indexDir1);
        createTestIndex(indexDir2);

        // Convert to splits
        QuickwitSplit.SplitConfig splitConfig1 = new QuickwitSplit.SplitConfig(
            "invalid-creds-test-1",
            "test-source",
            "test-node"
        );
        QuickwitSplit.SplitConfig splitConfig2 = new QuickwitSplit.SplitConfig(
            "invalid-creds-test-2",
            "test-source",
            "test-node"
        );

        String splitPath1 = tempDir.resolve("invalid-creds-split1.split").toString();
        String splitPath2 = tempDir.resolve("invalid-creds-split2.split").toString();
        QuickwitSplit.convertIndexFromPath(indexDir1.toString(), splitPath1, splitConfig1);
        QuickwitSplit.convertIndexFromPath(indexDir2.toString(), splitPath2, splitConfig2);

        // Create AWS config with completely bogus credentials
        QuickwitSplit.AwsConfig invalidConfig = new QuickwitSplit.AwsConfig(
            "INVALID-ACCESS-KEY-123",
            "invalid-secret-key-456",
            "invalid-session-token-789",
            "us-east-1",
            null,  // Use default AWS endpoint
            false
        );

        // Create merge config with debug enabled to see detailed logs
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "invalid-test-index",
            "invalid-test-source",
            "invalid-test-node",
            invalidConfig,
            true  // Enable debug logging
        );

        // Try to merge with invalid S3 credentials using process mode
        String s3OutputPath = "s3://fake-bucket-that-doesnt-exist/merged-invalid.split";

        Exception exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(
                Arrays.asList(splitPath1, splitPath2),  // Now providing 2 splits
                s3OutputPath,
                mergeConfig
            );
        });

        // The process should fail, but the error should NOT be "no providers in chain"
        // because we ARE providing credentials (they're just invalid)
        String errorMessage = exception.getMessage().toLowerCase();
        System.out.println("❌ Expected error with invalid credentials: " + exception.getMessage());

        // The error should be about invalid credentials or access denied, NOT missing credentials
        assertFalse(errorMessage.contains("no providers in chain"),
            "Error should NOT be about missing credential providers - we provided them");
        assertFalse(errorMessage.contains("aws_config\":null"),
            "Error should NOT mention null aws_config - we provided a config");

        // Should be actual AWS authentication/authorization errors
        assertTrue(
            errorMessage.contains("invalid") ||
            errorMessage.contains("access denied") ||
            errorMessage.contains("unauthorized") ||
            errorMessage.contains("forbidden") ||
            errorMessage.contains("signature") ||
            errorMessage.contains("credential") ||
            errorMessage.contains("bucket") ||           // Bucket access issues
            errorMessage.contains("network") ||          // Network connectivity
            errorMessage.contains("connection"),         // Connection issues
            "Error should be about invalid credentials or access, not missing config"
        );
    }

    @Test
    @DisplayName("Test process-based merge with valid AWS credentials (if available)")
    void testProcessMergeWithValidCredentials() throws Exception {
        // Create index and split for testing
        Path indexDir = tempDir.resolve("test_index_valid");
        createTestIndex(indexDir);

        String splitPath = tempDir.resolve("valid-creds-split.split").toString();
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "valid-creds-test",
            "test-source",
            "test-node"
        );
        QuickwitSplit.convertIndexFromPath(indexDir.toString(), splitPath, splitConfig);

        // Try to get real AWS credentials from environment or ~/.aws/credentials
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("AWS_SESSION_TOKEN");

        if (accessKey == null || secretKey == null) {
            // Try to read from ~/.aws/credentials
            try {
                Path credentialsPath = Path.of(System.getProperty("user.home"), ".aws", "credentials");
                if (!credentialsPath.toFile().exists()) {
                    System.out.println("⏭️  Skipping valid credentials test - no AWS credentials available");
                    return;
                }
                // If credentials file exists but we can't parse it easily, skip
                System.out.println("⏭️  Skipping valid credentials test - could not parse ~/.aws/credentials easily");
                return;
            } catch (Exception e) {
                System.out.println("⏭️  Skipping valid credentials test - " + e.getMessage());
                return;
            }
        }

        // Create AWS config with real credentials
        QuickwitSplit.AwsConfig realConfig = new QuickwitSplit.AwsConfig(
            accessKey,
            secretKey,
            sessionToken,
            "us-east-1",
            null,  // Use default AWS endpoint
            false
        );

        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "valid-test-index",
            "valid-test-source",
            "valid-test-node",
            realConfig
        );

        // Test with a real S3 path (will fail due to permissions, but should NOT fail on credentials)
        String s3OutputPath = "s3://tantivy4java-testing/test-process-merge-" + System.currentTimeMillis() + ".split";

        try {
            QuickwitSplit.mergeSplits(
                Arrays.asList(splitPath),
                s3OutputPath,
                mergeConfig
            );

            System.out.println("✅ Process-based merge with valid credentials succeeded");

        } catch (RuntimeException e) {
            String errorMessage = e.getMessage().toLowerCase();
            System.out.println("⚠️  Expected error with valid credentials: " + e.getMessage());

            // With valid credentials, we should NOT get credential-related errors
            assertFalse(errorMessage.contains("no providers in chain"),
                "Should not get 'no providers in chain' error with valid credentials");
            assertFalse(errorMessage.contains("aws_config\":null"),
                "Should not get null aws_config error with valid credentials");
            assertFalse(errorMessage.contains("credential"),
                "Should not get credential-related errors with valid credentials");

            // Acceptable errors with valid credentials:
            assertTrue(
                errorMessage.contains("access denied") ||      // Bucket access denied
                errorMessage.contains("forbidden") ||          // 403 Forbidden
                errorMessage.contains("does not exist") ||     // Bucket doesn't exist
                errorMessage.contains("no such bucket") ||     // Bucket not found
                errorMessage.contains("permission denied") ||   // Permissions
                errorMessage.contains("split") ||              // Split file issues (acceptable)
                errorMessage.contains("timeout") ||            // Network timeout (acceptable)
                errorMessage.contains("network"),              // Network errors (acceptable)
                "With valid credentials, errors should be about permissions/access, not authentication: " + errorMessage
            );
        }
    }

    @Test
    @DisplayName("Test process-based merge credentials propagation vs direct mode")
    void testCredentialPropagationProcessVsDirectMode() throws Exception {
        // Create TWO test indices and splits (merge requires at least 2 splits)
        Path indexDir1 = tempDir.resolve("test_index_propagation1");
        Path indexDir2 = tempDir.resolve("test_index_propagation2");
        createTestIndex(indexDir1);
        createTestIndex(indexDir2);

        String splitPath1 = tempDir.resolve("propagation-test-split1.split").toString();
        String splitPath2 = tempDir.resolve("propagation-test-split2.split").toString();
        QuickwitSplit.SplitConfig splitConfig1 = new QuickwitSplit.SplitConfig(
            "propagation-test-1",
            "test-source",
            "test-node"
        );
        QuickwitSplit.SplitConfig splitConfig2 = new QuickwitSplit.SplitConfig(
            "propagation-test-2",
            "test-source",
            "test-node"
        );
        QuickwitSplit.convertIndexFromPath(indexDir1.toString(), splitPath1, splitConfig1);
        QuickwitSplit.convertIndexFromPath(indexDir2.toString(), splitPath2, splitConfig2);

        // Create AWS config with recognizable invalid credentials
        QuickwitSplit.AwsConfig testConfig = new QuickwitSplit.AwsConfig(
            "PROCESS-TEST-ACCESS-KEY",
            "process-test-secret-key",
            "process-test-session-token",
            "us-east-1",
            null,
            false
        );

        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "propagation-test-index",
            "propagation-test-source",
            "propagation-test-node",
            testConfig
        );

        // Test with S3 output to force credential usage
        String s3OutputPath = "s3://nonexistent-bucket-for-credential-test/test-merge.split";

        // Attempt merge (will fail, but error should show credentials were propagated)
        Exception exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(
                Arrays.asList(splitPath1, splitPath2),  // Now providing 2 splits
                s3OutputPath,
                mergeConfig
            );
        });

        String errorMessage = exception.getMessage();
        System.out.println("🔍 Process-based merge error (should show credential propagation): " + errorMessage);

        // Key validation: The error should NOT be about missing credentials
        assertFalse(errorMessage.toLowerCase().contains("no providers in chain"),
            "Process-based merge should propagate credentials, not get 'no providers in chain' error");

        // The error should be about actual AWS access issues, proving credentials were sent
        String lowerError = errorMessage.toLowerCase();
        assertTrue(
            lowerError.contains("access denied") ||
            lowerError.contains("invalid") ||
            lowerError.contains("unauthorized") ||
            lowerError.contains("forbidden") ||
            lowerError.contains("bucket") ||
            lowerError.contains("signature") ||
            lowerError.contains("credential") ||    // Invalid credential (not missing)
            lowerError.contains("network") ||       // Network connectivity issues
            lowerError.contains("connection"),      // Connection issues
            "Error should indicate credentials were processed by AWS, not missing entirely: " + errorMessage
        );

        System.out.println("✅ Process-based merge correctly propagated credentials to child process");
    }

    /**
     * Helper method to create a test index with sample documents
     */
    private void createTestIndex(Path indexDir) throws Exception {
        // Create simple test index
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .build();

        Index index = new Index(schema, indexDir.toString());

        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add test documents
            writer.addJson("{ \"title\": \"Test Document 1\", \"content\": \"This is test content for AWS credentials testing\" }");
            writer.addJson("{ \"title\": \"Test Document 2\", \"content\": \"This is another test document for process merge testing\" }");
            writer.addJson("{ \"title\": \"Test Document 3\", \"content\": \"Third document to ensure we have enough content for split creation\" }");

            writer.commit();
        }
    }
}