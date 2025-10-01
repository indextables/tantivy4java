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

import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test cases that specifically validate AWS credential extraction and parameter 
 * passing through the JNI layer. These tests ensure that the native code 
 * receives the correct AWS configuration.
 */
public class S3CredentialExtractionTest {

    private List<String> testS3Splits;
    private String testOutputPath;

    @BeforeEach
    public void setUp() {
        // Use non-existent S3 URLs to test credential extraction
        // The test will fail on network/access, not parameter extraction
        testS3Splits = Arrays.asList(
            "s3://test-credential-bucket/split1.split",
            "s3://test-credential-bucket/split2.split"
        );
        testOutputPath = "/tmp/credential-test-merge.split";
    }

    @Test
    @DisplayName("Test basic AWS credential extraction")
    public void testBasicCredentialExtraction() {
        // Create AWS config with specific test values
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            "test-extraction-access-key",
            "test-extraction-secret-key", 
            "test-extraction-region"
        );
        
        QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
            "extraction-test-index", "extraction-source", "extraction-node", awsConfig);
        
        // This should fail on S3 access, not parameter extraction
        // The error message should NOT mention missing credentials
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
        
        // Verify the error is NOT about missing credentials
        String message = exception.getMessage().toLowerCase();
        assertFalse(message.contains("credential") && message.contains("missing"),
            "Should not fail on missing credentials when AWS config is provided: " + exception.getMessage());
        
        // Should fail on network/access issues instead
        assertTrue(
            message.contains("resolve") || message.contains("storage") || 
            message.contains("network") || message.contains("access") ||
            message.contains("bucket") || message.contains("endpoint"),
            "Should fail on network/storage access, not credential extraction: " + exception.getMessage()
        );
    }

    @Test
    @DisplayName("Test session token credential extraction")
    public void testSessionTokenCredentialExtraction() {
        // Test with temporary credentials including session token
        QuickwitSplit.AwsConfig sessionConfig = new QuickwitSplit.AwsConfig(
            "temp-access-key-for-extraction",
            "temp-secret-key-for-extraction",
            "temp-session-token-for-extraction", 
            "us-east-1",
            null,  // Default endpoint
            false  // Virtual hosted style
        );
        
        QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
            "session-test-index", "session-source", "session-node", sessionConfig);
        
        // This should fail on S3 access, not credential extraction
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
        
        // Verify the error is NOT about missing session token or credentials
        String message = exception.getMessage().toLowerCase();
        assertFalse(message.contains("session") && message.contains("missing"),
            "Should not fail on missing session token when provided: " + exception.getMessage());
        assertFalse(message.contains("credential") && message.contains("missing"),
            "Should not fail on missing credentials when AWS config with session token is provided: " + exception.getMessage());
    }

    @Test
    @DisplayName("Test custom endpoint credential extraction")
    public void testCustomEndpointCredentialExtraction() {
        // Test with MinIO-style configuration
        QuickwitSplit.AwsConfig minioConfig = new QuickwitSplit.AwsConfig(
            "minio-access-key",
            "minio-secret-key",
            null,  // No session token
            "us-east-1",
            "http://localhost:9000",  // MinIO endpoint
            true   // Force path style for MinIO
        );
        
        QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
            "minio-test-index", "minio-source", "minio-node", minioConfig);
        
        // Use localhost MinIO URL for more realistic test
        List<String> minioSplits = Arrays.asList(
            "s3://test-minio-bucket/split1.split",
            "s3://test-minio-bucket/split2.split"
        );
        
        // This should fail on MinIO connection, not credential extraction
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(minioSplits, testOutputPath, config);
        });
        
        // Verify credentials and endpoint are extracted correctly
        String message = exception.getMessage().toLowerCase();
        assertFalse(message.contains("credential") && message.contains("missing"),
            "Should not fail on missing credentials for MinIO config: " + exception.getMessage());
        assertFalse(message.contains("endpoint") && message.contains("missing"),
            "Should not fail on missing endpoint when custom endpoint provided: " + exception.getMessage());
    }

    @Test
    @DisplayName("Test credential extraction error handling")
    public void testCredentialExtractionErrorHandling() {
        // Test various AWS config edge cases
        
        // Test with empty access key - should fail validation
        assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.AwsConfig invalidConfig = new QuickwitSplit.AwsConfig("", "secret", "region");
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig("test", "test", "test", invalidConfig);
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
        
        // Test with empty secret key - should fail validation  
        assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.AwsConfig invalidConfig = new QuickwitSplit.AwsConfig("access", "", "region");
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig("test", "test", "test", invalidConfig);
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
        
        // Test with empty region - should fail validation
        assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.AwsConfig invalidConfig = new QuickwitSplit.AwsConfig("access", "secret", "");
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig("test", "test", "test", invalidConfig);
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
    }

    @Test
    @DisplayName("Test credential extraction with different regions")
    public void testRegionSpecificCredentialExtraction() {
        // Test that different AWS regions are properly extracted
        String[] testRegions = {
            "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", 
            "ca-central-1", "sa-east-1"
        };
        
        for (String region : testRegions) {
            QuickwitSplit.AwsConfig regionConfig = new QuickwitSplit.AwsConfig(
                "region-test-access", "region-test-secret", region);
            
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
                "region-test-index", "region-source", "region-node", regionConfig);
            
            // Each should fail on S3 access, not credential/region extraction
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
            });
            
            // Verify region is not the issue - should fail on credentials/access, not region parsing
            String message = exception.getMessage().toLowerCase();
            // AWS credential errors are expected and prove parameter extraction worked
            assertFalse(message.contains("region") && message.contains("parsing"),
                "Should not fail on region parsing for " + region + ": " + exception.getMessage());
        }
    }

    @Test
    @DisplayName("Test force path style parameter extraction")
    public void testForcePathStyleExtraction() {
        // Test both path style settings
        boolean[] pathStyleSettings = {true, false};
        
        for (boolean forcePathStyle : pathStyleSettings) {
            QuickwitSplit.AwsConfig pathStyleConfig = new QuickwitSplit.AwsConfig(
                "path-style-access", 
                "path-style-secret",
                null,  // No session token
                "us-east-1",
                "https://s3.amazonaws.com",
                forcePathStyle
            );
            
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
                "path-style-index", "path-style-source", "path-style-node", pathStyleConfig);
            
            // Should fail on S3 access, not path style parameter extraction
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
            });
            
            // Verify path style parameter is extracted correctly
            String message = exception.getMessage().toLowerCase();
            assertFalse(message.contains("path") && message.contains("style") && message.contains("missing"),
                "Should not fail on missing path style parameter: " + exception.getMessage());
        }
    }

    @Test
    @DisplayName("Test AWS credential parameter boundary conditions")
    public void testCredentialBoundaryConditions() {
        // Test with very long credential strings (within reasonable limits)
        String longAccessKey = "A".repeat(128);  // AWS access keys are typically 20 chars, test longer
        String longSecretKey = "S".repeat(256);  // AWS secret keys are typically 40 chars, test longer
        String longSessionToken = "T".repeat(2048); // Session tokens can be quite long
        
        QuickwitSplit.AwsConfig boundaryConfig = new QuickwitSplit.AwsConfig(
            longAccessKey, longSecretKey, longSessionToken, "us-east-1", null, false);
        
        QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
            "boundary-test", "boundary-source", "boundary-node", boundaryConfig);
        
        // Should handle long credentials without truncation or corruption
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, config);
        });
        
        // Should fail on S3 access or split processing, not credential parameter handling
        String message = exception.getMessage().toLowerCase();
        // With Quickwit merge integration, files that can't be accessed get added to skipped splits
        // This is expected behavior - the test verifies credential extraction worked
        assertTrue(message.contains("skipped") || message.contains("failed") || message.contains("access"),
            "Should fail on S3 access with new Quickwit merge behavior: " + exception.getMessage());
    }

    @Test
    @DisplayName("Test mixed configuration parameter extraction")
    public void testMixedConfigurationExtraction() {
        // Test complex configuration with all optional parameters
        QuickwitSplit.AwsConfig complexConfig = new QuickwitSplit.AwsConfig(
            "complex-access-key",
            "complex-secret-key", 
            "complex-session-token",
            "ap-southeast-2",
            "https://custom-s3.example.com:8443",
            true
        );
        
        List<String> deleteQueries = Arrays.asList("status:deleted", "expired:true");
        
        QuickwitSplit.MergeConfig complexMergeConfig = new QuickwitSplit.MergeConfig(
            "complex-index-uid",
            "complex-source-id",
            "complex-node-id", 
            "complex-mapping-v2",
            12345L,
            deleteQueries,
            complexConfig
        );
        
        // Verify all parameters are accessible before calling native code
        assertEquals("complex-access-key", complexMergeConfig.getAwsConfig().getAccessKey());
        assertEquals("complex-secret-key", complexMergeConfig.getAwsConfig().getSecretKey());
        assertEquals("complex-session-token", complexMergeConfig.getAwsConfig().getSessionToken());
        assertEquals("ap-southeast-2", complexMergeConfig.getAwsConfig().getRegion());
        assertEquals("https://custom-s3.example.com:8443", complexMergeConfig.getAwsConfig().getEndpoint());
        assertTrue(complexMergeConfig.getAwsConfig().isForcePathStyle());
        assertEquals("complex-index-uid", complexMergeConfig.getIndexUid());
        assertEquals(12345L, complexMergeConfig.getPartitionId());
        assertEquals(deleteQueries, complexMergeConfig.getDeleteQueries());
        
        // Should fail on custom endpoint connection, not parameter extraction
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QuickwitSplit.mergeSplits(testS3Splits, testOutputPath, complexMergeConfig);
        });
        
        // Verify complex parameters don't cause extraction errors
        String message = exception.getMessage().toLowerCase();
        assertFalse(message.contains("parameter") && message.contains("extraction"),
            "Should not fail on parameter extraction for complex config: " + exception.getMessage());
    }
}
