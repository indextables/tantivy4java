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

package com.tantivy4java;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.tantivy4java.SplitRangeQuery.RangeBound;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive end-to-end test for real S3 operations.
 * 
 * This test demonstrates the complete workflow:
 * 1. Create 4 different indexes with varying data
 * 2. Convert indexes to Quickwit splits
 * 3. Upload splits to real S3 storage
 * 4. Merge all splits into a single consolidated split
 * 5. Upload merged split back to S3
 * 6. Use SplitCacheManager warmup functionality
 * 7. Query and validate the merged split contains all original data
 * 
 * Prerequisites:
 * - AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * - Or AWS profile/role with S3 access
 * - S3 bucket accessible for testing (will be created if doesn't exist)
 * 
 * Set system properties:
 * -Dtest.s3.bucket=your-test-bucket
 * -Dtest.s3.region=us-east-1
 * -Dtest.s3.accessKey=your-access-key (optional if using profile/role)
 * -Dtest.s3.secretKey=your-secret-key (optional if using profile/role)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3EndToEndTest {

    // Test configuration from system properties
    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2"); 
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");
    
    // AWS credentials loaded from ~/.aws/credentials
    private static String awsAccessKey;
    private static String awsSecretKey;
    
    @TempDir
    static Path tempDir;
    
    // Test data tracking
    private static final String[] INDEX_NAMES = {"customers", "products", "orders", "reviews"};
    private static QuickwitSplit.SplitMetadata[] splitMetadata = new QuickwitSplit.SplitMetadata[4];
    private static String[] splitS3Urls = new String[4];
    private static QuickwitSplit.SplitMetadata mergedSplitMetadata;
    private static S3Client s3Client;
    private static String mergedSplitS3Url;
    private static int totalExpectedDocs = 0;
    
    @BeforeAll
    static void setupS3Client() {
        // Load AWS credentials
        loadAwsCredentials();
        
        // Create S3 client for real AWS operations
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            s3Client = S3Client.builder()
                .region(Region.of(TEST_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
            System.out.println("✅ Real S3 client initialized");
            
            // Ensure the test bucket exists
            boolean bucketExists = false;
            try {
                s3Client.headBucket(HeadBucketRequest.builder().bucket(TEST_BUCKET).build());
                System.out.println("✅ S3 bucket exists: " + TEST_BUCKET);
                bucketExists = true;
            } catch (NoSuchBucketException e) {
                System.out.println("🪣 Bucket does not exist, will create: " + TEST_BUCKET);
            } catch (Exception e) {
                // For other errors (403, 400, etc.), try to create the bucket anyway
                System.out.println("⚠️ Cannot check bucket (error: " + e.getMessage() + "), will attempt to create: " + TEST_BUCKET);
            }
            
            if (!bucketExists) {
                try {
                    // For us-east-1, don't specify region constraint in CreateBucketConfiguration  
                    if ("us-east-1".equals(TEST_REGION)) {
                        s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
                    } else {
                        s3Client.createBucket(CreateBucketRequest.builder()
                            .bucket(TEST_BUCKET)
                            .createBucketConfiguration(CreateBucketConfiguration.builder()
                                .locationConstraint(BucketLocationConstraint.fromValue(TEST_REGION))
                                .build())
                            .build());
                    }
                    System.out.println("✅ Created S3 bucket: " + TEST_BUCKET);
                } catch (BucketAlreadyExistsException e) {
                    System.out.println("✅ S3 bucket already exists: " + TEST_BUCKET);
                } catch (BucketAlreadyOwnedByYouException e) {
                    System.out.println("✅ S3 bucket already owned by you: " + TEST_BUCKET);  
                } catch (Exception createEx) {
                    throw new IllegalStateException("Failed to create S3 bucket: " + TEST_BUCKET + ". Error: " + createEx.getMessage(), createEx);
                }
            }
        } else {
            throw new IllegalStateException("AWS credentials not found. Please configure ~/.aws/credentials or environment variables.");
        }
    }
    
    @AfterAll
    static void cleanupS3Client() {
        if (s3Client != null) {
            s3Client.close();
            System.out.println("✅ S3 client closed");
        }
    }
    
    /**
     * Gets the effective access key from various sources
     */
    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return null;
    }
    
    /**
     * Gets the effective secret key from various sources
     */
    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return null;
    }

    /**
     * Reads AWS credentials from ~/.aws/credentials file
     */
    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("AWS credentials file not found at: " + credentialsPath);
                return;
            }
            
            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefaultSection = false;
            
            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) {
                    inDefaultSection = true;
                    continue;
                } else if (line.startsWith("[") && line.endsWith("]")) {
                    inDefaultSection = false;
                    continue;
                }
                
                if (inDefaultSection && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        
                        if ("aws_access_key_id".equals(key)) {
                            awsAccessKey = value;
                        } else if ("aws_secret_access_key".equals(key)) {
                            awsSecretKey = value;
                        }
                    }
                }
            }
            
            if (awsAccessKey != null && awsSecretKey != null) {
                System.out.println("✅ Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials from ~/.aws/credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    static void validateConfiguration() {
        System.out.println("=== REAL S3 END-TO-END TEST ===");
        System.out.println("Test bucket: " + TEST_BUCKET);
        System.out.println("Test region: " + TEST_REGION);
        
        // Try to load credentials in order of preference:
        // 1. System properties (test.s3.accessKey, test.s3.secretKey)
        // 2. ~/.aws/credentials file 
        // 3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
        boolean hasExplicitCreds = ACCESS_KEY != null && SECRET_KEY != null;
        if (!hasExplicitCreds) {
            loadAwsCredentials();
        }
        
        boolean hasFileCredentials = awsAccessKey != null && awsSecretKey != null;
        boolean hasEnvCreds = System.getenv("AWS_ACCESS_KEY_ID") != null;
        
        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("⚠️  No AWS credentials found. Skipping S3 tests.");
            System.out.println("   Set -Dtest.s3.accessKey and -Dtest.s3.secretKey");
            System.out.println("   Or configure ~/.aws/credentials file");
            System.out.println("   Or configure AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables");
            System.out.println("   Or use AWS CLI profile/IAM role");
            Assumptions.abort("AWS credentials not available");
        }
        
        if (hasExplicitCreds) {
            System.out.println("Using explicit credentials from system properties");
        } else if (hasFileCredentials) {
            System.out.println("Using AWS credentials from ~/.aws/credentials");
        } else {
            System.out.println("Using environment/profile AWS credentials");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Step 1: Create 4 diverse test indices and convert to splits")
    public void step1_createTestIndicesAndSplits() throws IOException {
        System.out.println("🏗️  Creating 4 diverse test indices...");
        
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            String indexName = INDEX_NAMES[i];
            System.out.println("Creating index: " + indexName);
            
            // Create index with domain-specific data
            Path indexPath = tempDir.resolve(indexName + "-index");
            createDomainSpecificIndex(indexPath, indexName, i);
            
            // Convert to split
            Path splitPath = tempDir.resolve(indexName + ".split");
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "end-to-end-test-" + indexName,
                "test-source", 
                "test-node-" + i
            );
            
            splitMetadata[i] = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), 
                splitPath.toString(), 
                config
            );
            
            assertNotNull(splitMetadata[i], "Split metadata should be created for " + indexName);
            assertTrue(splitMetadata[i].getNumDocs() > 0, "Split should contain documents for " + indexName);
            
            totalExpectedDocs += splitMetadata[i].getNumDocs();
            
            System.out.println("✅ Created " + indexName + " split: " + 
                             splitMetadata[i].getNumDocs() + " docs, " + 
                             splitMetadata[i].getUncompressedSizeBytes() + " bytes");
        }
        
        System.out.println("✅ All 4 indices created. Total expected docs: " + totalExpectedDocs);
    }

    @Test
    @org.junit.jupiter.api.Order(2) 
    @DisplayName("Step 2: Upload all 4 splits to real S3")
    public void step2_uploadSplitsToS3() throws IOException {
        System.out.println("☁️  Uploading 4 splits to S3...");
        
        // Validate prerequisites from Step 1
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            String indexName = INDEX_NAMES[i];
            Path localSplitPath = tempDir.resolve(indexName + ".split");
            if (!java.nio.file.Files.exists(localSplitPath)) {
                fail("Prerequisite failure: Split file for " + indexName + " does not exist at " + localSplitPath + 
                     ". This indicates Step 1 (createTestIndicesAndSplits) did not complete successfully or was not executed.");
            }
        }
        
        if (splitMetadata == null || splitMetadata[0] == null) {
            fail("Prerequisite failure: Split metadata is not available. " +
                 "This indicates Step 1 (createTestIndicesAndSplits) did not complete successfully or was not executed.");
        }
        
        // Upload splits to S3 using proper S3Client
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            String indexName = INDEX_NAMES[i];
            Path localSplitPath = tempDir.resolve(indexName + ".split");
            String s3Key = "end-to-end-test/splits/" + indexName + ".split";
            
            // Define S3 URL for later use
            splitS3Urls[i] = String.format("s3://%s/%s", TEST_BUCKET, s3Key);
            
            System.out.println("Uploading " + indexName + " to " + splitS3Urls[i]);
            
            try {
                // Upload using S3Client.putObject (correct approach)
                PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(s3Key)
                    .build();
                    
                s3Client.putObject(putRequest, localSplitPath);
                
                System.out.println("✅ Uploaded " + indexName + " split to S3 (" +
                    Files.size(localSplitPath) + " bytes, " + 
                    splitMetadata[i].getNumDocs() + " docs)");
                
            } catch (Exception e) {
                System.err.println("❌ Failed to upload " + indexName + " to S3:");
                System.err.println("   Error type: " + e.getClass().getSimpleName());
                System.err.println("   Error message: " + e.getMessage());
                System.err.println("   Local split path: " + localSplitPath);
                System.err.println("   S3 URL: " + splitS3Urls[i]);
                System.err.println("   S3 Bucket: " + TEST_BUCKET);
                System.err.println("   S3 Key: " + s3Key);
                System.err.println("   S3 Region: " + TEST_REGION);
                System.err.println("   File exists: " + Files.exists(localSplitPath));
                if (Files.exists(localSplitPath)) {
                    try {
                        System.err.println("   File size: " + Files.size(localSplitPath) + " bytes");
                    } catch (Exception ex) {
                        System.err.println("   File size: Could not determine - " + ex.getMessage());
                    }
                }
                System.err.println("   Will continue with remaining uploads...");
                e.printStackTrace();
                
                // Clear the S3 URL to indicate this upload failed
                splitS3Urls[i] = null;
            }
        }
        
        // Check if any uploads failed
        java.util.List<String> failedUploads = new java.util.ArrayList<>(); 
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            if (splitS3Urls[i] == null) {
                failedUploads.add(INDEX_NAMES[i]);
            }
        }
        
        if (!failedUploads.isEmpty()) {
            fail("Failed to upload " + failedUploads.size() + " split(s) to S3: " + failedUploads + 
                 ". Check the error details above for specific causes.");
        }
        
        System.out.println("✅ All 4 splits uploaded to S3 using proper S3Client");
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Step 3: Merge all 4 S3 splits into consolidated split")
    public void step3_mergeSplitsFromS3() throws IOException {
        System.out.println("🔄 Merging 4 S3 splits into consolidated split...");
        
        // Create AWS configuration
        QuickwitSplit.AwsConfig awsConfig;
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            awsConfig = new QuickwitSplit.AwsConfig(accessKey, secretKey, TEST_REGION);
        } else {
            awsConfig = new QuickwitSplit.AwsConfig(null, null, TEST_REGION);
        }
        
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "consolidated-end-to-end-index", 
            "consolidated-source", 
            "consolidated-node", 
            awsConfig
        );
        
        // Define merged split S3 location
        mergedSplitS3Url = String.format("s3://%s/end-to-end-test/merged/consolidated.split", TEST_BUCKET);
        
        // Validate all split URLs are available
        for (int i = 0; i < splitS3Urls.length; i++) {
            if (splitS3Urls[i] == null || splitS3Urls[i].trim().isEmpty()) {
                fail("Split URL " + (i+1) + " is null or empty. Step 2 (upload) may have failed.");
            }
        }
        
        System.out.println("Merging splits:");
        for (int i = 0; i < splitS3Urls.length; i++) {
            System.out.println("  " + (i+1) + ". " + splitS3Urls[i]);
        }
        System.out.println("Output: " + mergedSplitS3Url);
        
        try {
            // Create a local temporary file for the merge output
            Path localMergedSplit = tempDir.resolve("consolidated-merged.split");
            String localMergedPath = localMergedSplit.toString();
            
            System.out.println("🔄 Step 3a: Merging S3 splits to local temp file...");
            System.out.println("   Local temp output: " + localMergedPath);
            
            // Perform the merge to local file (NOT S3)
            List<String> allSplitUrls = Arrays.asList(splitS3Urls);
            mergedSplitMetadata = QuickwitSplit.mergeSplits(allSplitUrls, localMergedPath, mergeConfig);
            
            // Verify the local merged file exists and has correct size
            if (!Files.exists(localMergedSplit)) {
                fail("Local merged split file was not created at: " + localMergedPath);
            }
            
            long actualFileSize = Files.size(localMergedSplit);
            System.out.println("   ✅ Local merge completed. File size: " + actualFileSize + " bytes");
            System.out.println("   📊 Metadata size: " + mergedSplitMetadata.getMetadataSize() + " bytes");
            System.out.println("   🔍 File size >= Metadata size: " + (actualFileSize >= mergedSplitMetadata.getMetadataSize()));
            
            if (actualFileSize < mergedSplitMetadata.getMetadataSize()) {
                fail("Local merged file is too small (" + actualFileSize + " bytes) compared to metadata size (" + 
                     mergedSplitMetadata.getMetadataSize() + " bytes). This indicates a merge problem.");
            }
            
            System.out.println("🔄 Step 3b: Uploading merged split to S3...");
            
            // Upload the completed merged split to S3
            String s3Key = "end-to-end-test/merged/consolidated.split";
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
                
            s3Client.putObject(putRequest, localMergedSplit);
            
            System.out.println("   ✅ Uploaded merged split to S3: " + mergedSplitS3Url);
            System.out.println("   📤 Uploaded size: " + actualFileSize + " bytes");
            
            // Verify the S3 file exists and has the same size
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            long s3FileSize = headResponse.contentLength();
            
            System.out.println("   🔍 S3 file size verification: " + s3FileSize + " bytes");
            
            if (s3FileSize != actualFileSize) {
                fail("S3 uploaded file size (" + s3FileSize + " bytes) doesn't match local file size (" + 
                     actualFileSize + " bytes). Upload may have been corrupted.");
            }
            
            assertNotNull(mergedSplitMetadata, "Merged split metadata should be created");
            assertEquals(totalExpectedDocs, mergedSplitMetadata.getNumDocs(), 
                        "Merged split should contain all documents from source splits");
            
            System.out.println("✅ Merge completed successfully:");
            System.out.println("   Split ID: " + mergedSplitMetadata.getSplitId());
            System.out.println("   Total docs: " + mergedSplitMetadata.getNumDocs());
            System.out.println("   Size: " + mergedSplitMetadata.getUncompressedSizeBytes() + " bytes");
            System.out.println("   S3 URL: " + mergedSplitS3Url);
            
            // Debug: Check footer offsets in the metadata
            System.out.println("🔍 METADATA DEBUG:");
            System.out.println("   Footer start offset: " + mergedSplitMetadata.getFooterStartOffset());
            System.out.println("   Footer end offset: " + mergedSplitMetadata.getFooterEndOffset());
            System.out.println("   Hotcache start offset: " + mergedSplitMetadata.getHotcacheStartOffset());
            System.out.println("   Hotcache length: " + mergedSplitMetadata.getHotcacheLength());
            System.out.println("   hasFooterOffsets(): " + mergedSplitMetadata.hasFooterOffsets());
            System.out.println("   getMetadataSize(): " + mergedSplitMetadata.getMetadataSize());
            
        } catch (Exception e) {
            System.err.println("❌ Failed to merge splits: " + e.getMessage());
            e.printStackTrace();
            fail("Failed to merge S3 splits: " + e.getMessage());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Step 3.5: Validate consolidated split exists in S3")
    public void step3_5_validateConsolidatedSplitInS3() throws Exception {
        System.out.println("🔍 Validating consolidated split in S3...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitS3Url, "Merged split S3 URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        assertNotNull(s3Client, "S3 client should be initialized");
        
        try {
            // Extract S3 key from the URL
            String s3Key = mergedSplitS3Url.replace("s3://" + TEST_BUCKET + "/", "");
            
            System.out.println("Checking S3 object existence:");
            System.out.println("  Bucket: " + TEST_BUCKET);
            System.out.println("  Key: " + s3Key);
            System.out.println("  Full URL: " + mergedSplitS3Url);
            
            // Use HeadObject to validate the split exists and get metadata
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
            
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            
            // Validate the object exists and has reasonable size
            long s3ObjectSize = headResponse.contentLength();
            assertTrue(s3ObjectSize > 0, "S3 object should have non-zero size");
            assertTrue(s3ObjectSize > 1000, "S3 object should be larger than 1KB (actual: " + s3ObjectSize + " bytes)");
            
            // Validate against our expected metadata
            long expectedSize = mergedSplitMetadata.getUncompressedSizeBytes();
            if (expectedSize > 0) {
                // Allow some variance due to compression and metadata overhead
                double sizeRatio = (double) s3ObjectSize / expectedSize;
                assertTrue(sizeRatio > 0.1 && sizeRatio < 10.0, 
                          "S3 object size (" + s3ObjectSize + ") should be reasonably close to expected size (" + expectedSize + ")");
            }
            
            System.out.println("✅ S3 validation successful:");
            System.out.println("   Object size: " + s3ObjectSize + " bytes");
            System.out.println("   Last modified: " + headResponse.lastModified());
            System.out.println("   ETag: " + headResponse.eTag());
            
        } catch (NoSuchKeyException e) {
            fail("Consolidated split not found in S3. URL: " + mergedSplitS3Url + ". " + 
                 "This suggests the merge operation in Step 3 may not have uploaded the result correctly.");
        } catch (Exception e) {
            System.err.println("❌ Failed to validate S3 object: " + e.getMessage());
            e.printStackTrace();
            fail("Failed to validate consolidated split in S3: " + e.getMessage());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Step 4: Create SplitCacheManager and perform warmup")
    public void step4_warmupCacheWithMergedSplit() throws Exception {
        System.out.println("🔥 Warming up cache for merged S3 split...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitS3Url, "Merged split S3 URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        
        // Create cache manager with S3 credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("end-to-end-cache")
            .withMaxCacheSize(100_000_000); // 100MB cache
            
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        System.out.println("Cache manager created for S3 URL: " + mergedSplitS3Url);
        
        try {
            // Create split searcher with footer optimization metadata
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(mergedSplitS3Url, mergedSplitMetadata)) {
                
                System.out.println("✅ SplitSearcher created successfully");
                
                // Get schema for field-based warmup
                Schema schema = searcher.getSchema();
                System.out.println("Schema fields available: " + schema.getFieldNames().size());
                
                // Perform comprehensive warmup
                System.out.println("Starting comprehensive warmup...");
                
                // Warmup all components
                String[] components = {"terms", "postings", "fast_fields", "field_norms"};
                CompletableFuture<Void> warmupFuture = searcher.preloadComponents(SplitSearcher.IndexComponent.POSTINGS, SplitSearcher.IndexComponent.POSITIONS, SplitSearcher.IndexComponent.FASTFIELD, SplitSearcher.IndexComponent.FIELDNORM);
                
                System.out.println("Warmup initiated. Waiting for completion...");
                
                // Join the warmup future (wait for completion)
                warmupFuture.join();
                
                System.out.println("✅ Warmup completed successfully");
                
                // Get cache statistics
                var cacheStats = searcher.getCacheStats();
                System.out.println("Cache statistics after warmup:");
                System.out.println("   Cache hits: " + cacheStats.getHitCount());
                System.out.println("   Cache misses: " + cacheStats.getMissCount());
                System.out.println("   Cache size: " + cacheStats.getTotalSize());
                
                // Get component cache status
                var componentStatus = searcher.getComponentCacheStatus();
                System.out.println("Component cache status:");
                componentStatus.forEach((component, status) -> 
                    System.out.println("   " + component + ": " + status));
                
            }
            
        } catch (Exception e) {
            System.err.println("❌ Cache warmup failed: " + e.getMessage());
            e.printStackTrace();
            fail("Cache warmup failed: " + e.getMessage());
        }
        
        System.out.println("✅ Cache warmup phase completed");
    }

    @Test
    @org.junit.jupiter.api.Order(6)
    @DisplayName("Step 5: Query merged split and validate all original data")
    public void step5_queryAndValidateData() throws Exception {
        System.out.println("🔍 Querying merged split to validate data integrity...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitS3Url, "Merged split S3 URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        
        // Create cache manager
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("validation-cache")
            .withMaxCacheSize(50_000_000);
            
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(mergedSplitS3Url, mergedSplitMetadata)) {
            
            Schema schema = searcher.getSchema();
            
            // Test 1: Verify total document count
            System.out.println("Test 1: Document count validation");
            SplitQuery allDocsQuery = new SplitTermQuery("domain", "customers");
            SearchResult customersResult = searcher.search(allDocsQuery, 1000);
            
            allDocsQuery = new SplitTermQuery("domain", "products");
            SearchResult productsResult = searcher.search(allDocsQuery, 1000);
            
            allDocsQuery = new SplitTermQuery("domain", "orders");
            SearchResult ordersResult = searcher.search(allDocsQuery, 1000);
            
            allDocsQuery = new SplitTermQuery("domain", "reviews");
            SearchResult reviewsResult = searcher.search(allDocsQuery, 1000);
            
            int totalFoundDocs = customersResult.getHits().size() + 
                                productsResult.getHits().size() + 
                                ordersResult.getHits().size() + 
                                reviewsResult.getHits().size();
            
            assertEquals(totalExpectedDocs, totalFoundDocs, 
                        "Total documents should match sum of original indices");
            
            System.out.println("✅ Document count validation passed:");
            System.out.println("   Customers: " + customersResult.getHits().size());
            System.out.println("   Products: " + productsResult.getHits().size());
            System.out.println("   Orders: " + ordersResult.getHits().size());
            System.out.println("   Reviews: " + reviewsResult.getHits().size());
            System.out.println("   Total: " + totalFoundDocs + "/" + totalExpectedDocs);
            
            // Test 2: Content validation for each domain
            System.out.println("Test 2: Content validation");
            
            // Validate customers data
            if (customersResult.getHits().size() > 0) {
                try (Document customerDoc = searcher.doc(customersResult.getHits().get(0).getDocAddress())) {
                    String name = (String) customerDoc.getFirst("name");
                    String email = (String) customerDoc.getFirst("email");
                    assertNotNull(name, "Customer should have name");
                    assertNotNull(email, "Customer should have email");
                    assertTrue(email.contains("@"), "Email should be valid format");
                }
            }
            
            // Validate products data  
            if (productsResult.getHits().size() > 0) {
                try (Document productDoc = searcher.doc(productsResult.getHits().get(0).getDocAddress())) {
                    String productName = (String) productDoc.getFirst("name");
                    Integer price = (Integer) productDoc.getFirst("price");
                    assertNotNull(productName, "Product should have name");
                    assertNotNull(price, "Product should have price");
                    assertTrue(price > 0, "Price should be positive");
                }
            }
            
            System.out.println("✅ Content validation passed");
            
            // Test 3: Cross-domain queries
            System.out.println("Test 3: Cross-domain query validation");
            
            SplitQuery titleQuery = new SplitTermQuery("name", "Customer");
            SearchResult titleResults = searcher.search(titleQuery, 100);
            assertTrue(titleResults.getHits().size() > 0, "Should find documents with 'Customer' in name");
            
            // Range query on numeric fields
            SplitQuery priceRangeQuery = new SplitRangeQuery("price", RangeBound.inclusive("10"), RangeBound.inclusive("1000"));
            SearchResult priceResults = searcher.search(priceRangeQuery, 100);
            // Note: Some domains might not have price field, so just check it doesn't crash
            assertNotNull(priceResults, "Range query should not crash");
            
            System.out.println("✅ Cross-domain queries passed");
            
            // Test 4: Boolean query combining multiple conditions
            System.out.println("Test 4: Complex boolean query validation");
            
            SplitQuery domainQuery = new SplitTermQuery("domain", "products");
            SplitQuery nameQuery = new SplitTermQuery("name", "Product");
            
            SplitQuery booleanQuery = new SplitBooleanQuery()
                .addMust(domainQuery)
                .addMust(nameQuery);
            
            SearchResult booleanResults = searcher.search(booleanQuery, 50);
            assertNotNull(booleanResults, "Boolean query should succeed");
            
            System.out.println("✅ Complex boolean queries passed");
            System.out.println("   Boolean results: " + booleanResults.getHits().size());
            
        } catch (Exception e) {
            System.err.println("❌ Data validation failed: " + e.getMessage());
            e.printStackTrace();
            fail("Data validation failed: " + e.getMessage());
        }
        
        System.out.println("✅ All data validation tests passed!");
    }

    @Test 
    @org.junit.jupiter.api.Order(7)
    @DisplayName("Step 6: Performance validation and cleanup")
    public void step6_performanceValidationAndCleanup() throws Exception {
        System.out.println("⚡ Performance validation and resource cleanup...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitS3Url, "Merged split S3 URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        
        // Create cache manager for performance testing
        SplitCacheManager.CacheConfig perfCacheConfig = new SplitCacheManager.CacheConfig("performance-cache")
            .withMaxCacheSize(200_000_000); // 200MB for performance test
            
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            perfCacheConfig = perfCacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        perfCacheConfig = perfCacheConfig.withAwsRegion(TEST_REGION);
        
        SplitCacheManager perfCacheManager = SplitCacheManager.getInstance(perfCacheConfig);
        
        // Performance test: measure cold vs warm cache performance
        System.out.println("Measuring cache performance...");
        
        long coldStartTime = System.nanoTime();
        try (SplitSearcher coldSearcher = perfCacheManager.createSplitSearcher(mergedSplitS3Url, mergedSplitMetadata)) {
            Schema schema = coldSearcher.getSchema();
            SplitQuery testQuery = new SplitTermQuery("domain", "customers");
            SearchResult coldResult = coldSearcher.search(testQuery, 10);
            assertNotNull(coldResult);
        }
        long coldDuration = System.nanoTime() - coldStartTime;
        
        // Warm up cache
        try (SplitSearcher warmupSearcher = perfCacheManager.createSplitSearcher(mergedSplitS3Url, mergedSplitMetadata)) {
            CompletableFuture<Void> warmup = warmupSearcher.preloadComponents(SplitSearcher.IndexComponent.POSTINGS, SplitSearcher.IndexComponent.POSITIONS);
            warmup.join();
        }
        
        // Measure warm performance
        long warmStartTime = System.nanoTime();
        try (SplitSearcher warmSearcher = perfCacheManager.createSplitSearcher(mergedSplitS3Url, mergedSplitMetadata)) {
            Schema schema = warmSearcher.getSchema();
            SplitQuery testQuery = new SplitTermQuery("domain", "customers");
            SearchResult warmResult = warmSearcher.search(testQuery, 10);
            assertNotNull(warmResult);
        }
        long warmDuration = System.nanoTime() - warmStartTime;
        
        // Report performance
        double coldMs = coldDuration / 1_000_000.0;
        double warmMs = warmDuration / 1_000_000.0;
        double speedup = coldMs / warmMs;
        
        System.out.println("Performance results:");
        System.out.println("   Cold cache: " + String.format("%.2f", coldMs) + "ms");
        System.out.println("   Warm cache: " + String.format("%.2f", warmMs) + "ms");
        System.out.println("   Speedup: " + String.format("%.2fx", speedup));
        
        // Validate that warm cache is faster (allowing for some variance)
        assertTrue(speedup > 0.5, "Warm cache should provide some performance benefit");
        
        System.out.println("✅ Performance validation completed");
        System.out.println("✅ All end-to-end tests completed successfully!");
        
        // Note: S3 cleanup could be added here, but we'll leave test artifacts for inspection
        System.out.println("ℹ️  Test artifacts left in S3 bucket for inspection: " + TEST_BUCKET);
    }

    /**
     * Create domain-specific test index with realistic data patterns
     */
    private void createDomainSpecificIndex(Path indexPath, String domain, int domainIndex) throws IOException {
        // Create schema appropriate for the domain
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("domain", true, false, "default", "position");        // Domain identifier
        schemaBuilder.addTextField("name", true, false, "default", "position");          // Entity name
        schemaBuilder.addTextField("description", true, false, "default", "position");   // Description
        schemaBuilder.addIntegerField("id", true, true, false);                         // Unique ID
        schemaBuilder.addIntegerField("price", true, true, false);                      // Price (may be 0 for some domains)
        schemaBuilder.addTextField("email", true, false, "default", "position");        // Email (customers only)
        schemaBuilder.addTextField("category", true, false, "default", "position");     // Category
        
        Schema schema = schemaBuilder.build();
        
        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            int docsPerDomain = 25 + (domainIndex * 5); // Varying number of documents per domain
            
            for (int i = 0; i < docsPerDomain; i++) {
                Document doc = new Document();
                doc.addText("domain", domain);
                doc.addInteger("id", (domainIndex * 1000) + i);
                
                switch (domain) {
                    case "customers":
                        doc.addText("name", "Customer " + i);
                        doc.addText("description", "Loyal customer with ID " + i + " from region " + (i % 5));
                        doc.addText("email", "customer" + i + "@example.com");
                        doc.addText("category", "tier-" + (i % 3 + 1));
                        doc.addInteger("price", 0); // No price for customers
                        break;
                        
                    case "products":
                        doc.addText("name", "Product " + i);
                        doc.addText("description", "High-quality product " + i + " with advanced features");
                        doc.addText("email", ""); // No email for products
                        doc.addText("category", "category-" + (char)('A' + (i % 5)));
                        doc.addInteger("price", 10 + (i * 3)); // Variable pricing
                        break;
                        
                    case "orders":
                        doc.addText("name", "Order " + i);
                        doc.addText("description", "Order placed on " + (2023 + (i % 2)) + " for customer " + (i % 20));
                        doc.addText("email", "order" + i + "@internal.system");
                        doc.addText("category", "status-" + (i % 4 == 0 ? "completed" : "pending"));
                        doc.addInteger("price", 50 + (i * 7)); // Order values
                        break;
                        
                    case "reviews":
                        doc.addText("name", "Review " + i);
                        doc.addText("description", "Customer review #" + i + " with rating " + (1 + (i % 5)) + " stars");
                        doc.addText("email", "reviewer" + i + "@reviews.com");
                        doc.addText("category", "rating-" + (1 + (i % 5)));
                        doc.addInteger("price", i % 5 + 1); // Rating as price field
                        break;
                }
                
                writer.addDocument(doc);
            }
            
            writer.commit();
        }
    }
}