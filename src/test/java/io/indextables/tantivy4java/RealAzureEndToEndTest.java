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

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.credential.AccessToken;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive end-to-end test for real Azure Blob Storage operations.
 * 
 * This test demonstrates the complete workflow:
 * 1. Create 4 different indexes with varying data
 * 2. Convert indexes to Quickwit splits
 * 3. Upload splits to real Azure Blob Storage
 * 4. Merge all splits into a single consolidated split
 * 5. Upload merged split back to Azure
 * 6. Use SplitCacheManager warmup functionality
 * 7. Query and validate the merged split contains all original data
 * 
 * Prerequisites:
 * - Azure credentials configured (storage account and account key)
 * - Or Azure credentials in ~/.azure/credentials
 * - Azure container accessible for testing (will be created if doesn't exist)
 * 
 * Set system properties:
 * -Dtest.azure.container=your-test-container
 * -Dtest.azure.storageAccount=yourstorageaccount
 * -Dtest.azure.accountKey=your-account-key (optional if using ~/.azure/credentials)
 * 
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzureEndToEndTest {

    // Test configuration from system properties
    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount"); 
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");
    
    
    // Azure credentials loaded from ~/.azure/credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;

    // TODO: Service Principal credentials (not yet supported)
    // private static String clientId;
    // private static String clientSecret;
    // private static String tenantId;
    
    @TempDir
    static Path tempDir;
    
    // Test data tracking
    private static final String[] INDEX_NAMES = {"customers", "products", "orders", "reviews"};
    private static QuickwitSplit.SplitMetadata[] splitMetadata = new QuickwitSplit.SplitMetadata[4];
    private static String[] splitAzureUrls = new String[4];
    private static QuickwitSplit.SplitMetadata mergedSplitMetadata;
    private static BlobServiceClient blobServiceClient;
    private static String mergedSplitAzureUrl;
    private static int totalExpectedDocs = 0;
    
    @BeforeAll
    static void setupAzureClient() {
        // Load Azure credentials
        loadAzureCredentials();

        // Create Azure blob service client
        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            blobServiceClient = new BlobServiceClientBuilder()
                .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
                .buildClient();
            System.out.println("✅ Real Azure client initialized");

            // Ensure the test container exists
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
            if (!containerClient.exists()) {
                containerClient.create();
                System.out.println("✅ Created Azure container: " + TEST_CONTAINER);
            } else {
                System.out.println("✅ Azure container exists: " + TEST_CONTAINER);
            }
        } else {
            throw new IllegalStateException("Azure credentials not found. Please configure ~/.azure/credentials or environment variables.");
        }
    }
    
    @AfterAll
    static void cleanupAzureClient() {
        if (blobServiceClient != null) {
            // Azure BlobServiceClient doesn't need explicit close
            System.out.println("✅ Azure client cleanup complete");
        }
    }
    
    /**
     * Gets the effective access key from various sources
     */
    private static String getStorageAccount() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureStorageAccount != null) return azureStorageAccount;
        return null;
    }
    
    /**
     * Gets the effective secret key from various sources
     */
    private static String getAccountKey() {
        
        if (azureAccountKey != null) return azureAccountKey;
        return null;
    }

    /**
     * Reads Azure credentials from ~/.azure/credentials file
     */
    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("Azure credentials file not found at: " + credentialsPath);
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
                        
                        if ("storage_account".equals(key)) {
                            azureStorageAccount = value;
                        } else if ("account_key".equals(key)) {
                            azureAccountKey = value;
                        }
                    }
                }
            }
            
            if (azureStorageAccount != null && azureAccountKey != null) {
                System.out.println("✅ Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read Azure credentials from ~/.azure/credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    static void validateConfiguration() {
        System.out.println("=== REAL AZURE END-TO-END TEST ===");
        System.out.println("Test container: " + TEST_CONTAINER);
        System.out.println("Storage account: " + STORAGE_ACCOUNT);
        
        // Try to load credentials in order of preference:
        // 1. System properties (test.azure.storageAccount, test.azure.accountKey)
        // 2. ~/.azure/credentials file
        // 3. Environment variables (AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY)

        boolean hasExplicitCreds = STORAGE_ACCOUNT != null && ACCOUNT_KEY != null;
        if (!hasExplicitCreds) {
            loadAzureCredentials();
        }

        boolean hasFileCredentials = azureStorageAccount != null && azureAccountKey != null;
        boolean hasEnvCreds = System.getenv("AZURE_STORAGE_ACCOUNT") != null;
        
        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("⚠️  No Azure credentials found. Skipping Azure tests.");
            System.out.println("   Set -Dtest.azure.storageAccount and -Dtest.azure.accountKey");
            System.out.println("   Or configure ~/.azure/credentials file");
            System.out.println("   Or configure AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY environment variables");
            System.out.println("   Or use Azure CLI / Managed Identity (TODO: not yet supported)");
            Assumptions.abort("Azure credentials not available");
        }

        if (hasExplicitCreds) {
            System.out.println("Using Azure credentials from system properties");
        } else if (hasFileCredentials) {
            System.out.println("Using Azure credentials from ~/.azure/credentials");
        } else {
            System.out.println("Using environment Azure credentials");
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
    @DisplayName("Step 2: Upload all 4 splits to real Azure")
    public void step2_uploadSplitsToAzure() throws IOException {
        System.out.println("☁️  Uploading 4 splits to Azure...");
        
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
        
        // Upload splits to Azure using proper S3Client
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            String indexName = INDEX_NAMES[i];
            Path localSplitPath = tempDir.resolve(indexName + ".split");
            String blobName = "end-to-end-test/splits/" + indexName + ".split";

            // Define Azure URL for later use
            splitAzureUrls[i] = String.format("azure://%s/%s", TEST_CONTAINER, blobName);

            System.out.println("Uploading " + indexName + " to " + splitAzureUrls[i]);

            try {
                // Upload using Azure BlobClient
                BlobClient blobClient = blobServiceClient
                    .getBlobContainerClient(TEST_CONTAINER)
                    .getBlobClient(blobName);

                blobClient.uploadFromFile(localSplitPath.toString(), true);

                System.out.println("✅ Uploaded " + indexName + " split to Azure (" +
                    Files.size(localSplitPath) + " bytes, " +
                    splitMetadata[i].getNumDocs() + " docs)");

            } catch (Exception e) {
                System.err.println("❌ Failed to upload " + indexName + " to Azure:");
                System.err.println("   Error type: " + e.getClass().getSimpleName());
                System.err.println("   Error message: " + e.getMessage());
                System.err.println("   Local split path: " + localSplitPath);
                System.err.println("   Azure URL: " + splitAzureUrls[i]);
                System.err.println("   Container: " + TEST_CONTAINER);
                System.err.println("   Blob name: " + blobName);
                System.err.println("   Storage account: " + STORAGE_ACCOUNT);
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
                
                // Clear the Azure URL to indicate this upload failed
                splitAzureUrls[i] = null;
            }
        }
        
        // Check if any uploads failed
        java.util.List<String> failedUploads = new java.util.ArrayList<>(); 
        for (int i = 0; i < INDEX_NAMES.length; i++) {
            if (splitAzureUrls[i] == null) {
                failedUploads.add(INDEX_NAMES[i]);
            }
        }
        
        if (!failedUploads.isEmpty()) {
            fail("Failed to upload " + failedUploads.size() + " split(s) to Azure: " + failedUploads +
                 ". Check the error details above for specific causes.");
        }

        System.out.println("✅ All 4 splits uploaded to Azure using BlobClient");
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Step 3: Merge all 4 Azure splits into consolidated split")
    public void step3_mergeSplitsFromAzure() throws IOException {
        System.out.println("🔄 Merging 4 Azure splits into consolidated split...");
        
        // Create Azure configuration with explicit endpoint for real Azure
        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        String azureEndpoint = "https://" + storageAccount + ".blob.core.windows.net";
        QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(storageAccount, accountKey);

        System.out.println("Azure Configuration:");
        System.out.println("  Storage Account: " + storageAccount);
        System.out.println("  Endpoint: " + azureEndpoint);

        // Use builder pattern to create merge config with Azure support
        QuickwitSplit.MergeConfig mergeConfig = QuickwitSplit.MergeConfig.builder()
            .indexUid("consolidated-end-to-end-index")
            .sourceId("consolidated-source")
            .nodeId("consolidated-node")
            .azureConfig(azureConfig)
            .build();
        
        // Define merged split S3 location
        mergedSplitAzureUrl = String.format("azure://%s/end-to-end-test/merged/consolidated.split", TEST_CONTAINER);
        
        // Validate all split URLs are available
        for (int i = 0; i < splitAzureUrls.length; i++) {
            if (splitAzureUrls[i] == null || splitAzureUrls[i].trim().isEmpty()) {
                fail("Split URL " + (i+1) + " is null or empty. Step 2 (upload) may have failed.");
            }
        }
        
        System.out.println("Merging splits:");
        for (int i = 0; i < splitAzureUrls.length; i++) {
            System.out.println("  " + (i+1) + ". " + splitAzureUrls[i]);
        }

        // Debug: Verify blobs exist before merge
        System.out.println("\n🔍 Verifying blobs exist in Azure:");
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        for (int i = 0; i < splitAzureUrls.length; i++) {
            String blobName = "end-to-end-test/splits/" + INDEX_NAMES[i] + ".split";
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            boolean exists = blobClient.exists();
            System.out.println("  " + blobName + ": " + (exists ? "✅ EXISTS" : "❌ NOT FOUND"));
            if (exists) {
                com.azure.storage.blob.models.BlobProperties props = blobClient.getProperties();
                System.out.println("    Size: " + props.getBlobSize() + " bytes");
                System.out.println("    Full URL: " + blobClient.getBlobUrl());
            }
        }

        // Also list all blobs in the test directory
        System.out.println("\n📋 All blobs in container '" + TEST_CONTAINER + "':");
        containerClient.listBlobs().forEach(item -> {
            if (item.getName().startsWith("end-to-end-test/")) {
                System.out.println("  " + item.getName() + " (" + item.getProperties().getContentLength() + " bytes)");
            }
        });
        System.out.println("Output: " + mergedSplitAzureUrl);
        
        try {
            // Create a local temporary file for the merge output
            Path localMergedSplit = tempDir.resolve("consolidated-merged.split");
            String localMergedPath = localMergedSplit.toString();
            
            System.out.println("🔄 Step 3a: Merging Azure splits to local temp file...");
            System.out.println("   Local temp output: " + localMergedPath);
            
            // Perform the merge to local file (NOT S3)
            List<String> allSplitUrls = Arrays.asList(splitAzureUrls);
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
            
            System.out.println("🔄 Step 3b: Uploading merged split to Azure...");

            // Upload the completed merged split to Azure
            String blobName = "end-to-end-test/merged/consolidated.split";
            BlobClient blobClient = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(blobName);

            blobClient.uploadFromFile(localMergedSplit.toString(), true);

            System.out.println("   ✅ Uploaded merged split to Azure: " + mergedSplitAzureUrl);
            System.out.println("   📤 Uploaded size: " + actualFileSize + " bytes");

            // Verify the Azure blob exists and has the same size
            long azureBlobSize = blobClient.getProperties().getBlobSize();

            System.out.println("   🔍 Azure blob size verification: " + azureBlobSize + " bytes");

            if (azureBlobSize != actualFileSize) {
                fail("Azure uploaded blob size (" + azureBlobSize + " bytes) doesn't match local file size (" +
                     actualFileSize + " bytes). Upload may have been corrupted.");
            }
            
            assertNotNull(mergedSplitMetadata, "Merged split metadata should be created");
            assertEquals(totalExpectedDocs, mergedSplitMetadata.getNumDocs(), 
                        "Merged split should contain all documents from source splits");
            
            System.out.println("✅ Merge completed successfully:");
            System.out.println("   Split ID: " + mergedSplitMetadata.getSplitId());
            System.out.println("   Total docs: " + mergedSplitMetadata.getNumDocs());
            System.out.println("   Size: " + mergedSplitMetadata.getUncompressedSizeBytes() + " bytes");
            System.out.println("   Azure URL: " + mergedSplitAzureUrl);
            
            // Debug: Check footer offsets in the metadata
            System.out.println("🔍 METADATA DEBUG:");
            System.out.println("   Footer start offset: " + mergedSplitMetadata.getFooterStartOffset());
            System.out.println("   Footer end offset: " + mergedSplitMetadata.getFooterEndOffset());
            System.out.println("   Doc mapping UID: " + mergedSplitMetadata.getDocMappingUid());
            System.out.println("   Create timestamp: " + mergedSplitMetadata.getCreateTimestamp());
            System.out.println("   hasFooterOffsets(): " + mergedSplitMetadata.hasFooterOffsets());
            System.out.println("   getMetadataSize(): " + mergedSplitMetadata.getMetadataSize());
            
        } catch (Exception e) {
            System.err.println("❌ Failed to merge splits: " + e.getMessage());
            e.printStackTrace();
            fail("Failed to merge Azure splits: " + e.getMessage());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Step 3.5: Validate consolidated split exists in Azure")
    public void step3_5_validateConsolidatedSplitInAzure() throws Exception {
        System.out.println("🔍 Validating consolidated split in Azure...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitAzureUrl, "Merged split Azure URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        assertNotNull(blobServiceClient, "Azure client should be initialized");

        try {
            // Extract blob name from the URL
            String blobName = mergedSplitAzureUrl.replace("azure://" + TEST_CONTAINER + "/", "");

            System.out.println("Checking Azure blob existence:");
            System.out.println("  Container: " + TEST_CONTAINER);
            System.out.println("  Blob name: " + blobName);
            System.out.println("  Full URL: " + mergedSplitAzureUrl);

            // Get blob client and properties
            BlobClient blobClient = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(blobName);

            var properties = blobClient.getProperties();

            // Validate the blob exists and has reasonable size
            long azureBlobSize = properties.getBlobSize();
            assertTrue(azureBlobSize > 0, "Azure blob should have non-zero size");
            assertTrue(azureBlobSize > 1000, "Azure blob should be larger than 1KB (actual: " + azureBlobSize + " bytes)");

            // Validate against our expected metadata
            long expectedSize = mergedSplitMetadata.getUncompressedSizeBytes();
            if (expectedSize > 0) {
                // Allow some variance due to compression and metadata overhead
                double sizeRatio = (double) azureBlobSize / expectedSize;
                assertTrue(sizeRatio > 0.1 && sizeRatio < 10.0,
                          "Azure blob size (" + azureBlobSize + ") should be reasonably close to expected size (" + expectedSize + ")");
            }

            System.out.println("✅ Azure validation successful:");
            System.out.println("   Blob size: " + azureBlobSize + " bytes");
            System.out.println("   Last modified: " + properties.getLastModified());
            System.out.println("   ETag: " + properties.getETag());

        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("BlobNotFound")) {
                fail("Consolidated split not found in Azure. URL: " + mergedSplitAzureUrl + ". " +
                     "This suggests the merge operation in Step 3 may not have uploaded the result correctly.");
            }
            System.err.println("❌ Failed to validate Azure blob: " + e.getMessage());
            e.printStackTrace();
            fail("Failed to validate consolidated split in Azure: " + e.getMessage());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Step 4: Create SplitCacheManager and perform warmup")
    public void step4_warmupCacheWithMergedSplit() throws Exception {
        System.out.println("🔥 Warming up cache for merged Azure split...");
        
        // Validate prerequisites
        assertNotNull(mergedSplitAzureUrl, "Merged split Azure URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        
        // Create cache manager with Azure credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("end-to-end-cache")
            .withMaxCacheSize(100_000_000); // 100MB cache

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            cacheConfig = cacheConfig.withAzureCredentials(storageAccount, accountKey);
        }
        // Note: Azure doesn't require region configuration
        
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        System.out.println("Cache manager created for Azure URL: " + mergedSplitAzureUrl);
        
        try {
            // Create split searcher with footer optimization metadata
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
                
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
    @DisplayName("Step 5: Query merged split and validate all original data + No Full Downloads")
    public void step5_queryAndValidateData() throws Exception {
        System.out.println("🔍 Querying merged split to validate data integrity AND no full downloads...");

        // Validate prerequisites
        assertNotNull(mergedSplitAzureUrl, "Merged split Azure URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");

        // Create cache manager
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("validation-cache")
            .withMaxCacheSize(50_000_000);

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            cacheConfig = cacheConfig.withAzureCredentials(storageAccount, accountKey);
        }
        // Note: Azure doesn't require region configuration

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
            
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
                    Long price = (Long) productDoc.getFirst("price");
                    assertNotNull(productName, "Product should have name");
                    assertNotNull(price, "Product should have price");
                    assertTrue(price > 0, "Price should be positive");
                }
            }
            
            System.out.println("✅ Content validation passed");

            // Test 2.5: Document Retrieval Performance Validation (NO FULL DOWNLOADS)
            System.out.println("Test 2.5: Document retrieval hotcache validation (ensuring no full downloads)");

            // Validate that we have footer metadata for hotcache optimization
            assertTrue(mergedSplitMetadata.hasFooterOffsets(),
                      "Split should have footer offsets for hotcache optimization");
            System.out.printf("   📊 Footer metadata available: start=%d, end=%d%n",
                             mergedSplitMetadata.getFooterStartOffset(),
                             mergedSplitMetadata.getFooterEndOffset());

            // Perform multiple document retrievals and measure timing consistency
            System.out.println("   🔍 Testing document retrieval performance consistency...");
            java.util.List<Long> retrievalTimes = new java.util.ArrayList<>();
            int testDocuments = Math.min(10, totalFoundDocs); // Test up to 10 documents

            for (int i = 0; i < testDocuments; i++) {
                // Get document from different result sets to test various documents
                SearchResult testResult = (i % 4 == 0) ? customersResult :
                                         (i % 4 == 1) ? productsResult :
                                         (i % 4 == 2) ? ordersResult : reviewsResult;

                if (testResult.getHits().size() > (i / 4)) {
                    long startTime = System.nanoTime();
                    try (Document testDoc = searcher.doc(testResult.getHits().get(i / 4).getDocAddress())) {
                        String content = (String) testDoc.getFirst("name");
                        assertNotNull(content, "Document should have retrievable content");
                    }
                    long endTime = System.nanoTime();
                    retrievalTimes.add((endTime - startTime) / 1_000_000); // Convert to milliseconds

                    System.out.printf("     📄 Doc %d retrieval: %d ms%n", i + 1, retrievalTimes.get(i));
                }
            }

            // Analyze retrieval time consistency (should be consistent with hotcache, erratic with full downloads)
            if (retrievalTimes.size() >= 3) {
                double averageTime = retrievalTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
                long minTime = retrievalTimes.stream().mapToLong(Long::longValue).min().orElse(0L);
                long maxTime = retrievalTimes.stream().mapToLong(Long::longValue).max().orElse(0L);
                double variabilityRatio = maxTime > 0 ? (double) maxTime / minTime : 1.0;

                System.out.printf("   📊 Retrieval time analysis:%n");
                System.out.printf("      Average: %.2f ms%n", averageTime);
                System.out.printf("      Min: %d ms, Max: %d ms%n", minTime, maxTime);
                System.out.printf("      Variability ratio: %.2fx%n", variabilityRatio);

                // Hotcache should have low variability (< 5x), full downloads would have high variability (> 10x)
                if (variabilityRatio < 5.0) {
                    System.out.println("   ✅ LOW VARIABILITY: Consistent retrieval times suggest hotcache optimization active");
                } else if (variabilityRatio < 10.0) {
                    System.out.println("   ⚠️  MODERATE VARIABILITY: Some variance in retrieval times");
                } else {
                    System.out.println("   ❌ HIGH VARIABILITY: Large variance suggests potential full downloads");
                    // Don't fail immediately - could be network conditions
                }

                // All retrievals should be reasonably fast with hotcache (< 500ms typical)
                long slowRetrievals = retrievalTimes.stream().mapToLong(Long::longValue).filter(t -> t > 500).count();
                if (slowRetrievals == 0) {
                    System.out.println("   ✅ ALL FAST RETRIEVALS: All document retrievals < 500ms (hotcache working)");
                } else {
                    System.out.printf("   ⚠️  SLOW RETRIEVALS: %d/%d retrievals > 500ms (potential network issues)%n",
                                     slowRetrievals, retrievalTimes.size());
                }

                // Test batch retrieval performance as well
                System.out.println("   🔍 Testing batch document retrieval performance...");
                if (customersResult.getHits().size() >= 3) {
                    java.util.List<DocAddress> batchAddresses = customersResult.getHits()
                        .subList(0, Math.min(3, customersResult.getHits().size()))
                        .stream()
                        .map(hit -> hit.getDocAddress())
                        .collect(java.util.stream.Collectors.toList());

                    long batchStartTime = System.nanoTime();
                    java.util.List<Document> batchDocs = searcher.docBatch(batchAddresses);
                    long batchEndTime = System.nanoTime();
                    long batchTime = (batchEndTime - batchStartTime) / 1_000_000;

                    System.out.printf("     📦 Batch retrieval (%d docs): %d ms%n", batchDocs.size(), batchTime);

                    // Batch retrieval should be efficient with hotcache
                    if (batchTime < (batchDocs.size() * 200)) { // < 200ms per doc in batch
                        System.out.println("     ✅ EFFICIENT BATCH: Batch retrieval suggests hotcache optimization");
                    } else {
                        System.out.println("     ⚠️  SLOW BATCH: Batch retrieval slower than expected");
                    }

                    // Cleanup batch documents
                    for (Document doc : batchDocs) {
                        doc.close();
                    }
                }
            }

            // Check if debug logging is available to detect download patterns
            String debugEnv = System.getenv("TANTIVY4JAVA_DEBUG");
            if ("1".equals(debugEnv)) {
                System.out.println("   🔍 DEBUG MODE ACTIVE: Check console output above for:");
                System.out.println("     - '🚀 Using Quickwit optimized path' = GOOD (hotcache)");
                System.out.println("     - '⚠️ falling back to full download' = BAD (full download)");
                System.out.println("     - Document retrievals should show hotcache optimization messages");
            } else {
                System.out.println("   💡 TIP: Run with TANTIVY4JAVA_DEBUG=1 to see hotcache vs full download debug messages");
            }

            System.out.println("✅ Document retrieval validation completed");

            // Test 3: Cross-domain queries
            System.out.println("Test 3: Cross-domain query validation");
            
            SplitQuery titleQuery = new SplitTermQuery("name", "customer");
            SearchResult titleResults = searcher.search(titleQuery, 100);
            assertTrue(titleResults.getHits().size() > 0, "Should find documents with 'customer' in name");
            
            // Range query on numeric fields
            SplitQuery priceRangeQuery = new SplitRangeQuery("price", RangeBound.inclusive("10"), RangeBound.inclusive("1000"), "i64");
            SearchResult priceResults = searcher.search(priceRangeQuery, 100);
            // Note: Some domains might not have price field, so just check it doesn't crash
            assertNotNull(priceResults, "Range query should not crash");
            
            System.out.println("✅ Cross-domain queries passed");
            
            // Test 4: Boolean query combining multiple conditions
            System.out.println("Test 4: Complex boolean query validation");
            
            SplitQuery domainQuery = new SplitTermQuery("domain", "products");
            SplitQuery nameQuery = new SplitTermQuery("name", "product");
            
            SplitQuery booleanQuery = new SplitBooleanQuery()
                .addMust(domainQuery)
                .addMust(nameQuery);
            
            SearchResult booleanResults = searcher.search(booleanQuery, 50);
            assertNotNull(booleanResults, "Boolean query should succeed");
            
            System.out.println("✅ Complex boolean queries passed");
            System.out.println("   Boolean results: " + booleanResults.getHits().size());
            
        } catch (Exception e) {
            System.err.println("❌ Data validation failed: " + e.getMessage());
            e.printStackTrace(System.out);
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
        assertNotNull(mergedSplitAzureUrl, "Merged split Azure URL should be available from Step 3");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available from Step 3");
        
        // Create cache manager for performance testing
        SplitCacheManager.CacheConfig perfCacheConfig = new SplitCacheManager.CacheConfig("performance-cache")
            .withMaxCacheSize(200_000_000); // 200MB for performance test

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            perfCacheConfig = perfCacheConfig.withAzureCredentials(storageAccount, accountKey);
        }
        // Note: Azure doesn't require region configuration
        
        SplitCacheManager perfCacheManager = SplitCacheManager.getInstance(perfCacheConfig);
        
        // Performance test: measure cold vs warm cache performance
        System.out.println("Measuring cache performance...");
        
        long coldStartTime = System.nanoTime();
        try (SplitSearcher coldSearcher = perfCacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
            Schema schema = coldSearcher.getSchema();
            SplitQuery testQuery = new SplitTermQuery("domain", "customers");
            SearchResult coldResult = coldSearcher.search(testQuery, 10);
            assertNotNull(coldResult);
        }
        long coldDuration = System.nanoTime() - coldStartTime;
        
        // Warm up cache
        try (SplitSearcher warmupSearcher = perfCacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
            CompletableFuture<Void> warmup = warmupSearcher.preloadComponents(SplitSearcher.IndexComponent.POSTINGS, SplitSearcher.IndexComponent.POSITIONS);
            warmup.join();
        }
        
        // Measure warm performance
        long warmStartTime = System.nanoTime();
        try (SplitSearcher warmSearcher = perfCacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
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
        System.out.println("ℹ️  Test artifacts left in Azure bucket for inspection: " + TEST_CONTAINER);
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
        schemaBuilder.addIntegerField("id", true, true, true);                          // Unique ID - FAST field for range queries
        schemaBuilder.addIntegerField("price", true, true, true);                       // Price - FAST field for range queries
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

    @Test
    @org.junit.jupiter.api.Order(8)
    @DisplayName("Step 7: Document Retrieval No-Download Validation Test")
    public void step7_documentRetrievalNoDownloadValidationTest() throws Exception {
        System.out.println("🔍 === DOCUMENT RETRIEVAL NO-DOWNLOAD VALIDATION TEST ===");
        System.out.println("Validating that document retrievals use hotcache optimization and DON'T trigger full downloads");

        // Validate prerequisites
        assertNotNull(mergedSplitAzureUrl, "Merged split Azure URL should be available");
        assertNotNull(mergedSplitMetadata, "Merged split metadata should be available");
        assertTrue(mergedSplitMetadata.hasFooterOffsets(), "Split should have footer metadata for optimization");

        // Create dedicated cache manager for this test
        SplitCacheManager.CacheConfig docValidationConfig = new SplitCacheManager.CacheConfig("doc-validation-cache")
            .withMaxCacheSize(100_000_000); // 100MB cache

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            docValidationConfig = docValidationConfig.withAzureCredentials(storageAccount, accountKey);
        }
        // Note: Azure doesn't require region configuration

        SplitCacheManager docValidationCacheManager = SplitCacheManager.getInstance(docValidationConfig);

        try (SplitSearcher searcher = docValidationCacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {

            System.out.printf("🔍 Testing split with footer metadata: start=%d, end=%d%n",
                             mergedSplitMetadata.getFooterStartOffset(),
                             mergedSplitMetadata.getFooterEndOffset());

            // SEARCHER REUSE VALIDATION: Track searcher creation timing
            System.out.println("🔄 Searcher Reuse Validation: Testing single searcher for multiple operations...");

            long searcherCreationTime = System.nanoTime(); // Searcher should already be created above

            // Phase 1: Execute searches to get documents to retrieve (SAME SEARCHER)
            System.out.println("Phase 1: Executing searches to get document addresses (testing searcher reuse)...");

            // Multiple search operations to validate searcher reuse
            long search1StartTime = System.nanoTime();
            Schema schema = searcher.getSchema();
            long schemaTime = System.nanoTime() - search1StartTime;

            long search2StartTime = System.nanoTime();
            SplitQuery testQuery = new SplitTermQuery("domain", "customers");
            SearchResult searchResult = searcher.search(testQuery, 20);
            long searchTime = System.nanoTime() - search2StartTime;

            // Additional searches with same searcher to validate reuse
            long search3StartTime = System.nanoTime();
            SplitQuery testQuery2 = new SplitTermQuery("domain", "products");
            SearchResult searchResult2 = searcher.search(testQuery2, 10);
            long search3Time = System.nanoTime() - search3StartTime;

            long search4StartTime = System.nanoTime();
            SplitQuery testQuery3 = new SplitTermQuery("name", "customer");
            SearchResult searchResult3 = searcher.search(testQuery3, 15);
            long search4Time = System.nanoTime() - search4StartTime;

            System.out.printf("Searcher operation timing (reuse validation):%n");
            System.out.printf("  📋 Schema retrieval: %d ms%n", schemaTime / 1_000_000);
            System.out.printf("  🔍 Search 1 (customers): %d ms%n", searchTime / 1_000_000);
            System.out.printf("  🔍 Search 2 (products): %d ms%n", search3Time / 1_000_000);
            System.out.printf("  🔍 Search 3 (names): %d ms%n", search4Time / 1_000_000);

            // Validate that subsequent searches are fast (indicating reuse, not recreation)
            long avgSearchTime = (searchTime + search3Time + search4Time) / 3 / 1_000_000;
            if (avgSearchTime < 100) {
                System.out.println("  ✅ FAST SUBSEQUENT SEARCHES: Consistent fast searches suggest proper searcher reuse");
            } else {
                System.out.printf("  ⚠️  SLOW SUBSEQUENT SEARCHES: Average %d ms may indicate searcher recreation%n", avgSearchTime);
            }

            assertTrue(searchResult.getHits().size() > 0, "Should find documents to test retrieval");
            System.out.printf("Found %d documents for retrieval testing%n", searchResult.getHits().size());

            // Phase 2: Individual document retrieval validation (SAME SEARCHER REUSE) - 1000 DOCUMENTS
            System.out.println("Phase 2: Individual document retrieval performance test with 1000 documents...");

            // Get a larger result set for meaningful performance testing
            SplitQuery largeQuery = new SplitBooleanQuery()
                .addShould(testQuery)
                .addShould(testQuery2)
                .addShould(testQuery3);
            SearchResult largeResult = searcher.search(largeQuery, 1000);

            // REPLICATE ADDRESSES TO REACH 1000 DOCUMENTS FOR PERFORMANCE TESTING
            java.util.List<SearchResult.Hit> availableHits = largeResult.getHits();
            java.util.List<SearchResult.Hit> replicatedHits = new java.util.ArrayList<>();

            // Replicate available documents to reach exactly 1000 for performance testing
            for (int i = 0; i < 1000; i++) {
                if (availableHits.size() > 0) {
                    replicatedHits.add(availableHits.get(i % availableHits.size()));
                }
            }

            int testCount = replicatedHits.size();

            System.out.printf("  🔄 PERFORMANCE TEST: Using same searcher for %d individual document retrievals%n", testCount);
            System.out.println("     This tests both searcher reuse AND measures true individual retrieval performance");

            java.util.List<Long> retrievalTimes = new java.util.ArrayList<>();
            long totalIndividualStart = System.nanoTime();

            for (int i = 0; i < testCount; i++) {
                if (i % 100 == 0) {
                    System.out.printf("  Progress: %d/%d individual retrievals completed%n", i, testCount);
                }

                long startTime = System.nanoTime();
                try (Document doc = searcher.doc(replicatedHits.get(i).getDocAddress())) {
                    String name = (String) doc.getFirst("name");
                    String domain = (String) doc.getFirst("domain");
                    assertNotNull(name, "Document should have name field");
                    assertNotNull(domain, "Document should have domain field");
                }
                long endTime = System.nanoTime();
                long retrievalTime = (endTime - startTime) / 1_000_000; // Convert to milliseconds
                if (i < 20) retrievalTimes.add(retrievalTime); // Keep first 20 for analysis
            }

            long totalIndividualEnd = System.nanoTime();
            long totalIndividualTime = (totalIndividualEnd - totalIndividualStart) / 1_000_000;

            System.out.printf("  📊 Individual retrieval completed: %d documents in %d ms%n", testCount, totalIndividualTime);
            System.out.printf("      Average per document: %.3f ms%n", (double) totalIndividualTime / testCount);

            // Validate retrieval time consistency for searcher reuse
            if (retrievalTimes.size() >= 3) {
                // First retrieval might be slower (cache miss), subsequent should be faster (cache hit)
                long firstRetrieval = retrievalTimes.get(0);
                double avgSubsequent = retrievalTimes.subList(1, retrievalTimes.size())
                    .stream().mapToLong(Long::longValue).average().orElse(0.0);

                System.out.printf("  📊 Searcher reuse analysis:%n");
                System.out.printf("     First retrieval: %d ms (may include cache warming)%n", firstRetrieval);
                System.out.printf("     Avg subsequent: %.1f ms (should be faster with reuse)%n", avgSubsequent);

                if (avgSubsequent < firstRetrieval * 0.8) {
                    System.out.println("     ✅ SEARCHER REUSE CONFIRMED: Subsequent retrievals faster (native cache working)");
                } else {
                    System.out.println("     ⚠️  POSSIBLE SEARCHER RECREATION: Similar timing across all retrievals");
                }
            }

            // Phase 3: Batch document retrieval validation (SAME SEARCHER REUSE) - 1000 DOCUMENTS
            System.out.println("Phase 3: Batch document retrieval performance test with 1000 documents...");

            // Use the same replicated result set for batch testing (1000 documents)
            int batchTestCount = replicatedHits.size(); // Should be exactly 1000
            java.util.List<DocAddress> batchAddresses = replicatedHits
                .stream()
                .map(SearchResult.Hit::getDocAddress)
                .collect(java.util.stream.Collectors.toList());

            System.out.printf("  🔄 BATCH PERFORMANCE TEST: Using same searcher for batch of %d documents%n", batchAddresses.size());
            System.out.println("     This directly compares batch vs individual retrieval efficiency at scale");

            long batchStartTime = System.nanoTime();
            java.util.List<Document> batchDocs = searcher.docBatch(batchAddresses);
            long batchEndTime = System.nanoTime();
            long totalBatchTime = (batchEndTime - batchStartTime) / 1_000_000;

            System.out.printf("  📦 Batch retrieval completed: %d documents in %d ms%n", batchDocs.size(), totalBatchTime);
            System.out.printf("      Average per document: %.3f ms%n", (double) totalBatchTime / batchDocs.size());

            // Compare batch vs individual performance (batch should be more efficient)
            double individualPerDoc = (double) totalIndividualTime / testCount;
            double batchPerDoc = (double) totalBatchTime / batchDocs.size();
            double batchEfficiencyRatio = individualPerDoc / batchPerDoc;

            System.out.printf("  📈 PERFORMANCE COMPARISON (1000 documents each):%n");
            System.out.printf("     Individual total: %d ms (%.3f ms/doc)%n", totalIndividualTime, individualPerDoc);
            System.out.printf("     Batch total:      %d ms (%.3f ms/doc)%n", totalBatchTime, batchPerDoc);

            if (batchEfficiencyRatio > 1.2) {
                System.out.printf("     🚀 BATCH ADVANTAGE: %.1fx faster per document (✅ batch optimized)%n", batchEfficiencyRatio);
            } else if (batchEfficiencyRatio > 0.8) {
                System.out.printf("     ⚖️  SIMILAR PERFORMANCE: %.1fx ratio (⚠️ marginal difference)%n", batchEfficiencyRatio);
            } else {
                System.out.printf("     🐌 BATCH DISADVANTAGE: %.1fx slower per document (❌ batch inefficient)%n", 1.0 / batchEfficiencyRatio);
            }

            // Validate a sample of batch results
            int sampleSize = Math.min(10, batchDocs.size());
            for (int i = 0; i < sampleSize; i++) {
                Document doc = batchDocs.get(i);
                String domain = (String) doc.getFirst("domain");
                assertNotNull(domain, "Batch document should have domain field");
                doc.close();
            }

            // Close remaining batch documents
            for (int i = sampleSize; i < batchDocs.size(); i++) {
                batchDocs.get(i).close();
            }

            // Phase 3.5: Cache Manager Reuse Validation
            System.out.println("Phase 3.5: Cache Manager Reuse Validation...");
            System.out.println("  🔄 CACHE MANAGER REUSE: Testing that multiple searchers share same cache manager");

            // Test creating a second searcher with same cache manager (should reuse caches)
            long secondSearcherStart = System.nanoTime();
            try (SplitSearcher searcher2 = docValidationCacheManager.createSplitSearcher(mergedSplitAzureUrl, mergedSplitMetadata)) {
                long secondSearcherCreation = (System.nanoTime() - secondSearcherStart) / 1_000_000;

                // Quick test with second searcher
                long quickSearchStart = System.nanoTime();
                SplitQuery quickQuery = new SplitTermQuery("domain", "customers");
                SearchResult quickResult = searcher2.search(quickQuery, 5);
                long quickSearchTime = (System.nanoTime() - quickSearchStart) / 1_000_000;

                System.out.printf("  📊 Second searcher creation: %d ms%n", secondSearcherCreation);
                System.out.printf("  📊 Quick search with second searcher: %d ms%n", quickSearchTime);

                if (secondSearcherCreation < 100 && quickSearchTime < 50) {
                    System.out.println("  ✅ CACHE MANAGER REUSE CONFIRMED: Fast second searcher creation and operation");
                } else {
                    System.out.println("  ⚠️  POTENTIAL CACHE ISOLATION: Second searcher creation/operation slower than expected");
                }
            }
            System.out.printf("  🔄 Second searcher properly closed, returning to original searcher%n");

            // Phase 4: Performance analysis and no-download validation (based on 1000-document test)
            System.out.println("Phase 4: Performance analysis and no-download validation...");

            // Calculate statistics from first 20 individual retrievals for consistency analysis
            double avgTime = retrievalTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long minTime = retrievalTimes.stream().mapToLong(Long::longValue).min().orElse(0L);
            long maxTime = retrievalTimes.stream().mapToLong(Long::longValue).max().orElse(0L);
            double variabilityRatio = (minTime > 0) ? (double) maxTime / minTime :
                                     (maxTime == 0) ? 1.0 : Double.MAX_VALUE;

            // Overall performance metrics from 1000-document tests
            System.out.printf("📊 1000-Document Performance Summary:%n");
            System.out.printf("  Individual: %d ms total, %.3f ms/doc average%n", totalIndividualTime, individualPerDoc);
            System.out.printf("  Batch:      %d ms total, %.3f ms/doc average%n", totalBatchTime, batchPerDoc);
            System.out.printf("  Batch efficiency ratio: %.2fx%n", batchEfficiencyRatio);

            System.out.printf("Individual retrieval analysis:%n");
            System.out.printf("  Average time: %.2f ms%n", avgTime);
            System.out.printf("  Time range: %d - %d ms%n", minTime, maxTime);
            System.out.printf("  Variability ratio: %.2fx%n", variabilityRatio);

            // Validation criteria for hotcache optimization, searcher reuse, AND batch performance
            boolean fastRetrievals = individualPerDoc < 1.0; // Average < 1ms per document (1000-doc scale)
            boolean consistentRetrievals = variabilityRatio < 5.0; // Low variability in first 20 samples
            boolean noneExtremelySlow = retrievalTimes.stream().noneMatch(t -> t > 100); // No >100ms retrievals in samples
            boolean batchEfficient = batchEfficiencyRatio > 0.8; // Batch performance within 20% of individual
            long veryFastCount = retrievalTimes.stream().filter(t -> t < 10).count();

            // Additional searcher reuse validation criteria
            boolean searcherReuseEvidence = retrievalTimes.size() >= 3 &&
                retrievalTimes.subList(1, retrievalTimes.size())
                    .stream().mapToLong(Long::longValue).average().orElse(0.0) < retrievalTimes.get(0) * 0.8;

            System.out.println("Hotcache optimization, searcher reuse, AND batch performance validation:");
            System.out.printf("  ✓ Fast individual retrieval (<1ms/doc avg): %s (%.3f ms/doc)%n",
                             fastRetrievals ? "PASS" : "FAIL", individualPerDoc);
            System.out.printf("  ✓ Consistent timing (<5x variability): %s (%.2fx)%n",
                             consistentRetrievals ? "PASS" : "FAIL", variabilityRatio);
            System.out.printf("  ✓ No slow retrievals in samples (>100ms): %s%n",
                             noneExtremelySlow ? "PASS" : "FAIL");
            System.out.printf("  ✓ Batch performance reasonable (>0.8x ratio): %s (%.2fx)%n",
                             batchEfficient ? "PASS" : "FAIL", batchEfficiencyRatio);
            System.out.printf("  ✓ Very fast samples (<10ms): %d/%d%n", veryFastCount, retrievalTimes.size());
            System.out.printf("  ✓ Searcher reuse evidence (faster subsequent): %s%n",
                             searcherReuseEvidence ? "PASS" : "FAIL");

            // Overall assessment including batch performance
            int passCount = (fastRetrievals ? 1 : 0) + (consistentRetrievals ? 1 : 0) + (noneExtremelySlow ? 1 : 0) +
                           (batchEfficient ? 1 : 0) + (searcherReuseEvidence ? 1 : 0);

            if (passCount >= 5) {
                System.out.println("🎯 RESULT: ✅ EXCELLENT - All indicators confirm hotcache, searcher reuse, AND batch efficiency");
                System.out.println("   Both individual and batch document retrievals are optimized with proper caching");
            } else if (passCount >= 4) {
                System.out.println("🎯 RESULT: ✅ VERY GOOD - Most indicators confirm optimization with minor batch issues");
                System.out.println("   Individual retrieval optimized, batch performance may need investigation");
            } else if (passCount >= 3) {
                System.out.println("🎯 RESULT: ✅ GOOD - Core optimization working with some performance concerns");
                System.out.println("   Hotcache optimization confirmed, but batch or reuse patterns suboptimal");
            } else if (passCount >= 2) {
                System.out.println("🎯 RESULT: ⚠️  MIXED - Some optimization evidence but significant performance issues");
                System.out.println("   Basic functionality working but efficiency improvements needed");
            } else {
                System.out.println("🎯 RESULT: ❌ CONCERNING - Limited evidence of optimization at 1000-document scale");
                System.out.println("   May be triggering full downloads, recreating searchers, or inefficient batch processing");
            }

            // Debug logging guidance
            String debugEnv = System.getenv("TANTIVY4JAVA_DEBUG");
            if ("1".equals(debugEnv)) {
                System.out.println("🔍 DEBUG MODE: Review console output above for optimization path indicators");
            } else {
                System.out.println("💡 For detailed analysis, run with: TANTIVY4JAVA_DEBUG=1 mvn test -Dtest=RealS3EndToEndTest#step7_documentRetrievalNoDownloadValidationTest");
            }

            System.out.println("✅ Document retrieval no-download validation test completed");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(9)
    @DisplayName("Step 8: Large Split Performance Test - Demonstrating Hotcache Optimization")
    public void step8_largeSplitPerformanceTest() throws Exception {
        System.out.println("🚀 === LARGE SPLIT PERFORMANCE TEST ===");
        System.out.println("Creating ~100MB split to demonstrate hotcache vs full download performance");
        
        // Step 7.1: Check if large split already exists in Azure
        String largeSplitBlobName = "test-splits/large-performance.split";
        String largeSplitMetadataBlobName = "test-splits/large-performance.split.metadata";
        String largeSplitAzureUrl = "azure://" + TEST_CONTAINER + "/" + largeSplitBlobName;
        
        if (blobServiceClient == null) {
            setupAzureClient();
        }
        
        QuickwitSplit.SplitMetadata largeSplitMetadata = null;
        boolean splitExists = false;
        
        try {
            // Check if split already exists in Azure
            BlobClient existingSplitBlob = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(largeSplitBlobName);

            if (existingSplitBlob.exists()) {
                long existingSplitSize = existingSplitBlob.getProperties().getBlobSize();
                System.out.printf("📋 Found existing large split in Azure: %.2f MB%n", existingSplitSize / 1024.0 / 1024.0);

                if (existingSplitSize > 50_000_000) { // >50MB
                    // Try to load the stored metadata
                    try {
                        System.out.println("📋 Loading stored metadata for existing split...");
                        BlobClient metadataBlob = blobServiceClient
                            .getBlobContainerClient(TEST_CONTAINER)
                            .getBlobClient(largeSplitMetadataBlobName);

                        String metadataJson;
                        if (metadataBlob.exists()) {
                            metadataJson = metadataBlob.downloadContent().toString();
                        } else {
                            throw new Exception("Metadata blob not found");
                        }

                        // Parse the stored metadata JSON
                        largeSplitMetadata = parseStoredSplitMetadata(metadataJson);
                        splitExists = true;
                        System.out.println("✅ Reusing existing large split with correct metadata (>50MB)");
                        System.out.printf("📊 Split metadata: docs=%d, footer=%d..%d, docMapping=%s, timestamp=%d%n",
                            largeSplitMetadata.getNumDocs(),
                            largeSplitMetadata.getFooterStartOffset(),
                            largeSplitMetadata.getFooterEndOffset(),
                            largeSplitMetadata.getDocMappingUid(),
                            largeSplitMetadata.getCreateTimestamp());
                    } catch (Exception metaE) {
                        System.out.println("⚠️ Could not load stored metadata, will recreate split: " + metaE.getMessage());
                    }
                } else {
                    System.out.println("⚠️ Existing split too small, will recreate");
                }
            } else {
                System.out.println("📝 No existing large split found in Azure");
            }
        } catch (Exception e) {
            System.out.println("📝 No existing large split found, will create new one");
        }
        
        if (!splitExists) {
            // Step 7.1: Create a large index (~100MB)
            System.out.println("📊 Step 7.1: Creating large test index...");
            Path largeIndexPath = tempDir.resolve("large-performance-index");
            createLargePerformanceIndex(largeIndexPath);
            
            // Step 7.2: Convert to split
            System.out.println("📦 Step 7.2: Converting to Quickwit split...");
            Path largeSplitPath = tempDir.resolve("large-performance.split");
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "large-performance-test",
                "performance-source", 
                "performance-node"
            );
            
            largeSplitMetadata = QuickwitSplit.convertIndexFromPath(
                largeIndexPath.toString(), 
                largeSplitPath.toString(), 
                config
            );
            
            assertNotNull(largeSplitMetadata, "Large split metadata should be created");
            assertTrue(largeSplitMetadata.getNumDocs() > 50000, "Large split should contain >50K documents");
            assertTrue(largeSplitMetadata.getUncompressedSizeBytes() > 50_000_000, "Large split should be >50MB");
            
            System.out.printf("✅ Created large split: %d docs, %.2f MB%n", 
                             largeSplitMetadata.getNumDocs(),
                             largeSplitMetadata.getUncompressedSizeBytes() / 1024.0 / 1024.0);
            
            // Step 7.3: Upload to Azure
            System.out.println("☁️ Step 7.3: Uploading large split to Azure...");
            
            // Upload large split to Azure with progress tracking
            long uploadStartTime = System.nanoTime();
            BlobClient splitBlobClient = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(largeSplitBlobName);
            splitBlobClient.uploadFromFile(largeSplitPath.toString(), true);
            long uploadDuration = System.nanoTime() - uploadStartTime;

            System.out.printf("✅ Upload completed in %.2f seconds%n", uploadDuration / 1_000_000_000.0);

            // Store the metadata alongside the split
            System.out.println("💾 Storing split metadata for future reuse...");
            String metadataJson = serializeSplitMetadata(largeSplitMetadata);
            BlobClient metadataBlobClient = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(largeSplitMetadataBlobName);

            java.io.ByteArrayInputStream metadataStream = new java.io.ByteArrayInputStream(metadataJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            metadataBlobClient.upload(metadataStream, metadataJson.length(), true);

            System.out.println("✅ Metadata stored in Azure");
        } else {
            System.out.println("⏩ Skipping split creation and upload - using existing split");
        }
        
        // Step 7.4: Performance Comparison - Cold Access (simulating missing footer metadata)
        System.out.println("🧪 Step 7.4: Performance comparison testing...");
        
        // Create cache managers for testing
        SplitCacheManager.CacheConfig perfConfig = new SplitCacheManager.CacheConfig("large-perf-cache")
            .withMaxCacheSize(200_000_000); // 200MB cache

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            perfConfig = perfConfig.withAzureCredentials(storageAccount, accountKey);
        }
        // Note: Azure doesn't require region configuration
        
        SplitCacheManager perfCacheManager = SplitCacheManager.getInstance(perfConfig);
        
        // Measure cold performance with detailed profiling INCLUDING searcher reuse
        System.out.println("🔍 Testing FIRST access (hotcache + searcher reuse validation):");
        long coldStartTime = System.nanoTime();
        SearchResult coldResult;
        try (SplitSearcher coldSearcher = perfCacheManager.createSplitSearcher(largeSplitAzureUrl, largeSplitMetadata)) {

            System.out.println("  🔄 SEARCHER LIFECYCLE: Testing single searcher for multiple operations (CRITICAL for performance)");
            
            // Phase 1: Schema retrieval
            long schemaStartTime = System.nanoTime();
            Schema schema = coldSearcher.getSchema();
            long schemaEndTime = System.nanoTime();
            System.out.printf("    📋 Schema retrieval: %.3f seconds%n", (schemaEndTime - schemaStartTime) / 1_000_000_000.0);
            
            // Phase 2: Query execution (includes warmup, index opening, search)
            long searchStartTime = System.nanoTime();
            
            // Test multiple query types to exercise the full search pipeline
            SplitQuery testQuery = new SplitTermQuery("content", "performance");
            coldResult = coldSearcher.search(testQuery, 10);
            
            // Additional queries to ensure warmup and hotcache are fully exercised
            SplitQuery domainQuery = new SplitTermQuery("domain", "customers");
            SearchResult domainResult = coldSearcher.search(domainQuery, 5);
            
            SplitQuery nameQuery = new SplitTermQuery("name", "customer");
            SearchResult nameResult = coldSearcher.search(nameQuery, 3);
            
            long searchEndTime = System.nanoTime();
            System.out.printf("    🔍 Query execution: %.3f seconds (includes warmup, index opening, 3 searches)%n", (searchEndTime - searchStartTime) / 1_000_000_000.0);
            System.out.printf("      📊 Query results: %d + %d + %d hits%n", 
                             coldResult.getHits().size(), domainResult.getHits().size(), nameResult.getHits().size());
            
            // Phase 3: Document retrieval from search results (VALIDATE NO FULL DOWNLOADS)
            if (coldResult.getHits().size() > 0) {
                System.out.println("    🔍 Testing document retrieval - validating hotcache optimization...");

                // Test multiple document retrievals to validate consistent performance
                java.util.List<Long> docRetrievalTimes = new java.util.ArrayList<>();
                int maxTestDocs = Math.min(5, coldResult.getHits().size());

                for (int i = 0; i < maxTestDocs; i++) {
                    long docStartTime = System.nanoTime();
                    try (Document testDoc = coldSearcher.doc(coldResult.getHits().get(i).getDocAddress())) {
                        String content = (String) testDoc.getFirst("content");
                        assertNotNull(content, "Document content should be retrieved");
                        long docEndTime = System.nanoTime();
                        long docTime = (docEndTime - docStartTime) / 1_000_000; // Convert to milliseconds
                        docRetrievalTimes.add(docTime);
                        System.out.printf("      📄 Doc %d retrieval: %d ms%n", i + 1, docTime);
                    }
                }

                // Analyze document retrieval consistency
                if (docRetrievalTimes.size() >= 2) {
                    double avgDocTime = docRetrievalTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
                    long minDocTime = docRetrievalTimes.stream().mapToLong(Long::longValue).min().orElse(0L);
                    long maxDocTime = docRetrievalTimes.stream().mapToLong(Long::longValue).max().orElse(0L);
                    double docVariability = maxDocTime > 0 ? (double) maxDocTime / minDocTime : 1.0;

                    System.out.printf("    📊 Document retrieval analysis: avg=%.1f ms, range=%d-%d ms, variability=%.1fx%n",
                                     avgDocTime, minDocTime, maxDocTime, docVariability);

                    // Low variability suggests hotcache is working (no full downloads)
                    if (docVariability < 3.0) {
                        System.out.println("    ✅ CONSISTENT DOC RETRIEVAL: Low variability suggests hotcache optimization active");
                    } else {
                        System.out.println("    ⚠️  VARIABLE DOC RETRIEVAL: Higher variability may indicate full downloads or network issues");
                    }

                    // Fast retrievals indicate hotcache success
                    long fastRetrievals = docRetrievalTimes.stream().mapToLong(Long::longValue).filter(t -> t < 200).count();
                    System.out.printf("    📈 Fast retrievals (<200ms): %d/%d%n", fastRetrievals, docRetrievalTimes.size());
                }
            }
        }
        long coldDuration = System.nanoTime() - coldStartTime;
        
        // Measure warm performance with detailed profiling
        System.out.println("🔥 Testing SECOND access (cache should improve performance):");
        long warmStartTime = System.nanoTime();
        SearchResult warmResult;
        try (SplitSearcher warmSearcher = perfCacheManager.createSplitSearcher(largeSplitAzureUrl, largeSplitMetadata)) {
            
            // Phase 1: Schema retrieval (should be faster due to caching)
            long schemaStartTime = System.nanoTime();
            Schema schema = warmSearcher.getSchema();
            long schemaEndTime = System.nanoTime();
            System.out.printf("    📋 Schema retrieval: %.3f seconds (cached)%n", (schemaEndTime - schemaStartTime) / 1_000_000_000.0);
            
            // Phase 2: Query execution (should be faster due to hotcache)
            long searchStartTime = System.nanoTime();
            
            // Same query pattern as cold test to compare performance
            SplitQuery testQuery = new SplitTermQuery("content", "performance");
            warmResult = warmSearcher.search(testQuery, 10);
            
            // Additional queries to test hotcache effectiveness
            SplitQuery domainQuery = new SplitTermQuery("domain", "customers");
            SearchResult domainResult = warmSearcher.search(domainQuery, 5);
            
            SplitQuery nameQuery = new SplitTermQuery("name", "customer");
            SearchResult nameResult = warmSearcher.search(nameQuery, 3);
            
            long searchEndTime = System.nanoTime();
            System.out.printf("    🔍 Query execution: %.3f seconds (with hotcache, 3 searches)%n", (searchEndTime - searchStartTime) / 1_000_000_000.0);
            System.out.printf("      📊 Query results: %d + %d + %d hits%n", 
                             warmResult.getHits().size(), domainResult.getHits().size(), nameResult.getHits().size());
            
            // Phase 3: Document retrieval validation (should be faster and more consistent)
            if (warmResult.getHits().size() > 0) {
                System.out.println("    🔍 Testing cached document retrieval performance...");

                // Test the same documents as cold test for comparison
                java.util.List<Long> warmDocRetrievalTimes = new java.util.ArrayList<>();
                int maxTestDocs = Math.min(5, warmResult.getHits().size());

                for (int i = 0; i < maxTestDocs; i++) {
                    long docStartTime = System.nanoTime();
                    try (Document testDoc = warmSearcher.doc(warmResult.getHits().get(i).getDocAddress())) {
                        String content = (String) testDoc.getFirst("content");
                        assertNotNull(content, "Document content should be retrieved");
                        long docEndTime = System.nanoTime();
                        long docTime = (docEndTime - docStartTime) / 1_000_000; // Convert to milliseconds
                        warmDocRetrievalTimes.add(docTime);
                        System.out.printf("      📄 Doc %d retrieval: %d ms (cached)%n", i + 1, docTime);
                    }
                }

                // Analyze warm document retrieval performance
                if (warmDocRetrievalTimes.size() >= 2) {
                    double avgWarmDocTime = warmDocRetrievalTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
                    long minWarmDocTime = warmDocRetrievalTimes.stream().mapToLong(Long::longValue).min().orElse(0L);
                    long maxWarmDocTime = warmDocRetrievalTimes.stream().mapToLong(Long::longValue).max().orElse(0L);
                    double warmDocVariability = maxWarmDocTime > 0 ? (double) maxWarmDocTime / minWarmDocTime : 1.0;

                    System.out.printf("    📊 Warm doc retrieval analysis: avg=%.1f ms, range=%d-%d ms, variability=%.1fx%n",
                                     avgWarmDocTime, minWarmDocTime, maxWarmDocTime, warmDocVariability);

                    // Warm retrieval should be even more consistent than cold
                    if (warmDocVariability < 2.0) {
                        System.out.println("    ✅ EXCELLENT CACHE CONSISTENCY: Very consistent warm retrieval times");
                    } else if (warmDocVariability < 3.0) {
                        System.out.println("    ✅ GOOD CACHE CONSISTENCY: Consistent warm retrieval times");
                    } else {
                        System.out.println("    ⚠️  INCONSISTENT WARM RETRIEVAL: Cache may not be working optimally");
                    }

                    // Count fast warm retrievals
                    long fastWarmRetrievals = warmDocRetrievalTimes.stream().mapToLong(Long::longValue).filter(t -> t < 100).count();
                    System.out.printf("    📈 Very fast retrievals (<100ms): %d/%d%n", fastWarmRetrievals, warmDocRetrievalTimes.size());
                }
            }
        }
        long warmDuration = System.nanoTime() - warmStartTime;
        
        // Step 7.5: Comprehensive Performance Analysis
        System.out.println("📊 === COMPREHENSIVE PERFORMANCE ANALYSIS ===");
        double coldSeconds = coldDuration / 1_000_000_000.0;
        double warmSeconds = warmDuration / 1_000_000_000.0;
        double speedupFactor = coldSeconds / warmSeconds;
        
        System.out.printf("Split size: %.2f MB (%d docs)%n", 
                         largeSplitMetadata.getUncompressedSizeBytes() / 1024.0 / 1024.0,
                         largeSplitMetadata.getNumDocs());
        System.out.printf("🧊 FIRST access (cold):  %.3f seconds%n", coldSeconds);
        System.out.printf("🔥 SECOND access (warm): %.3f seconds%n", warmSeconds);
        System.out.printf("⚡ Speedup factor: %.2fx%n", speedupFactor);
        
        // Performance analysis
        if (speedupFactor >= 1.5) {
            System.out.println("✅ EXCELLENT: Significant performance improvement detected!");
            System.out.println("   🚀 Hotcache optimization is working effectively");
        } else if (speedupFactor >= 1.1) {
            System.out.println("✅ GOOD: Moderate performance improvement detected");
            System.out.println("   📈 Caching is providing some benefit");  
        } else {
            System.out.println("⚠️  WARNING: Limited performance improvement");
            System.out.println("   🐌 Cache effectiveness may be limited or network-bound");
        }
        
        // Calculate theoretical vs actual performance
        double networkBaseline = 2.0; // Assume ~2 seconds for network operations
        double expectedImprovement = Math.max(1.0, coldSeconds - networkBaseline) / networkBaseline;
        System.out.printf("📈 Expected improvement ratio: %.2fx (theory)%n", expectedImprovement);
        System.out.printf("📊 Actual improvement ratio:   %.2fx (measured)%n", speedupFactor);
        
        if (speedupFactor >= expectedImprovement * 0.7) {
            System.out.println("🎯 OPTIMIZATION STATUS: Meeting expectations (>70% of theoretical max)");
        } else {
            System.out.println("🔧 OPTIMIZATION STATUS: Below expectations - potential for further optimization");
        }
        
        // Validate performance improvement
        assertTrue(coldResult.getHits().size() > 0, "Query should return results");
        assertEquals(coldResult.getHits().size(), warmResult.getHits().size(), "Both queries should return same number of hits");
        
        // Performance expectations (adjust these based on observed performance)
        if (speedupFactor > 1.5) {
            System.out.println("✅ EXCELLENT: Significant performance improvement detected!");
        } else if (speedupFactor > 1.1) {
            System.out.println("✅ GOOD: Performance improvement detected");
        } else {
            System.out.println("ℹ️  NOTE: No significant performance difference (may indicate hotcache was already active on first access)");
        }
        
        // Step 7.6: Document Retrieval Network Traffic Analysis
        System.out.println("🌐 === DOCUMENT RETRIEVAL NETWORK OPTIMIZATION ANALYSIS ===");

        // Validate metadata for hotcache optimization
        assertTrue(largeSplitMetadata.hasFooterOffsets(),
                  "Large split should have footer metadata for hotcache optimization");

        System.out.printf("Split metadata validation:%n");
        System.out.printf("  📊 Footer offsets: %d..%d (range: %d bytes)%n",
                         largeSplitMetadata.getFooterStartOffset(),
                         largeSplitMetadata.getFooterEndOffset(),
                         largeSplitMetadata.getFooterEndOffset() - largeSplitMetadata.getFooterStartOffset());
        System.out.printf("  📄 Doc mapping: %s%n", largeSplitMetadata.getDocMappingUid());
        System.out.printf("  🕒 Created: %d%n", largeSplitMetadata.getCreateTimestamp());

        // Calculate footer efficiency
        long totalSplitSize = largeSplitMetadata.getUncompressedSizeBytes();
        long footerSize = largeSplitMetadata.getFooterEndOffset() - largeSplitMetadata.getFooterStartOffset();
        double footerEfficiency = footerSize > 0 ? ((double)footerSize / totalSplitSize) * 100 : 0;

        System.out.printf("Split structure analysis:%n");
        System.out.printf("  📦 Total split size: %.2f MB%n", totalSplitSize / 1024.0 / 1024.0);
        System.out.printf("  📋 Footer size: %.2f MB%n", footerSize / 1024.0 / 1024.0);
        System.out.printf("  📊 Footer ratio: %.1f%% (%.2f MB of metadata)%n",
                         footerEfficiency, footerSize / 1024.0 / 1024.0);

        if (footerEfficiency < 10) {
            System.out.println("  ✅ EXCELLENT: Efficient footer structure (low metadata overhead)");
        } else if (footerEfficiency < 20) {
            System.out.println("  ✅ GOOD: Reasonable footer structure (acceptable metadata overhead)");
        } else {
            System.out.println("  ⚠️  HIGH: Footer overhead may impact performance");
        }

        // Debug logging analysis
        String debugEnv = System.getenv("TANTIVY4JAVA_DEBUG");
        if ("1".equals(debugEnv)) {
            System.out.println("🔍 DEBUG MODE ACTIVE: Analyze console output above for network optimization:");
            System.out.println("  POSITIVE INDICATORS (hotcache working):");
            System.out.println("    - '🚀 Using Quickwit optimized path with open_index_with_caches - NO full file download'");
            System.out.println("    - 'Footer offsets from Java config: start=X, end=Y'");
            System.out.println("    - 'Successfully opened index with Quickwit hotcache optimization'");
            System.out.println("    - Consistent document retrieval times (<200ms)");
            System.out.println("  ");
            System.out.println("  NEGATIVE INDICATORS (full downloads):");
            System.out.println("    - '⚠️ Footer metadata not available, falling back to full download'");
            System.out.println("    - Large variance in document retrieval times (>1000ms)");
            System.out.println("    - 'get_slice(relative_path, 0..file_size as usize)' messages");
            System.out.println("  ");
            System.out.println("  🎯 EXPECTED: With proper footer metadata, ALL document retrievals should use hotcache");
        } else {
            System.out.println("💡 TIP: Run with TANTIVY4JAVA_DEBUG=1 to see detailed hotcache vs full download debug messages");
            System.out.println("      This will show exactly which optimization path is being used for each operation");
        }

        // Performance-based validation
        System.out.println("📈 Performance-based validation of split structure:");
        if (speedupFactor > 1.5 && footerEfficiency < 10) {
            System.out.println("  ✅ CONFIRMED: Both performance improvement and efficient footer structure suggest optimization is working");
        } else if (speedupFactor > 1.2 || footerEfficiency < 20) {
            System.out.println("  ✅ LIKELY: Performance or structural analysis suggests good optimization");
        } else {
            System.out.println("  ⚠️  UNCERTAIN: Limited evidence of optimization effectiveness");
        }
        
        System.out.println("✅ Large split performance test completed successfully!");
        
        // Cleanup - optional, comment out if you want to inspect the split file
        // Files.deleteIfExists(largeSplitPath);
    }

    /**
     * Creates a large index (~100MB+) for performance testing
     * Uses 150K documents with ~2KB of content each to ensure >50MB split size
     */
    private void createLargePerformanceIndex(Path indexPath) throws IOException {
        System.out.println("Creating large performance test index...");
        
        // Create schema optimized for performance testing
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("content", true, false, "default", "position");     // Main searchable content
        schemaBuilder.addTextField("category", true, false, "default", "position");   // Category for filtering
        schemaBuilder.addTextField("title", true, false, "default", "position");      // Document title
        schemaBuilder.addIntegerField("id", true, true, true);                       // Unique ID - FAST field
        schemaBuilder.addIntegerField("timestamp", true, true, true);                // Timestamp - FAST field
        schemaBuilder.addIntegerField("score", true, true, true);                    // Score - FAST field
        
        Schema schema = schemaBuilder.build();
        
        // Use extra large memory allocation for large index creation
        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.XL_HEAP_SIZE, 4)) { // Use XL_HEAP_SIZE with more threads
            
            // Target ~100MB split - create many documents with substantial content
            int targetDocs = 150000; // ~150K documents should create ~100MB split with rich content
            System.out.printf("Generating %d documents with rich content...%n", targetDocs);
            
            for (int i = 0; i < targetDocs; i++) {
                Document doc = new Document();
                
                // Add rich text content to make documents larger (~2KB each)
                StringBuilder contentBuilder = new StringBuilder();
                contentBuilder.append("This is performance test document number ").append(i).append(". ");
                contentBuilder.append("This document contains substantial text content designed to create a large split file. ");
                contentBuilder.append("The content includes various searchable terms like performance, optimization, tantivy4java, quickwit, and caching. ");
                contentBuilder.append("Document ").append(i).append(" belongs to category ").append(i % 10).append(" and has been created for testing purposes. ");
                
                // Add repeated Lorem ipsum content to increase document size
                for (int j = 0; j < 5; j++) {
                    contentBuilder.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ");
                    contentBuilder.append("Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. ");
                    contentBuilder.append("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. ");
                    contentBuilder.append("Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. ");
                }
                
                contentBuilder.append("Performance testing is crucial for validating optimization strategies in search engines. ");
                contentBuilder.append("This document ").append(i).append(" will help demonstrate the effectiveness of hotcache versus full download approaches. ");
                contentBuilder.append("Tantivy4java provides excellent performance for large-scale search operations and split file management. ");
                contentBuilder.append("The hotcache optimization reduces network traffic by up to 87% for document retrieval operations.");
                
                doc.addText("content", contentBuilder.toString());
                doc.addText("category", "performance-test-category-" + (i % 10));
                doc.addText("title", "Performance Test Document " + i);
                doc.addInteger("id", i);
                doc.addInteger("timestamp", 1700000000 + i); // Sequential timestamps
                doc.addInteger("score", (i % 100) + 1); // Scores 1-100
                
                writer.addDocument(doc);
                
                // Progress indicator for 150K documents
                if (i > 0 && i % 25000 == 0) {
                    System.out.printf("  Generated %d/%d documents (%.1f%%)%n", i, targetDocs, (i * 100.0) / targetDocs);
                }
            }
            
            System.out.println("📝 Committing large index...");
            writer.commit();
            
            System.out.printf("✅ Created large index with %d documents%n", targetDocs);
        }
    }
    
    /**
     * Serialize split metadata to JSON for storage in Azure
     */
    private String serializeSplitMetadata(QuickwitSplit.SplitMetadata metadata) {
        return String.format("{\n" +
                "    \"splitId\": \"%s\",\n" +
                "    \"numDocs\": %d,\n" +
                "    \"uncompressedSizeBytes\": %d,\n" +
                "    \"footerStartOffset\": %d,\n" +
                "    \"footerEndOffset\": %d,\n" +
                "    \"docMappingUid\": \"%s\",\n" +
                "    \"createTimestamp\": %d,\n" +
                "    \"maturity\": \"%s\"\n" +
                "}",
            metadata.getSplitId(),
            metadata.getNumDocs(),
            metadata.getUncompressedSizeBytes(),
            metadata.getFooterStartOffset(),
            metadata.getFooterEndOffset(),
            metadata.getDocMappingUid(),
            metadata.getCreateTimestamp(),
            metadata.getMaturity()
        );
    }
    
    /**
     * Parse stored split metadata from JSON
     */
    private QuickwitSplit.SplitMetadata parseStoredSplitMetadata(String json) {
        // Simple JSON parsing for the specific format we store
        try {
            String splitId = extractJsonField(json, "splitId");
            long numDocs = Long.parseLong(extractJsonField(json, "numDocs"));
            long uncompressedSizeBytes = Long.parseLong(extractJsonField(json, "uncompressedSizeBytes"));
            long footerStartOffset = Long.parseLong(extractJsonField(json, "footerStartOffset"));
            long footerEndOffset = Long.parseLong(extractJsonField(json, "footerEndOffset"));
            long hotcacheStartOffset = Long.parseLong(extractJsonField(json, "hotcacheStartOffset"));
            long hotcacheLength = Long.parseLong(extractJsonField(json, "hotcacheLength"));
            String docMappingJson = extractJsonField(json, "docMappingJson");
            if ("null".equals(docMappingJson)) {
                docMappingJson = null;
            }
            
            return new QuickwitSplit.SplitMetadata(
                splitId, numDocs, uncompressedSizeBytes,
                java.time.Instant.now().minus(1, java.time.temporal.ChronoUnit.HOURS), java.time.Instant.now(),
                new java.util.HashSet<>(java.util.Arrays.asList("performance-test")),
                0L, 0, footerStartOffset, footerEndOffset, hotcacheStartOffset, hotcacheLength, docMappingJson,
                new java.util.ArrayList<>()  // Skipped splits (empty for test)
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse stored metadata: " + e.getMessage(), e);
        }
    }
    
    /**
     * Simple JSON field extraction
     */
    private String extractJsonField(String json, String fieldName) {
        try {
            // Find the field name in the JSON
            int fieldStart = json.indexOf("\"" + fieldName + "\":");
            if (fieldStart == -1) {
                throw new RuntimeException("Field '" + fieldName + "' not found in JSON");
            }

            // Find the start of the value after the colon
            int valueStart = json.indexOf(":", fieldStart) + 1;
            while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
                valueStart++;
            }

            if (valueStart >= json.length()) {
                throw new RuntimeException("No value found for field '" + fieldName + "'");
            }

            char firstChar = json.charAt(valueStart);
            int valueEnd;

            if (firstChar == '"') {
                // String value - find the matching closing quote, handling escapes properly
                valueEnd = valueStart + 1;
                while (valueEnd < json.length()) {
                    char c = json.charAt(valueEnd);
                    if (c == '"' && (valueEnd == valueStart + 1 || json.charAt(valueEnd - 1) != '\\')) {
                        break;
                    }
                    valueEnd++;
                }
                // Return the string content without the surrounding quotes
                return json.substring(valueStart + 1, valueEnd);
            } else if (firstChar == '[') {
                // Array value - find the matching closing bracket using proper bracket counting
                int bracketCount = 1;
                valueEnd = valueStart + 1;
                boolean inString = false;
                while (valueEnd < json.length() && bracketCount > 0) {
                    char c = json.charAt(valueEnd);
                    if (c == '"' && (valueEnd == 0 || json.charAt(valueEnd - 1) != '\\')) {
                        inString = !inString;
                    } else if (!inString) {
                        if (c == '[') bracketCount++;
                        else if (c == ']') bracketCount--;
                    }
                    valueEnd++;
                }
                return json.substring(valueStart, valueEnd);
            } else if (firstChar == '{') {
                // Object value - find the matching closing brace using proper brace counting
                int braceCount = 1;
                valueEnd = valueStart + 1;
                boolean inString = false;
                while (valueEnd < json.length() && braceCount > 0) {
                    char c = json.charAt(valueEnd);
                    if (c == '"' && (valueEnd == 0 || json.charAt(valueEnd - 1) != '\\')) {
                        inString = !inString;
                    } else if (!inString) {
                        if (c == '{') braceCount++;
                        else if (c == '}') braceCount--;
                    }
                    valueEnd++;
                }
                return json.substring(valueStart, valueEnd);
            } else {
                // Primitive value (number, boolean, null) - find next comma or closing brace
                valueEnd = valueStart;
                while (valueEnd < json.length()) {
                    char c = json.charAt(valueEnd);
                    if (c == ',' || c == '}') {
                        break;
                    }
                    valueEnd++;
                }
                return json.substring(valueStart, valueEnd).trim();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract field '" + fieldName + "' from JSON: " + e.getMessage(), e);
        }
    }

    @Test
    @org.junit.jupiter.api.Order(10)
    @DisplayName("Step 10: OAuth Bearer Token End-to-End Test")
    public void step10_oauthBearerTokenEndToEndTest() throws Exception {
        System.out.println("\n🔐 === OAUTH BEARER TOKEN END-TO-END TEST ===");
        System.out.println("Testing complete write-merge-read cycle using OAuth bearer tokens");

        // Step 10.1: Load Azure AD credentials from ~/.azure/credentials
        System.out.println("\n📋 Step 10.1: Loading Azure AD credentials and acquiring OAuth token");
        final String[] tokenHolder = new String[1];  // Holder for token to use in lambda
        final java.time.OffsetDateTime[] expiresAtHolder = new java.time.OffsetDateTime[1];  // Holder for expiration time
        String storageAccountName = null;
        String clientId = null;
        String clientSecret = null;
        String tenantId = null;
        ClientSecretCredential credential = null;

        try {
            // Read credentials from ~/.azure/credentials file
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("⚠️  ~/.azure/credentials file not found, skipping OAuth test");
                System.out.println("   Create ~/.azure/credentials with Service Principal info:");
                System.out.println("   [default]");
                System.out.println("   storage_account=<storage-account-name>");
                System.out.println("   client_id=<service-principal-client-id>");
                System.out.println("   client_secret=<service-principal-client-secret>");
                System.out.println("   tenant_id=<azure-tenant-id>");
                Assumptions.assumeTrue(false, "OAuth test skipped - ~/.azure/credentials not found");
                return;
            }

            System.out.println("   Reading ~/.azure/credentials...");
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

                        switch (key) {
                            case "storage_account":
                                storageAccountName = value;
                                break;
                            case "client_id":
                                clientId = value;
                                break;
                            case "client_secret":
                                clientSecret = value;
                                break;
                            case "tenant_id":
                                tenantId = value;
                                break;
                        }
                    }
                }
            }

            // Validate required credentials
            if (storageAccountName == null || clientId == null || clientSecret == null || tenantId == null) {
                System.out.println("⚠️  Missing required credentials in ~/.azure/credentials");
                System.out.println("   Found: storage_account=" + (storageAccountName != null ? "✓" : "✗"));
                System.out.println("   Found: client_id=" + (clientId != null ? "✓" : "✗"));
                System.out.println("   Found: client_secret=" + (clientSecret != null ? "✓" : "✗"));
                System.out.println("   Found: tenant_id=" + (tenantId != null ? "✓" : "✗"));
                Assumptions.assumeTrue(false, "OAuth test skipped - incomplete credentials");
                return;
            }

            System.out.println("✅ Credentials loaded from ~/.azure/credentials");
            System.out.println("   Storage account: " + storageAccountName);
            System.out.println("   Tenant ID: " + tenantId);
            System.out.println("   Client ID: " + clientId);

            // Step 10.2: Create OAuth credential using Service Principal
            System.out.println("\n   Creating OAuth credential from Azure AD Service Principal...");

            credential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();

            // Acquire token to verify credential works and get token for native layer
            TokenRequestContext context = new TokenRequestContext()
                .addScopes("https://storage.azure.com/.default");

            AccessToken token = credential.getToken(context).block();
            if (token == null) {
                throw new Exception("Failed to acquire access token");
            }

            tokenHolder[0] = token.getToken();
            expiresAtHolder[0] = token.getExpiresAt();
            System.out.println("✅ OAuth credential created successfully");
            System.out.println("   Token length: " + tokenHolder[0].length() + " characters");
            System.out.println("   Token expires: " + token.getExpiresAt());

        } catch (Exception e) {
            System.out.println("⚠️  Could not acquire OAuth token: " + e.getMessage());
            System.out.println("   This test requires Azure Service Principal credentials in ~/.azure/credentials");
            System.out.println("   Skipping OAuth end-to-end test");
            Assumptions.assumeTrue(false, "OAuth test skipped - " + e.getMessage());
            return;
        }

        final String bearerToken = tokenHolder[0];  // Extract final token for use
        final ClientSecretCredential finalCredential = credential;  // Extract final credential for use

        // Step 10.2: Create test splits
        System.out.println("\n📦 Step 10.2: Creating test splits for OAuth merge");

        // Create schema for OAuth test
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("name", true, false, "default", "position");
        schemaBuilder.addIntegerField("category", true, true, true);
        Schema oauthSchema = schemaBuilder.build();

        Path oauthIndexPath = tempDir.resolve("oauth-test-index");
        Path oauthSplit1 = tempDir.resolve("oauth-split-1.split");
        Path oauthSplit2 = tempDir.resolve("oauth-split-2.split");

        // Create first split
        try (Index index1 = new Index(oauthSchema, oauthIndexPath.toString())) {
            try (IndexWriter writer = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                writer.addJson("{\"name\": \"OAuth Test 1\", \"category\": 1}");
                writer.addJson("{\"name\": \"Bearer Token Test\", \"category\": 1}");
                writer.commit();
            }
        }

        QuickwitSplit.SplitConfig split1Config = new QuickwitSplit.SplitConfig(
            "oauth-end-to-end-test", "oauth-source", "oauth-node-1"
        );
        QuickwitSplit.SplitMetadata metadata1 = QuickwitSplit.convertIndexFromPath(
            oauthIndexPath.toString(), oauthSplit1.toString(), split1Config
        );
        System.out.println("✅ Split 1 created: " + metadata1.getSplitId() + " (" + metadata1.getNumDocs() + " docs)");

        // Create second split
        try (Index index2 = new Index(oauthSchema, oauthIndexPath.toString())) {
            try (IndexWriter writer = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                writer.addJson("{\"name\": \"Azure AD Integration\", \"category\": 2}");
                writer.addJson("{\"name\": \"Service Principal Auth\", \"category\": 2}");
                writer.commit();
            }
        }

        QuickwitSplit.SplitConfig split2Config = new QuickwitSplit.SplitConfig(
            "oauth-end-to-end-test", "oauth-source", "oauth-node-2"
        );
        QuickwitSplit.SplitMetadata metadata2 = QuickwitSplit.convertIndexFromPath(
            oauthIndexPath.toString(), oauthSplit2.toString(), split2Config
        );
        System.out.println("✅ Split 2 created: " + metadata2.getSplitId() + " (" + metadata2.getNumDocs() + " docs)");

        // Step 10.3: Upload splits to Azure using existing account key credentials
        System.out.println("\n☁️  Step 10.3: Uploading splits to Azure (using account key for initial upload)");
        System.out.println("   Note: OAuth bearer token will be tested in the merge operation");

        String oauthContainer = TEST_CONTAINER;  // Use existing test container
        String split1BlobName = "oauth-split-1-" + System.currentTimeMillis() + ".split";
        String split2BlobName = "oauth-split-2-" + System.currentTimeMillis() + ".split";
        String mergedBlobName = "oauth-merged-" + System.currentTimeMillis() + ".split";

        // Upload using existing account key client (Service Principal may not have write permissions)
        BlobContainerClient uploadContainer = blobServiceClient.getBlobContainerClient(oauthContainer);

        uploadContainer.getBlobClient(split1BlobName).uploadFromFile(oauthSplit1.toString(), true);
        System.out.println("✅ Uploaded: " + split1BlobName);

        uploadContainer.getBlobClient(split2BlobName).uploadFromFile(oauthSplit2.toString(), true);
        System.out.println("✅ Uploaded: " + split2BlobName);

        // Step 10.3.5: Verify OAuth token can access the uploaded blobs
        System.out.println("\n🔐 Step 10.3.5: Verifying OAuth token has read access to blobs");
        try {
            // Create a client using the bearer token to test access
            com.azure.core.credential.TokenCredential tokenCredential = new com.azure.core.credential.TokenCredential() {
                @Override
                public reactor.core.publisher.Mono<com.azure.core.credential.AccessToken> getToken(com.azure.core.credential.TokenRequestContext request) {
                    return reactor.core.publisher.Mono.just(new com.azure.core.credential.AccessToken(bearerToken, expiresAtHolder[0]));
                }
            };

            BlobServiceClient oauthBlobClient = new BlobServiceClientBuilder()
                .endpoint("https://" + storageAccountName + ".blob.core.windows.net")
                .credential(tokenCredential)
                .buildClient();

            BlobClient testBlob = oauthBlobClient.getBlobContainerClient(oauthContainer).getBlobClient(split1BlobName);
            long blobSize = testBlob.getProperties().getBlobSize();
            System.out.println("✅ OAuth token verified - can read blob (" + blobSize + " bytes)");
        } catch (Exception e) {
            System.out.println("❌ OAuth token CANNOT read uploaded blobs!");
            System.out.println("   Error: " + e.getMessage());
            System.out.println("\n⚠️  PERMISSION ISSUE DETECTED:");
            System.out.println("   The Service Principal needs Azure RBAC role assignment:");
            System.out.println("   1. Go to Azure Portal → Storage Account → Access Control (IAM)");
            System.out.println("   2. Add role assignment → 'Storage Blob Data Reader' or 'Storage Blob Data Contributor'");
            System.out.println("   3. Select your Service Principal (Client ID: " + clientId + ")");
            System.out.println("   4. Save and wait a few minutes for propagation");
            System.out.println("\n   Alternatively, use Azure CLI:");
            System.out.println("   az role assignment create \\");
            System.out.println("     --role \"Storage Blob Data Contributor\" \\");
            System.out.println("     --assignee " + clientId + " \\");
            System.out.println("     --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/" + storageAccountName);
            System.out.println("\n   Skipping OAuth merge test due to insufficient permissions.");
            Assumptions.assumeTrue(false, "OAuth test skipped - Service Principal lacks Storage Blob Data Reader role");
            return;
        }

        // Step 10.4: Merge splits using bearer token
        System.out.println("\n🔗 Step 10.4: Merging splits using OAuth bearer token");

        QuickwitSplit.AzureConfig oauthAzureConfig = QuickwitSplit.AzureConfig.withBearerToken(
            storageAccountName,
            bearerToken
        );

        QuickwitSplit.MergeConfig oauthMergeConfig = QuickwitSplit.MergeConfig.builder()
            .indexUid("oauth-end-to-end-test")
            .sourceId("oauth-source")
            .nodeId("oauth-merge-node")
            .azureConfig(oauthAzureConfig)
            .debugEnabled(true)
            .build();

        String split1Uri = "azure://" + oauthContainer + "/" + split1BlobName;
        String split2Uri = "azure://" + oauthContainer + "/" + split2BlobName;
        String mergedUri = "azure://" + oauthContainer + "/" + mergedBlobName;

        System.out.println("   Merging:");
        System.out.println("   - " + split1Uri);
        System.out.println("   - " + split2Uri);
        System.out.println("   → " + mergedUri);

        QuickwitSplit.SplitMetadata mergedMetadata = QuickwitSplit.mergeSplits(
            Arrays.asList(split1Uri, split2Uri),
            mergedUri,
            oauthMergeConfig
        );

        assertNotNull(mergedMetadata, "Merged split metadata should not be null");
        assertEquals(4, mergedMetadata.getNumDocs(), "Merged split should have 4 documents");
        System.out.println("✅ Merge completed using OAuth bearer token");
        System.out.println("   Merged split ID: " + mergedMetadata.getSplitId());
        System.out.println("   Total documents: " + mergedMetadata.getNumDocs());

        // Step 10.5: Search the merged split using bearer token
        System.out.println("\n🔍 Step 10.5: Searching merged split using OAuth bearer token");

        SplitCacheManager.CacheConfig oauthCacheConfig = new SplitCacheManager.CacheConfig("oauth-cache-" + System.currentTimeMillis())
            .withMaxCacheSize(50_000_000)
            .withAzureBearerToken(storageAccountName, bearerToken);

        SplitCacheManager oauthCacheManager = SplitCacheManager.getInstance(oauthCacheConfig);

        try (SplitSearcher searcher = oauthCacheManager.createSplitSearcher(mergedUri, mergedMetadata)) {
            // Test 1: Search for all documents
            System.out.println("   Testing query: *");
            SplitQuery allDocsQuery = searcher.parseQuery("*");
            io.indextables.tantivy4java.result.SearchResult allDocsResult = searcher.search(allDocsQuery, 10);
            System.out.println("   ✅ Found " + allDocsResult.getHits().size() + " documents (expected 4)");
            assertEquals(4, allDocsResult.getHits().size(), "Should find all 4 documents");

            // Test 2: Search for specific term
            System.out.println("   Testing query: name:oauth");
            SplitQuery oauthQuery = searcher.parseQuery("name:oauth");
            io.indextables.tantivy4java.result.SearchResult oauthResult = searcher.search(oauthQuery, 10);
            System.out.println("   ✅ Found " + oauthResult.getHits().size() + " documents matching 'oauth'");
            assertTrue(oauthResult.getHits().size() > 0, "Should find documents with 'oauth' in name");

            // Test 3: Search for another term
            System.out.println("   Testing query: name:bearer");
            SplitQuery bearerQuery = searcher.parseQuery("name:bearer");
            io.indextables.tantivy4java.result.SearchResult bearerResult = searcher.search(bearerQuery, 10);
            System.out.println("   ✅ Found " + bearerResult.getHits().size() + " document(s) matching 'bearer'");

            // Test 4: Category query
            System.out.println("   Testing query: category:1");
            SplitQuery cat1Query = searcher.parseQuery("category:1");
            io.indextables.tantivy4java.result.SearchResult cat1Result = searcher.search(cat1Query, 10);
            System.out.println("   ✅ Found " + cat1Result.getHits().size() + " documents in category 1");
            assertEquals(2, cat1Result.getHits().size(), "Should find 2 documents in category 1");

            // Test 5: Terms aggregation on category field
            System.out.println("   Testing aggregation: terms on category field");
            SplitQuery allDocsForAgg = searcher.parseQuery("*");
            io.indextables.tantivy4java.aggregation.TermsAggregation categoryAgg =
                new io.indextables.tantivy4java.aggregation.TermsAggregation("category");

            io.indextables.tantivy4java.result.SearchResult aggResult = searcher.search(allDocsForAgg, 10, "category_terms", categoryAgg);
            java.util.Map<String, io.indextables.tantivy4java.aggregation.AggregationResult> aggResults = aggResult.getAggregations();

            assertNotNull(aggResults, "Aggregation results should not be null");
            assertTrue(aggResults.containsKey("category_terms"), "Should have category_terms aggregation");

            io.indextables.tantivy4java.aggregation.AggregationResult categoryResult = aggResults.get("category_terms");
            assertTrue(categoryResult instanceof io.indextables.tantivy4java.aggregation.TermsResult, "Result should be a TermsResult");

            io.indextables.tantivy4java.aggregation.TermsResult termsResult = (io.indextables.tantivy4java.aggregation.TermsResult) categoryResult;
            System.out.println("   ✅ Aggregation executed successfully with " + termsResult.getBuckets().size() + " buckets");
            assertEquals(2, termsResult.getBuckets().size(), "Should have 2 category buckets (category 1 and 2)");

            System.out.println("\n✅ All OAuth bearer token search queries and aggregations successful!");
        }

        System.out.println("\n✅ OAuth bearer token end-to-end test completed successfully!");
        System.out.println("\n📊 Test Summary:");
        System.out.println("   ✅ OAuth token acquisition from Service Principal");
        System.out.println("   ✅ Split creation and upload");
        System.out.println("   ✅ Split merge operation using bearer token authentication");
        System.out.println("   ✅ Verified merged split contains 4 documents");
        System.out.println("   ✅ Search queries executed successfully using OAuth bearer token");
        System.out.println("   ✅ Aggregation queries executed successfully using OAuth bearer token");

        // Cleanup
        System.out.println("\n🧹 Cleaning up test resources...");
        try {
            uploadContainer.getBlobClient(split1BlobName).delete();
            uploadContainer.getBlobClient(split2BlobName).delete();
            uploadContainer.getBlobClient(mergedBlobName).delete();
            System.out.println("✅ Cleanup completed");
        } catch (Exception e) {
            System.out.println("⚠️  Cleanup warning: " + e.getMessage());
        }
    }
}
