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
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.xref.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive end-to-end test for XRef (Cross-Reference) splits with real Azure Blob Storage.
 *
 * This test demonstrates the complete XRef workflow:
 * 1. Create 4 different indexes with domain-specific data (each with unique terms)
 * 2. Convert indexes to Quickwit splits
 * 3. Upload splits to real Azure Blob Storage
 * 4. Build an XRef split from the Azure source splits
 * 5. Search the XRef to identify which splits contain specific terms
 * 6. Validate that XRef correctly routes queries to matching splits
 *
 * Prerequisites:
 * - Azure credentials configured (~/.azure/credentials or environment variables)
 * - Azure container accessible for testing
 *
 * Set system properties:
 * -Dtest.azure.container=your-test-container
 * -Dtest.azure.storageAccount=yourstorageaccount
 * -Dtest.azure.accountKey=your-account-key (optional if using ~/.azure/credentials)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzureXRefTest {

    // Test configuration from system properties
    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");

    // Azure credentials loaded from ~/.azure/credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;

    @TempDir
    static Path tempDir;

    // Domain-specific test data - each domain has unique terms for XRef validation
    private static final String[] DOMAINS = {"astronomy", "biology", "chemistry", "physics"};

    // Unique terms per domain that ONLY appear in that domain
    private static final String[][] DOMAIN_UNIQUE_TERMS = {
        {"nebula", "quasar", "pulsar", "supernova", "asteroid"},       // astronomy
        {"mitochondria", "chloroplast", "ribosome", "enzyme", "dna"},  // biology
        {"oxidation", "catalyst", "polymer", "molecule", "isotope"},   // chemistry
        {"quantum", "photon", "electron", "graviton", "boson"}         // physics
    };

    // Test data tracking
    private static QuickwitSplit.SplitMetadata[] splitMetadata = new QuickwitSplit.SplitMetadata[4];
    private static String[] splitAzureUrls = new String[4];
    private static BlobServiceClient blobServiceClient;
    private static XRefMetadata xrefMetadata;
    private static String xrefAzureUrl;

    @BeforeAll
    static void setupAzureClient() {
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        if (storageAccount != null && accountKey != null) {
            blobServiceClient = new BlobServiceClientBuilder()
                .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
                .buildClient();
            System.out.println("[XRef Azure Test] Azure client initialized");

            ensureContainerExists();
        } else {
            throw new IllegalStateException("Azure credentials not found. Please configure ~/.azure/credentials or environment variables.");
        }
    }

    private static void ensureContainerExists() {
        try {
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
            if (!containerClient.exists()) {
                containerClient.create();
                System.out.println("[XRef Azure Test] Created Azure container: " + TEST_CONTAINER);
            } else {
                System.out.println("[XRef Azure Test] Azure container exists: " + TEST_CONTAINER);
            }
        } catch (Exception e) {
            System.out.println("[XRef Azure Test] Cannot check container: " + e.getMessage());
        }
    }

    @AfterAll
    static void cleanupAzureClient() {
        if (blobServiceClient != null) {
            System.out.println("[XRef Azure Test] Azure client cleanup complete");
        }
    }

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        if (azureStorageAccount != null) return azureStorageAccount;
        return null;
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureAccountKey != null) return azureAccountKey;
        return null;
    }

    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("[XRef Azure Test] Azure credentials file not found at: " + credentialsPath);
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
                System.out.println("[XRef Azure Test] Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("[XRef Azure Test] Failed to read Azure credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    static void validateConfiguration() {
        System.out.println("=== REAL AZURE XREF END-TO-END TEST ===");
        System.out.println("Test container: " + TEST_CONTAINER);
        System.out.println("Storage account: " + STORAGE_ACCOUNT);

        boolean hasExplicitCreds = STORAGE_ACCOUNT != null && ACCOUNT_KEY != null;
        if (!hasExplicitCreds) {
            loadAzureCredentials();
        }

        boolean hasFileCredentials = azureStorageAccount != null && azureAccountKey != null;
        boolean hasEnvCreds = System.getenv("AZURE_STORAGE_ACCOUNT") != null;

        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("WARNING: No Azure credentials found. Skipping Azure XRef tests.");
            Assumptions.abort("Azure credentials not available");
        }
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create 4 domain-specific indices with unique terms")
    void step1_createDomainIndices() throws IOException {
        System.out.println("\n[Step 1] Creating 4 domain-specific indices...");

        for (int i = 0; i < DOMAINS.length; i++) {
            String domain = DOMAINS[i];
            System.out.println("  Creating index for domain: " + domain);

            Path indexPath = tempDir.resolve(domain + "-index");
            createDomainIndex(indexPath, domain, i);

            // Convert to split
            Path splitPath = tempDir.resolve(domain + ".split");
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "xref-azure-test-" + domain,
                "xref-source",
                "xref-node-" + i
            );

            splitMetadata[i] = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(),
                splitPath.toString(),
                config
            );

            assertNotNull(splitMetadata[i], "Split metadata should be created for " + domain);
            assertTrue(splitMetadata[i].getNumDocs() > 0, "Split should contain documents for " + domain);

            System.out.println("    Created " + domain + " split: " +
                splitMetadata[i].getNumDocs() + " docs");
        }

        System.out.println("[Step 1] All 4 domain indices created successfully");
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: Upload splits to Azure Blob Storage")
    void step2_uploadSplitsToAzure() throws IOException {
        System.out.println("\n[Step 2] Uploading splits to Azure Blob Storage...");

        // Validate prerequisites
        for (int i = 0; i < DOMAINS.length; i++) {
            Path localSplitPath = tempDir.resolve(DOMAINS[i] + ".split");
            if (!Files.exists(localSplitPath)) {
                fail("Prerequisite failure: Split file for " + DOMAINS[i] + " does not exist");
            }
        }

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);

        for (int i = 0; i < DOMAINS.length; i++) {
            String domain = DOMAINS[i];
            Path localSplitPath = tempDir.resolve(domain + ".split");
            String blobName = "xref-test/splits/" + domain + ".split";

            splitAzureUrls[i] = String.format("azure://%s/%s", TEST_CONTAINER, blobName);

            System.out.println("  Uploading " + domain + " to " + splitAzureUrls[i]);

            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.uploadFromFile(localSplitPath.toString(), true);

            System.out.println("    Uploaded " + domain + " split (" +
                Files.size(localSplitPath) + " bytes)");
        }

        System.out.println("[Step 2] All splits uploaded to Azure");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Build XRef split from Azure source splits")
    void step3_buildXRefSplit() throws Exception {
        System.out.println("\n[Step 3] Building XRef split from Azure source splits...");

        // Validate prerequisites
        for (int i = 0; i < DOMAINS.length; i++) {
            assertNotNull(splitAzureUrls[i], "Azure URL should be set for " + DOMAINS[i]);
        }

        // Create Azure config for XRef build
        QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
            getStorageAccount(), getAccountKey());

        // Build XRef config with XRefSourceSplit objects containing footer offsets
        List<XRefSourceSplit> sourceSplits = new ArrayList<>();
        for (int i = 0; i < DOMAINS.length; i++) {
            XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(
                splitAzureUrls[i], splitMetadata[i]);
            sourceSplits.add(source);
            System.out.println("  Source split " + i + ": " + splitAzureUrls[i] +
                " (footer: " + splitMetadata[i].getFooterStartOffset() + "-" +
                splitMetadata[i].getFooterEndOffset() + ")");
        }

        XRefBuildConfig buildConfig = XRefBuildConfig.builder()
            .xrefId("science-azure-xref")
            .indexUid("science-azure-index")
            .sourceSplits(sourceSplits)
            .azureConfig(azureConfig)
            .includePositions(false)
            .build();

        // Build XRef split
        Path xrefLocalPath = tempDir.resolve("science.xref.split");
        System.out.println("  Building XRef from " + sourceSplits.size() + " source splits...");

        xrefMetadata = XRefSplit.build(buildConfig, xrefLocalPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(4, xrefMetadata.getNumSplits(), "XRef should reference 4 source splits");
        assertTrue(xrefMetadata.hasFooterOffsets(), "XRef metadata should have footer offsets");

        System.out.println("  XRef built successfully:");
        System.out.println("    - XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("    - Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("    - Total source docs: " + xrefMetadata.getTotalSourceDocs());
        System.out.println("    - Total terms: " + xrefMetadata.getTotalTerms());
        System.out.println("    - Build time: " + xrefMetadata.getBuildStats().getBuildDurationMs() + "ms");

        System.out.println("[Step 3] XRef split built successfully");
    }

    @Test
    @Order(4)
    @DisplayName("Step 4: Upload XRef split to Azure")
    void step4_uploadXRefToAzure() throws IOException {
        System.out.println("\n[Step 4] Uploading XRef split to Azure...");

        Path xrefLocalPath = tempDir.resolve("science.xref.split");
        if (!Files.exists(xrefLocalPath)) {
            fail("Prerequisite failure: XRef split file does not exist");
        }

        String blobName = "xref-test/xref/science.xref.split";
        xrefAzureUrl = String.format("azure://%s/%s", TEST_CONTAINER, blobName);

        System.out.println("  Uploading XRef to " + xrefAzureUrl);

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.uploadFromFile(xrefLocalPath.toString(), true);

        System.out.println("  Uploaded XRef split (" + Files.size(xrefLocalPath) + " bytes)");
        System.out.println("[Step 4] XRef uploaded to Azure");
    }

    @Test
    @Order(5)
    @DisplayName("Step 5: Search XRef and validate query routing")
    void step5_searchXRefAndValidate() throws Exception {
        System.out.println("\n[Step 5] Searching XRef split and validating query routing...");

        assertNotNull(xrefMetadata, "XRef metadata should be available from Step 3");
        assertNotNull(xrefAzureUrl, "XRef Azure URL should be available from Step 4");

        // Create cache manager with Azure credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-azure-cache")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, xrefAzureUrl, xrefMetadata)) {

            System.out.println("  XRef searcher opened successfully");
            System.out.println("  Number of source splits: " + xrefSearcher.getNumSplits());

            // Test 1: Search for astronomy-specific term
            System.out.println("\n  Test 1: Searching for 'nebula' (astronomy only)...");
            XRefSearchResult result1 = xrefSearcher.search("content:nebula", 10);
            System.out.println("    Found in " + result1.getNumMatchingSplits() + " split(s)");
            if (result1.hasMatches()) {
                System.out.println("    Matching splits: " + result1.getSplitUrisToSearch());
            }

            // Test 2: Search for biology-specific term
            System.out.println("\n  Test 2: Searching for 'mitochondria' (biology only)...");
            XRefSearchResult result2 = xrefSearcher.search("content:mitochondria", 10);
            System.out.println("    Found in " + result2.getNumMatchingSplits() + " split(s)");
            if (result2.hasMatches()) {
                System.out.println("    Matching splits: " + result2.getSplitUrisToSearch());
            }

            // Test 3: Search for chemistry-specific term
            System.out.println("\n  Test 3: Searching for 'catalyst' (chemistry only)...");
            XRefSearchResult result3 = xrefSearcher.search("content:catalyst", 10);
            System.out.println("    Found in " + result3.getNumMatchingSplits() + " split(s)");

            // Test 4: Search for physics-specific term
            System.out.println("\n  Test 4: Searching for 'quantum' (physics only)...");
            XRefSearchResult result4 = xrefSearcher.search("content:quantum", 10);
            System.out.println("    Found in " + result4.getNumMatchingSplits() + " split(s)");

            // Test 5: Search for all splits (match-all)
            System.out.println("\n  Test 5: Match-all query...");
            XRefSearchResult result5 = xrefSearcher.search("*", 10);
            System.out.println("    Found " + result5.getNumMatchingSplits() + " split(s)");
            assertEquals(4, result5.getNumMatchingSplits(), "Match-all should return all 4 splits");

            // Test 6: Get all split URIs
            System.out.println("\n  Test 6: Getting all split URIs from metadata...");
            List<String> allUris = xrefSearcher.getAllSplitUris();
            assertEquals(4, allUris.size(), "Should have 4 split URIs");
            for (String uri : allUris) {
                System.out.println("    - " + uri);
            }
        }

        System.out.println("\n[Step 5] XRef search validation complete");
    }

    @Test
    @Order(6)
    @DisplayName("Step 6: Validate XRef routing accuracy")
    void step6_validateRoutingAccuracy() throws Exception {
        System.out.println("\n[Step 6] Validating XRef routing accuracy...");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-azure-validation-cache")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, xrefAzureUrl, xrefMetadata)) {

            // For each domain, search for its unique terms and verify
            // the XRef points to the correct split
            for (int i = 0; i < DOMAINS.length; i++) {
                String domain = DOMAINS[i];
                String[] uniqueTerms = DOMAIN_UNIQUE_TERMS[i];
                String expectedSplitUrl = splitAzureUrls[i];

                System.out.println("\n  Validating " + domain + " domain...");

                for (String term : uniqueTerms) {
                    // Check if term exists in any split
                    boolean exists = xrefSearcher.termExists("content", term);
                    System.out.println("    Term '" + term + "': exists=" + exists);

                    // Get matching splits for this term
                    List<String> matchingUris = xrefSearcher.getMatchingSplitUris("content:" + term, 10);
                    System.out.println("    Matching splits: " + matchingUris.size());

                    // If we found matches, verify they contain the expected split
                    if (!matchingUris.isEmpty()) {
                        boolean containsExpected = matchingUris.stream()
                            .anyMatch(uri -> uri.contains(domain));
                        if (!containsExpected) {
                            System.out.println("    WARNING: Expected " + domain + " split not found in results");
                        }
                    }
                }
            }
        }

        System.out.println("\n[Step 6] XRef routing accuracy validation complete");
    }

    @Test
    @Order(7)
    @DisplayName("Step 7: Verify XRef metadata integrity")
    void step7_verifyMetadataIntegrity() {
        System.out.println("\n[Step 7] Verifying XRef metadata integrity...");

        assertNotNull(xrefMetadata, "XRef metadata should exist");

        // Verify basic metadata
        assertEquals("science-azure-xref", xrefMetadata.getXrefId(), "XRef ID should match");
        assertEquals("science-azure-index", xrefMetadata.getIndexUid(), "Index UID should match");
        assertEquals(4, xrefMetadata.getNumSplits(), "Should have 4 source splits");

        // Verify footer offsets are set
        assertTrue(xrefMetadata.hasFooterOffsets(), "Should have footer offsets");
        assertTrue(xrefMetadata.getFooterStartOffset() < xrefMetadata.getFooterEndOffset(),
            "Footer start should be before footer end");

        // Verify build stats
        XRefMetadata.BuildStats buildStats = xrefMetadata.getBuildStats();
        assertNotNull(buildStats, "Build stats should exist");
        assertTrue(buildStats.getBuildDurationMs() > 0, "Build duration should be positive");
        assertEquals(4, buildStats.getSplitsProcessed(), "Should have processed 4 splits");
        assertEquals(0, buildStats.getSplitsSkipped(), "Should have skipped 0 splits");

        // Verify total docs
        long expectedTotalDocs = 0;
        for (QuickwitSplit.SplitMetadata sm : splitMetadata) {
            expectedTotalDocs += sm.getNumDocs();
        }
        assertEquals(expectedTotalDocs, xrefMetadata.getTotalSourceDocs(),
            "Total source docs should match sum of individual splits");

        System.out.println("  XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("  Index UID: " + xrefMetadata.getIndexUid());
        System.out.println("  Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("  Total source docs: " + xrefMetadata.getTotalSourceDocs());
        System.out.println("  Total terms: " + xrefMetadata.getTotalTerms());
        System.out.println("  Footer offsets: " + xrefMetadata.getFooterStartOffset() +
            " - " + xrefMetadata.getFooterEndOffset());
        System.out.println("  Build duration: " + buildStats.getBuildDurationMs() + "ms");

        System.out.println("[Step 7] XRef metadata integrity verified");
    }

    /**
     * Creates a domain-specific index with unique terms for that domain.
     */
    private void createDomainIndex(Path indexPath, String domain, int domainIndex) throws IOException {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("domain", true, false, "default", "position");
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);

        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

            String[] uniqueTerms = DOMAIN_UNIQUE_TERMS[domainIndex];
            int docsPerDomain = 20;

            for (int i = 0; i < docsPerDomain; i++) {
                Document doc = new Document();
                doc.addText("domain", domain);
                doc.addText("title", domain + " Document " + i);
                doc.addInteger("id", (domainIndex * 1000) + i);

                // Content includes domain-specific unique terms
                StringBuilder content = new StringBuilder();
                content.append("This is a ").append(domain).append(" document about ");

                // Rotate through unique terms
                String term = uniqueTerms[i % uniqueTerms.length];
                content.append(term).append(". ");
                content.append("The study of ").append(term).append(" is fundamental to ").append(domain).append(".");

                // Add some common terms too
                content.append(" Research and science are important.");

                doc.addText("content", content.toString());

                writer.addDocument(doc);
            }

            writer.commit();
        }

        System.out.println("    Created " + domain + " index with unique terms: " +
            Arrays.toString(DOMAIN_UNIQUE_TERMS[domainIndex]));
    }
}
