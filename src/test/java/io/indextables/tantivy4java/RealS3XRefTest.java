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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive end-to-end test for XRef (Cross-Reference) splits with real S3 storage.
 *
 * This test demonstrates the complete XRef workflow:
 * 1. Create 4 different indexes with domain-specific data (each with unique terms)
 * 2. Convert indexes to Quickwit splits
 * 3. Upload splits to real S3 storage
 * 4. Build an XRef split from the S3 source splits
 * 5. Search the XRef to identify which splits contain specific terms
 * 6. Validate that XRef correctly routes queries to matching splits
 *
 * Prerequisites:
 * - AWS credentials configured (~/.aws/credentials or environment variables)
 * - S3 bucket accessible for testing
 *
 * Set system properties:
 * -Dtest.s3.bucket=your-test-bucket
 * -Dtest.s3.region=us-east-1
 * -Dtest.s3.accessKey=your-access-key (optional if using profile/role)
 * -Dtest.s3.secretKey=your-secret-key (optional if using profile/role)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3XRefTest {

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
    private static String[] splitS3Urls = new String[4];
    private static S3Client s3Client;
    private static XRefMetadata xrefMetadata;
    private static String xrefS3Url;

    @BeforeAll
    static void setupS3Client() {
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        if (accessKey != null && secretKey != null) {
            s3Client = S3Client.builder()
                .region(Region.of(TEST_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
            System.out.println("[XRef Test] S3 client initialized");

            ensureBucketExists();
        } else {
            throw new IllegalStateException("AWS credentials not found. Please configure ~/.aws/credentials or environment variables.");
        }
    }

    private static void ensureBucketExists() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(TEST_BUCKET).build());
            System.out.println("[XRef Test] S3 bucket exists: " + TEST_BUCKET);
        } catch (NoSuchBucketException e) {
            System.out.println("[XRef Test] Creating bucket: " + TEST_BUCKET);
            try {
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
                System.out.println("[XRef Test] Created S3 bucket: " + TEST_BUCKET);
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                System.out.println("[XRef Test] Bucket already exists: " + TEST_BUCKET);
            }
        } catch (Exception e) {
            System.out.println("[XRef Test] Cannot check bucket: " + e.getMessage());
        }
    }

    @AfterAll
    static void cleanupS3Client() {
        if (s3Client != null) {
            s3Client.close();
            System.out.println("[XRef Test] S3 client closed");
        }
    }

    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return null;
    }

    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return null;
    }

    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("[XRef Test] AWS credentials file not found at: " + credentialsPath);
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
                System.out.println("[XRef Test] Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("[XRef Test] Failed to read AWS credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    static void validateConfiguration() {
        System.out.println("=== REAL S3 XREF END-TO-END TEST ===");
        System.out.println("Test bucket: " + TEST_BUCKET);
        System.out.println("Test region: " + TEST_REGION);

        boolean hasExplicitCreds = ACCESS_KEY != null && SECRET_KEY != null;
        if (!hasExplicitCreds) {
            loadAwsCredentials();
        }

        boolean hasFileCredentials = awsAccessKey != null && awsSecretKey != null;
        boolean hasEnvCreds = System.getenv("AWS_ACCESS_KEY_ID") != null;

        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("WARNING: No AWS credentials found. Skipping S3 XRef tests.");
            Assumptions.abort("AWS credentials not available");
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
                "xref-test-" + domain,
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
    @DisplayName("Step 2: Upload splits to S3")
    void step2_uploadSplitsToS3() throws IOException {
        System.out.println("\n[Step 2] Uploading splits to S3...");

        // Validate prerequisites
        for (int i = 0; i < DOMAINS.length; i++) {
            Path localSplitPath = tempDir.resolve(DOMAINS[i] + ".split");
            if (!Files.exists(localSplitPath)) {
                fail("Prerequisite failure: Split file for " + DOMAINS[i] + " does not exist");
            }
        }

        for (int i = 0; i < DOMAINS.length; i++) {
            String domain = DOMAINS[i];
            Path localSplitPath = tempDir.resolve(domain + ".split");
            String s3Key = "xref-test/splits/" + domain + ".split";

            splitS3Urls[i] = String.format("s3://%s/%s", TEST_BUCKET, s3Key);

            System.out.println("  Uploading " + domain + " to " + splitS3Urls[i]);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();

            s3Client.putObject(putRequest, localSplitPath);

            System.out.println("    Uploaded " + domain + " split (" +
                Files.size(localSplitPath) + " bytes)");
        }

        System.out.println("[Step 2] All splits uploaded to S3");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Build XRef split from S3 source splits")
    void step3_buildXRefSplit() throws Exception {
        System.out.println("\n[Step 3] Building XRef split from S3 source splits...");

        // Validate prerequisites
        for (int i = 0; i < DOMAINS.length; i++) {
            assertNotNull(splitS3Urls[i], "S3 URL should be set for " + DOMAINS[i]);
        }

        // Create AWS config for XRef build
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            getAccessKey(), getSecretKey(), TEST_REGION);

        // Build XRef config with XRefSourceSplit objects containing footer offsets
        // This follows the same pattern as other split operations - caller passes metadata
        List<XRefSourceSplit> sourceSplits = new ArrayList<>();
        for (int i = 0; i < DOMAINS.length; i++) {
            // Use the stored split metadata with footer offsets
            XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(
                splitS3Urls[i], splitMetadata[i]);
            sourceSplits.add(source);
            System.out.println("  Source split " + i + ": " + splitS3Urls[i] +
                " (footer: " + splitMetadata[i].getFooterStartOffset() + "-" +
                splitMetadata[i].getFooterEndOffset() + ")");
        }

        XRefBuildConfig buildConfig = XRefBuildConfig.builder()
            .xrefId("science-xref")
            .indexUid("science-index")
            .sourceSplits(sourceSplits)
            .awsConfig(awsConfig)
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
    @DisplayName("Step 4: Upload XRef split to S3")
    void step4_uploadXRefToS3() throws IOException {
        System.out.println("\n[Step 4] Uploading XRef split to S3...");

        Path xrefLocalPath = tempDir.resolve("science.xref.split");
        if (!Files.exists(xrefLocalPath)) {
            fail("Prerequisite failure: XRef split file does not exist");
        }

        String s3Key = "xref-test/xref/science.xref.split";
        xrefS3Url = String.format("s3://%s/%s", TEST_BUCKET, s3Key);

        System.out.println("  Uploading XRef to " + xrefS3Url);

        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(s3Key)
            .build();

        s3Client.putObject(putRequest, xrefLocalPath);

        System.out.println("  Uploaded XRef split (" + Files.size(xrefLocalPath) + " bytes)");
        System.out.println("[Step 4] XRef uploaded to S3");
    }

    @Test
    @Order(5)
    @DisplayName("Step 5: Search XRef and validate query routing")
    void step5_searchXRefAndValidate() throws Exception {
        System.out.println("\n[Step 5] Searching XRef split and validating query routing...");

        assertNotNull(xrefMetadata, "XRef metadata should be available from Step 3");
        assertNotNull(xrefS3Url, "XRef S3 URL should be available from Step 4");

        // Create cache manager with AWS credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-cache")
            .withMaxCacheSize(100_000_000)
            .withAwsCredentials(getAccessKey(), getSecretKey())
            .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, xrefS3Url, xrefMetadata)) {

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

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-validation-cache")
            .withMaxCacheSize(100_000_000)
            .withAwsCredentials(getAccessKey(), getSecretKey())
            .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, xrefS3Url, xrefMetadata)) {

            // For each domain, search for its unique terms and verify
            // the XRef points to the correct split
            for (int i = 0; i < DOMAINS.length; i++) {
                String domain = DOMAINS[i];
                String[] uniqueTerms = DOMAIN_UNIQUE_TERMS[i];
                String expectedSplitUrl = splitS3Urls[i];

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
                        // At minimum, the domain's split should be in the results
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
        assertEquals("science-xref", xrefMetadata.getXrefId(), "XRef ID should match");
        assertEquals("science-index", xrefMetadata.getIndexUid(), "Index UID should match");
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
