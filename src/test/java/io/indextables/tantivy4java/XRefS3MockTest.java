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

import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3Mock-based tests for XRef (Cross-Reference) split S3 functionality.
 *
 * This test validates that XRef builds work correctly when source splits are stored in S3.
 * Uses S3Mock server to avoid needing real AWS credentials.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class XRefS3MockTest {

    private static final String TEST_BUCKET = "xref-test-bucket";
    private static final String ACCESS_KEY = "xref-test-access";
    private static final String SECRET_KEY = "xref-test-secret";
    private static final int S3_MOCK_PORT = 8003; // Different port from other S3Mock tests

    private static S3Mock s3Mock;
    private static S3Client s3Client;
    private static QuickwitSplit.SplitMetadata[] splitMetadata = new QuickwitSplit.SplitMetadata[2];
    private static String[] splitS3Urls = new String[2];

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUpS3Mock() throws Exception {
        // Start S3Mock server
        s3Mock = new S3Mock.Builder()
                .withPort(S3_MOCK_PORT)
                .withInMemoryBackend()
                .build();
        s3Mock.start();

        // Create S3 client pointing to mock server
        s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .endpointOverride(java.net.URI.create("http://localhost:" + S3_MOCK_PORT))
                .forcePathStyle(true) // Required for S3Mock
                .build();

        // Create test bucket
        s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());

        System.out.println("[XRef S3Mock Test] S3Mock server started on port " + S3_MOCK_PORT);
    }

    @AfterAll
    static void tearDownS3Mock() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (s3Mock != null) {
            s3Mock.shutdown();
        }
        System.out.println("[XRef S3Mock Test] S3Mock server stopped");
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create and upload source splits to S3Mock")
    void step1_createAndUploadSplits() throws IOException {
        System.out.println("\n[Step 1] Creating and uploading source splits...");

        for (int i = 0; i < 2; i++) {
            String domain = "domain" + i;
            System.out.println("  Creating split for: " + domain);

            // Create index
            Path indexPath = tempDir.resolve(domain + "-index");
            createTestIndex(indexPath, domain, i);

            // Convert to split
            Path splitPath = tempDir.resolve(domain + ".split");
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "xref-test-" + domain, "xref-source", "xref-node-" + i
            );

            splitMetadata[i] = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), splitPath.toString(), config
            );

            assertNotNull(splitMetadata[i], "Split metadata should be created");
            assertTrue(splitMetadata[i].getNumDocs() > 0, "Split should contain documents");
            assertTrue(splitMetadata[i].hasFooterOffsets(), "Split should have footer offsets");

            // Upload to S3Mock
            String s3Key = "splits/" + domain + ".split";
            splitS3Urls[i] = String.format("s3://%s/%s", TEST_BUCKET, s3Key);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
            s3Client.putObject(putRequest, splitPath);

            System.out.println("    Created and uploaded: " + splitS3Urls[i]);
            System.out.println("    Footer offsets: " + splitMetadata[i].getFooterStartOffset() +
                " - " + splitMetadata[i].getFooterEndOffset());
        }

        System.out.println("[Step 1] All splits created and uploaded");
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: Build XRef split from S3Mock source splits")
    void step2_buildXRefFromS3() throws Exception {
        System.out.println("\n[Step 2] Building XRef split from S3Mock source splits...");

        // Validate prerequisites
        assertNotNull(splitS3Urls[0], "S3 URL should be set for split 0");
        assertNotNull(splitS3Urls[1], "S3 URL should be set for split 1");

        // Create AWS config pointing to S3Mock
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            ACCESS_KEY,
            SECRET_KEY,
            null, // no session token
            "us-east-1",
            "http://localhost:" + S3_MOCK_PORT,
            true  // force path style for S3Mock
        );

        // Build source split list with footer offsets
        List<XRefSourceSplit> sourceSplits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(
                splitS3Urls[i], splitMetadata[i]
            );
            sourceSplits.add(source);
            System.out.println("  Source split " + i + ": " + splitS3Urls[i]);
            System.out.println("    Footer: " + splitMetadata[i].getFooterStartOffset() +
                "-" + splitMetadata[i].getFooterEndOffset());
        }

        // Build XRef config
        XRefBuildConfig buildConfig = XRefBuildConfig.builder()
            .xrefId("test-xref")
            .indexUid("test-index")
            .sourceSplits(sourceSplits)
            .awsConfig(awsConfig)
            .includePositions(false)
            .build();

        // Build XRef split
        Path xrefPath = tempDir.resolve("test.xref.split");
        System.out.println("  Building XRef from " + sourceSplits.size() + " S3 source splits...");

        XRefMetadata xrefMetadata = XRefSplit.build(buildConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(2, xrefMetadata.getNumSplits(), "XRef should reference 2 source splits");
        assertTrue(xrefMetadata.hasFooterOffsets(), "XRef metadata should have footer offsets");

        System.out.println("  XRef built successfully:");
        System.out.println("    - XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("    - Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("    - Total source docs: " + xrefMetadata.getTotalSourceDocs());
        System.out.println("    - Total terms: " + xrefMetadata.getTotalTerms());

        System.out.println("[Step 2] XRef build from S3Mock completed successfully");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Verify XRef split file was created")
    void step3_verifyXRefCreated() throws Exception {
        System.out.println("\n[Step 3] Verifying XRef split file was created...");

        // Check the XRef we created in step 2
        Path xrefPath = tempDir.resolve("test.xref.split");
        assertTrue(Files.exists(xrefPath), "XRef split should exist");
        assertTrue(Files.size(xrefPath) > 0, "XRef split should not be empty");

        System.out.println("  XRef file exists: " + xrefPath);
        System.out.println("  XRef file size: " + Files.size(xrefPath) + " bytes");

        System.out.println("[Step 3] XRef file verification complete");
    }

    /**
     * Creates a test index with domain-specific content.
     */
    private void createTestIndex(Path indexPath, String domain, int domainIndex) throws IOException {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("domain", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);

        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.addText("domain", domain);
                doc.addText("content", "Document " + i + " about " + domain + " with unique term " + domain + "_term_" + i);
                doc.addInteger("id", (domainIndex * 1000) + i);
                writer.addDocument(doc);
            }

            writer.commit();
        }
    }
}
