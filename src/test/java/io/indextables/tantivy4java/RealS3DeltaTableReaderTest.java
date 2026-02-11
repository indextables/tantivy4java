package io.indextables.tantivy4java;

import io.indextables.tantivy4java.delta.DeltaFileEntry;
import io.indextables.tantivy4java.delta.DeltaTableReader;

import org.junit.jupiter.api.*;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live S3 integration tests for DeltaTableReader.
 *
 * <p>Creates a minimal Delta table on S3 (commit log + dummy parquet files),
 * then reads it back with DeltaTableReader. Skipped if AWS credentials are
 * not available.
 *
 * <p>Credentials loaded from (in order):
 * <ol>
 *   <li>System properties: {@code -Dtest.s3.accessKey}, {@code -Dtest.s3.secretKey}</li>
 *   <li>{@code ~/.aws/credentials} file ([default] section)</li>
 *   <li>Environment variables: {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY}</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3DeltaTableReaderTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");
    private static final String TABLE_PREFIX = "delta-reader-test/test_delta_table";

    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;
    private static Map<String, String> deltaConfig;
    private static String deltaTableUrl;

    @BeforeAll
    static void setup() {
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        boolean hasCreds = accessKey != null && secretKey != null;
        boolean hasEnvCreds = System.getenv("AWS_ACCESS_KEY_ID") != null;

        if (!hasCreds && !hasEnvCreds) {
            System.out.println("No AWS credentials found. Skipping S3 Delta tests.");
            Assumptions.abort("AWS credentials not available");
        }

        // Build S3 client
        if (hasCreds) {
            s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(accessKey, secretKey)))
                    .build();
        } else {
            s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .build();
        }

        // Ensure bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(TEST_BUCKET).build());
        } catch (NoSuchBucketException e) {
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
            } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
            }
        } catch (Exception e) {
            Assumptions.abort("Cannot access S3 bucket: " + e.getMessage());
        }

        // Build DeltaTableReader config
        deltaConfig = new HashMap<>();
        if (accessKey != null) deltaConfig.put("aws_access_key_id", accessKey);
        if (secretKey != null) deltaConfig.put("aws_secret_access_key", secretKey);
        deltaConfig.put("aws_region", TEST_REGION);

        deltaTableUrl = "s3://" + TEST_BUCKET + "/" + TABLE_PREFIX;
        System.out.println("S3 Delta table URL: " + deltaTableUrl);
    }

    @AfterAll
    static void cleanup() {
        // Clean up test objects
        if (s3Client != null) {
            String[] keys = {
                TABLE_PREFIX + "/_delta_log/00000000000000000000.json",
                TABLE_PREFIX + "/_delta_log/00000000000000000001.json",
                TABLE_PREFIX + "/part-00000.parquet",
                TABLE_PREFIX + "/part-00001.parquet",
            };
            for (String key : keys) {
                try {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(TEST_BUCKET).key(key).build());
                } catch (Exception ignored) {
                }
            }
            s3Client.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create a Delta table on S3")
    void step1_createDeltaTableOnS3() {
        // Commit 0: protocol + metadata + add first file
        String commit0 = String.join("\n",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"s3-test-table\"," +
                        "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                        "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                        "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                        "{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                        "]}\"," +
                        "\"partitionColumns\":[]," +
                        "\"configuration\":{}," +
                        "\"createdTime\":1700000000000}}",
                "{\"add\":{\"path\":\"part-00000.parquet\"," +
                        "\"partitionValues\":{}," +
                        "\"size\":5000," +
                        "\"modificationTime\":1700000000000," +
                        "\"dataChange\":true," +
                        "\"stats\":\"{\\\"numRecords\\\":50}\"}}"
        );

        // Commit 1: add second file
        String commit1 = "{\"add\":{\"path\":\"part-00001.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":7000," +
                "\"modificationTime\":1700000001000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":75}\"}}";

        // Upload commit files
        putObject(TABLE_PREFIX + "/_delta_log/00000000000000000000.json", commit0);
        putObject(TABLE_PREFIX + "/_delta_log/00000000000000000001.json", commit1);

        // Upload dummy parquet files (just need to exist for listing)
        putObject(TABLE_PREFIX + "/part-00000.parquet", "dummy");
        putObject(TABLE_PREFIX + "/part-00001.parquet", "dummy");

        System.out.println("Created Delta table on S3: " + deltaTableUrl);
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: List files from S3 Delta table at latest version")
    void step2_listFilesLatestVersion() {
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(deltaTableUrl, deltaConfig);

        assertNotNull(files);
        assertEquals(2, files.size(), "Latest version should have 2 files");

        System.out.println("Found " + files.size() + " files at version " + files.get(0).getTableVersion());
        for (DeltaFileEntry entry : files) {
            System.out.println("  " + entry);
            assertFalse(entry.getPath().isEmpty());
            assertTrue(entry.getSize() > 0);
            assertTrue(entry.hasNumRecords());
        }

        // Verify total records
        long totalRecords = files.stream().mapToLong(DeltaFileEntry::getNumRecords).sum();
        assertEquals(125, totalRecords, "Total records should be 50 + 75 = 125");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: List files at version 0 (time travel)")
    void step3_listFilesVersion0() {
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(deltaTableUrl, deltaConfig, 0);

        assertNotNull(files);
        assertEquals(1, files.size(), "Version 0 should have 1 file");
        assertEquals(0, files.get(0).getTableVersion());
        assertEquals("part-00000.parquet", files.get(0).getPath());
        assertEquals(50, files.get(0).getNumRecords());

        System.out.println("Version 0: " + files.get(0));
    }

    @Test
    @Order(4)
    @DisplayName("Step 4: Validate file entry metadata")
    void step4_validateMetadata() {
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(deltaTableUrl, deltaConfig);

        for (DeltaFileEntry entry : files) {
            assertTrue(entry.getPath().endsWith(".parquet"),
                    "Path should end with .parquet: " + entry.getPath());
            assertTrue(entry.getModificationTime() > 0, "Modification time should be positive");
            assertNotNull(entry.getPartitionValues());
            assertTrue(entry.getPartitionValues().isEmpty(), "Non-partitioned table should have empty partition values");
            assertFalse(entry.hasDeletionVector(), "Test table has no deletion vectors");
            assertEquals(1, entry.getTableVersion(), "Latest version should be 1");
        }
    }

    private void putObject(String key, String content) {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build(),
                RequestBody.fromBytes(content.getBytes(StandardCharsets.UTF_8)));
    }

    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return System.getenv("AWS_ACCESS_KEY_ID");
    }

    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return System.getenv("AWS_SECRET_ACCESS_KEY");
    }

    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) return;

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefault = false;
            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) { inDefault = true; continue; }
                if (line.startsWith("[")) { inDefault = false; continue; }
                if (inDefault && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        if ("aws_access_key_id".equals(key)) awsAccessKey = value;
                        else if ("aws_secret_access_key".equals(key)) awsSecretKey = value;
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Could not read AWS credentials: " + e.getMessage());
        }
    }
}
