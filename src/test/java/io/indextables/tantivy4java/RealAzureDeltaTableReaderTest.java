package io.indextables.tantivy4java;

import io.indextables.tantivy4java.delta.DeltaFileEntry;
import io.indextables.tantivy4java.delta.DeltaTableReader;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
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
 * Live Azure integration tests for DeltaTableReader.
 *
 * <p>Creates a minimal Delta table on Azure Blob Storage (commit log + dummy
 * parquet files), then reads it back with DeltaTableReader. Skipped if Azure
 * credentials are not available.
 *
 * <p>Credentials loaded from (in order):
 * <ol>
 *   <li>System properties: {@code -Dtest.azure.storageAccount}, {@code -Dtest.azure.accountKey}</li>
 *   <li>{@code ~/.azure/credentials} file ([default] section)</li>
 *   <li>Environment variables: {@code AZURE_STORAGE_ACCOUNT}, {@code AZURE_STORAGE_KEY}</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzureDeltaTableReaderTest {

    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");
    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-testing");
    private static final String TABLE_PREFIX = "delta-reader-test/test_delta_table";

    private static String azureStorageAccount;
    private static String azureAccountKey;
    private static BlobServiceClient blobServiceClient;
    private static BlobContainerClient containerClient;
    private static Map<String, String> deltaConfig;
    private static String deltaTableUrl;

    @BeforeAll
    static void setup() {
        loadAzureCredentials();

        String account = getStorageAccount();
        String key = getAccountKey();
        boolean hasCreds = account != null && key != null;
        boolean hasEnvCreds = System.getenv("AZURE_STORAGE_ACCOUNT") != null;

        if (!hasCreds && !hasEnvCreds) {
            System.out.println("No Azure credentials found. Skipping Azure Delta tests.");
            Assumptions.abort("Azure credentials not available");
        }

        // Build Azure client
        blobServiceClient = new BlobServiceClientBuilder()
                .endpoint("https://" + account + ".blob.core.windows.net")
                .credential(new StorageSharedKeyCredential(account, key))
                .buildClient();

        // Ensure container exists
        containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        if (!containerClient.exists()) {
            containerClient.create();
            System.out.println("Created Azure container: " + TEST_CONTAINER);
        }

        // Build DeltaTableReader config
        deltaConfig = new HashMap<>();
        deltaConfig.put("azure_account_name", account);
        deltaConfig.put("azure_access_key", key);

        deltaTableUrl = "az://" + TEST_CONTAINER + "/" + TABLE_PREFIX;
        System.out.println("Azure Delta table URL: " + deltaTableUrl);
    }

    @AfterAll
    static void cleanup() {
        if (containerClient != null) {
            String[] blobs = {
                TABLE_PREFIX + "/_delta_log/00000000000000000000.json",
                TABLE_PREFIX + "/_delta_log/00000000000000000001.json",
                TABLE_PREFIX + "/part-00000.parquet",
                TABLE_PREFIX + "/part-00001.parquet",
            };
            for (String blob : blobs) {
                try {
                    containerClient.getBlobClient(blob).deleteIfExists();
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create a Delta table on Azure")
    void step1_createDeltaTableOnAzure() {
        // Commit 0: protocol + metadata + add first file
        String commit0 = String.join("\n",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"azure-test-table\"," +
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
        uploadBlob(TABLE_PREFIX + "/_delta_log/00000000000000000000.json", commit0);
        uploadBlob(TABLE_PREFIX + "/_delta_log/00000000000000000001.json", commit1);

        // Upload dummy parquet files
        uploadBlob(TABLE_PREFIX + "/part-00000.parquet", "dummy");
        uploadBlob(TABLE_PREFIX + "/part-00001.parquet", "dummy");

        System.out.println("Created Delta table on Azure: " + deltaTableUrl);
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: List files from Azure Delta table at latest version")
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
            assertTrue(entry.getPartitionValues().isEmpty());
            assertFalse(entry.hasDeletionVector());
            assertEquals(1, entry.getTableVersion());
        }
    }

    private void uploadBlob(String blobName, String content) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        blobClient.upload(new ByteArrayInputStream(bytes), bytes.length, true);
    }

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        if (azureStorageAccount != null) return azureStorageAccount;
        return System.getenv("AZURE_STORAGE_ACCOUNT");
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureAccountKey != null) return azureAccountKey;
        return System.getenv("AZURE_STORAGE_KEY");
    }

    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
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
                        if ("storage_account".equals(key)) azureStorageAccount = value;
                        else if ("account_key".equals(key)) azureAccountKey = value;
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Could not read Azure credentials: " + e.getMessage());
        }
    }
}
