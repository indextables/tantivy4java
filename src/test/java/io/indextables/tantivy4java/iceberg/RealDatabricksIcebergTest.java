package io.indextables.tantivy4java.iceberg;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier 4: Live Databricks Unity Catalog integration test via REST API.
 *
 * Activated by system properties:
 * <ul>
 *   <li>{@code test.iceberg.databricks.workspace} — Databricks workspace URL
 *       (e.g. "https://myworkspace.cloud.databricks.com")</li>
 *   <li>{@code test.iceberg.databricks.token} — Databricks PAT or OAuth token</li>
 *   <li>{@code test.iceberg.databricks.warehouse} — Unity Catalog name</li>
 *   <li>{@code test.iceberg.databricks.namespace} — Database name (default: "default")</li>
 *   <li>{@code test.iceberg.databricks.table} — Iceberg table name</li>
 * </ul>
 *
 * Optional cloud storage credentials:
 * <ul>
 *   <li>{@code test.iceberg.databricks.s3.accessKey} / {@code .secretKey} / {@code .region}</li>
 *   <li>{@code test.iceberg.databricks.adls.accountName} / {@code .accountKey}</li>
 * </ul>
 *
 * Skips gracefully when config is absent.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealDatabricksIcebergTest {

    private static String workspaceUrl;
    private static String token;
    private static String warehouseName;
    private static String namespace;
    private static String tableName;

    @BeforeAll
    static void setup() {
        workspaceUrl = System.getProperty("test.iceberg.databricks.workspace");
        token = System.getProperty("test.iceberg.databricks.token");
        warehouseName = System.getProperty("test.iceberg.databricks.warehouse");
        namespace = System.getProperty("test.iceberg.databricks.namespace", "default");
        tableName = System.getProperty("test.iceberg.databricks.table");

        boolean hasConfig = workspaceUrl != null && token != null
                && warehouseName != null && tableName != null;
        Assumptions.assumeTrue(hasConfig,
                "Databricks config not available. Set test.iceberg.databricks.workspace, " +
                "test.iceberg.databricks.token, test.iceberg.databricks.warehouse, " +
                "and test.iceberg.databricks.table system properties");
    }

    private Map<String, String> databricksConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog_type", "rest");
        config.put("uri", workspaceUrl + "/api/2.1/unity-catalog/iceberg-rest");
        config.put("token", token);
        config.put("warehouse", warehouseName);

        // Optional S3 storage credentials
        String s3Key = System.getProperty("test.iceberg.databricks.s3.accessKey");
        String s3Secret = System.getProperty("test.iceberg.databricks.s3.secretKey");
        String s3Region = System.getProperty("test.iceberg.databricks.s3.region", "us-east-1");
        if (s3Key != null && s3Secret != null) {
            config.put("s3.access-key-id", s3Key);
            config.put("s3.secret-access-key", s3Secret);
            config.put("s3.region", s3Region);
        }

        // Optional Azure ADLS storage credentials
        String adlsAccount = System.getProperty("test.iceberg.databricks.adls.accountName");
        String adlsKey = System.getProperty("test.iceberg.databricks.adls.accountKey");
        if (adlsAccount != null && adlsKey != null) {
            config.put("adls.account-name", adlsAccount);
            config.put("adls.account-key", adlsKey);
        }

        // Optional Azure bearer token
        String adlsBearer = System.getProperty("test.iceberg.databricks.adls.bearerToken");
        if (adlsBearer != null) {
            config.put("adls.bearer-token", adlsBearer);
        }

        return config;
    }

    @Test
    @Order(1)
    void testReadSchemaFromDatabricks() {
        IcebergTableSchema schema = IcebergTableReader.readSchema(
                "databricks-catalog", namespace, tableName, databricksConfig());

        assertNotNull(schema);
        assertTrue(schema.getFieldCount() > 0, "Schema should have fields");
        assertNotNull(schema.getSchemaJson());

        System.out.println("Databricks schema for " + namespace + "." + tableName + ":");
        for (IcebergSchemaField field : schema.getFields()) {
            System.out.printf("  %s: %s (id=%d, nullable=%s)%n",
                    field.getName(), field.getDataType(), field.getFieldId(), field.isNullable());
        }
    }

    @Test
    @Order(2)
    void testListSnapshotsFromDatabricks() {
        List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
                "databricks-catalog", namespace, tableName, databricksConfig());

        assertNotNull(snapshots);
        System.out.println("Databricks snapshots for " + namespace + "." + tableName +
                ": " + snapshots.size());
        for (IcebergSnapshot snap : snapshots) {
            System.out.printf("  Snapshot %d: op=%s, ts=%d, parent=%s%n",
                    snap.getSnapshotId(), snap.getOperation(),
                    snap.getTimestampMs(), snap.hasParent() ? snap.getParentSnapshotId() : "none");
        }
    }

    @Test
    @Order(3)
    void testListFilesFromDatabricks() {
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "databricks-catalog", namespace, tableName, databricksConfig());

        assertNotNull(entries);
        assertTrue(entries.size() > 0, "Table should have data files");

        System.out.println("Databricks files for " + namespace + "." + tableName +
                ": " + entries.size());
        long totalRecords = 0;
        long totalBytes = 0;
        for (IcebergFileEntry entry : entries) {
            totalRecords += entry.getRecordCount();
            totalBytes += entry.getFileSizeBytes();
            System.out.printf("  %s (%s, %d records, %d bytes)%n",
                    entry.getPath(), entry.getFileFormat(),
                    entry.getRecordCount(), entry.getFileSizeBytes());
        }
        System.out.printf("  Total: %d records, %d bytes across %d files%n",
                totalRecords, totalBytes, entries.size());
    }
}
