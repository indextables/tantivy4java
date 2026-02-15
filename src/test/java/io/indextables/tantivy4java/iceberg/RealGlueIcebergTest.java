package io.indextables.tantivy4java.iceberg;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier 4: Live AWS Glue catalog integration test.
 *
 * Activated by credentials in ~/.aws/credentials [default] section
 * and system properties:
 * <ul>
 *   <li>{@code test.iceberg.glue.warehouse} — S3 warehouse path</li>
 *   <li>{@code test.iceberg.glue.namespace} — Glue database name</li>
 *   <li>{@code test.iceberg.glue.table} — Iceberg table name</li>
 *   <li>{@code test.iceberg.glue.region} — AWS region (default: us-east-1)</li>
 * </ul>
 *
 * Skips gracefully when credentials/config are absent.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealGlueIcebergTest {

    private static String accessKey;
    private static String secretKey;
    private static String region;
    private static String warehouse;
    private static String namespace;
    private static String tableName;

    @BeforeAll
    static void setup() {
        // Try system properties first
        warehouse = System.getProperty("test.iceberg.glue.warehouse");
        namespace = System.getProperty("test.iceberg.glue.namespace", "default");
        tableName = System.getProperty("test.iceberg.glue.table");
        region = System.getProperty("test.iceberg.glue.region", "us-east-1");
        accessKey = System.getProperty("test.iceberg.glue.accessKey");
        secretKey = System.getProperty("test.iceberg.glue.secretKey");

        // Fall back to ~/.aws/credentials
        if (accessKey == null || secretKey == null) {
            loadAwsCredentials();
        }

        boolean hasCreds = accessKey != null && secretKey != null
                && warehouse != null && tableName != null;
        Assumptions.assumeTrue(hasCreds,
                "Glue credentials not available. Set test.iceberg.glue.* system properties " +
                "or configure ~/.aws/credentials and test.iceberg.glue.warehouse/table");
    }

    private static void loadAwsCredentials() {
        File credsFile = new File(System.getProperty("user.home"), ".aws/credentials");
        if (!credsFile.exists()) return;
        try {
            Properties props = new Properties();
            props.load(new FileInputStream(credsFile));
            if (accessKey == null) accessKey = props.getProperty("aws_access_key_id");
            if (secretKey == null) secretKey = props.getProperty("aws_secret_access_key");
        } catch (Exception e) {
            // ignore
        }
    }

    private Map<String, String> glueConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog_type", "glue");
        config.put("warehouse", warehouse);
        config.put("aws_access_key_id", accessKey);
        config.put("aws_secret_access_key", secretKey);
        config.put("region_name", region);
        config.put("s3.access-key-id", accessKey);
        config.put("s3.secret-access-key", secretKey);
        config.put("s3.region", region);
        return config;
    }

    @Test
    @Order(1)
    void testReadSchemaFromGlue() {
        IcebergTableSchema schema = IcebergTableReader.readSchema(
                "glue-catalog", namespace, tableName, glueConfig());

        assertNotNull(schema);
        assertTrue(schema.getFieldCount() > 0, "Schema should have fields");
        assertNotNull(schema.getSchemaJson());

        System.out.println("Glue schema for " + namespace + "." + tableName + ":");
        for (IcebergSchemaField field : schema.getFields()) {
            System.out.printf("  %s: %s (id=%d, nullable=%s)%n",
                    field.getName(), field.getDataType(), field.getFieldId(), field.isNullable());
        }
    }

    @Test
    @Order(2)
    void testListSnapshotsFromGlue() {
        List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
                "glue-catalog", namespace, tableName, glueConfig());

        assertNotNull(snapshots);
        System.out.println("Glue snapshots for " + namespace + "." + tableName + ": " + snapshots.size());
        for (IcebergSnapshot snap : snapshots) {
            System.out.printf("  Snapshot %d: op=%s, ts=%d, parent=%s%n",
                    snap.getSnapshotId(), snap.getOperation(),
                    snap.getTimestampMs(), snap.hasParent() ? snap.getParentSnapshotId() : "none");
        }
    }

    @Test
    @Order(3)
    void testListFilesFromGlue() {
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "glue-catalog", namespace, tableName, glueConfig());

        assertNotNull(entries);
        assertTrue(entries.size() > 0, "Table should have data files");

        System.out.println("Glue files for " + namespace + "." + tableName + ": " + entries.size());
        for (IcebergFileEntry entry : entries) {
            System.out.printf("  %s (%s, %d records, %d bytes)%n",
                    entry.getPath(), entry.getFileFormat(),
                    entry.getRecordCount(), entry.getFileSizeBytes());
        }
    }
}
