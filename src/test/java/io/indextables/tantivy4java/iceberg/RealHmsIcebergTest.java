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
 * Tier 4: Live Hive Metastore (HMS) catalog integration test.
 *
 * Activated by system properties:
 * <ul>
 *   <li>{@code test.iceberg.hms.uri} — Thrift URI (thrift://host:port)</li>
 *   <li>{@code test.iceberg.hms.warehouse} — Storage root path</li>
 *   <li>{@code test.iceberg.hms.namespace} — Database name (default: "default")</li>
 *   <li>{@code test.iceberg.hms.table} — Iceberg table name</li>
 * </ul>
 *
 * Optional storage credentials (S3 or ADLS):
 * <ul>
 *   <li>{@code test.iceberg.hms.s3.accessKey} / {@code test.iceberg.hms.s3.secretKey}</li>
 *   <li>{@code test.iceberg.hms.adls.accountName} / {@code test.iceberg.hms.adls.accountKey}</li>
 * </ul>
 *
 * Skips gracefully when config is absent.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealHmsIcebergTest {

    private static String hmsUri;
    private static String warehouse;
    private static String namespace;
    private static String tableName;

    @BeforeAll
    static void setup() {
        hmsUri = System.getProperty("test.iceberg.hms.uri");
        warehouse = System.getProperty("test.iceberg.hms.warehouse");
        namespace = System.getProperty("test.iceberg.hms.namespace", "default");
        tableName = System.getProperty("test.iceberg.hms.table");

        boolean hasConfig = hmsUri != null && warehouse != null && tableName != null;
        Assumptions.assumeTrue(hasConfig,
                "HMS config not available. Set test.iceberg.hms.uri, " +
                "test.iceberg.hms.warehouse, and test.iceberg.hms.table system properties");
    }

    private Map<String, String> hmsConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog_type", "hms");
        config.put("uri", hmsUri);
        config.put("warehouse", warehouse);

        // Optional S3 storage credentials
        String s3Key = System.getProperty("test.iceberg.hms.s3.accessKey");
        String s3Secret = System.getProperty("test.iceberg.hms.s3.secretKey");
        String s3Region = System.getProperty("test.iceberg.hms.s3.region", "us-east-1");
        if (s3Key != null && s3Secret != null) {
            config.put("s3.access-key-id", s3Key);
            config.put("s3.secret-access-key", s3Secret);
            config.put("s3.region", s3Region);
        }

        // Optional Azure ADLS storage credentials
        String adlsAccount = System.getProperty("test.iceberg.hms.adls.accountName");
        String adlsKey = System.getProperty("test.iceberg.hms.adls.accountKey");
        if (adlsAccount != null && adlsKey != null) {
            config.put("adls.account-name", adlsAccount);
            config.put("adls.account-key", adlsKey);
        }

        return config;
    }

    @Test
    @Order(1)
    void testReadSchemaFromHms() {
        IcebergTableSchema schema = IcebergTableReader.readSchema(
                "hms-catalog", namespace, tableName, hmsConfig());

        assertNotNull(schema);
        assertTrue(schema.getFieldCount() > 0, "Schema should have fields");
        assertNotNull(schema.getSchemaJson());

        System.out.println("HMS schema for " + namespace + "." + tableName + ":");
        for (IcebergSchemaField field : schema.getFields()) {
            System.out.printf("  %s: %s (id=%d, nullable=%s)%n",
                    field.getName(), field.getDataType(), field.getFieldId(), field.isNullable());
        }
    }

    @Test
    @Order(2)
    void testListSnapshotsFromHms() {
        List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
                "hms-catalog", namespace, tableName, hmsConfig());

        assertNotNull(snapshots);
        System.out.println("HMS snapshots for " + namespace + "." + tableName + ": " + snapshots.size());
        for (IcebergSnapshot snap : snapshots) {
            System.out.printf("  Snapshot %d: op=%s, ts=%d%n",
                    snap.getSnapshotId(), snap.getOperation(), snap.getTimestampMs());
        }
    }

    @Test
    @Order(3)
    void testListFilesFromHms() {
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "hms-catalog", namespace, tableName, hmsConfig());

        assertNotNull(entries);
        assertTrue(entries.size() > 0, "Table should have data files");

        System.out.println("HMS files for " + namespace + "." + tableName + ": " + entries.size());
        for (IcebergFileEntry entry : entries) {
            System.out.printf("  %s (%s, %d records, %d bytes)%n",
                    entry.getPath(), entry.getFileFormat(),
                    entry.getRecordCount(), entry.getFileSizeBytes());
        }
    }
}
