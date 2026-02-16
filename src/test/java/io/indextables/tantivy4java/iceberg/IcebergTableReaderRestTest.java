package io.indextables.tantivy4java.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier 3: REST catalog integration test using Testcontainers.
 * Requires Docker to be running.
 *
 * Uses tabulario/iceberg-rest Docker image and Apache Iceberg Java SDK
 * to create test tables and verify our Rust-backed JNI reader can
 * read the metadata correctly.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergTableReaderRestTest {

    private static GenericContainer<?> restCatalog;
    private static String catalogUri;
    private static Path warehousePath;
    private static long firstSnapshotId;

    private static boolean dockerAvailable() {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"docker", "info"});
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        Assumptions.assumeTrue(dockerAvailable(), "Docker not available, skipping REST catalog tests");

        // Create a local warehouse directory for the REST catalog
        warehousePath = Files.createTempDirectory("iceberg-rest-warehouse");

        try {
            restCatalog = new GenericContainer<>("tabulario/iceberg-rest:1.6.1")
                    .withExposedPorts(8181)
                    .withEnv("CATALOG_WAREHOUSE", "file://" + warehousePath.toAbsolutePath())
                    .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.io.ResolvingFileIO")
                    .withFileSystemBind(warehousePath.toString(), warehousePath.toString())
                    .waitingFor(Wait.forHttp("/v1/config").forStatusCode(200));
            restCatalog.start();
        } catch (Exception e) {
            Assumptions.abort("Failed to start REST catalog container: " + e.getMessage());
        }

        catalogUri = "http://" + restCatalog.getHost() + ":" + restCatalog.getMappedPort(8181);

        // Create test data using Apache Iceberg Java SDK
        try {
            createTestData();
        } catch (Exception e) {
            Assumptions.abort("Failed to create test data: " + e.getMessage());
        }
    }

    @AfterAll
    static void teardown() {
        if (restCatalog != null) {
            restCatalog.stop();
        }
        // Clean up warehouse dir
        if (warehousePath != null) {
            deleteDir(warehousePath.toFile());
        }
    }

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDir(child);
                }
            }
        }
        dir.delete();
    }

    private static void createTestData() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, catalogUri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + warehousePath.toAbsolutePath());

        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("test-catalog", properties);

        // Create namespace
        Namespace ns = Namespace.of("test_db");
        if (!catalog.namespaceExists(ns)) {
            catalog.createNamespace(ns);
        }

        // Create table with schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "score", Types.DoubleType.get()),
                Types.NestedField.optional(4, "active", Types.BooleanType.get())
        );

        TableIdentifier tableId = TableIdentifier.of(ns, "test_table");
        Table table;
        if (catalog.tableExists(tableId)) {
            table = catalog.loadTable(tableId);
        } else {
            table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
        }

        // First append: create a synthetic data file
        DataFile dataFile1 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(warehousePath.resolve("data/batch1.parquet").toAbsolutePath().toString())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(100)
                .withFileSizeInBytes(4096)
                .build();

        // Create the dummy file on disk so the catalog accepts it
        try {
            Files.createDirectories(warehousePath.resolve("data"));
            Files.createFile(warehousePath.resolve("data/batch1.parquet"));
        } catch (Exception e) {
            // ignore if exists
        }

        table.newFastAppend().appendFile(dataFile1).commit();
        firstSnapshotId = table.currentSnapshot().snapshotId();

        // Second append: another data file
        DataFile dataFile2 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(warehousePath.resolve("data/batch2.parquet").toAbsolutePath().toString())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(200)
                .withFileSizeInBytes(8192)
                .build();

        try {
            Files.createFile(warehousePath.resolve("data/batch2.parquet"));
        } catch (Exception e) {
            // ignore if exists
        }

        table.newFastAppend().appendFile(dataFile2).commit();

        try {
            catalog.close();
        } catch (Exception e) {
            // ignore close errors
        }
    }

    private Map<String, String> restConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog_type", "rest");
        config.put("uri", catalogUri);
        config.put("warehouse", "file://" + warehousePath.toAbsolutePath());
        return config;
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void testReadSchemaFromRestCatalog() {
        IcebergTableSchema schema = IcebergTableReader.readSchema(
                "rest-test", "test_db", "test_table", restConfig());

        assertNotNull(schema);
        assertEquals(4, schema.getFieldCount());

        // Verify field names and types
        List<IcebergSchemaField> fields = schema.getFields();
        assertEquals("id", fields.get(0).getName());
        assertEquals("long", fields.get(0).getDataType());
        assertEquals(1, fields.get(0).getFieldId());
        assertFalse(fields.get(0).isNullable()); // required

        assertEquals("name", fields.get(1).getName());
        assertEquals("string", fields.get(1).getDataType());
        assertTrue(fields.get(1).isNullable()); // optional

        assertEquals("score", fields.get(2).getName());
        assertEquals("double", fields.get(2).getDataType());

        assertEquals("active", fields.get(3).getName());
        assertEquals("boolean", fields.get(3).getDataType());

        // Schema JSON should be valid
        assertNotNull(schema.getSchemaJson());
        assertFalse(schema.getSchemaJson().isEmpty());
    }

    @Test
    @Order(2)
    void testReadSchemaFieldDetails() {
        IcebergTableSchema schema = IcebergTableReader.readSchema(
                "rest-test", "test_db", "test_table", restConfig());

        for (IcebergSchemaField field : schema.getFields()) {
            assertTrue(field.getFieldId() > 0, "Field ID should be positive: " + field.getName());
            assertNotNull(field.getName());
            assertNotNull(field.getDataType());
            assertTrue(field.isPrimitive(), "All test fields should be primitive");
        }
    }

    @Test
    @Order(3)
    void testListSnapshotsFromRestCatalog() {
        List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
                "rest-test", "test_db", "test_table", restConfig());

        assertNotNull(snapshots);
        assertEquals(2, snapshots.size(), "Should have 2 snapshots from 2 appends");

        for (IcebergSnapshot snap : snapshots) {
            assertTrue(snap.getSnapshotId() > 0);
            assertTrue(snap.getTimestampMs() > 0);
            assertNotNull(snap.getManifestList());
            assertFalse(snap.getManifestList().isEmpty());
        }
    }

    @Test
    @Order(4)
    void testSnapshotParentChain() {
        List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
                "rest-test", "test_db", "test_table", restConfig());

        assertEquals(2, snapshots.size());

        // First snapshot has no parent
        IcebergSnapshot first = snapshots.get(0);
        assertFalse(first.hasParent(), "First snapshot should have no parent");

        // Second snapshot should reference first as parent
        IcebergSnapshot second = snapshots.get(1);
        assertTrue(second.hasParent(), "Second snapshot should have a parent");
        assertEquals(first.getSnapshotId(), second.getParentSnapshotId());

        // Sequence numbers should increase
        assertTrue(second.getSequenceNumber() > first.getSequenceNumber());
    }

    @Test
    @Order(5)
    void testListFilesFromRestCatalog() {
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "rest-test", "test_db", "test_table", restConfig());

        assertNotNull(entries);
        assertEquals(2, entries.size(), "Current snapshot should have 2 files");

        for (IcebergFileEntry entry : entries) {
            assertNotNull(entry.getPath());
            assertFalse(entry.getPath().isEmpty());
            assertEquals("parquet", entry.getFileFormat());
            assertTrue(entry.getRecordCount() > 0);
            assertTrue(entry.getFileSizeBytes() > 0);
            assertEquals("data", entry.getContentType());
            assertTrue(entry.getSnapshotId() > 0);
        }
    }

    @Test
    @Order(6)
    void testListFilesAtSpecificSnapshot() {
        // List files at first snapshot — should see only batch1
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "rest-test", "test_db", "test_table", restConfig(), firstSnapshotId);

        assertNotNull(entries);
        assertEquals(1, entries.size(), "First snapshot should have 1 file");
        assertTrue(entries.get(0).getPath().contains("batch1"), "Should be batch1 file");
        assertEquals(100, entries.get(0).getRecordCount());
    }

    @Test
    @Order(7)
    void testListFilesCompactMode() {
        List<IcebergFileEntry> entries = IcebergTableReader.listFiles(
                "rest-test", "test_db", "test_table", restConfig(), true);

        assertNotNull(entries);
        assertEquals(2, entries.size());

        // In compact mode, partition_values default to empty and content_type defaults to "data"
        for (IcebergFileEntry entry : entries) {
            assertNotNull(entry.getPath());
            assertEquals("parquet", entry.getFileFormat());
            assertTrue(entry.getRecordCount() > 0);
        }
    }
}
