package io.indextables.tantivy4java.iceberg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable result class representing an Iceberg table's schema.
 *
 * <p>Contains the list of top-level fields with their Iceberg data types, field IDs,
 * nullability, and documentation, plus the raw schema JSON for advanced use.
 *
 * <p>Usage:
 * <pre>{@code
 * IcebergTableSchema schema = IcebergTableReader.readSchema(
 *     "my-catalog", "default", "my_table", config);
 * System.out.println("Snapshot: " + schema.getSnapshotId());
 * for (IcebergSchemaField field : schema.getFields()) {
 *     System.out.println(field.getName() + " : " + field.getDataType()
 *         + " (id=" + field.getFieldId() + ")");
 * }
 * }</pre>
 */
public class IcebergTableSchema {

    private final List<IcebergSchemaField> fields;
    private final String schemaJson;
    private final long snapshotId;

    IcebergTableSchema(List<IcebergSchemaField> fields, String schemaJson, long snapshotId) {
        this.fields = Collections.unmodifiableList(fields);
        this.schemaJson = schemaJson;
        this.snapshotId = snapshotId;
    }

    /**
     * @return the list of top-level columns in the Iceberg table schema
     */
    public List<IcebergSchemaField> getFields() {
        return fields;
    }

    /**
     * @return the full Iceberg schema as a JSON string
     */
    public String getSchemaJson() {
        return schemaJson;
    }

    /**
     * @return the snapshot ID this schema was read from (-1 if table has no snapshots)
     */
    public long getSnapshotId() {
        return snapshotId;
    }

    /**
     * @return the number of top-level columns
     */
    public int getFieldCount() {
        return fields.size();
    }

    /**
     * Build a map from Iceberg field ID to logical column name.
     *
     * <p>This is useful for resolving physical parquet column names (e.g. "col_1")
     * to their logical Iceberg names (e.g. "id") when the parquet file uses
     * column name mapping (common in Databricks Unity Catalog).
     *
     * @return field ID to logical name map
     * @see io.indextables.tantivy4java.parquet.ParquetSchemaReader#readColumnMapping
     */
    public Map<Integer, String> getFieldIdToNameMap() {
        Map<Integer, String> map = new HashMap<>(fields.size());
        for (IcebergSchemaField field : fields) {
            map.put(field.getFieldId(), field.getName());
        }
        return map;
    }

    @Override
    public String toString() {
        return String.format("IcebergTableSchema{fields=%d, snapshot=%d}", fields.size(), snapshotId);
    }
}
