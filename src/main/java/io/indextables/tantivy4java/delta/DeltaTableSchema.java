package io.indextables.tantivy4java.delta;

import java.util.Collections;
import java.util.List;

/**
 * Immutable result class representing a Delta table's schema read from the transaction log.
 *
 * <p>Contains the list of top-level fields, the raw Delta schema JSON for advanced use,
 * and the snapshot version the schema was read from.
 *
 * <p>Usage:
 * <pre>{@code
 * DeltaTableSchema schema = DeltaTableReader.readSchema("s3://bucket/my_delta_table", config);
 * System.out.println("Version: " + schema.getTableVersion());
 * for (DeltaSchemaField field : schema.getFields()) {
 *     System.out.println(field.getName() + " : " + field.getDataType());
 * }
 * // For advanced use: full Delta schema JSON
 * String json = schema.getSchemaJson();
 * }</pre>
 */
public class DeltaTableSchema {

    private final List<DeltaSchemaField> fields;
    private final String schemaJson;
    private final long tableVersion;

    public DeltaTableSchema(List<DeltaSchemaField> fields, String schemaJson, long tableVersion) {
        this.fields = Collections.unmodifiableList(fields);
        this.schemaJson = schemaJson;
        this.tableVersion = tableVersion;
    }

    /**
     * @return the list of top-level columns in the Delta table schema
     */
    public List<DeltaSchemaField> getFields() {
        return fields;
    }

    /**
     * @return the full Delta schema as a JSON string (Delta Lake's native schema format)
     */
    public String getSchemaJson() {
        return schemaJson;
    }

    /**
     * @return the snapshot version this schema was read from
     */
    public long getTableVersion() {
        return tableVersion;
    }

    /**
     * @return the number of top-level columns
     */
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public String toString() {
        return String.format("DeltaTableSchema{fields=%d, version=%d}", fields.size(), tableVersion);
    }
}
