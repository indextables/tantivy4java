package io.indextables.tantivy4java.delta;

import java.util.Map;

/**
 * Immutable data class representing a single field (column) in a Delta table schema.
 *
 * <p>Instances are returned by {@link DeltaTableReader#readSchema(String)} and contain
 * the column name, Delta data type, nullability, and any column-level metadata from
 * the Delta transaction log.
 *
 * <p>For primitive types, {@link #getDataType()} returns a simple string like
 * {@code "string"}, {@code "long"}, {@code "integer"}, {@code "double"}, {@code "boolean"},
 * {@code "date"}, {@code "timestamp"}, etc.
 *
 * <p>For complex types (struct, array, map), {@link #getDataType()} returns the
 * JSON representation of the type definition.
 */
public class DeltaSchemaField {

    private final String name;
    private final String dataType;
    private final boolean nullable;
    private final String metadata;

    DeltaSchemaField(String name, String dataType, boolean nullable, String metadata) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata != null ? metadata : "{}";
    }

    /**
     * @return the column name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the Delta data type (e.g. "string", "long", "double", or JSON for complex types)
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * @return true if this column allows null values
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return column metadata as a JSON string (e.g. column mapping info)
     */
    public String getMetadata() {
        return metadata;
    }

    /**
     * @return true if this is a primitive type (string, long, integer, double, etc.)
     */
    public boolean isPrimitive() {
        return !dataType.startsWith("{");
    }

    @Override
    public String toString() {
        return String.format("DeltaSchemaField{name='%s', type='%s', nullable=%s}",
                name, dataType, nullable);
    }

    /**
     * Construct a DeltaSchemaField from a parsed TANT byte buffer map.
     * Package-private; used by DeltaTableReader.
     */
    static DeltaSchemaField fromMap(Map<String, Object> map) {
        String name = (String) map.get("name");
        String dataType = (String) map.get("data_type");
        boolean nullable = toBoolean(map.get("nullable"));
        String metadata = (String) map.get("metadata");
        return new DeltaSchemaField(name, dataType, nullable, metadata);
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return false;
    }
}
