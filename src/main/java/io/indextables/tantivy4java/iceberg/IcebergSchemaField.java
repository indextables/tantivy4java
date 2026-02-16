package io.indextables.tantivy4java.iceberg;

import java.util.Map;

/**
 * Immutable data class representing a single field (column) in an Iceberg table schema.
 *
 * <p>Instances are returned by {@link IcebergTableReader#readSchema} and contain
 * the column name, Iceberg data type, field ID, nullability, and documentation.
 *
 * <p>For primitive types, {@link #getDataType()} returns a simple string like
 * {@code "long"}, {@code "string"}, {@code "double"}, {@code "boolean"}, etc.
 *
 * <p>For complex types (struct, list, map), {@link #getDataType()} returns the
 * JSON representation of the type definition.
 */
public class IcebergSchemaField {

    private final String name;
    private final String dataType;
    private final int fieldId;
    private final boolean nullable;
    private final String doc;

    IcebergSchemaField(String name, String dataType, int fieldId, boolean nullable, String doc) {
        this.name = name;
        this.dataType = dataType;
        this.fieldId = fieldId;
        this.nullable = nullable;
        this.doc = doc;
    }

    /**
     * @return the column name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the Iceberg data type (e.g. "long", "string", "double", or JSON for complex types)
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * @return the Iceberg field ID (unique within the schema, used for schema evolution)
     */
    public int getFieldId() {
        return fieldId;
    }

    /**
     * @return true if this column allows null values
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return field documentation from the schema, or null if not set
     */
    public String getDoc() {
        return doc;
    }

    /**
     * @return true if this is a primitive type (not struct/list/map)
     */
    public boolean isPrimitive() {
        return dataType != null && !dataType.startsWith("{");
    }

    @Override
    public String toString() {
        return String.format("IcebergSchemaField{name='%s', type='%s', id=%d, nullable=%s}",
                name, dataType, fieldId, nullable);
    }

    /**
     * Construct from a parsed TANT byte buffer map.
     * Package-private; used by IcebergTableReader.
     */
    static IcebergSchemaField fromMap(Map<String, Object> map) {
        String name = (String) map.get("name");
        String dataType = (String) map.get("data_type");
        int fieldId = toInt(map.get("field_id"));
        boolean nullable = toBoolean(map.get("nullable"));
        String doc = (String) map.get("doc");
        if (doc != null && doc.isEmpty()) {
            doc = null;
        }
        return new IcebergSchemaField(name, dataType, fieldId, nullable, doc);
    }

    private static int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return -1;
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return false;
    }
}
