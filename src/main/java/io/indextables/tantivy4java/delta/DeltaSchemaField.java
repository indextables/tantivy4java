package io.indextables.tantivy4java.delta;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName";
    private static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";

    private final String name;
    private final String dataType;
    private final boolean nullable;
    private final String metadata;
    private volatile Map<String, String> parsedMetadata;

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

    /**
     * Get the physical column name used in parquet files when Delta column mapping is enabled.
     *
     * <p>When {@code delta.columnMapping.mode} is set to {@code "name"} or {@code "id"},
     * parquet files use physical column names (e.g. "col-abc123") that differ from the
     * logical column names in the Delta schema. This method extracts the physical name
     * from the field's metadata.
     *
     * @return the physical column name, or the logical name if column mapping is not configured
     */
    public String getPhysicalName() {
        Map<String, String> meta = getMetadataMap();
        String physicalName = meta.get(PHYSICAL_NAME_KEY);
        return physicalName != null ? physicalName : name;
    }

    /**
     * @return true if this field has Delta column mapping metadata
     */
    public boolean hasColumnMapping() {
        Map<String, String> meta = getMetadataMap();
        return meta.containsKey(PHYSICAL_NAME_KEY);
    }

    /**
     * Get the Delta column mapping ID, if present.
     *
     * @return the column mapping ID, or -1 if not set
     */
    public int getColumnMappingId() {
        Map<String, String> meta = getMetadataMap();
        String idStr = meta.get(COLUMN_MAPPING_ID_KEY);
        if (idStr != null) {
            try {
                return Integer.parseInt(idStr);
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Parse the metadata JSON string into a map, caching the result.
     */
    private Map<String, String> getMetadataMap() {
        if (parsedMetadata == null) {
            if (metadata == null || metadata.isEmpty() || "{}".equals(metadata)) {
                parsedMetadata = Collections.emptyMap();
            } else {
                try {
                    parsedMetadata = MAPPER.readValue(metadata,
                            new TypeReference<Map<String, String>>() {});
                } catch (Exception e) {
                    parsedMetadata = Collections.emptyMap();
                }
            }
        }
        return parsedMetadata;
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
    public static DeltaSchemaField fromMap(Map<String, Object> map) {
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
