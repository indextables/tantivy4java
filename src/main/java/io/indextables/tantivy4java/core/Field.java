package io.indextables.tantivy4java.core;

/**
 * Represents a field handle in a Tantivy schema.
 *
 * <p>Fields are lightweight handles that reference a field by name and numeric ID.
 * They are created by SchemaBuilder when adding fields to a schema.
 *
 * <p>Example:
 * <pre>
 * SchemaBuilder builder = new SchemaBuilder();
 * Field titleField = builder.addTextField("title", true, false, "default", "position");
 * Field jsonField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());
 * </pre>
 */
public class Field {
    private final String name;
    private final int fieldId;

    /**
     * Create a field handle.
     *
     * @param name Field name
     * @param fieldId Numeric field ID
     */
    public Field(String name, int fieldId) {
        this.name = name;
        this.fieldId = fieldId;
    }

    /**
     * Get the field name.
     *
     * @return Field name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the numeric field ID.
     *
     * @return Field ID
     */
    public int getFieldId() {
        return fieldId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return fieldId == field.fieldId && name.equals(field.name);
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + fieldId;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", fieldId=" + fieldId +
                '}';
    }
}
