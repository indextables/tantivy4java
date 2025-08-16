package com.tantivy4java;

public class SchemaBuilder {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;

    public SchemaBuilder() {
        this.nativeHandle = nativeNew();
    }

    public SchemaBuilder addTextField(String name, FieldType fieldType) {
        nativeAddTextField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public SchemaBuilder addIntField(String name, FieldType fieldType) {
        nativeAddIntField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public SchemaBuilder addUIntField(String name, FieldType fieldType) {
        nativeAddUIntField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public SchemaBuilder addFloatField(String name, FieldType fieldType) {
        nativeAddFloatField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public SchemaBuilder addDateField(String name, FieldType fieldType) {
        nativeAddDateField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public SchemaBuilder addBytesField(String name, FieldType fieldType) {
        nativeAddBytesField(nativeHandle, name, fieldType.getValue());
        return this;
    }

    public Schema build() {
        long schemaHandle = nativeBuild(nativeHandle);
        return new Schema(schemaHandle);
    }

    private native long nativeNew();
    private native void nativeAddTextField(long builderHandle, String name, int fieldType);
    private native void nativeAddIntField(long builderHandle, String name, int fieldType);
    private native void nativeAddUIntField(long builderHandle, String name, int fieldType);
    private native void nativeAddFloatField(long builderHandle, String name, int fieldType);
    private native void nativeAddDateField(long builderHandle, String name, int fieldType);
    private native void nativeAddBytesField(long builderHandle, String name, int fieldType);
    private native long nativeBuild(long builderHandle);
}