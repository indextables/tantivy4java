package com.tantivy4java;

public class Schema implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    Schema(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public static Schema fromJson(String json) {
        long handle = nativeFromJson(json);
        return new Schema(handle);
    }

    public String toJson() {
        checkNotClosed();
        return nativeToJson(nativeHandle);
    }

    public Field getField(String fieldName) {
        checkNotClosed();
        long fieldHandle = nativeGetField(nativeHandle, fieldName);
        if (fieldHandle == 0) {
            return null;
        }
        return new Field(fieldHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativeHandle);
            closed = true;
        }
    }

    long getNativeHandle() {
        return nativeHandle;
    }

    private static native long nativeFromJson(String json);
    private native String nativeToJson(long schemaHandle);
    private native long nativeGetField(long schemaHandle, String fieldName);
    private native void nativeClose(long schemaHandle);
}