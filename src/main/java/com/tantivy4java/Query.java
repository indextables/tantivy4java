package com.tantivy4java;

public abstract class Query implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    protected long nativeHandle;
    protected boolean closed = false;

    protected Query(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public static TermQuery term(Field field, String term) {
        long handle = nativeTermQuery(field.getNativeHandle(), term);
        return new TermQuery(handle);
    }

    public static RangeQuery range(Field field, String lowerBound, String upperBound) {
        long handle = nativeRangeQuery(field.getNativeHandle(), lowerBound, upperBound);
        return new RangeQuery(handle);
    }

    public static BooleanQuery bool() {
        long handle = nativeBooleanQuery();
        return new BooleanQuery(handle);
    }

    public static AllQuery all() {
        long handle = nativeAllQuery();
        return new AllQuery(handle);
    }

    protected void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Query has been closed");
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

    private static native long nativeTermQuery(long fieldHandle, String term);
    private static native long nativeRangeQuery(long fieldHandle, String lowerBound, String upperBound);
    private static native long nativeBooleanQuery();
    private static native long nativeAllQuery();
    private native void nativeClose(long queryHandle);
}