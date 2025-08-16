package com.tantivy4java;

public class Document implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    public Document() {
        this.nativeHandle = nativeNew();
    }

    Document(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public void addText(Field field, String value) {
        checkNotClosed();
        nativeAddText(nativeHandle, field.getNativeHandle(), value);
    }

    public void addInteger(Field field, long value) {
        checkNotClosed();
        nativeAddInteger(nativeHandle, field.getNativeHandle(), value);
    }

    public void addUInteger(Field field, long value) {
        checkNotClosed();
        nativeAddUInteger(nativeHandle, field.getNativeHandle(), value);
    }

    public void addFloat(Field field, double value) {
        checkNotClosed();
        nativeAddFloat(nativeHandle, field.getNativeHandle(), value);
    }

    public void addDate(Field field, long timestamp) {
        checkNotClosed();
        nativeAddDate(nativeHandle, field.getNativeHandle(), timestamp);
    }

    public void addBytes(Field field, byte[] value) {
        checkNotClosed();
        nativeAddBytes(nativeHandle, field.getNativeHandle(), value);
    }

    public String getText(Field field) {
        checkNotClosed();
        return nativeGetText(nativeHandle, field.getNativeHandle());
    }

    public long getInteger(Field field) {
        checkNotClosed();
        return nativeGetInteger(nativeHandle, field.getNativeHandle());
    }

    public long getUInteger(Field field) {
        checkNotClosed();
        return nativeGetUInteger(nativeHandle, field.getNativeHandle());
    }

    public double getFloat(Field field) {
        checkNotClosed();
        return nativeGetFloat(nativeHandle, field.getNativeHandle());
    }

    public long getDate(Field field) {
        checkNotClosed();
        return nativeGetDate(nativeHandle, field.getNativeHandle());
    }

    public byte[] getBytes(Field field) {
        checkNotClosed();
        return nativeGetBytes(nativeHandle, field.getNativeHandle());
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
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

    private native long nativeNew();
    private native void nativeAddText(long docHandle, long fieldHandle, String value);
    private native void nativeAddInteger(long docHandle, long fieldHandle, long value);
    private native void nativeAddUInteger(long docHandle, long fieldHandle, long value);
    private native void nativeAddFloat(long docHandle, long fieldHandle, double value);
    private native void nativeAddDate(long docHandle, long fieldHandle, long timestamp);
    private native void nativeAddBytes(long docHandle, long fieldHandle, byte[] value);
    private native String nativeGetText(long docHandle, long fieldHandle);
    private native long nativeGetInteger(long docHandle, long fieldHandle);
    private native long nativeGetUInteger(long docHandle, long fieldHandle);
    private native double nativeGetFloat(long docHandle, long fieldHandle);
    private native long nativeGetDate(long docHandle, long fieldHandle);
    private native byte[] nativeGetBytes(long docHandle, long fieldHandle);
    private native void nativeClose(long docHandle);
}