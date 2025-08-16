package com.tantivy4java;

import java.nio.file.Path;

public class Index implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    Index(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public static Index open(Path indexPath) {
        long handle = nativeOpen(indexPath.toString());
        return new Index(handle);
    }

    public static Index create(Schema schema, Path indexPath) {
        long handle = nativeCreate(schema.getNativeHandle(), indexPath.toString());
        return new Index(handle);
    }

    public IndexWriter writer(int heapSizeInMB) {
        checkNotClosed();
        long writerHandle = nativeGetWriter(nativeHandle, heapSizeInMB);
        return new IndexWriter(writerHandle);
    }

    public IndexReader reader() {
        checkNotClosed();
        long readerHandle = nativeGetReader(nativeHandle);
        return new IndexReader(readerHandle);
    }

    public Schema schema() {
        checkNotClosed();
        long schemaHandle = nativeGetSchema(nativeHandle);
        return new Schema(schemaHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
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

    private static native long nativeOpen(String indexPath);
    private static native long nativeCreate(long schemaHandle, String indexPath);
    private native long nativeGetWriter(long indexHandle, int heapSizeInMB);
    private native long nativeGetReader(long indexHandle);
    private native long nativeGetSchema(long indexHandle);
    private native void nativeClose(long indexHandle);
}