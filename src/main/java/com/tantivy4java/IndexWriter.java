package com.tantivy4java;

public class IndexWriter implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    IndexWriter(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public void addDocument(Document document) {
        checkNotClosed();
        nativeAddDocument(nativeHandle, document.getNativeHandle());
    }

    public void deleteDocuments(String field, String value) {
        checkNotClosed();
        nativeDeleteDocuments(nativeHandle, field, value);
    }

    public void commit() {
        checkNotClosed();
        nativeCommit(nativeHandle);
    }

    public void rollback() {
        checkNotClosed();
        nativeRollback(nativeHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativeHandle);
            closed = true;
        }
    }

    private native void nativeAddDocument(long writerHandle, long documentHandle);
    private native void nativeDeleteDocuments(long writerHandle, String field, String value);
    private native void nativeCommit(long writerHandle);
    private native void nativeRollback(long writerHandle);
    private native void nativeClose(long writerHandle);
}