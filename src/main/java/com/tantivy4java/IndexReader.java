package com.tantivy4java;

public class IndexReader implements AutoCloseable {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    IndexReader(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public Searcher searcher() {
        checkNotClosed();
        long searcherHandle = nativeGetSearcher(nativeHandle);
        return new Searcher(searcherHandle);
    }

    public void reload() {
        checkNotClosed();
        nativeReload(nativeHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IndexReader has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativeHandle);
            closed = true;
        }
    }

    private native long nativeGetSearcher(long readerHandle);
    private native void nativeReload(long readerHandle);
    private native void nativeClose(long readerHandle);
}