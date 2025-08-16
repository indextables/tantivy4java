package com.tantivy4java;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SearchResults implements AutoCloseable, Iterable<SearchResult> {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;
    private boolean closed = false;

    SearchResults(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public int size() {
        checkNotClosed();
        return nativeSize(nativeHandle);
    }

    public SearchResult get(int index) {
        checkNotClosed();
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        long resultHandle = nativeGet(nativeHandle, index);
        return new SearchResult(resultHandle);
    }

    @Override
    public Iterator<SearchResult> iterator() {
        return new SearchResultIterator();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("SearchResults has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativeHandle);
            closed = true;
        }
    }

    private class SearchResultIterator implements Iterator<SearchResult> {
        private int currentIndex = 0;
        private final int size = size();

        @Override
        public boolean hasNext() {
            return currentIndex < size;
        }

        @Override
        public SearchResult next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return get(currentIndex++);
        }
    }

    private native int nativeSize(long resultsHandle);
    private native long nativeGet(long resultsHandle, int index);
    private native void nativeClose(long resultsHandle);
}