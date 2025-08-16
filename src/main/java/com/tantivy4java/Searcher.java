package com.tantivy4java;

public class Searcher {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;

    Searcher(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public SearchResults search(Query query, int limit) {
        long resultsHandle = nativeSearch(nativeHandle, query.getNativeHandle(), limit);
        return new SearchResults(resultsHandle);
    }

    public Document doc(int docId) {
        long docHandle = nativeGetDocument(nativeHandle, docId);
        return new Document(docHandle);
    }

    private native long nativeSearch(long searcherHandle, long queryHandle, int limit);
    private native long nativeGetDocument(long searcherHandle, int docId);
}