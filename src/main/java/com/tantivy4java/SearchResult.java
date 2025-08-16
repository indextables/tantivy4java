package com.tantivy4java;

public class SearchResult {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;

    SearchResult(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public float getScore() {
        return nativeGetScore(nativeHandle);
    }

    public int getDocId() {
        return nativeGetDocId(nativeHandle);
    }

    private native float nativeGetScore(long resultHandle);
    private native int nativeGetDocId(long resultHandle);
}