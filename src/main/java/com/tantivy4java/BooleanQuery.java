package com.tantivy4java;

public class BooleanQuery extends Query {
    BooleanQuery(long nativeHandle) {
        super(nativeHandle);
    }

    public BooleanQuery must(Query query) {
        checkNotClosed();
        nativeMust(nativeHandle, query.getNativeHandle());
        return this;
    }

    public BooleanQuery should(Query query) {
        checkNotClosed();
        nativeShould(nativeHandle, query.getNativeHandle());
        return this;
    }

    public BooleanQuery mustNot(Query query) {
        checkNotClosed();
        nativeMustNot(nativeHandle, query.getNativeHandle());
        return this;
    }

    private native void nativeMust(long boolQueryHandle, long queryHandle);
    private native void nativeShould(long boolQueryHandle, long queryHandle);
    private native void nativeMustNot(long boolQueryHandle, long queryHandle);
}