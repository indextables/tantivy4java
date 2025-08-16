package com.tantivy4java;

public class Field {
    static {
        NativeLibLoader.loadNativeLibrary();
    }

    private long nativeHandle;

    Field(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    public String getName() {
        return nativeGetName(nativeHandle);
    }

    public FieldType getType() {
        int typeValue = nativeGetType(nativeHandle);
        return FieldType.fromValue(typeValue);
    }

    long getNativeHandle() {
        return nativeHandle;
    }

    private native String nativeGetName(long fieldHandle);
    private native int nativeGetType(long fieldHandle);
}