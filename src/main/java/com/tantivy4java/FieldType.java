package com.tantivy4java;

public enum FieldType {
    TEXT(0),
    INTEGER(1),
    UNSIGNED_INTEGER(2),
    FLOAT(3),
    DATE(4),
    BYTES(5),
    STORED(6),
    INDEXED(7),
    FAST(8);

    private final int value;

    FieldType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static FieldType fromValue(int value) {
        for (FieldType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown field type value: " + value);
    }
}