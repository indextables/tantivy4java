/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.tantivy4java;

/**
 * Enumeration for field data types in Tantivy schema.
 */
public enum FieldType {
    TEXT(1),
    UNSIGNED(2),
    INTEGER(3),
    FLOAT(4),
    BOOLEAN(5),
    DATE(6),
    FACET(7),
    BYTES(8),
    JSON(9),
    IP_ADDR(10);

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
        throw new IllegalArgumentException("Unknown FieldType value: " + value);
    }
}