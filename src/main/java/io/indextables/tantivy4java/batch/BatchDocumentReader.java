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

package io.indextables.tantivy4java.batch;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reader for parsing documents from a byte buffer serialized by the native layer.
 *
 * This class reverses the serialization performed by BatchDocumentBuilder, enabling
 * efficient bulk document retrieval where documents are returned in a single byte buffer
 * instead of individual JNI objects.
 *
 * Binary Format (same as BatchDocumentBuilder):
 * <pre>
 * [Header Magic: 4 bytes]           // 0x54414E54 ("TANT")
 * [Document 1 Data]
 * [Document 2 Data]
 * ...
 * [Document N Data]
 * [Offset 1: 4 bytes]               // Offset to Document 1
 * [Offset 2: 4 bytes]               // Offset to Document 2
 * ...
 * [Offset N: 4 bytes]               // Offset to Document N
 * [Offset Table Position: 4 bytes]  // Position where offset table starts
 * [Document Count: 4 bytes]         // Number of documents
 * [Footer Magic: 4 bytes]           // Same as header magic for validation
 * </pre>
 *
 * Usage:
 * <pre>{@code
 * // Get byte buffer from native bulk retrieval
 * ByteBuffer buffer = splitSearcher.docsBulk(docAddresses);
 *
 * // Parse documents
 * BatchDocumentReader reader = new BatchDocumentReader();
 * List<BatchDocument> docs = reader.parse(buffer);
 *
 * // Or get document count first
 * int count = reader.getDocumentCount(buffer);
 * }</pre>
 */
public class BatchDocumentReader {

    private static final int MAGIC_NUMBER = 0x54414E54; // "TANT" in ASCII
    private static final int FOOTER_SIZE = 12; // offset_table_pos + doc_count + footer_magic
    private static final int MIN_BUFFER_SIZE = 16; // header magic + footer

    /**
     * Parse a byte buffer into a list of BatchDocument objects.
     *
     * @param buffer ByteBuffer containing serialized documents (from native layer)
     * @return List of parsed BatchDocument objects
     * @throws IllegalArgumentException if buffer is null, too small, or has invalid format
     */
    public List<BatchDocument> parse(ByteBuffer buffer) {
        validateBuffer(buffer);

        // Ensure native byte order for correct parsing
        ByteBuffer readBuffer = buffer.duplicate();
        readBuffer.order(ByteOrder.nativeOrder());

        // Read and validate header magic
        int headerMagic = readBuffer.getInt(0);
        if (headerMagic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                String.format("Invalid header magic: expected 0x%08X, got 0x%08X", MAGIC_NUMBER, headerMagic));
        }

        // Read footer (at end of buffer)
        int footerStart = readBuffer.capacity() - FOOTER_SIZE;
        int offsetTablePos = readBuffer.getInt(footerStart);
        int docCount = readBuffer.getInt(footerStart + 4);
        int footerMagic = readBuffer.getInt(footerStart + 8);

        // Validate footer magic
        if (footerMagic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                String.format("Invalid footer magic: expected 0x%08X, got 0x%08X", MAGIC_NUMBER, footerMagic));
        }

        // Read document offsets
        int[] offsets = new int[docCount];
        for (int i = 0; i < docCount; i++) {
            offsets[i] = readBuffer.getInt(offsetTablePos + (i * 4));
        }

        // Parse each document
        List<BatchDocument> documents = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            int offset = offsets[i];
            BatchDocument doc = parseDocument(readBuffer, offset);
            documents.add(doc);
        }

        return documents;
    }

    /**
     * Get the number of documents in the buffer without full parsing.
     *
     * @param buffer ByteBuffer containing serialized documents
     * @return Number of documents in the buffer
     * @throws IllegalArgumentException if buffer is invalid
     */
    public int getDocumentCount(ByteBuffer buffer) {
        validateBuffer(buffer);

        ByteBuffer readBuffer = buffer.duplicate();
        readBuffer.order(ByteOrder.nativeOrder());

        // Read doc count from footer
        int footerStart = readBuffer.capacity() - FOOTER_SIZE;
        return readBuffer.getInt(footerStart + 4);
    }

    /**
     * Validate the buffer format without parsing documents.
     *
     * @param buffer ByteBuffer to validate
     * @return true if the buffer has valid format
     */
    public boolean isValid(ByteBuffer buffer) {
        try {
            validateBuffer(buffer);

            ByteBuffer readBuffer = buffer.duplicate();
            readBuffer.order(ByteOrder.nativeOrder());

            int headerMagic = readBuffer.getInt(0);
            int footerStart = readBuffer.capacity() - FOOTER_SIZE;
            int footerMagic = readBuffer.getInt(footerStart + 8);

            return headerMagic == MAGIC_NUMBER && footerMagic == MAGIC_NUMBER;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Convert parsed BatchDocuments to a Map representation for easier consumption.
     * Dates are converted to epoch nanoseconds (Long) for interoperability.
     * Bytes are kept as byte[].
     *
     * @param buffer ByteBuffer containing serialized documents
     * @return List of Maps where each map contains field name to value(s) mapping
     */
    public List<Map<String, Object>> parseToMaps(ByteBuffer buffer) {
        List<BatchDocument> docs = parse(buffer);
        List<Map<String, Object>> result = new ArrayList<>(docs.size());

        for (BatchDocument doc : docs) {
            Map<String, Object> docMap = new HashMap<>();
            for (String fieldName : doc.getFieldNames()) {
                List<BatchDocument.FieldValue> values = doc.getFieldValues(fieldName);
                if (values.size() == 1) {
                    docMap.put(fieldName, convertValue(values.get(0).getValue()));
                } else {
                    List<Object> multiValues = new ArrayList<>(values.size());
                    for (BatchDocument.FieldValue fv : values) {
                        multiValues.add(convertValue(fv.getValue()));
                    }
                    docMap.put(fieldName, multiValues);
                }
            }
            result.add(docMap);
        }

        return result;
    }

    /**
     * Convert field values to interoperable types.
     * - LocalDateTime -> epoch nanoseconds (Long)
     * - Other types unchanged
     */
    private Object convertValue(Object value) {
        if (value instanceof LocalDateTime) {
            LocalDateTime dt = (LocalDateTime) value;
            long epochSecond = dt.toEpochSecond(ZoneOffset.UTC);
            int nano = dt.getNano();
            return epochSecond * 1_000_000_000L + nano;
        }
        return value;
    }

    private void validateBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        if (buffer.capacity() < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException(
                "Buffer too small: minimum size is " + MIN_BUFFER_SIZE + " bytes, got " + buffer.capacity());
        }
    }

    private BatchDocument parseDocument(ByteBuffer buffer, int offset) {
        BatchDocument doc = new BatchDocument();

        // Read field count
        int fieldCount = buffer.getShort(offset) & 0xFFFF;
        int pos = offset + 2;

        // Parse each field
        for (int i = 0; i < fieldCount; i++) {
            pos = parseField(buffer, pos, doc);
        }

        return doc;
    }

    private int parseField(ByteBuffer buffer, int pos, BatchDocument doc) {
        // Read field name length
        int nameLen = buffer.getShort(pos) & 0xFFFF;
        pos += 2;

        // Read field name
        byte[] nameBytes = new byte[nameLen];
        buffer.position(pos);
        buffer.get(nameBytes);
        String fieldName = new String(nameBytes, StandardCharsets.UTF_8);
        pos += nameLen;

        // Read field type
        byte fieldType = buffer.get(pos);
        pos += 1;

        // Read value count
        int valueCount = buffer.getShort(pos) & 0xFFFF;
        pos += 2;

        // Read values
        for (int i = 0; i < valueCount; i++) {
            pos = parseFieldValue(buffer, pos, fieldType, fieldName, doc);
        }

        return pos;
    }

    private int parseFieldValue(ByteBuffer buffer, int pos, byte fieldType, String fieldName, BatchDocument doc) {
        switch (fieldType) {
            case 0: // TEXT
                return parseStringValue(buffer, pos, fieldName, doc, BatchDocument.FieldType.TEXT);

            case 1: // INTEGER
                long intVal = buffer.getLong(pos);
                doc.addInteger(fieldName, intVal);
                return pos + 8;

            case 2: // FLOAT
                double floatVal = buffer.getDouble(pos);
                doc.addFloat(fieldName, floatVal);
                return pos + 8;

            case 3: // BOOLEAN
                boolean boolVal = buffer.get(pos) != 0;
                doc.addBoolean(fieldName, boolVal);
                return pos + 1;

            case 4: // DATE
                long dateNanos = buffer.getLong(pos);
                // Convert nanoseconds to LocalDateTime
                long epochSecond = dateNanos / 1_000_000_000L;
                int nano = (int) (dateNanos % 1_000_000_000L);
                if (nano < 0) {
                    nano += 1_000_000_000;
                    epochSecond -= 1;
                }
                LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, nano, ZoneOffset.UTC);
                doc.addDate(fieldName, dateTime);
                return pos + 8;

            case 5: // BYTES
                int bytesLen = buffer.getInt(pos);
                pos += 4;
                byte[] bytes = new byte[bytesLen];
                buffer.position(pos);
                buffer.get(bytes);
                doc.addBytes(fieldName, bytes);
                return pos + bytesLen;

            case 6: // JSON
                return parseStringValue(buffer, pos, fieldName, doc, BatchDocument.FieldType.JSON);

            case 7: // IP_ADDR
                return parseStringValue(buffer, pos, fieldName, doc, BatchDocument.FieldType.IP_ADDR);

            case 8: // UNSIGNED
                long unsignedVal = buffer.getLong(pos);
                doc.addUnsigned(fieldName, unsignedVal);
                return pos + 8;

            case 9: // FACET
                return parseStringValue(buffer, pos, fieldName, doc, BatchDocument.FieldType.FACET);

            default:
                throw new IllegalArgumentException("Unknown field type: " + fieldType);
        }
    }

    private int parseStringValue(ByteBuffer buffer, int pos, String fieldName,
                                  BatchDocument doc, BatchDocument.FieldType type) {
        int strLen = buffer.getInt(pos);
        pos += 4;

        byte[] strBytes = new byte[strLen];
        buffer.position(pos);
        buffer.get(strBytes);
        String strVal = new String(strBytes, StandardCharsets.UTF_8);

        switch (type) {
            case TEXT:
                doc.addText(fieldName, strVal);
                break;
            case JSON:
                doc.addJson(fieldName, strVal);
                break;
            case IP_ADDR:
                doc.addIpAddr(fieldName, strVal);
                break;
            case FACET:
                // For facets, we store as text since BatchDocument doesn't have addFacet with string
                doc.addText(fieldName, strVal);
                break;
            default:
                doc.addText(fieldName, strVal);
        }

        return pos + strLen;
    }

    /**
     * Memory statistics for a parsed batch.
     */
    public static class ParseStats {
        private final int documentCount;
        private final int totalFields;
        private final int totalValues;
        private final long parseTimeNanos;

        public ParseStats(int documentCount, int totalFields, int totalValues, long parseTimeNanos) {
            this.documentCount = documentCount;
            this.totalFields = totalFields;
            this.totalValues = totalValues;
            this.parseTimeNanos = parseTimeNanos;
        }

        public int getDocumentCount() { return documentCount; }
        public int getTotalFields() { return totalFields; }
        public int getTotalValues() { return totalValues; }
        public long getParseTimeNanos() { return parseTimeNanos; }
        public double getParseTimeMillis() { return parseTimeNanos / 1_000_000.0; }
        public double getDocsPerSecond() {
            return parseTimeNanos > 0 ? documentCount * 1_000_000_000.0 / parseTimeNanos : 0;
        }
    }

    /**
     * Parse with statistics collection.
     *
     * @param buffer ByteBuffer containing serialized documents
     * @return ParseResult containing both documents and statistics
     */
    public ParseResult parseWithStats(ByteBuffer buffer) {
        long startTime = System.nanoTime();
        List<BatchDocument> documents = parse(buffer);
        long parseTime = System.nanoTime() - startTime;

        int totalFields = 0;
        int totalValues = 0;
        for (BatchDocument doc : documents) {
            totalFields += doc.getNumFields();
            for (String fieldName : doc.getFieldNames()) {
                totalValues += doc.getFieldValues(fieldName).size();
            }
        }

        ParseStats stats = new ParseStats(documents.size(), totalFields, totalValues, parseTime);
        return new ParseResult(documents, stats);
    }

    /**
     * Result of parsing with statistics.
     */
    public static class ParseResult {
        private final List<BatchDocument> documents;
        private final ParseStats stats;

        public ParseResult(List<BatchDocument> documents, ParseStats stats) {
            this.documents = documents;
            this.stats = stats;
        }

        public List<BatchDocument> getDocuments() { return documents; }
        public ParseStats getStats() { return stats; }
    }
}
