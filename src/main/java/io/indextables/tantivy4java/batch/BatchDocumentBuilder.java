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

import io.indextables.tantivy4java.util.Facet;

import io.indextables.tantivy4java.core.IndexWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builder for creating batches of documents that can be efficiently serialized
 * to byte arrays for high-performance bulk indexing operations.
 * 
 * This class provides zero-copy semantics where possible and minimizes JNI calls
 * by collecting multiple documents into a single byte array that can be passed
 * to the native layer in one operation.
 * 
 * Binary Format:
 * [Header Magic: 4 bytes]
 * [Document 1 Data]
 * [Document 2 Data]
 * ...
 * [Document N Data]
 * [Offset 1: 4 bytes] // Offset to Document 1
 * [Offset 2: 4 bytes] // Offset to Document 2
 * ...
 * [Offset N: 4 bytes] // Offset to Document N
 * [Offset Table Position: 4 bytes] // Position where offset table starts
 * [Document Count: 4 bytes] // Number of documents
 * [Footer Magic: 4 bytes] // Same as header magic for validation
 * 
 * Usage:
 * <pre>
 * // Manual approach
 * BatchDocumentBuilder builder = new BatchDocumentBuilder();
 * BatchDocument doc1 = new BatchDocument();
 * doc1.addText("title", "Example Title");
 * doc1.addInteger("count", 42);
 * builder.addDocument(doc1);
 * 
 * // ... add more documents ...
 * 
 * // Serialize to byte array for indexing
 * ByteBuffer buffer = builder.build();
 * writer.addDocumentsByBuffer(buffer);
 * 
 * // Automatic flushing with try-with-resources (recommended)
 * try (BatchDocumentBuilder builder = new BatchDocumentBuilder(writer)) {
 *     BatchDocument doc = new BatchDocument();
 *     doc.addText("title", "Auto-flushed Document");
 *     builder.addDocument(doc);
 *     // Documents are automatically flushed when builder is closed
 * }
 * </pre>
 */
public class BatchDocumentBuilder implements AutoCloseable {
    private static final int MAGIC_NUMBER = 0x54414E54; // "TANT" in ASCII
    private static final int INITIAL_CAPACITY = 8192; // 8KB initial buffer
    
    private final List<BatchDocument> documents = new ArrayList<>();
    private int estimatedSize = 0;
    private IndexWriter associatedWriter = null;
    private boolean closed = false;
    
    /**
     * Create a new batch document builder.
     */
    public BatchDocumentBuilder() {
    }
    
    /**
     * Create a new batch document builder associated with an IndexWriter.
     * When close() is called, any pending documents will be automatically
     * flushed to the associated writer.
     * 
     * @param writer IndexWriter to associate with this builder
     */
    public BatchDocumentBuilder(IndexWriter writer) {
        this.associatedWriter = writer;
    }
    
    /**
     * Add a document to the batch.
     * @param document Document to add to the batch
     * @return This builder for method chaining
     */
    public BatchDocumentBuilder addDocument(BatchDocument document) {
        if (closed) {
            throw new IllegalStateException("Cannot add documents to a closed BatchDocumentBuilder");
        }
        if (document == null) {
            throw new IllegalArgumentException("Document cannot be null");
        }
        documents.add(document);
        estimatedSize += estimateDocumentSize(document);
        return this;
    }
    
    /**
     * Add multiple documents to the batch.
     * @param documents List of documents to add
     * @return This builder for method chaining
     */
    public BatchDocumentBuilder addDocuments(List<BatchDocument> documents) {
        if (documents == null) {
            throw new IllegalArgumentException("Documents list cannot be null");
        }
        for (BatchDocument doc : documents) {
            addDocument(doc);
        }
        return this;
    }
    
    /**
     * Create a document from a map and add it to the batch.
     * @param fields Map of field names to values
     * @return This builder for method chaining
     */
    public BatchDocumentBuilder addDocumentFromMap(Map<String, Object> fields) {
        BatchDocument doc = BatchDocument.fromMap(fields);
        return addDocument(doc);
    }
    
    /**
     * Get the number of documents currently in the batch.
     * @return Number of documents
     */
    public int getDocumentCount() {
        return documents.size();
    }
    
    /**
     * Get the estimated size of the serialized batch in bytes.
     * This is an approximation and the actual size may vary.
     * @return Estimated size in bytes
     */
    public int getEstimatedSize() {
        return estimatedSize + 16; // Header overhead
    }
    
    /**
     * Get the current serialized size of the batch in bytes.
     * This provides the actual size that would be used for the ByteBuffer.
     * Use this for accurate memory planning and batch size control.
     * @return Current serialized size in bytes
     * @throws IllegalStateException if no documents have been added
     */
    public int getCurrentSize() {
        if (documents.isEmpty()) {
            return 0;
        }
        
        // Create a minimal buffer to measure exact size
        int estimatedSize = getEstimatedSize() + 4 + (documents.size() * 4) + 12;
        ByteBuffer tempBuffer = ByteBuffer.allocateDirect((int)(estimatedSize * 1.2));
        tempBuffer.order(ByteOrder.nativeOrder());
        
        try {
            serializeBatch(tempBuffer);
            return tempBuffer.position(); // Actual bytes written
        } catch (Exception e) {
            // Fall back to estimation if serialization fails
            return estimatedSize;
        }
    }
    
    /**
     * Check if adding another document would exceed the specified size limit.
     * This helps with memory-conscious batch building.
     * @param maxSize Maximum allowed batch size in bytes
     * @param additionalDocument Document to potentially add
     * @return true if adding the document would exceed the limit
     */
    public boolean wouldExceedSize(int maxSize, BatchDocument additionalDocument) {
        if (documents.isEmpty()) {
            return false; // First document is always allowed
        }
        
        int currentSize = getEstimatedSize();
        int additionalSize = estimateDocumentSize(additionalDocument);
        
        return (currentSize + additionalSize) > maxSize;
    }
    
    /**
     * Get the size of the current batch in a human-readable format.
     * @return Formatted size string (e.g., "1.2 MB", "542 KB", "1,234 bytes")
     */
    public String getFormattedSize() {
        int size = getEstimatedSize();
        return formatByteSize(size);
    }
    
    /**
     * Get memory usage statistics for this batch.
     * @return BatchMemoryStats object with detailed information
     */
    public BatchMemoryStats getMemoryStats() {
        return new BatchMemoryStats(this);
    }
    
    /**
     * Clear all documents from the batch.
     * @return This builder for method chaining
     */
    public BatchDocumentBuilder clear() {
        documents.clear();
        estimatedSize = 0;
        return this;
    }
    
    /**
     * Check if the batch is empty.
     * @return true if no documents have been added, false otherwise
     */
    public boolean isEmpty() {
        return documents.isEmpty();
    }
    
    /**
     * Build the batch into a ByteBuffer for efficient JNI transfer.
     * The buffer uses direct memory allocation for zero-copy semantics
     * and native byte order for optimal performance.
     * 
     * @return Direct ByteBuffer containing the serialized batch
     * @throws RuntimeException if serialization fails
     */
    public ByteBuffer build() {
        if (documents.isEmpty()) {
            throw new IllegalStateException("No documents to serialize");
        }
        
        // Get the exact size needed first
        int actualSize = getCurrentSize();
        if (actualSize == 0) {
            // Fallback to conservative estimation
            int protocolOverhead = 4 + (documents.size() * 4) + 12;
            actualSize = Math.max(getEstimatedSize() + protocolOverhead, INITIAL_CAPACITY);
            actualSize = (int) (actualSize * 1.2); // 20% padding for safety
        }
        
        // Allocate buffer with exact size needed
        ByteBuffer buffer = ByteBuffer.allocateDirect(actualSize);
        buffer.order(ByteOrder.nativeOrder()); // Use native byte order for performance
        
        try {
            serializeBatch(buffer);
            buffer.flip(); // Prepare for reading
            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize document batch", e);
        }
    }
    
    /**
     * Build the batch into a regular byte array.
     * This method is less efficient than build() but may be useful
     * for testing or compatibility scenarios.
     * 
     * @return Byte array containing the serialized batch
     */
    public byte[] buildArray() {
        ByteBuffer buffer = build();
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }
    
    private void serializeBatch(ByteBuffer buffer) {
        // Write header with magic number only
        buffer.putInt(MAGIC_NUMBER);
        
        // Write documents and collect offsets
        List<Integer> offsets = new ArrayList<>(documents.size());
        for (BatchDocument document : documents) {
            offsets.add(buffer.position());
            serializeDocument(buffer, document);
        }
        
        // Write offset table in footer
        int offsetTableStart = buffer.position();
        for (int offset : offsets) {
            buffer.putInt(offset);
        }
        
        // Write footer with document count and offset table position
        buffer.putInt(offsetTableStart);  // Position of offset table
        buffer.putInt(documents.size());  // Document count
        buffer.putInt(MAGIC_NUMBER);      // Footer magic for validation
    }
    
    private void serializeDocument(ByteBuffer buffer, BatchDocument document) {
        Map<String, List<BatchDocument.FieldValue>> fields = document.getAllFields();
        
        // Write field count
        if (fields.size() > 65535) {
            throw new RuntimeException("Too many fields in document: " + fields.size());
        }
        buffer.putShort((short) fields.size());
        
        // Write fields
        for (Map.Entry<String, List<BatchDocument.FieldValue>> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            List<BatchDocument.FieldValue> values = entry.getValue();
            
            serializeField(buffer, fieldName, values);
        }
    }
    
    private void serializeField(ByteBuffer buffer, String fieldName, List<BatchDocument.FieldValue> values) {
        // Write field name
        byte[] nameBytes = fieldName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 65535) {
            throw new RuntimeException("Field name too long: " + fieldName);
        }
        buffer.putShort((short) nameBytes.length);
        buffer.put(nameBytes);
        
        // Write field type (use first value's type)
        BatchDocument.FieldType fieldType = values.get(0).getType();
        buffer.put(fieldType.getCode());
        
        // Write value count
        if (values.size() > 65535) {
            throw new RuntimeException("Too many values for field: " + fieldName);
        }
        buffer.putShort((short) values.size());
        
        // Write values
        for (BatchDocument.FieldValue value : values) {
            serializeFieldValue(buffer, value);
        }
    }
    
    private void serializeFieldValue(ByteBuffer buffer, BatchDocument.FieldValue fieldValue) {
        Object value = fieldValue.getValue();
        BatchDocument.FieldType type = fieldValue.getType();
        
        switch (type) {
            case TEXT:
            case JSON:
            case IP_ADDR:
                String strValue = value.toString();
                byte[] strBytes = strValue.getBytes(StandardCharsets.UTF_8);
                buffer.putInt(strBytes.length);
                buffer.put(strBytes);
                break;
                
            case INTEGER:
            case UNSIGNED:
                buffer.putLong(((Number) value).longValue());
                break;
                
            case FLOAT:
                buffer.putDouble(((Number) value).doubleValue());
                break;
                
            case BOOLEAN:
                buffer.put((byte) (((Boolean) value) ? 1 : 0));
                break;
                
            case DATE:
                LocalDateTime date = (LocalDateTime) value;
                // Use nanoseconds since epoch for microsecond precision
                // Tantivy DateTime with microsecond precision requires nanosecond timestamps
                long epochSecond = date.toEpochSecond(ZoneOffset.UTC);
                int nano = date.getNano();
                long epochNanos = epochSecond * 1_000_000_000L + nano;
                buffer.putLong(epochNanos);
                break;
                
            case BYTES:
                byte[] bytes = (byte[]) value;
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                break;
                
            case FACET:
                // For facets, we need to get the path as a string
                String facetPath;
                if (value instanceof Facet) {
                    // This would require accessing the facet's path
                    // For now, use toString() as a placeholder
                    facetPath = value.toString();
                } else {
                    facetPath = value.toString();
                }
                byte[] facetBytes = facetPath.getBytes(StandardCharsets.UTF_8);
                buffer.putInt(facetBytes.length);
                buffer.put(facetBytes);
                break;
                
            default:
                throw new RuntimeException("Unsupported field type: " + type);
        }
    }
    
    private int estimateDocumentSize(BatchDocument document) {
        int size = 2; // Field count
        
        for (Map.Entry<String, List<BatchDocument.FieldValue>> entry : document.getAllFields().entrySet()) {
            String fieldName = entry.getKey();
            List<BatchDocument.FieldValue> values = entry.getValue();
            
            // Field name length + field name + field type + value count
            size += 2 + fieldName.getBytes(StandardCharsets.UTF_8).length + 1 + 2;
            
            // Estimate value sizes
            for (BatchDocument.FieldValue value : values) {
                size += estimateValueSize(value);
            }
        }
        
        return size;
    }
    
    private int estimateValueSize(BatchDocument.FieldValue fieldValue) {
        BatchDocument.FieldType type = fieldValue.getType();
        Object value = fieldValue.getValue();
        
        switch (type) {
            case TEXT:
            case JSON:
            case IP_ADDR:
            case FACET:
                return 4 + value.toString().getBytes(StandardCharsets.UTF_8).length;
            case INTEGER:
            case UNSIGNED:
            case FLOAT:
            case DATE:
                return 8;
            case BOOLEAN:
                return 1;
            case BYTES:
                return 4 + ((byte[]) value).length;
            default:
                return 32; // Conservative estimate
        }
    }
    
    private static String formatByteSize(long bytes) {
        if (bytes < 1024) {
            return String.format("%,d bytes", bytes);
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
    
    /**
     * Associate this builder with an IndexWriter for automatic flushing on close.
     * 
     * @param writer IndexWriter to associate with this builder
     * @return This builder for method chaining
     */
    public BatchDocumentBuilder associateWith(IndexWriter writer) {
        this.associatedWriter = writer;
        return this;
    }
    
    /**
     * Flushes any pending documents to the associated IndexWriter if one is set.
     * This is called automatically when close() is invoked.
     * 
     * @return Array of opstamps from the indexing operation, or empty array if no writer associated
     * @throws RuntimeException if indexing fails
     */
    public long[] flush() {
        if (associatedWriter == null) {
            return new long[0]; // No writer associated, nothing to flush
        }
        
        if (documents.isEmpty()) {
            return new long[0]; // No documents to flush
        }
        
        try {
            long[] opstamps = associatedWriter.addDocumentsBatch(this);
            clear(); // Clear documents after successful flush
            return opstamps;
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush batch documents to IndexWriter", e);
        }
    }
    
    /**
     * Closes this builder and flushes any pending documents to the associated IndexWriter.
     * After calling close(), this builder should not be used for further operations.
     * This method is idempotent - calling it multiple times has no additional effect.
     */
    @Override
    public void close() {
        if (closed) {
            return; // Already closed
        }
        
        try {
            flush(); // Flush any pending documents
        } finally {
            closed = true;
        }
    }
    
    /**
     * Checks if this builder has been closed.
     * 
     * @return true if close() has been called, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Checks if this builder has an associated IndexWriter for automatic flushing.
     * 
     * @return true if an IndexWriter is associated, false otherwise
     */
    public boolean hasAssociatedWriter() {
        return associatedWriter != null;
    }
    
    /**
     * Memory statistics for a batch document builder.
     * Provides detailed information about memory usage and characteristics.
     */
    public static class BatchMemoryStats {
        private final int documentCount;
        private final int estimatedSize;
        private final int averageDocumentSize;
        private final int maxDocumentSize;
        private final int minDocumentSize;
        private final String formattedSize;
        
        BatchMemoryStats(BatchDocumentBuilder builder) {
            this.documentCount = builder.getDocumentCount();
            this.estimatedSize = builder.getEstimatedSize();
            this.formattedSize = formatByteSize(estimatedSize);
            
            if (documentCount > 0) {
                this.averageDocumentSize = estimatedSize / documentCount;
                
                // Calculate min/max document sizes
                int minSize = Integer.MAX_VALUE;
                int maxSize = Integer.MIN_VALUE;
                
                for (BatchDocument doc : builder.documents) {
                    int docSize = builder.estimateDocumentSize(doc);
                    minSize = Math.min(minSize, docSize);
                    maxSize = Math.max(maxSize, docSize);
                }
                
                this.minDocumentSize = minSize;
                this.maxDocumentSize = maxSize;
            } else {
                this.averageDocumentSize = 0;
                this.minDocumentSize = 0;
                this.maxDocumentSize = 0;
            }
        }
        
        /**
         * Get the number of documents in the batch.
         * @return Document count
         */
        public int getDocumentCount() {
            return documentCount;
        }
        
        /**
         * Get the estimated total size in bytes.
         * @return Estimated size in bytes
         */
        public int getEstimatedSize() {
            return estimatedSize;
        }
        
        /**
         * Get the average document size in bytes.
         * @return Average document size
         */
        public int getAverageDocumentSize() {
            return averageDocumentSize;
        }
        
        /**
         * Get the largest document size in bytes.
         * @return Maximum document size
         */
        public int getMaxDocumentSize() {
            return maxDocumentSize;
        }
        
        /**
         * Get the smallest document size in bytes.
         * @return Minimum document size
         */
        public int getMinDocumentSize() {
            return minDocumentSize;
        }
        
        /**
         * Get the total size in human-readable format.
         * @return Formatted size string
         */
        public String getFormattedSize() {
            return formattedSize;
        }
        
        /**
         * Get the average document size in human-readable format.
         * @return Formatted average document size
         */
        public String getFormattedAverageDocumentSize() {
            return formatByteSize(averageDocumentSize);
        }
        
        /**
         * Calculate memory efficiency as documents per MB.
         * @return Number of documents that would fit in 1MB
         */
        public double getDocumentsPerMB() {
            if (averageDocumentSize == 0) {
                return 0.0;
            }
            return (1024.0 * 1024.0) / averageDocumentSize;
        }
        
        /**
         * Get memory usage summary as a formatted string.
         * @return Multi-line summary of memory statistics
         */
        public String getSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Batch Memory Statistics:\n");
            sb.append(String.format("  Documents: %,d\n", documentCount));
            sb.append(String.format("  Total Size: %s (%,d bytes)\n", formattedSize, estimatedSize));
            if (documentCount > 0) {
                sb.append(String.format("  Avg Doc Size: %s (%,d bytes)\n", 
                    getFormattedAverageDocumentSize(), averageDocumentSize));
                sb.append(String.format("  Min Doc Size: %,d bytes\n", minDocumentSize));
                sb.append(String.format("  Max Doc Size: %,d bytes\n", maxDocumentSize));
                sb.append(String.format("  Docs per MB: %.1f\n", getDocumentsPerMB()));
            }
            return sb.toString();
        }
        
        @Override
        public String toString() {
            return getSummary();
        }
    }
}