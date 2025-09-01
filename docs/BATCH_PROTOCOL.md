# Tantivy4Java Batch Document Protocol

This document describes the binary protocol used for batch document indexing in Tantivy4Java, enabling high-performance bulk operations with minimal JNI calls.

## Overview

The batch document protocol allows multiple documents to be serialized into a single byte buffer for efficient transfer across the JNI boundary. The format uses zero-copy semantics where possible and native byte order for optimal performance.

## Binary Format Specification

### Overall Structure

```
[Header Magic: 4 bytes]
[Document Data Section - Variable Length]
[Offset Table - 4 bytes × document_count]
[Footer - 12 bytes]
```

### Header
- **Magic Number**: `0x54414E54` ("TANT" in ASCII)
- **Purpose**: Validates buffer format and detects corruption

### Document Data Section
Variable-length section containing serialized document data in sequence:
```
[Document 1 Data]
[Document 2 Data]
...
[Document N Data]
```

### Offset Table
Array of 32-bit integers pointing to the start position of each document:
```
[Document 1 Offset: 4 bytes]
[Document 2 Offset: 4 bytes]
...
[Document N Offset: 4 bytes]
```

### Footer (12 bytes total)
```
[Offset Table Position: 4 bytes] // Position where offset table starts
[Document Count: 4 bytes]         // Number of documents in batch
[Footer Magic: 4 bytes]           // Same as header magic (0x54414E54)
```

## Document Format

Each document contains:

```
[Field Count: 2 bytes]     // Number of fields (max 65535)
[Field 1 Data]
[Field 2 Data]
...
[Field N Data]
```

## Field Format

Each field contains:

```
[Field Name Length: 2 bytes]      // UTF-8 byte length (max 65535)
[Field Name: variable]            // UTF-8 encoded field name
[Field Type: 1 byte]              // Field type code (see below)
[Value Count: 2 bytes]            // Number of values (max 65535)
[Value 1 Data]
[Value 2 Data]
...
[Value N Data]
```

## Field Type Codes

| Type     | Code | Description                          |
|----------|------|--------------------------------------|
| TEXT     | 0    | UTF-8 text strings                   |
| INTEGER  | 1    | Signed 64-bit integers               |
| FLOAT    | 2    | IEEE 754 double-precision floats     |
| BOOLEAN  | 3    | Boolean values (0/1)                 |
| DATE     | 4    | UTC timestamps (epoch milliseconds)  |
| BYTES    | 5    | Arbitrary byte arrays                |
| JSON     | 6    | JSON-encoded objects                 |
| IP_ADDR  | 7    | IP addresses as UTF-8 strings        |
| UNSIGNED | 8    | Unsigned 64-bit integers             |
| FACET    | 9    | Facet paths as UTF-8 strings         |

## Value Format by Type

### TEXT, JSON, IP_ADDR (Types 0, 6, 7)
```
[Length: 4 bytes]          // UTF-8 byte length
[Data: variable]           // UTF-8 encoded string
```

### INTEGER, UNSIGNED (Types 1, 8)
```
[Value: 8 bytes]           // 64-bit integer (native byte order)
```

### FLOAT (Type 2)
```
[Value: 8 bytes]           // IEEE 754 double (native byte order)
```

### BOOLEAN (Type 3)
```
[Value: 1 byte]            // 0 = false, 1 = true
```

### DATE (Type 4)
```
[Value: 8 bytes]           // UTC timestamp as epoch milliseconds
```

### BYTES (Type 5)
```
[Length: 4 bytes]          // Byte array length
[Data: variable]           // Raw byte data
```

### FACET (Type 9)
```
[Length: 4 bytes]          // UTF-8 byte length of facet path
[Data: variable]           // UTF-8 encoded facet path
```

## Reading Algorithm

1. **Validate Header**: Read and verify magic number (0x54414E54)
2. **Read Footer**: 
   - Seek to end of buffer minus 12 bytes
   - Read footer magic and validate
   - Read document count and offset table position
3. **Read Offset Table**: 
   - Seek to offset table position
   - Read `document_count` × 4 bytes of offsets
4. **Read Documents**: 
   - For each offset, seek to position and read document data
   - Parse fields according to field format specification

## Performance Considerations

- **Native Byte Order**: All multi-byte integers use the platform's native byte order for optimal performance
- **Direct ByteBuffer**: Uses `ByteBuffer.allocateDirect()` for zero-copy JNI transfer
- **Sequential Write**: Documents are written sequentially to maximize cache locality
- **Minimal Parsing**: Rust deserializer can parse incrementally without full buffer allocation

## Performance Characteristics

The batch indexing system provides significant performance improvements over single document indexing:

- **Small batches (100 docs)**: ~1.5x speedup
- **Medium batches (1,000 docs)**: ~2.7x speedup  
- **Large batches (5,000+ docs)**: ~2.7-3.0x speedup
- **100,000 documents**: ~3.0x speedup with excellent scalability

### Performance Benefits

The performance improvement comes from:
1. **Reduced JNI overhead** - Single native call instead of per-document calls
2. **Zero-copy operations** - Direct ByteBuffer transfer without data marshalling
3. **Bulk processing** - Rust processes entire batches efficiently in native code
4. **Sequential I/O** - Optimized disk access patterns for large-scale indexing

## Resource Management

The batch document system provides comprehensive resource management with automatic cleanup:

### AutoCloseable Interface

`BatchDocumentBuilder` implements `AutoCloseable` for safe resource management:

```java
// Automatic flushing with try-with-resources (recommended)
try (BatchDocumentBuilder builder = new BatchDocumentBuilder(writer)) {
    BatchDocument doc = new BatchDocument();
    doc.addText("title", "Auto-flushed Document");
    builder.addDocument(doc);
    // Documents are automatically flushed when builder is closed
}
```

### Manual Resource Management

For cases requiring explicit control:

```java
BatchDocumentBuilder builder = new BatchDocumentBuilder();
builder.associateWith(writer);

// Add documents
builder.addDocument(doc1);
builder.addDocument(doc2);

// Manual flush
long[] opstamps = builder.flush();

// Safe cleanup
builder.close();
```

### Key Features

- **Automatic flushing** - Pending documents are flushed when `close()` is called
- **IndexWriter association** - Associate builders with writers for auto-flushing
- **Closed state protection** - Prevents adding documents to closed builders
- **Idempotent operations** - Safe to call `close()` multiple times
- **Error handling** - Comprehensive exception handling during flush operations

### API Methods

- **`BatchDocumentBuilder(IndexWriter writer)`** - Constructor with associated writer
- **`associateWith(IndexWriter writer)`** - Associate writer after construction
- **`flush()`** - Manually flush pending documents to associated writer
- **`close()`** - Auto-flush and close builder (implements AutoCloseable)
- **`isClosed()`** - Check if builder has been closed
- **`hasAssociatedWriter()`** - Check if writer is associated

## Version History

- **v1.0**: Initial implementation with footer-based format
  - Magic number: 0x54414E54
  - Document count in footer for streaming write capability
  - Support for all Tantivy field types
- **v1.1**: Added resource management with AutoCloseable interface
  - Automatic flushing on close
  - IndexWriter association
  - Comprehensive error handling

## Error Handling

- **Magic Number Mismatch**: Invalid buffer format or corruption
- **Field Count Overflow**: More than 65535 fields in a document
- **Field Name Length Overflow**: Field name exceeds 65535 UTF-8 bytes
- **Value Count Overflow**: More than 65535 values for a single field
- **Buffer Underflow**: Unexpected end of buffer during parsing
- **Invalid Type Code**: Unknown field type code encountered
- **Closed Builder Exception**: Attempting to add documents to a closed builder
- **Flush Errors**: IndexWriter errors during automatic or manual flushing

## Implementation Notes

- All string data is encoded as UTF-8
- Byte arrays are stored with explicit length prefixes
- Multi-value fields store each value independently
- Date values are normalized to UTC epoch milliseconds
- Boolean values use single-byte encoding (0/1)
- Resource management follows Java AutoCloseable patterns
- Thread safety is maintained through proper synchronization