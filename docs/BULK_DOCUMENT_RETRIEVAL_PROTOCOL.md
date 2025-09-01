# Tantivy4Java Bulk Document Retrieval Protocol

This document describes the binary protocol used for zero-copy bulk document retrieval in Tantivy4Java SplitSearcher, enabling high-performance bulk operations with minimal JNI calls.

## 🚧 **IMPLEMENTATION STATUS: IN DEVELOPMENT**

**Current Status (August 2025):**
- ✅ **API Design Complete** - `docsBulk()` and `parseBulkDocs()` methods defined
- ✅ **Test Framework Complete** - Comprehensive test suite with 1,000+ document validation
- ✅ **Performance Baseline Established** - Individual retrieval: 0.01ms per document
- 🚧 **Native Implementation In Progress** - Core JNI methods being developed
- 🚧 **Binary Protocol Implementation** - Serialization logic under development

**Expected Performance Improvements:**
- **Small batches (10-50 docs)**: ~2-3x speedup over individual calls
- **Medium batches (100-500 docs)**: ~5-10x speedup  
- **Large batches (1000+ docs)**: ~10-20x speedup

## Overview

The bulk document retrieval protocol allows multiple documents to be serialized into a single ByteBuffer for efficient transfer across the JNI boundary. The format mirrors the batch indexing protocol but is used for retrieval rather than insertion.

## Key Features

- **Zero-Copy Semantics**: Direct ByteBuffer transfer without memory marshalling
- **Native Byte Order**: Platform-native endianness for optimal performance
- **Batch Efficiency**: Single JNI call retrieves multiple documents
- **Protocol Reuse**: Same binary format as batch indexing for consistency

## Binary Format Specification

### Overall Structure

```
[Header Magic: 4 bytes]
[Document Data Section - Variable Length]
[Offset Table - 4 bytes × document_count]
[Footer - 12 bytes]
```

**Note**: This format is identical to the batch indexing protocol to enable code reuse and consistency.

### Header
- **Magic Number**: `0x54414E54` ("TANT" in ASCII)
- **Purpose**: Validates buffer format and detects corruption

### Document Data Section
Variable-length section containing serialized document data in retrieval order:
```
[Document 1 Data]
[Document 2 Data]
...
[Document N Data]
```

**Important**: Documents are returned in the same order as the DocAddress list provided to `docsBulk()`.

### Document Format

Each document contains:

```
[Field Count: 2 bytes]     // Number of fields (max 65535)
[Field 1 Data]
[Field 2 Data]
...
[Field N Data]
```

### Field Format

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

### Field Type Codes

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
[Document Count: 4 bytes]         // Number of documents in buffer
[Footer Magic: 4 bytes]           // Same as header magic (0x54414E54)
```

## API Usage Examples

### Zero-Copy Bulk Retrieval

```java
// Get search results
SearchResult results = splitSearcher.search(query, 100);

// Extract document addresses
List<DocAddress> addresses = new ArrayList<>();
for (Hit hit : results.getHits()) {
    addresses.add(hit.getDocAddress());
}

// Zero-copy bulk retrieval
ByteBuffer docBuffer = splitSearcher.docsBulk(addresses);

// Option 1: Process buffer directly (maximum performance)
// ... custom buffer parsing for specific use cases

// Option 2: Convert to Document objects (convenience)
List<Document> documents = splitSearcher.parseBulkDocs(docBuffer);
for (Document doc : documents) {
    String title = doc.getFirst("title");
    Integer score = doc.getFirst("score");
    // ... process document
    doc.close(); // Important: close when done
}
```

### Performance Comparison

```java
// Traditional approach (N JNI calls)
List<Document> docs1 = new ArrayList<>();
for (Hit hit : results.getHits()) {
    docs1.add(splitSearcher.doc(hit.getDocAddress()));
}

// Zero-copy approach (1 JNI call + parsing)
ByteBuffer buffer = splitSearcher.docsBulk(
    results.getHits().stream()
           .map(Hit::getDocAddress)
           .collect(Collectors.toList())
);
List<Document> docs2 = splitSearcher.parseBulkDocs(buffer);
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
5. **Create Document Objects**: Convert parsed data to Document instances

## Performance Considerations

- **Direct ByteBuffer**: Uses `ByteBuffer.allocateDirect()` for zero-copy JNI transfer
- **Native Byte Order**: All multi-byte integers use platform's native byte order
- **Sequential Access**: Documents are serialized sequentially for cache efficiency
- **Minimal JNI Overhead**: Single native call regardless of document count

## Expected Performance Improvements

Based on the batch indexing protocol performance characteristics:

- **Small requests (10-50 docs)**: ~2-3x speedup over individual calls
- **Medium requests (100-500 docs)**: ~5-10x speedup
- **Large requests (1000+ docs)**: ~10-20x speedup
- **Memory efficiency**: Single buffer allocation vs. N Document objects

## Error Handling

- **Magic Number Mismatch**: Invalid buffer format or corruption
- **Buffer Underflow**: Unexpected end of buffer during parsing
- **Invalid Field Types**: Unknown field type code encountered
- **Offset Out of Bounds**: Invalid document offset in offset table
- **Document Address Errors**: Invalid segment/doc ID in request

## Implementation Notes

### Native Layer Responsibilities
- Retrieve documents from Tantivy index using provided addresses
- Serialize documents using the batch protocol format
- Return direct ByteBuffer with serialized data

### Java Layer Responsibilities
- Convert DocAddress list to native arrays
- Provide convenience parsing methods
- Manage Document object lifecycle in parsed results

### Memory Management
- ByteBuffer is allocated in native memory (direct allocation)
- Document objects created by `parseBulkDocs()` must be explicitly closed
- Raw ByteBuffer can be processed directly for maximum efficiency

## Version Compatibility

This protocol uses the same format as the batch indexing protocol (v1.0):
- **Magic Number**: 0x54414E54
- **Document Format**: Identical to batch indexing
- **Field Types**: Same type codes and serialization

## Future Enhancements

Potential improvements for future versions:

1. **Selective Field Retrieval**: Only retrieve specified fields
2. **Compression**: Optional compression for large document sets
3. **Streaming**: Support for very large result sets
4. **Field Filtering**: Server-side filtering before serialization

## Conclusion

The bulk document retrieval protocol provides significant performance improvements for applications that need to retrieve multiple documents from SplitSearcher instances. By reusing the proven batch indexing protocol design, it maintains consistency and enables code reuse while delivering zero-copy performance characteristics.