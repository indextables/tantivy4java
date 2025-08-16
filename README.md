# Tantivy4Java

A complete Java language binding for the [Tantivy search library](https://github.com/quickwit-oss/tantivy), providing high-performance full-text search capabilities with zero-copy JNI integration.

## Features

- **Complete Java API** - Full coverage of Tantivy search functionality
- **High Performance** - JNI with direct memory sharing for maximum speed
- **Zero Copy** - Minimal marshalling between Rust and Java
- **Memory Efficient** - Direct memory sharing minimizes overhead
- **Java 11+ Compatible** - Targets Java 11 and above
- **Maven Integration** - Standard Maven build with native library packaging
- **Comprehensive Tests** - Test coverage matching the Tantivy library itself

## Prerequisites

- **Java 11 or higher**
- **Rust (latest stable version)**
- **Cargo** (comes with Rust)
- **Maven 3.6 or higher**

## Quick Start

### Building the Project

```bash
# Clone and build
git clone <repository-url>
cd tantivy4java

# Compile both Java and Rust components
mvn compile

# Run tests
mvn test

# Create JAR with native libraries
mvn package
```

### Basic Usage

```java
import com.tantivy4java.*;

// Create schema
SchemaBuilder builder = Schema.builder();
builder.addTextField("title", FieldType.TEXT)
       .addTextField("body", FieldType.TEXT);
Schema schema = builder.build();

// Create index
try (Index index = Index.create(schema, Paths.get("/tmp/my-index"))) {
    
    // Add documents
    try (IndexWriter writer = index.writer(50)) {
        try (Document doc = new Document()) {
            Field titleField = schema.getField("title");
            Field bodyField = schema.getField("body");
            
            doc.addText(titleField, "Hello World");
            doc.addText(bodyField, "This is a sample document");
            writer.addDocument(doc);
        }
        writer.commit();
    }
    
    // Search documents
    try (IndexReader reader = index.reader()) {
        Searcher searcher = reader.searcher();
        Field titleField = schema.getField("title");
        
        try (TermQuery query = Query.term(titleField, "Hello");
             SearchResults results = searcher.search(query, 10)) {
            
            System.out.println("Found " + results.size() + " documents");
            for (SearchResult result : results) {
                System.out.println("Score: " + result.getScore() + 
                                 ", Doc ID: " + result.getDocId());
            }
        }
    }
}
```

## Project Structure

```
tantivy4java/
├── src/main/java/com/tantivy4java/    # Java API classes
│   ├── Index.java                     # Main index interface
│   ├── Schema.java                    # Schema management
│   ├── SchemaBuilder.java             # Schema construction
│   ├── Document.java                  # Document handling
│   ├── Field.java                     # Field definitions
│   ├── IndexWriter.java               # Index writing operations
│   ├── IndexReader.java               # Index reading operations
│   ├── Searcher.java                  # Search functionality
│   ├── Query.java                     # Query base class
│   ├── TermQuery.java                 # Term-based queries
│   ├── RangeQuery.java                # Range queries
│   ├── BooleanQuery.java              # Boolean combination queries
│   ├── AllQuery.java                  # Match-all queries
│   ├── SearchResults.java             # Search result container
│   └── SearchResult.java              # Individual search result
├── src/main/rust/                     # Rust JNI implementation
│   ├── Cargo.toml                     # Rust dependencies
│   └── src/lib.rs                     # JNI bridge implementation
└── src/test/java/com/tantivy4java/    # Comprehensive test suite
    ├── BasicIndexTest.java            # Basic index operations
    ├── SchemaTest.java                # Schema functionality
    ├── DocumentTest.java              # Document operations
    ├── QueryTest.java                 # Query construction
    ├── IndexWriterTest.java           # Index writing tests
    └── SearchTest.java                # Search functionality tests
```

## API Reference

### Core Classes

#### Schema Management
- `Schema` - Represents the document schema
- `SchemaBuilder` - Fluent builder for creating schemas
- `Field` - Represents a field in the schema
- `FieldType` - Enumeration of supported field types

#### Index Operations
- `Index` - Main index interface
- `IndexWriter` - For adding/updating documents
- `IndexReader` - For reading from the index
- `Searcher` - For performing searches

#### Document Handling
- `Document` - Represents a document with typed fields
- Supports text, integer, float, date, and binary field types
- Automatic type conversion and validation

#### Query System
- `Query` - Base class for all queries
- `TermQuery` - Search for specific terms
- `RangeQuery` - Search within value ranges
- `BooleanQuery` - Combine queries with AND/OR/NOT logic
- `AllQuery` - Match all documents

#### Search Results
- `SearchResults` - Container for search results with iteration support
- `SearchResult` - Individual result with score and document ID

### Field Types

| Java Type | Rust Type | Description |
|-----------|-----------|-------------|
| `TEXT` | String | Full-text searchable content |
| `INTEGER` | i64 | Signed 64-bit integers |
| `UNSIGNED_INTEGER` | u64 | Unsigned 64-bit integers |
| `FLOAT` | f64 | Double-precision floating point |
| `DATE` | DateTime | Timestamp values |
| `BYTES` | Vec<u8> | Binary data |

## Building and Testing

### Maven Commands

```bash
# Clean build
mvn clean compile

# Run specific test class
mvn test -Dtest=BasicIndexTest

# Run specific test method
mvn test -Dtest=BasicIndexTest#testCreateIndex

# Generate test reports
mvn surefire-report:report

# Package with dependencies
mvn package

# Install to local repository
mvn install
```

### Rust Development

```bash
# Build Rust library only
cd src/main/rust
cargo build --release

# Run Rust tests
cargo test

# Check for issues
cargo clippy
```

## Performance Characteristics

- **Zero-copy operations** where possible between Java and Rust
- **Direct memory mapping** for index files
- **Efficient serialization** using native Rust types
- **Thread-safe** operations with proper synchronization
- **Memory-efficient** document processing

## Memory Management

The library implements proper resource management:

- All native resources implement `AutoCloseable`
- Use try-with-resources for automatic cleanup
- Native handles are automatically freed when Java objects are garbage collected
- Thread-safe reference counting for shared resources

## Error Handling

- JNI errors are converted to appropriate Java exceptions
- Rust panics are caught and converted to `RuntimeException`
- Resource cleanup is guaranteed even in error conditions
- Detailed error messages preserve the original Rust error information

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `mvn test`
5. Submit a pull request

### Development Guidelines

- Follow Java coding conventions for the Java API
- Follow Rust conventions for the native implementation
- Maintain comprehensive test coverage
- Update documentation for API changes
- Use semantic versioning for releases

## License

This project follows the same license as the underlying Tantivy library.

## Related Projects

- [Tantivy](https://github.com/quickwit-oss/tantivy) - The underlying Rust search library
- [Quickwit](https://github.com/quickwit-oss/quickwit) - Distributed search engine built on Tantivy

## Support

For issues and questions:
- Check the [test suite](src/test/java/com/tantivy4java/) for usage examples
- Review the [Tantivy documentation](https://docs.rs/tantivy/) for search concepts
- Open an issue for bugs or feature requests# tantivy4java
