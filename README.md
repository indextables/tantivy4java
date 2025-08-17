# Tantivy4Java

A complete Java port of the Python Tantivy language bindings, providing high-performance full-text search capabilities for Java applications.

## Overview

Tantivy4Java brings the power of the Rust-based Tantivy search engine to Java through JNI (Java Native Interface) bindings. It offers:

- **Zero-copy operations** between Rust and Java for maximum performance
- **Direct memory sharing** to minimize memory usage
- **Complete API coverage** matching the Python tantivy bindings
- **Java 11+ compatibility** with modern Java features
- **Maven integration** for easy dependency management

## Features

- Full-text search with various query types (term, phrase, fuzzy, boolean, range)
- Schema-based document indexing with multiple field types
- Real-time indexing and searching
- Faceted search capabilities
- Aggregation support
- Snippet generation for search results
- Customizable text analysis and tokenization

## Quick Start

### Prerequisites

- Java 11 or higher
- Maven 3.6+
- Rust 1.70+ (for building native components)

### Building

```bash
git clone <repository-url>
cd tantivy4java_pyport
mvn clean compile
```

### Basic Usage

```java
import com.tantivy4java.*;

// Create a schema
try (SchemaBuilder builder = new SchemaBuilder()) {
    builder.addTextField("title", true, false, "default", "position")
           .addTextField("body", true, false, "default", "position");
    
    try (Schema schema = builder.build()) {
        // Create an index
        try (Index index = new Index(schema)) {
            // Add documents
            try (IndexWriter writer = index.writer()) {
                Document doc = new Document();
                doc.addText("title", "Sample Document");
                doc.addText("body", "This is a sample document for indexing");
                
                writer.addDocument(doc);
                writer.commit();
            }
            
            // Search the index
            try (Searcher searcher = index.searcher()) {
                Query query = Query.termQuery(schema, "body", "sample");
                SearchResult result = searcher.search(query);
                
                System.out.println("Found " + result.getHits().size() + " documents");
            }
        }
    }
}
```

## API Documentation

### Core Classes

- **`Index`** - Main index class for creating and managing search indices
- **`Schema`** / **`SchemaBuilder`** - Define document structure and field types
- **`Document`** - Represents documents to be indexed
- **`Query`** - Factory class for creating different types of search queries
- **`IndexWriter`** - Interface for adding/updating/deleting documents
- **`Searcher`** - Interface for searching and retrieving documents

### Field Types

- Text fields with customizable tokenization
- Integer, Float, and Unsigned numeric fields  
- Boolean fields
- Date/DateTime fields
- JSON object fields
- Facet fields for hierarchical categorization
- Bytes fields for binary data
- IP Address fields

### Query Types

- **Term queries** - Exact term matching
- **Phrase queries** - Sequence of terms with optional slop
- **Boolean queries** - Combine multiple queries with AND/OR/NOT
- **Fuzzy queries** - Approximate string matching
- **Range queries** - Numeric and date range filtering
- **Regex queries** - Regular expression matching
- **More-like-this queries** - Find similar documents

## Architecture

Tantivy4Java uses JNI to bridge Java and Rust code:

```
Java API Layer
     ↓
JNI Binding Layer (Rust)
     ↓  
Tantivy Core (Rust)
```

Key design principles:
- **Zero-copy operations** where possible to minimize overhead
- **Resource management** through AutoCloseable interfaces
- **Memory safety** with automatic cleanup of native resources
- **Thread safety** for concurrent access patterns

## Testing

Run the test suite:

```bash
mvn test
```

**Note**: Many tests are currently disabled as they require the complete native implementation.

## Current Status

This project provides the complete Java API structure and JNI scaffolding. The Rust native implementation currently consists of stubs that need to be completed with actual Tantivy integration.

### Completed
- ✅ Complete Java API matching Python tantivy bindings
- ✅ Maven project structure and build configuration
- ✅ JNI method signatures and scaffolding
- ✅ Resource management and memory safety patterns
- ✅ Comprehensive test framework
- ✅ Example usage code

### TODO
- 🔄 Complete Rust JNI implementation
- 🔄 Implement actual Tantivy integration
- 🔄 Add comprehensive test coverage
- 🔄 Performance optimization and zero-copy operations
- 🔄 Documentation and examples

## Building Native Components

The native Rust library is built automatically during Maven compilation:

```bash
# Build just the Rust component
cd native
cargo build --release

# Build the complete project
mvn clean package
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit a pull request

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

## Acknowledgments

- [Tantivy](https://github.com/quickwit-oss/tantivy) - The underlying Rust search engine
- [tantivy-py](https://github.com/quickwit-oss/tantivy-py) - Python bindings that inspired this API design