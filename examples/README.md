# Tantivy4Java Examples

This directory contains comprehensive examples demonstrating how to use Tantivy4Java for various search scenarios. Each example is self-contained and includes detailed comments explaining the concepts.

## Example Categories

### Basic Examples
- **[BasicSearch](BasicSearch.java)** - Simple indexing and search operations
- **[SchemaBuilding](SchemaBuilding.java)** - Creating schemas with different field types
- **[DocumentOperations](DocumentOperations.java)** - Adding, updating, and deleting documents

### Query Examples
- **[QueryTypes](QueryTypes.java)** - Comprehensive coverage of all query types
- **[BooleanQueries](BooleanQueries.java)** - Complex boolean logic combinations
- **[RangeQueries](RangeQueries.java)** - Numeric and date range searching
- **[FuzzySearch](FuzzySearch.java)** - Handling typos and spelling variations

### Advanced Examples
- **[AdvancedSearch](AdvancedSearch.java)** - Multi-field search with boosting and scoring
- **[ProductCatalog](ProductCatalog.java)** - E-commerce search with filtering and faceting
- **[ContentManagement](ContentManagement.java)** - Document management system search
- **[LogAnalytics](LogAnalytics.java)** - Log search and analysis patterns

### Performance Examples
- **[BulkIndexing](BulkIndexing.java)** - Efficient bulk document indexing
- **[SearchOptimization](SearchOptimization.java)** - Performance optimization techniques
- **[MemoryManagement](MemoryManagement.java)** - Proper resource management patterns

### Integration Examples
- **[WebService](WebService.java)** - REST API integration example
- **[BatchProcessor](BatchProcessor.java)** - Batch processing workflows
- **[RealTimeUpdates](RealTimeUpdates.java)** - Real-time document updates

## Running the Examples

### Prerequisites
- Java 11 or higher
- Maven 3.6+
- Tantivy4Java installed in your local repository

### Compilation and Execution

```bash
# Compile all examples
javac -cp "../target/classes:../target/dependency/*" examples/*.java

# Run a specific example
java -cp "../target/classes:../target/dependency/*:." BasicSearch

# Or use Maven to run examples
mvn exec:java -Dexec.mainClass="BasicSearch" -Dexec.args="sample-data.json"
```

### Example Data

Some examples use sample data files located in the `data/` subdirectory:
- `products.json` - Sample product catalog data
- `articles.json` - Sample article/blog post data  
- `logs.json` - Sample log entries for analytics examples

## Example Patterns

### Resource Management Pattern

All examples follow the proper resource management pattern:

```java
try (SchemaBuilder builder = new SchemaBuilder();
     Schema schema = builder.build();
     Index index = new Index(schema);
     IndexWriter writer = index.writer()) {
    
    // Operations here
    
} // Resources automatically closed
```

### Error Handling Pattern

Examples demonstrate proper error handling:

```java
try {
    // Operations
} catch (Exception e) {
    System.err.println("Operation failed: " + e.getMessage());
    // Appropriate recovery or cleanup
}
```

### Configuration Pattern

Examples show how to configure various components:

```java
// Schema configuration
SchemaBuilder builder = new SchemaBuilder()
    .addTextField("title", true, true, "default", "position")
    .addIntegerField("id", true, true, true);

// Writer configuration
IndexWriter writer = index.writer(
    50_000_000,  // 50MB heap size
    1            // Single thread
);
```

## Learning Path

### Beginner
1. Start with **BasicSearch** to understand core concepts
2. Explore **SchemaBuilding** to learn about field types
3. Try **DocumentOperations** for CRUD operations

### Intermediate  
4. Study **QueryTypes** for comprehensive query coverage
5. Learn **BooleanQueries** for complex search logic
6. Practice **RangeQueries** for numeric/date searches

### Advanced
7. Master **AdvancedSearch** for production-ready search
8. Implement **ProductCatalog** for real-world scenarios
9. Optimize with **BulkIndexing** and **SearchOptimization**

## Best Practices Demonstrated

### Index Design
- Appropriate field type selection
- Tokenizer configuration for different content types
- Fast field usage for aggregatable data

### Query Construction
- Building reusable query components
- Proper boost value selection
- Efficient boolean query combinations

### Performance Optimization
- Batch document processing
- Resource pooling and reuse
- Memory-efficient search patterns

### Production Readiness
- Comprehensive error handling
- Resource cleanup patterns
- Logging and monitoring integration

## Contributing Examples

When contributing new examples:

1. Follow the established naming convention
2. Include comprehensive comments explaining concepts
3. Demonstrate proper resource management
4. Add sample data if needed
5. Update this README with the new example

## Support

If you have questions about any examples:
- Check the main [Documentation](../docs/)
- Review the [API Reference](../docs/reference.md)
- Look at similar examples for patterns
- Create an issue if you find bugs or have suggestions

---

**These examples provide practical, production-ready patterns for building search applications with Tantivy4Java.**