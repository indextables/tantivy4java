# Contributing to Tantivy4Java

Thank you for your interest in contributing to Tantivy4Java! This document provides guidelines and information for contributors.

## 🎯 Project Overview

Tantivy4Java provides 100% functional compatibility with the Python tantivy library, bringing powerful full-text search capabilities to Java applications. Our goal is to maintain complete API parity while delivering production-ready performance.

## 📋 Ways to Contribute

### 🐛 Bug Reports
- Search existing issues first
- Provide detailed reproduction steps
- Include Java version, OS, and Tantivy4Java version
- Add relevant code snippets or test cases

### 💡 Feature Requests
- Check if the feature exists in Python tantivy for API parity
- Describe the use case and expected behavior
- Consider providing a prototype or RFC

### 🔧 Code Contributions
- Bug fixes and performance improvements
- New features that maintain Python API compatibility
- Documentation improvements
- Test coverage enhancements

### 📚 Documentation
- API documentation improvements
- Tutorial and example additions  
- Migration guide updates
- Translation contributions

## 🚀 Getting Started

### Prerequisites

- **Java 11+**: Required for development and testing
- **Maven 3.6+**: Build system and dependency management
- **Rust 1.70+**: Required for native component compilation
- **Git**: Version control

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/tantivy4java.git
   cd tantivy4java
   ```

2. **Build Project**
   ```bash
   mvn clean compile
   ```

3. **Run Tests**
   ```bash
   mvn test
   ```

4. **Verify Native Components**
   ```bash
   # Test JNI bindings
   mvn test -Dtest="*Integration*"
   ```

### Project Structure

```
tantivy4java/
├── src/main/java/com/tantivy4java/     # Java API implementation
├── src/test/java/com/tantivy4java/     # Comprehensive test suite
├── native/                             # Rust JNI bindings
│   ├── src/                           # Rust source code
│   └── Cargo.toml                     # Rust dependencies
├── docs/                              # Documentation
├── examples/                          # Usage examples
├── pom.xml                            # Maven configuration
└── README.md                          # Project overview
```

## 🧪 Testing Standards

### Test Requirements

All contributions must include appropriate tests:

- **Unit tests** for new functionality
- **Integration tests** for API changes
- **Python parity tests** for compatibility verification
- **Performance tests** for optimization changes

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test categories
mvn test -Dtest="*PythonParity*"        # Python compatibility tests
mvn test -Dtest="*Integration*"         # Integration tests
mvn test -Dtest="*Performance*"         # Performance tests

# Run with coverage
mvn jacoco:prepare-agent test jacoco:report
```

### Test Coverage Requirements

- **New features**: 90%+ test coverage required
- **Bug fixes**: Must include regression tests
- **API changes**: Must maintain existing test compatibility
- **Python parity**: Must verify equivalent Python behavior

### Test Writing Guidelines

```java
@Test
public void testNewFeature() throws Exception {
    // Arrange - Set up test data and conditions
    try (SchemaBuilder builder = new SchemaBuilder()) {
        builder.addTextField("content", true, true, "default", "position");
        
        try (Schema schema = builder.build();
             Index index = new Index(schema);
             IndexWriter writer = index.writer()) {
            
            // Act - Perform the operation being tested
            writer.addJson("{\"content\": \"test document\"}");
            writer.commit();
            
            // Assert - Verify expected outcomes
            index.reload();
            try (Searcher searcher = index.searcher();
                 Query query = Query.termQuery(schema, "content", "test");
                 SearchResult result = searcher.search(query, 10)) {
                
                assertEquals(1, result.getHits().size());
            }
        }
    }
}
```

## 📝 Code Standards

### Java Code Style

- **Java 11+ features**: Use modern Java features appropriately
- **Resource management**: Always use try-with-resources for AutoCloseable objects
- **Error handling**: Proper exception handling with meaningful messages
- **Documentation**: JavaDoc for all public APIs
- **Naming**: Clear, descriptive variable and method names

### Example Code Style

```java
/**
 * Performs a multi-field search with boosting.
 * 
 * @param searcher The searcher instance
 * @param schema The index schema
 * @param searchTerm The term to search for
 * @param titleBoost Boost factor for title matches
 * @return SearchResult containing matching documents
 * @throws Exception if search fails
 */
public SearchResult performBoostedSearch(Searcher searcher, Schema schema, 
                                       String searchTerm, double titleBoost) 
        throws Exception {
    
    try (Query titleQuery = Query.boostQuery(
             Query.termQuery(schema, "title", searchTerm), titleBoost);
         Query contentQuery = Query.termQuery(schema, "content", searchTerm);
         Query combinedQuery = Query.booleanQuery(Arrays.asList(
             new Query.OccurQuery(Occur.SHOULD, titleQuery),
             new Query.OccurQuery(Occur.SHOULD, contentQuery)
         ))) {
        
        return searcher.search(combinedQuery, 20);
    }
}
```

### Rust Code Style

- **Follow Rust conventions**: Use `cargo fmt` and `cargo clippy`
- **Memory safety**: Leverage Rust's ownership system
- **JNI bindings**: Proper error propagation to Java
- **Performance**: Zero-copy operations where possible

## 🔄 Python API Compatibility

### Compatibility Requirements

All new features and changes must maintain **100% functional compatibility** with the Python tantivy library:

1. **Behavioral compatibility**: Same results for equivalent operations
2. **API patterns**: Similar usage patterns where possible
3. **Error handling**: Consistent error conditions and messages
4. **Performance characteristics**: Comparable or better performance

### Verifying Python Compatibility

1. **Review Python implementation**:
   ```bash
   # Check Python tantivy source
   cd /path/to/tantivy-py
   grep -r "function_name" .
   ```

2. **Create equivalent tests**:
   ```java
   // Test that mimics Python behavior
   @Test
   public void testPythonEquivalentBehavior() throws Exception {
       // Implement Java version of Python test case
   }
   ```

3. **Document differences**:
   - If behavior must differ, document why
   - Provide migration notes in documentation
   - Update compatibility matrix

## 🏗️ Development Workflow

### Branch Strategy

- **`main`**: Stable, production-ready code
- **`develop`**: Integration branch for new features  
- **`feature/feature-name`**: Individual feature development
- **`fix/bug-description`**: Bug fixes
- **`docs/topic`**: Documentation updates

### Contribution Process

1. **Create Issue**: Discuss your contribution idea
2. **Fork Repository**: Create your own fork
3. **Create Branch**: Use descriptive branch names
4. **Implement Changes**: Follow code standards
5. **Add Tests**: Comprehensive test coverage
6. **Update Documentation**: Keep docs current
7. **Submit Pull Request**: Clear description and context

### Pull Request Guidelines

#### PR Description Template

```markdown
## Description
Brief description of changes and motivation.

## Type of Change
- [ ] Bug fix
- [ ] New feature  
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Python compatibility fix

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Python parity verified
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style
- [ ] Tests pass locally
- [ ] Documentation updated
- [ ] No breaking changes (or properly documented)
```

#### Review Process

1. **Automated checks**: All CI checks must pass
2. **Code review**: At least one maintainer review
3. **Testing**: Comprehensive test validation
4. **Documentation**: Docs review if applicable
5. **Python compatibility**: Parity verification

## 🔧 Development Tools

### Recommended IDE Setup

**IntelliJ IDEA** (recommended):
- Install Rust plugin for native development
- Configure Maven integration
- Set up code style formatting
- Enable automatic imports

**VS Code**:
- Java Extension Pack
- Rust analyzer extension
- Maven for Java extension

### Useful Commands

```bash
# Build and test everything
mvn clean verify

# Fast compilation check
mvn compile

# Format Rust code
cd native && cargo fmt

# Check Rust code quality  
cd native && cargo clippy

# Build native components only
cd native && cargo build --release

# Run specific test class
mvn test -Dtest=PythonParityTest

# Debug mode with extra logging
mvn test -Dtest.debug=true
```

### Debugging

```java
// Enable debug logging in tests
Logger.getLogger("com.tantivy4java").setLevel(Level.DEBUG);

// Native debugging
System.setProperty("tantivy4java.debug", "true");
```

## 📖 Documentation Standards

### JavaDoc Requirements

```java
/**
 * Creates a new fuzzy term query for handling spelling variations.
 * 
 * <p>This method provides functionality equivalent to Python tantivy's
 * fuzzy query with configurable edit distance and transposition support.</p>
 * 
 * @param schema The index schema containing field definitions
 * @param field The field name to search in
 * @param term The search term (may contain typos)
 * @param distance Maximum edit distance allowed (1-2 recommended)
 * @param transposition Whether to allow character transpositions
 * @param prefix Whether to require prefix matching
 * @return A new Query instance for fuzzy searching
 * @throws IllegalArgumentException if distance > 2 or field not found
 * @throws RuntimeException if native query construction fails
 * 
 * @see <a href="https://python-tantivy-docs.com/fuzzy-queries">Python equivalent</a>
 */
```

### Documentation Updates

When adding features:

1. **Update API reference** (`docs/reference.md`)
2. **Add examples** (`examples/` directory)  
3. **Update tutorials** if applicable (`docs/tutorials.md`)
4. **Update README** for major features
5. **Add migration notes** if behavior differs from Python

## 🚨 Issue Reporting

### Bug Report Template

```markdown
**Description**
Clear description of the bug.

**Expected Behavior**
What you expected to happen.

**Actual Behavior**  
What actually happened.

**Reproduction Steps**
1. Step 1
2. Step 2
3. Step 3

**Environment**
- Tantivy4Java version:
- Java version:
- OS:
- Maven version:

**Additional Context**
Any other relevant information.
```

### Feature Request Template

```markdown
**Feature Description**
Clear description of the proposed feature.

**Python Tantivy Equivalent**
Does this feature exist in Python tantivy? Include links/examples.

**Use Case**
Why is this feature needed?

**Proposed API**
How should this feature work?

**Additional Context**
Any other relevant information.
```

## 🏆 Recognition

Contributors will be recognized in:

- **CONTRIBUTORS.md**: List of all contributors
- **Release notes**: Major contribution acknowledgments
- **Documentation**: Example and tutorial credits
- **GitHub**: Contributor badges and statistics

## 📞 Getting Help

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and community support
- **Documentation**: Comprehensive guides and examples
- **Code Examples**: Real-world usage patterns

## 📄 License

By contributing to Tantivy4Java, you agree that your contributions will be licensed under the Apache License 2.0.

---

**Thank you for contributing to Tantivy4Java! Your efforts help bring powerful search capabilities to the Java ecosystem.** 🙏