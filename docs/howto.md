# How-To Guides

This section provides task-focused guides for accomplishing specific goals with Tantivy4Java.

## How to Set Up Different Index Types

### In-Memory Index for Testing

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    builder.addTextField("content", true, true, "default", "position");
    
    try (Schema schema = builder.build();
         Index index = new Index(schema, "", true)) {
        
        // Index exists only in memory - perfect for testing
        try (IndexWriter writer = index.writer()) {
            writer.addJson("{ \"content\": \"Test document\" }");
            writer.commit();
        }
    }
}
```

### Persistent Index for Production

```java
String indexPath = "/var/lib/myapp/search-index";

try (SchemaBuilder builder = new SchemaBuilder()) {
    // Configure production schema
    builder.addTextField("title", true, true, "default", "position")
           .addTextField("body", false, true, "en_stem", "position")
           .addIntegerField("doc_id", true, true, true)
           .addBooleanField("is_published", true, true, true);
    
    try (Schema schema = builder.build();
         Index index = new Index(schema, indexPath, false)) {
        
        // Index persisted to disk
        populateIndex(index);
    }
}
```

### Opening Existing Index

```java
String indexPath = "/var/lib/myapp/search-index";

if (Index.exists(indexPath)) {
    try (Index index = Index.openOrCreate(indexPath);
         Schema schema = index.getSchema()) {
        
        // Work with existing index
        searchExistingIndex(index, schema);
    }
} else {
    System.err.println("Index does not exist at: " + indexPath);
}
```

## How to Handle Different Document Types

### Simple Text Documents

```java
try (IndexWriter writer = index.writer()) {
    // Blog post
    writer.addJson("{ " +
        "\"doc_id\": 1, " +
        "\"title\": \"Getting Started with Search\", " +
        "\"body\": \"Full-text search is essential...\", " +
        "\"author\": \"John Doe\", " +
        "\"published\": true " +
    "}");
    
    writer.commit();
}
```

### Documents with Multiple Values

```java
// Document with tags (array field)
writer.addJson("{ " +
    "\"doc_id\": 2, " +
    "\"title\": \"Java Programming\", " +
    "\"tags\": [\"java\", \"programming\", \"tutorial\"], " +
    "\"categories\": [\"tech\", \"education\"] " +
"}");
```

### Numeric and Date Documents

```java
long currentTime = System.currentTimeMillis();

writer.addJson("{ " +
    "\"doc_id\": 3, " +
    "\"title\": \"Product Review\", " +
    "\"rating\": 4.5, " +
    "\"price\": 29.99, " +
    "\"timestamp\": " + currentTime + ", " +
    "\"in_stock\": true " +
"}");
```

## How to Build Complex Search Queries

### Multi-Field Search

```java
// Search across multiple fields with different importance
try (Query titleQuery = Query.boostQuery(
         Query.termQuery(schema, "title", searchTerm), 2.0);
     Query bodyQuery = Query.termQuery(schema, "body", searchTerm);
     Query authorQuery = Query.termQuery(schema, "author", searchTerm);
     
     Query multiFieldQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.SHOULD, titleQuery),   // Most important
         new Query.OccurQuery(Occur.SHOULD, bodyQuery),    // Standard importance
         new Query.OccurQuery(Occur.SHOULD, authorQuery)   // Standard importance
     ))) {
    
    try (SearchResult result = searcher.search(multiFieldQuery, 20)) {
        // Process results
    }
}
```

### Filtered Search

```java
// Search with filters (must be published, recent documents)
long oneWeekAgo = System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000);

try (Query contentQuery = Query.termQuery(schema, "body", "machine learning");
     Query publishedFilter = Query.termQuery(schema, "published", true);
     Query recentFilter = Query.rangeQuery(schema, "timestamp", FieldType.INTEGER,
         oneWeekAgo, Long.MAX_VALUE, true, false);
     
     Query filteredQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, contentQuery),
         new Query.OccurQuery(Occur.MUST, publishedFilter),
         new Query.OccurQuery(Occur.MUST, recentFilter)
     ))) {
    
    try (SearchResult result = searcher.search(filteredQuery, 10)) {
        // Only published, recent documents about machine learning
    }
}
```

### Category and Tag Filtering

```java
// Search with category and tag constraints
try (Query contentQuery = Query.termQuery(schema, "title", "tutorial");
     Query categoryQuery = Query.termQuery(schema, "category", "programming");
     Query tagQuery = Query.termQuery(schema, "tags", "beginner");
     
     Query categorizedQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, contentQuery),      // Must match content
         new Query.OccurQuery(Occur.MUST, categoryQuery),     // Must be in category
         new Query.OccurQuery(Occur.SHOULD, tagQuery)         // Preferably tagged as beginner
     ))) {
    
    try (SearchResult result = searcher.search(categorizedQuery, 15)) {
        // Programming tutorials, preferably beginner-friendly
    }
}
```

## How to Handle Search Results

### Basic Result Processing

```java
try (SearchResult result = searcher.search(query, 20)) {
    System.out.println("Found " + result.getHits().size() + " results");
    
    for (Hit hit : result.getHits()) {
        System.out.println("Score: " + hit.getScore());
        
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            String title = (String) doc.get("title").get(0);
            Long docId = (Long) doc.get("doc_id").get(0);
            
            System.out.println("Doc " + docId + ": " + title);
        }
    }
}
```

### Paginated Results

```java
public class SearchPaginator {
    private final Searcher searcher;
    private final Query query;
    private final int pageSize;
    
    public SearchPaginator(Searcher searcher, Query query, int pageSize) {
        this.searcher = searcher;
        this.query = query;
        this.pageSize = pageSize;
    }
    
    public SearchPage getPage(int pageNumber) throws Exception {
        int offset = pageNumber * pageSize;
        
        try (SearchResult result = searcher.search(query, offset + pageSize)) {
            List<Hit> allHits = result.getHits();
            
            // Extract hits for this page
            List<Hit> pageHits = allHits.subList(
                Math.min(offset, allHits.size()),
                Math.min(offset + pageSize, allHits.size())
            );
            
            return new SearchPage(pageHits, pageNumber, pageSize, allHits.size());
        }
    }
}
```

### Result Ranking and Scoring

```java
// Custom scoring with boost queries
try (Query titleMatch = Query.boostQuery(
         Query.termQuery(schema, "title", keyword), 3.0);    // Title matches most important
     Query exactPhrase = Query.boostQuery(
         Query.phraseQuery(schema, "body", Arrays.asList(keywords), 0), 2.0); // Exact phrases important
     Query fuzzyMatch = Query.fuzzyTermQuery(schema, "body", keyword, 1, true, false); // Fuzzy matches least important
     
     Query scoringQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.SHOULD, titleMatch),
         new Query.OccurQuery(Occur.SHOULD, exactPhrase),
         new Query.OccurQuery(Occur.SHOULD, fuzzyMatch)
     ))) {
    
    try (SearchResult result = searcher.search(scoringQuery, 10)) {
        // Results automatically sorted by relevance score
        result.getHits().forEach(hit -> 
            System.out.println("Relevance: " + hit.getScore())
        );
    }
}
```

## How to Update and Delete Documents

### Update Documents (Delete + Add Pattern)

```java
// Update document with ID 123
Long docIdToUpdate = 123L;
String updatedContent = "{ \"doc_id\": 123, \"title\": \"Updated Title\", \"body\": \"New content...\" }";

try (IndexWriter writer = index.writer()) {
    // Delete existing document
    writer.deleteDocuments(schema, "doc_id", docIdToUpdate);
    
    // Add updated document
    writer.addJson(updatedContent);
    
    // Commit changes
    writer.commit();
}

// Reload to see changes
index.reload();
```

### Batch Updates

```java
try (IndexWriter writer = index.writer()) {
    List<Long> idsToUpdate = Arrays.asList(1L, 5L, 10L);
    
    // Delete old versions
    for (Long id : idsToUpdate) {
        writer.deleteDocuments(schema, "doc_id", id);
    }
    
    // Add updated versions
    writer.addJson("{ \"doc_id\": 1, \"title\": \"Updated Doc 1\" }");
    writer.addJson("{ \"doc_id\": 5, \"title\": \"Updated Doc 5\" }");
    writer.addJson("{ \"doc_id\": 10, \"title\": \"Updated Doc 10\" }");
    
    writer.commit();
}
```

### Conditional Updates

```java
// Only update if certain conditions are met
try (Searcher searcher = index.searcher();
     Query findQuery = Query.termQuery(schema, "doc_id", targetId);
     SearchResult result = searcher.search(findQuery, 1)) {
    
    if (!result.getHits().isEmpty()) {
        Hit hit = result.getHits().get(0);
        
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            Boolean isPublished = (Boolean) doc.get("published").get(0);
            
            if (!isPublished) {
                // Only update unpublished documents
                try (IndexWriter writer = index.writer()) {
                    writer.deleteDocuments(schema, "doc_id", targetId);
                    writer.addJson(createUpdatedDocument(targetId));
                    writer.commit();
                }
            }
        }
    }
}
```

## How to Handle Different Field Types in Queries

### Working with Numeric Fields

```java
// Price range search
try (Query priceRange = Query.rangeQuery(schema, "price", FieldType.FLOAT,
         10.0, 100.0, true, false)) {
    // Products between $10 (inclusive) and $100 (exclusive)
}

// Exact numeric match
try (Query ratingQuery = Query.termQuery(schema, "rating", 5.0)) {
    // 5-star rated items
}

// Multiple numeric conditions
try (Query minPrice = Query.rangeQuery(schema, "price", FieldType.FLOAT,
         25.0, Double.MAX_VALUE, true, false);
     Query maxRating = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
         4.0, Double.MAX_VALUE, true, false);
     Query qualityProducts = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, minPrice),
         new Query.OccurQuery(Occur.MUST, maxRating)
     ))) {
    // Premium products ($25+, 4+ stars)
}
```

### Working with Boolean Fields

```java
// Simple boolean filter
try (Query activeOnly = Query.termQuery(schema, "is_active", true)) {
    // Only active documents
}

// Boolean combinations
try (Query published = Query.termQuery(schema, "is_published", true);
     Query featured = Query.termQuery(schema, "is_featured", true);
     Query publicContent = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, published),
         new Query.OccurQuery(Occur.SHOULD, featured)  // Boost featured content
     ))) {
    // Published content, with preference for featured
}
```

### Working with Date Fields

```java
// Recent documents (last 7 days)
long sevenDaysAgo = System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000L);
try (Query recentDocs = Query.rangeQuery(schema, "timestamp", FieldType.INTEGER,
         sevenDaysAgo, System.currentTimeMillis(), true, true)) {
    // Documents from last week
}

// Date-based filtering
long startOfYear = getTimestamp(2024, 1, 1);
long endOfYear = getTimestamp(2024, 12, 31);

try (Query yearlyDocs = Query.rangeQuery(schema, "created_date", FieldType.INTEGER,
         startOfYear, endOfYear, true, true)) {
    // Documents from 2024
}
```

## How to Optimize Performance

### Indexing Performance

```java
// Configure writer for bulk operations
int heapSize = 200_000_000; // 200MB for large datasets
int numThreads = 1;         // Single thread usually optimal

try (IndexWriter writer = index.writer(heapSize, numThreads)) {
    
    // Batch process documents
    List<String> documentBatch = loadDocumentBatch();
    
    for (String doc : documentBatch) {
        writer.addJson(doc);
        
        // Commit every 1000 documents to manage memory
        if (documentBatch.indexOf(doc) % 1000 == 0) {
            writer.commit();
        }
    }
    
    // Final commit
    writer.commit();
}
```

### Search Performance

```java
// Reuse searcher for multiple queries (within same thread)
try (Searcher searcher = index.searcher()) {
    
    // Perform multiple searches with same searcher
    List<String> searchTerms = Arrays.asList("query1", "query2", "query3");
    
    for (String term : searchTerms) {
        try (Query query = Query.termQuery(schema, "content", term);
             SearchResult result = searcher.search(query, 10)) {
            
            processResults(result);
        }
    }
}
```

### Memory Management

```java
// Proper resource cleanup pattern
public class SearchService implements AutoCloseable {
    private final Index index;
    private final Schema schema;
    
    public SearchService(String indexPath) throws Exception {
        this.index = Index.openOrCreate(indexPath);
        this.schema = index.getSchema();
    }
    
    public List<SearchResult> search(String queryText) {
        try (Searcher searcher = index.searcher();
             Query query = Query.termQuery(schema, "content", queryText);
             SearchResult result = searcher.search(query, 20)) {
            
            return processResults(result);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (schema != null) schema.close();
        if (index != null) index.close();
    }
}
```

## How to Handle Errors and Edge Cases

### Safe Field Access

```java
try (Document doc = searcher.doc(hit.getDocAddress())) {
    // Safely access potentially missing fields
    List<Object> titleValues = doc.get("title");
    if (!titleValues.isEmpty()) {
        String title = (String) titleValues.get(0);
        System.out.println("Title: " + title);
    } else {
        System.out.println("No title available");
    }
    
    // Handle numeric fields with null checks
    List<Object> ratingValues = doc.get("rating");
    if (!ratingValues.isEmpty() && ratingValues.get(0) != null) {
        Double rating = (Double) ratingValues.get(0);
        System.out.println("Rating: " + rating);
    }
}
```

### Query Validation

```java
public Query buildSafeQuery(Schema schema, String field, String term) {
    if (term == null || term.trim().isEmpty()) {
        return null; // Handle empty queries appropriately
    }
    
    try {
        return Query.termQuery(schema, field, term.trim());
    } catch (Exception e) {
        System.err.println("Failed to build query: " + e.getMessage());
        return null;
    }
}
```

### Index Recovery

```java
public void recoverIndex(String indexPath, String backupPath) throws Exception {
    if (!Index.exists(indexPath) && Index.exists(backupPath)) {
        System.out.println("Recovering index from backup...");
        
        // Copy backup to main location (implementation depends on your backup strategy)
        Files.copy(Paths.get(backupPath), Paths.get(indexPath), 
                  StandardCopyOption.REPLACE_EXISTING);
        
        // Verify recovered index
        try (Index recovered = Index.openOrCreate(indexPath)) {
            System.out.println("Index recovered successfully");
        }
    }
}
```

These how-to guides cover the most common tasks you'll encounter when working with Tantivy4Java. For complete API details, see the [Reference](reference.md).