# Tutorials

## Building an Index and Populating It

Let's start by creating a basic search index with Tantivy4Java, following patterns familiar to Python tantivy users.

```java
import com.tantivy4java.*;
import java.nio.file.Path;
import java.nio.file.Paths;

// Declaring our schema
try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
    schemaBuilder.addTextField("title", true, false, "default", "position");
    schemaBuilder.addTextField("body", true, false, "default", "position");
    schemaBuilder.addIntegerField("doc_id", true, true, true);
    
    try (Schema schema = schemaBuilder.build()) {
        // Creating our index (in memory)
        try (Index index = new Index(schema, "", true)) {
            // Index is ready to use
        }
    }
}
```

To have a persistent index, use the path parameter to store the index on disk:

```java
Path indexPath = Paths.get("/tmp/tantivy-index");
try (Index persistentIndex = new Index(schema, indexPath.toString(), false)) {
    // Index will be persisted to disk
}
```

### Tokenizers

By default, Tantivy offers the following tokenizers which can be used in Tantivy4Java:

- **`default`**: The tokenizer used if you don't assign a specific tokenizer to your text field. It tokenizes on punctuation and whitespaces, removes tokens longer than 40 chars, and lowercases your text.

- **`raw`**: Does not actually tokenize your text. Keeps it entirely unprocessed. Useful for indexing UUIDs or URLs.

- **`en_stem`**: In addition to what `default` does, also applies stemming to tokens. Stemming trims words to remove inflection. Slower than default but improves recall.

To use these tokenizers, provide them as a parameter to `addTextField`:

```java
try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
    schemaBuilder.addTextField("body", true, false, "en_stem", "position");
    // ... rest of schema
}
```

## Adding Documents

### Single Document

```java
try (IndexWriter writer = index.writer(50_000_000, 1)) {
    // Using JSON (equivalent to Python's Document.from_dict)
    String jsonDoc = "{ " +
        "\"doc_id\": 1, " +
        "\"title\": \"The Old Man and the Sea\", " +
        "\"body\": \"He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.\" " +
        "}";
    
    writer.addJson(jsonDoc);
    
    // Committing changes
    writer.commit();
    // Note: writer cleanup happens automatically with try-with-resources
}
```

### Batch Document Addition

```java
try (IndexWriter writer = index.writer(50_000_000, 1)) {
    // Add multiple documents
    String[] documents = {
        "{ \"doc_id\": 1, \"title\": \"The Old Man and the Sea\", \"body\": \"A story of fishing...\" }",
        "{ \"doc_id\": 2, \"title\": \"To Kill a Mockingbird\", \"body\": \"A story of justice...\" }",
        "{ \"doc_id\": 3, \"title\": \"1984\", \"body\": \"A dystopian future...\" }"
    };
    
    for (String doc : documents) {
        writer.addJson(doc);
    }
    
    writer.commit();
}
```

## Building and Executing Queries with Direct Query Construction

With Tantivy4Java, you build queries directly using the Query class methods, providing fine-grained control similar to Python's programmatic query building.

First, get a searcher for the index:

```java
// Reload the index to ensure it points to the last commit
index.reload();
try (Searcher searcher = index.searcher()) {
    // Ready to search
}
```

### Simple Term Queries

```java
try (Query query = Query.termQuery(schema, "body", "fish");
     SearchResult result = searcher.search(query, 3)) {
    
    if (!result.getHits().isEmpty()) {
        Hit bestHit = result.getHits().get(0);
        try (Document bestDoc = searcher.doc(bestHit.getDocAddress())) {
            List<Object> titleValues = bestDoc.get("title");
            System.out.println("Best match: " + titleValues.get(0));
        }
    }
}
```

### Multi-field Queries

```java
// Search across multiple fields with boolean logic
try (Query titleQuery = Query.termQuery(schema, "title", "old");
     Query bodyQuery = Query.termQuery(schema, "body", "man");
     Query booleanQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.SHOULD, titleQuery),
         new Query.OccurQuery(Occur.SHOULD, bodyQuery)
     ))) {
    
    try (SearchResult result = searcher.search(booleanQuery, 10)) {
        System.out.println("Found " + result.getHits().size() + " documents");
    }
}
```

## Building Complex Queries Programmatically

For advanced query construction, Tantivy4Java supports complex nested queries similar to ElasticSearch/Lucene patterns.

### Boolean Query with Boosts

```java
try (Query boostedTitleQuery = Query.boostQuery(
         Query.termQuery(schema, "title", "sea"), 2.0);
     Query bodyQuery = Query.termQuery(schema, "body", "fishing");
     Query complexQuery = Query.booleanQuery(Arrays.asList(
         new Query.OccurQuery(Occur.MUST, boostedTitleQuery),
         new Query.OccurQuery(Occur.SHOULD, bodyQuery)
     ))) {
    
    try (SearchResult result = searcher.search(complexQuery, 10)) {
        for (Hit hit : result.getHits()) {
            System.out.println("Score: " + hit.getScore());
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                System.out.println("Title: " + doc.get("title").get(0));
            }
        }
    }
}
```

### Phrase Queries

```java
// Search for exact phrases with slop tolerance
List<Object> phraseTerms = Arrays.asList("old", "man");
try (Query phraseQuery = Query.phraseQuery(schema, "title", phraseTerms, 0)) {
    try (SearchResult result = searcher.search(phraseQuery, 10)) {
        System.out.println("Exact phrase matches: " + result.getHits().size());
    }
}

// With slop tolerance (words can be separated)
try (Query sloppyPhraseQuery = Query.phraseQuery(schema, "body", 
         Arrays.asList("fishing", "alone"), 2)) {
    try (SearchResult result = searcher.search(sloppyPhraseQuery, 10)) {
        System.out.println("Phrase matches with slop: " + result.getHits().size());
    }
}
```

### Fuzzy Queries

```java
// Allow spelling mistakes
try (Query fuzzyQuery = Query.fuzzyTermQuery(schema, "title", "sea", 1, true, false)) {
    try (SearchResult result = searcher.search(fuzzyQuery, 10)) {
        System.out.println("Fuzzy matches: " + result.getHits().size());
    }
}
```

### Range Queries

```java
// Numeric range queries
try (Query rangeQuery = Query.rangeQuery(schema, "doc_id", FieldType.INTEGER, 
         1, 10, true, true)) {
    try (SearchResult result = searcher.search(rangeQuery, 10)) {
        System.out.println("Documents with ID 1-10: " + result.getHits().size());
    }
}
```

## Working with Search Results

### Accessing Document Fields

```java
try (SearchResult result = searcher.search(query, 10)) {
    for (Hit hit : result.getHits()) {
        System.out.println("Score: " + hit.getScore());
        
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            // Multi-value field access (Python compatible)
            List<Object> titles = doc.get("title");
            List<Object> docIds = doc.get("doc_id");
            
            if (!titles.isEmpty()) {
                System.out.println("Title: " + titles.get(0));
            }
            if (!docIds.isEmpty()) {
                System.out.println("Doc ID: " + docIds.get(0));
            }
        }
    }
}
```

### Working with Different Field Types

```java
try (Document doc = searcher.doc(hit.getDocAddress())) {
    // Text fields
    List<Object> textValues = doc.get("title");
    String title = (String) textValues.get(0);
    
    // Integer fields  
    List<Object> intValues = doc.get("doc_id");
    Long docId = (Long) intValues.get(0);
    
    // Boolean fields (if you have them)
    List<Object> boolValues = doc.get("is_published");
    Boolean isPublished = (Boolean) boolValues.get(0);
    
    // Float fields (if you have them)
    List<Object> floatValues = doc.get("rating");
    Double rating = (Double) floatValues.get(0);
}
```

## Schema Building Best Practices

### Comprehensive Schema Example

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Text fields with different tokenizers
    builder.addTextField("title", true, true, "default", "position");     // stored, indexed
    builder.addTextField("body", false, true, "en_stem", "position");     // indexed only, with stemming
    builder.addTextField("url", true, true, "raw", "");                   // stored, no tokenization
    
    // Numeric fields
    builder.addIntegerField("doc_id", true, true, true);                  // stored, indexed, fast
    builder.addFloatField("rating", true, true, true);                    // for range queries
    
    // Boolean field
    builder.addBooleanField("is_published", true, true, true);
    
    // Date field (stored as integer timestamp)
    builder.addIntegerField("timestamp", true, true, true);
    
    try (Schema schema = builder.build()) {
        // Schema ready for use
    }
}
```

### Field Configuration Guidelines

- **stored=true**: Include if you need to retrieve field values in search results
- **indexed=true**: Include if you want to search on this field
- **fast=true**: Include for numeric/boolean fields used in range queries or sorting
- **tokenizer**: Choose based on field type:
  - `"default"`: General text search
  - `"raw"`: Exact matching (IDs, URLs, tags)
  - `"en_stem"`: Better recall for natural language text

## Index Persistence and Management

### Creating Persistent Indices

```java
String indexPath = "/path/to/index/directory";

// Create new index
try (Schema schema = buildSchema();
     Index index = new Index(schema, indexPath, false)) {
    
    // Populate the index
    try (IndexWriter writer = index.writer()) {
        // Add documents...
        writer.commit();
    }
}

// Later, open existing index
try (Index existingIndex = Index.openOrCreate("/path/to/index/directory")) {
    // Index is ready to use
    try (Searcher searcher = existingIndex.searcher()) {
        // Perform searches...
    }
}
```

### Index Lifecycle Management

```java
// Check if index exists
if (Index.exists(indexPath)) {
    try (Index index = Index.openOrCreate(indexPath)) {
        // Work with existing index
        index.reload();  // Ensure latest changes are visible
    }
} else {
    // Create new index
    try (Schema schema = buildSchema();
         Index index = new Index(schema, indexPath, false)) {
        // Initialize new index
    }
}
```

This tutorial covered the fundamental operations in Tantivy4Java. For more advanced topics, see the [How-to Guides](howto.md) and [API Reference](reference.md).