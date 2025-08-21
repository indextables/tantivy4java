import com.tantivy4java.*;
import java.util.List;

/**
 * BasicSearch - Demonstrates fundamental Tantivy4Java operations
 * 
 * This example shows:
 * - Creating a simple schema
 * - Indexing documents using JSON
 * - Performing basic term queries
 * - Retrieving and displaying results
 * 
 * Perfect starting point for learning Tantivy4Java basics.
 */
public class BasicSearch {
    
    public static void main(String[] args) {
        System.out.println("=== Tantivy4Java Basic Search Example ===\n");
        
        try {
            // Step 1: Create Schema
            System.out.println("1. Creating schema...");
            runBasicSearchExample();
            
        } catch (Exception e) {
            System.err.println("Example failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runBasicSearchExample() throws Exception {
        
        // Create schema with basic field types
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position")      // Searchable title
                   .addTextField("content", true, true, "default", "position")    // Searchable content  
                   .addIntegerField("doc_id", true, true, true)                   // Unique identifier
                   .addBooleanField("is_published", true, true, true);            // Publication status
            
            try (Schema schema = builder.build()) {
                System.out.println("   ✓ Schema created with 4 fields");
                
                // Step 2: Create in-memory index
                System.out.println("2. Creating index...");
                try (Index index = new Index(schema, "", true)) {
                    System.out.println("   ✓ In-memory index created");
                    
                    // Step 3: Index sample documents
                    System.out.println("3. Indexing documents...");
                    indexSampleDocuments(index);
                    
                    // Step 4: Perform searches
                    System.out.println("4. Performing searches...");
                    performSearches(index, schema);
                }
            }
        }
    }
    
    private static void indexSampleDocuments(Index index) throws Exception {
        try (IndexWriter writer = index.writer(15_000_000, 1)) {
            
            // Add sample documents using JSON format
            String[] sampleDocuments = {
                "{ \"doc_id\": 1, \"title\": \"Introduction to Search\", " +
                "\"content\": \"Full-text search engines help users find relevant information quickly.\", " +
                "\"is_published\": true }",
                
                "{ \"doc_id\": 2, \"title\": \"Advanced Search Techniques\", " +
                "\"content\": \"Boolean queries and phrase matching improve search precision.\", " +
                "\"is_published\": true }",
                
                "{ \"doc_id\": 3, \"title\": \"Search Engine Optimization\", " +
                "\"content\": \"Indexing strategies and query optimization for better performance.\", " +
                "\"is_published\": false }",
                
                "{ \"doc_id\": 4, \"title\": \"User Interface Design\", " +
                "\"content\": \"Creating intuitive search interfaces for end users.\", " +
                "\"is_published\": true }",
                
                "{ \"doc_id\": 5, \"title\": \"Data Analytics with Search\", " +
                "\"content\": \"Using search engines for data analysis and reporting.\", " +
                "\"is_published\": true }"
            };
            
            for (String doc : sampleDocuments) {
                writer.addJson(doc);
            }
            
            writer.commit();
            System.out.println("   ✓ Indexed " + sampleDocuments.length + " documents");
        }
        
        // Make changes visible for searching
        index.reload();
        System.out.println("   ✓ Index reloaded for searching");
    }
    
    private static void performSearches(Index index, Schema schema) throws Exception {
        try (Searcher searcher = index.searcher()) {
            
            // Search 1: Simple term query
            System.out.println("\n--- Search 1: Documents containing 'search' ---");
            performSimpleSearch(searcher, schema, "search");
            
            // Search 2: Title-specific search
            System.out.println("\n--- Search 2: 'optimization' in titles ---");
            performTitleSearch(searcher, schema, "optimization");
            
            // Search 3: Boolean field filtering
            System.out.println("\n--- Search 3: Only published documents ---");
            performPublishedSearch(searcher, schema);
            
            // Search 4: Numeric field search
            System.out.println("\n--- Search 4: Document with ID 3 ---");
            performIdSearch(searcher, schema, 3L);
            
            // Search 5: Combined boolean query
            System.out.println("\n--- Search 5: Published docs about 'search' ---");
            performCombinedSearch(searcher, schema);
        }
    }
    
    private static void performSimpleSearch(Searcher searcher, Schema schema, String term) 
            throws Exception {
        
        try (Query query = Query.termQuery(schema, "content", term);
             SearchResult result = searcher.search(query, 10)) {
            
            System.out.println("Found " + result.getHits().size() + " documents:");
            
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Long docId = (Long) doc.get("doc_id").get(0);
                    
                    System.out.printf("  [%d] %s (score: %.3f)%n", 
                                    docId, title, hit.getScore());
                }
            }
        }
    }
    
    private static void performTitleSearch(Searcher searcher, Schema schema, String term) 
            throws Exception {
        
        try (Query query = Query.termQuery(schema, "title", term);
             SearchResult result = searcher.search(query, 10)) {
            
            System.out.println("Found " + result.getHits().size() + " documents with '" + 
                             term + "' in title:");
            
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    String content = (String) doc.get("content").get(0);
                    
                    System.out.printf("  • %s%n    %s%n", title, content);
                }
            }
        }
    }
    
    private static void performPublishedSearch(Searcher searcher, Schema schema) 
            throws Exception {
        
        try (Query query = Query.termQuery(schema, "is_published", true);
             SearchResult result = searcher.search(query, 10)) {
            
            System.out.println("Found " + result.getHits().size() + " published documents:");
            
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Long docId = (Long) doc.get("doc_id").get(0);
                    Boolean published = (Boolean) doc.get("is_published").get(0);
                    
                    System.out.printf("  [%d] %s (published: %s)%n", 
                                    docId, title, published);
                }
            }
        }
    }
    
    private static void performIdSearch(Searcher searcher, Schema schema, Long targetId) 
            throws Exception {
        
        try (Query query = Query.termQuery(schema, "doc_id", targetId);
             SearchResult result = searcher.search(query, 1)) {
            
            if (!result.getHits().isEmpty()) {
                Hit hit = result.getHits().get(0);
                
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    String content = (String) doc.get("content").get(0);
                    Boolean published = (Boolean) doc.get("is_published").get(0);
                    
                    System.out.println("Document found:");
                    System.out.printf("  ID: %d%n", targetId);
                    System.out.printf("  Title: %s%n", title);
                    System.out.printf("  Content: %s%n", content);
                    System.out.printf("  Published: %s%n", published);
                }
            } else {
                System.out.println("No document found with ID " + targetId);
            }
        }
    }
    
    private static void performCombinedSearch(Searcher searcher, Schema schema) 
            throws Exception {
        
        // Combine content search with publication status filter
        try (Query contentQuery = Query.termQuery(schema, "content", "search");
             Query publishedQuery = Query.termQuery(schema, "is_published", true);
             Query combinedQuery = Query.booleanQuery(java.util.Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, contentQuery),      // Must contain "search"
                 new Query.OccurQuery(Occur.MUST, publishedQuery)     // Must be published
             ));
             SearchResult result = searcher.search(combinedQuery, 10)) {
            
            System.out.println("Found " + result.getHits().size() + 
                             " published documents about search:");
            
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Long docId = (Long) doc.get("doc_id").get(0);
                    
                    System.out.printf("  [%d] %s (score: %.3f)%n", 
                                    docId, title, hit.getScore());
                }
            }
        }
    }
}