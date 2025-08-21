import com.tantivy4java.*;
import java.util.*;

/**
 * AdvancedSearch - Demonstrates sophisticated search capabilities
 * 
 * This example shows:
 * - Complex multi-field queries with boosting
 * - Phrase queries with slop tolerance
 * - Fuzzy search for handling typos
 * - Range queries for numeric and date fields
 * - Advanced boolean query combinations
 * - Custom scoring and relevance tuning
 */
public class AdvancedSearch {
    
    public static void main(String[] args) {
        System.out.println("=== Tantivy4Java Advanced Search Example ===\n");
        
        try {
            runAdvancedSearchDemo();
        } catch (Exception e) {
            System.err.println("Advanced search demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runAdvancedSearchDemo() throws Exception {
        
        // Create comprehensive schema for advanced search
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position")
                   .addTextField("content", true, true, "en_stem", "position")    // Use stemming
                   .addTextField("author", true, true, "raw", "")                 // Exact matching
                   .addTextField("category", true, true, "raw", "")
                   .addIntegerField("doc_id", true, true, true)
                   .addFloatField("rating", true, true, true)                     // For range queries
                   .addIntegerField("publish_date", true, true, true)             // Unix timestamp
                   .addBooleanField("is_featured", true, true, true)
                   .addIntegerField("view_count", true, true, true);
            
            try (Schema schema = builder.build()) {
                System.out.println("✓ Advanced schema created");
                
                try (Index index = new Index(schema, "", true)) {
                    
                    // Index comprehensive test data
                    indexAdvancedDocuments(index);
                    
                    // Demonstrate various advanced search techniques
                    demonstrateAdvancedSearches(index, schema);
                }
            }
        }
    }
    
    private static void indexAdvancedDocuments(Index index) throws Exception {
        try (IndexWriter writer = index.writer(30_000_000, 1)) {
            
            long now = System.currentTimeMillis();
            long dayInMs = 24 * 60 * 60 * 1000L;
            
            String[] documents = {
                String.format("{ \"doc_id\": 1, \"title\": \"Machine Learning Fundamentals\", " +
                    "\"content\": \"Introduction to supervised and unsupervised learning algorithms.\", " +
                    "\"author\": \"Dr. Sarah Johnson\", \"category\": \"Technology\", " +
                    "\"rating\": 4.8, \"publish_date\": %d, \"is_featured\": true, \"view_count\": 15420 }", 
                    now - (7 * dayInMs)),
                
                String.format("{ \"doc_id\": 2, \"title\": \"Advanced Deep Learning Techniques\", " +
                    "\"content\": \"Neural networks, backpropagation, and gradient descent optimization.\", " +
                    "\"author\": \"Prof. Michael Chen\", \"category\": \"Technology\", " +
                    "\"rating\": 4.9, \"publish_date\": %d, \"is_featured\": true, \"view_count\": 8930 }",
                    now - (3 * dayInMs)),
                
                String.format("{ \"doc_id\": 3, \"title\": \"Data Science for Beginners\", " +
                    "\"content\": \"Statistical analysis, data visualization, and machine learning basics.\", " +
                    "\"author\": \"Dr. Sarah Johnson\", \"category\": \"Education\", " +
                    "\"rating\": 4.2, \"publish_date\": %d, \"is_featured\": false, \"view_count\": 12100 }",
                    now - (15 * dayInMs)),
                
                String.format("{ \"doc_id\": 4, \"title\": \"Natural Language Processing Applications\", " +
                    "\"content\": \"Text analysis, sentiment analysis, and language model applications.\", " +
                    "\"author\": \"Dr. Emily Rodriguez\", \"category\": \"Technology\", " +
                    "\"rating\": 4.6, \"publish_date\": %d, \"is_featured\": true, \"view_count\": 9450 }",
                    now - (10 * dayInMs)),
                
                String.format("{ \"doc_id\": 5, \"title\": \"Quantum Computing Explained\", " +
                    "\"content\": \"Quantum bits, superposition, and quantum algorithm implementations.\", " +
                    "\"author\": \"Prof. David Kim\", \"category\": \"Science\", " +
                    "\"rating\": 4.4, \"publish_date\": %d, \"is_featured\": false, \"view_count\": 6720 }",
                    now - (20 * dayInMs)),
                
                String.format("{ \"doc_id\": 6, \"title\": \"Web Development with Machine Learning\", " +
                    "\"content\": \"Integrating ML models into web applications and APIs.\", " +
                    "\"author\": \"Alex Thompson\", \"category\": \"Technology\", " +
                    "\"rating\": 3.9, \"publish_date\": %d, \"is_featured\": false, \"view_count\": 4250 }",
                    now - (5 * dayInMs))
            };
            
            for (String doc : documents) {
                writer.addJson(doc);
            }
            
            writer.commit();
            System.out.println("✓ Indexed " + documents.length + " advanced documents");
        }
        
        index.reload();
    }
    
    private static void demonstrateAdvancedSearches(Index index, Schema schema) throws Exception {
        try (Searcher searcher = index.searcher()) {
            
            // 1. Multi-field search with boosting
            System.out.println("\n=== 1. Multi-field Search with Boosting ===");
            performMultiFieldBoostedSearch(searcher, schema, "machine learning");
            
            // 2. Phrase queries with slop
            System.out.println("\n=== 2. Phrase Queries with Slop Tolerance ===");  
            performPhraseSearches(searcher, schema);
            
            // 3. Fuzzy search for typos
            System.out.println("\n=== 3. Fuzzy Search for Typos ===");
            performFuzzySearches(searcher, schema);
            
            // 4. Range queries
            System.out.println("\n=== 4. Range Queries ===");
            performRangeSearches(searcher, schema);
            
            // 5. Complex boolean combinations
            System.out.println("\n=== 5. Complex Boolean Queries ===");
            performComplexBooleanSearch(searcher, schema);
            
            // 6. Author and category filtering
            System.out.println("\n=== 6. Author and Category Filtering ===");
            performFilteredSearch(searcher, schema);
            
            // 7. Featured content with high ratings
            System.out.println("\n=== 7. Premium Content Discovery ===");
            performPremiumContentSearch(searcher, schema);
        }
    }
    
    private static void performMultiFieldBoostedSearch(Searcher searcher, Schema schema, 
                                                      String searchTerm) throws Exception {
        
        // Search across title, content, and author with different importance weights
        try (Query titleQuery = Query.boostQuery(
                 Query.termQuery(schema, "title", searchTerm), 3.0);           // Most important
             Query contentQuery = Query.boostQuery(  
                 Query.termQuery(schema, "content", searchTerm), 1.0);         // Standard weight
             Query authorQuery = Query.boostQuery(
                 Query.termQuery(schema, "author", searchTerm), 0.5);          // Least important
             
             Query multiFieldQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.SHOULD, titleQuery),
                 new Query.OccurQuery(Occur.SHOULD, contentQuery),
                 new Query.OccurQuery(Occur.SHOULD, authorQuery)
             ));
             
             SearchResult result = searcher.search(multiFieldQuery, 10)) {
            
            System.out.println("Multi-field search for '" + searchTerm + "':");
            displaySearchResults(searcher, result, true);
        }
    }
    
    private static void performPhraseSearches(Searcher searcher, Schema schema) throws Exception {
        
        // Exact phrase
        List<Object> exactPhrase = Arrays.asList("machine", "learning");
        try (Query exactPhraseQuery = Query.phraseQuery(schema, "content", exactPhrase, 0);
             SearchResult exactResult = searcher.search(exactPhraseQuery, 5)) {
            
            System.out.println("Exact phrase 'machine learning':");
            System.out.println("  Found " + exactResult.getHits().size() + " exact matches");
        }
        
        // Phrase with slop (allows words between phrase terms)
        try (Query sloppyPhraseQuery = Query.phraseQuery(schema, "content", exactPhrase, 2);
             SearchResult sloppyResult = searcher.search(sloppyPhraseQuery, 5)) {
            
            System.out.println("Phrase with slop=2 'machine [gap] learning':");
            System.out.println("  Found " + sloppyResult.getHits().size() + " matches with gaps");
        }
        
        // Complex phrase
        List<Object> complexPhrase = Arrays.asList("data", "analysis");
        try (Query complexPhraseQuery = Query.phraseQuery(schema, "content", complexPhrase, 1);
             SearchResult complexResult = searcher.search(complexPhraseQuery, 5)) {
            
            System.out.println("Complex phrase 'data analysis' (slop=1):");
            displaySearchResults(searcher, complexResult, false);
        }
    }
    
    private static void performFuzzySearches(Searcher searcher, Schema schema) throws Exception {
        
        // Handle common typos
        String[] typos = {"machien", "learing", "anaylsis"};
        
        for (String typo : typos) {
            try (Query fuzzyQuery = Query.fuzzyTermQuery(schema, "content", typo, 
                     2,     // Edit distance
                     true,  // Allow transpositions
                     false  // Don't require prefix match
                 );
                 SearchResult result = searcher.search(fuzzyQuery, 3)) {
                
                System.out.println("Fuzzy search for '" + typo + "' (edit distance=2):");
                if (!result.getHits().isEmpty()) {
                    System.out.println("  Found " + result.getHits().size() + " matches");
                    // Show first match to demonstrate correction
                    Hit firstHit = result.getHits().get(0);
                    try (Document doc = searcher.doc(firstHit.getDocAddress())) {
                        String title = (String) doc.get("title").get(0);
                        System.out.println("  Best match: " + title);
                    }
                } else {
                    System.out.println("  No matches found");
                }
            }
        }
    }
    
    private static void performRangeSearches(Searcher searcher, Schema schema) throws Exception {
        
        // High-rated articles (4.0+ rating)
        try (Query highRatedQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                 4.0, Double.MAX_VALUE, true, false);
             SearchResult highRatedResult = searcher.search(highRatedQuery, 10)) {
            
            System.out.println("High-rated articles (4.0+ stars):");
            for (Hit hit : highRatedResult.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Double rating = (Double) doc.get("rating").get(0);
                    System.out.printf("  • %s (%.1f stars)%n", title, rating);
                }
            }
        }
        
        // Recent articles (last 14 days)
        long twoWeeksAgo = System.currentTimeMillis() - (14 * 24 * 60 * 60 * 1000L);
        try (Query recentQuery = Query.rangeQuery(schema, "publish_date", FieldType.INTEGER,
                 twoWeeksAgo, System.currentTimeMillis(), true, true);
             SearchResult recentResult = searcher.search(recentQuery, 10)) {
            
            System.out.println("Recent articles (last 14 days):");
            displaySearchResults(searcher, recentResult, false);
        }
        
        // Popular articles (high view count)
        try (Query popularQuery = Query.rangeQuery(schema, "view_count", FieldType.INTEGER,
                 10000, Integer.MAX_VALUE, true, false);
             SearchResult popularResult = searcher.search(popularQuery, 10)) {
            
            System.out.println("Popular articles (10K+ views):");
            for (Hit hit : popularResult.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Long views = (Long) doc.get("view_count").get(0);
                    System.out.printf("  • %s (%,d views)%n", title, views);
                }
            }
        }
    }
    
    private static void performComplexBooleanSearch(Searcher searcher, Schema schema) 
            throws Exception {
        
        // Complex query: Technology articles about "learning" with high ratings, excluding beginners
        try (Query categoryQuery = Query.termQuery(schema, "category", "Technology");
             Query learningQuery = Query.termQuery(schema, "content", "learning");
             Query highRatingQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                 4.5, Double.MAX_VALUE, true, false);
             Query notBeginnerQuery = Query.termQuery(schema, "title", "beginners");
             
             Query complexQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, categoryQuery),        // Must be Technology
                 new Query.OccurQuery(Occur.MUST, learningQuery),        // Must mention learning
                 new Query.OccurQuery(Occur.SHOULD, highRatingQuery),    // Prefer high-rated
                 new Query.OccurQuery(Occur.MUST_NOT, notBeginnerQuery)  // Exclude beginner content
             ));
             
             SearchResult result = searcher.search(complexQuery, 10)) {
            
            System.out.println("Complex query: Technology + Learning + High-rated - Beginners:");
            displaySearchResults(searcher, result, true);
        }
    }
    
    private static void performFilteredSearch(Searcher searcher, Schema schema) throws Exception {
        
        // Search by specific author
        try (Query authorQuery = Query.termQuery(schema, "author", "Dr. Sarah Johnson");
             SearchResult authorResult = searcher.search(authorQuery, 10)) {
            
            System.out.println("Articles by Dr. Sarah Johnson:");
            displaySearchResults(searcher, authorResult, false);
        }
        
        // Search by category
        try (Query categoryQuery = Query.termQuery(schema, "category", "Technology");
             SearchResult categoryResult = searcher.search(categoryQuery, 10)) {
            
            System.out.println("Technology category articles:");
            System.out.println("  Found " + categoryResult.getHits().size() + " technology articles");
        }
    }
    
    private static void performPremiumContentSearch(Searcher searcher, Schema schema) 
            throws Exception {
        
        // Premium content: Featured + High rating + Recent
        long oneWeekAgo = System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000L);
        
        try (Query featuredQuery = Query.termQuery(schema, "is_featured", true);
             Query ratingQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                 4.5, Double.MAX_VALUE, true, false);
             Query recentQuery = Query.rangeQuery(schema, "publish_date", FieldType.INTEGER,
                 oneWeekAgo, System.currentTimeMillis(), true, true);
             
             Query premiumQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, featuredQuery),
                 new Query.OccurQuery(Occur.SHOULD, ratingQuery),    // Boost high-rated
                 new Query.OccurQuery(Occur.SHOULD, recentQuery)     // Boost recent
             ));
             
             SearchResult result = searcher.search(premiumQuery, 10)) {
            
            System.out.println("Premium content (featured + high-rated + recent):");
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.get("title").get(0);
                    Double rating = (Double) doc.get("rating").get(0);
                    Boolean featured = (Boolean) doc.get("is_featured").get(0);
                    Long views = (Long) doc.get("view_count").get(0);
                    
                    System.out.printf("  • %s%n", title);
                    System.out.printf("    Rating: %.1f, Featured: %s, Views: %,d, Score: %.3f%n",
                                    rating, featured, views, hit.getScore());
                }
            }
        }
    }
    
    private static void displaySearchResults(Searcher searcher, SearchResult result, 
                                           boolean showScores) throws Exception {
        
        if (result.getHits().isEmpty()) {
            System.out.println("  No results found");
            return;
        }
        
        System.out.println("  Found " + result.getHits().size() + " results:");
        
        for (Hit hit : result.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                String title = (String) doc.get("title").get(0);
                Long docId = (Long) doc.get("doc_id").get(0);
                
                if (showScores) {
                    System.out.printf("    [%d] %s (score: %.3f)%n", 
                                    docId, title, hit.getScore());
                } else {
                    System.out.printf("    [%d] %s%n", docId, title);
                }
            }
        }
    }
}