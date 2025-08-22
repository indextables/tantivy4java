package com.tantivy4java;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Example program demonstrating QuickwitSplit functionality.
 * Creates a large Tantivy index with many documents, converts it to a Quickwit split,
 * and leaves the split files in /tmp/splitexample for inspection.
 */
public class QuickwitSplitExample {
    
    public static void main(String[] args) {
        try {
            System.out.println("🚀 QuickwitSplit Example: Converting Tantivy Index to Quickwit Split");
            System.out.println("==================================================================");
            
            // Ensure output directory exists
            Path outputDir = Paths.get("/tmp/splitexample");
            Files.createDirectories(outputDir);
            System.out.println("📁 Created output directory: " + outputDir);
            
            // Create a temporary directory for the index
            Path indexDir = Files.createTempDirectory("tantivy_example_index");
            System.out.println("📁 Created temporary index directory: " + indexDir);
            
            // Build schema with multiple field types
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position")
                       .addTextField("content", true, false, "default", "position")
                       .addTextField("category", true, true, "default", "")
                       .addIntegerField("id", true, true, true)
                       .addFloatField("rating", true, true, true)
                       .addBooleanField("is_published", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("📝 Created schema with 6 fields");
                    
                    // Create index and populate with many documents
                    try (Index index = new Index(schema, indexDir.toString(), false)) {
                        System.out.println("🗂️ Created Tantivy index at: " + indexDir);
                        
                        // Add many documents to create a substantial index
                        try (IndexWriter writer = index.writer(500, 1)) {
                            Random random = new Random(42); // Fixed seed for reproducibility
                            
                            String[] categories = {"tech", "science", "business", "health", "education", "entertainment"};
                            String[] titleWords = {"Advanced", "Introduction", "Complete", "Comprehensive", "Modern", "Essential", "Ultimate", "Practical"};
                            String[] contentWords = {"analysis", "development", "research", "implementation", "optimization", "innovation", "methodology", "framework"};
                            
                            System.out.println("📄 Adding documents to index...");
                            for (int i = 1; i <= 250; i++) {
                                // Generate varied document content
                                String category = categories[random.nextInt(categories.length)];
                                String title = titleWords[random.nextInt(titleWords.length)] + " " + 
                                             category + " " + titleWords[random.nextInt(titleWords.length)];
                                String content = generateContent(contentWords, random, 50);
                                float rating = 1.0f + random.nextFloat() * 4.0f; // 1.0 to 5.0
                                boolean isPublished = random.nextBoolean();
                                
                                // Create document using JSON for convenience
                                String jsonDoc = String.format(
                                    "{ \"id\": %d, \"title\": \"%s\", \"content\": \"%s\", " +
                                    "\"category\": \"%s\", \"rating\": %.2f, \"is_published\": %s }",
                                    i, title, content, category, rating, isPublished
                                );
                                
                                writer.addJson(jsonDoc);
                                
                                // Progress indicator
                                if (i % 50 == 0) {
                                    System.out.println("  ✅ Added " + i + " documents");
                                }
                            }
                            
                            // Commit all documents
                            writer.commit();
                            System.out.println("💾 Committed 250 documents to index");
                        }
                        
                        // Reload index to make sure all documents are available
                        index.reload();
                        
                        // Verify index content
                        try (Searcher searcher = index.searcher()) {
                            int numDocs = searcher.getNumDocs();
                            System.out.println("🔍 Index contains " + numDocs + " documents");
                            
                            // Show some search results
                            try (Query query = Query.termQuery(schema, "category", "tech");
                                 SearchResult result = searcher.search(query, 5)) {
                                
                                System.out.println("📊 Sample search results for 'tech' category:");
                                for (var hit : result.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String title = (String) doc.get("title").get(0);
                                        long id = (Long) doc.get("id").get(0);
                                        System.out.println("  - [" + id + "] " + title + " (score: " + String.format("%.3f", hit.getScore()) + ")");
                                    }
                                }
                            }
                        }
                        
                        // Now convert the index to a Quickwit split
                        System.out.println("\n🔄 Converting Tantivy index to Quickwit split...");
                        
                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "example-index-2024", 
                            "demo-source", 
                            "example-node-1"
                        );
                        
                        Path splitPath = outputDir.resolve("example_index.split");
                        
                        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndex(
                            index, splitPath.toString(), config
                        );
                        
                        System.out.println("✅ Successfully converted to Quickwit split!");
                        System.out.println("📊 Split Metadata:");
                        System.out.println("  - Split ID: " + metadata.getSplitId());
                        System.out.println("  - Document Count: " + metadata.getNumDocs());
                        System.out.println("  - Uncompressed Size: " + metadata.getUncompressedSizeBytes() + " bytes");
                        System.out.println("  - Delete Opstamp: " + metadata.getDeleteOpstamp());
                        System.out.println("  - Merge Operations: " + metadata.getNumMergeOps());
                        
                        // List files in the split
                        System.out.println("\n📂 Files contained in the split:");
                        var splitFiles = QuickwitSplit.listSplitFiles(splitPath.toString());
                        for (String file : splitFiles) {
                            System.out.println("  - " + file);
                        }
                        
                        // Validate the split
                        boolean isValid = QuickwitSplit.validateSplit(splitPath.toString());
                        System.out.println("\n✅ Split validation: " + (isValid ? "PASSED" : "FAILED"));
                        
                        // Show file information
                        System.out.println("\n📁 Output files created:");
                        System.out.println("  - Split file: " + splitPath);
                        System.out.println("  - File size: " + Files.size(splitPath) + " bytes");
                        System.out.println("  - Directory: " + outputDir);
                        
                        System.out.println("\n🎉 Example completed successfully!");
                        System.out.println("💡 You can find the split files in: " + outputDir);
                        System.out.println("💡 The split file can be used with Quickwit for distributed search");
                        
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("❌ Error running QuickwitSplit example: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Generate random content text for documents
     */
    private static String generateContent(String[] words, Random random, int wordCount) {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) content.append(" ");
            content.append(words[random.nextInt(words.length)]);
        }
        return content.toString();
    }
}
