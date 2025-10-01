import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class TestSplitMerge {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Quickwit Split Merge Functionality ===");
        
        // Create temporary directory for test files
        Path tempDir = Files.createTempDirectory("split_merge_test");
        System.out.println("Test directory: " + tempDir);
        
        try {
            // Test Case 1: Create multiple splits to merge
            testCreateMultipleSplits(tempDir);
            
            // Test Case 2: Merge the splits
            testMergeSplits(tempDir);
            
            // Test Case 3: Validate merge results
            testValidateMergeResults(tempDir);
            
        } finally {
            // Clean up test directory
            deleteDirectoryRecursively(tempDir);
        }
        
        System.out.println("\n=== Split Merge Test Completed ===");
    }
    
    private static void testCreateMultipleSplits(Path tempDir) throws Exception {
        System.out.println("\n🔧 Test Case 1: Creating Multiple Splits for Merging");
        
        // Create first index and split
        Path index1Path = tempDir.resolve("index1");
        createTestIndexAndSplit(index1Path, tempDir.resolve("split1.split"), "Split1 content data");
        
        // Create second index and split
        Path index2Path = tempDir.resolve("index2");
        createTestIndexAndSplit(index2Path, tempDir.resolve("split2.split"), "Split2 additional data");
        
        // Create third index and split
        Path index3Path = tempDir.resolve("index3");
        createTestIndexAndSplit(index3Path, tempDir.resolve("split3.split"), "Split3 more content");
        
        System.out.println("✅ Created 3 test splits for merging");
    }
    
    private static void createTestIndexAndSplit(Path indexPath, Path splitPath, String content) throws Exception {
        // Create a Tantivy index with test data
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position");
            builder.addTextField("content", true, true, "default", "position");
            builder.addIntegerField("id", true, true, true);
            
            Schema schema = builder.build();
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
            
            // Add test document
            try (Document doc = new Document()) {
                doc.addText("title", "Test Document");
                doc.addText("content", content);
                doc.addInteger("id", 1L);
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            // Convert index to split
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "test-index-uid", "test-source", "test-node");
            
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), splitPath.toString(), config);
            
            System.out.println("  Created split: " + splitPath.getFileName() + 
                             " (docs: " + metadata.getNumDocs() + ", size: " + metadata.getUncompressedSizeBytes() + ")");
        }
    }
    
    private static void testMergeSplits(Path tempDir) throws Exception {
        System.out.println("\n🔀 Test Case 2: Merging Multiple Splits");
        
        // Prepare split URLs to merge
        List<String> splitUrls = Arrays.asList(
            tempDir.resolve("split1.split").toString(),
            tempDir.resolve("split2.split").toString(),
            tempDir.resolve("split3.split").toString()
        );
        
        String mergedSplitPath = tempDir.resolve("merged.split").toString();
        
        // Create merge configuration
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "merged-index-uid", "merged-source", "merge-node");
        
        System.out.println("Merging splits:");
        for (String splitUrl : splitUrls) {
            System.out.println("  - " + splitUrl);
        }
        System.out.println("Output: " + mergedSplitPath);
        
        // Perform the merge
        QuickwitSplit.SplitMetadata mergedMetadata = QuickwitSplit.mergeSplits(
            splitUrls, mergedSplitPath, mergeConfig);
        
        System.out.println("✅ Merge completed successfully!");
        System.out.println("  Merged Split ID: " + mergedMetadata.getSplitId());
        System.out.println("  Total Documents: " + mergedMetadata.getNumDocs());
        System.out.println("  Total Size: " + mergedMetadata.getUncompressedSizeBytes() + " bytes");
        System.out.println("  Merge Operations: " + mergedMetadata.getNumMergeOps());
    }
    
    private static void testValidateMergeResults(Path tempDir) throws Exception {
        System.out.println("\n✅ Test Case 3: Validating Merge Results");
        
        String mergedSplitPath = tempDir.resolve("merged.split").toString();
        
        // Validate the merged split
        boolean isValid = QuickwitSplit.validateSplit(mergedSplitPath);
        System.out.println("Merged split validation: " + (isValid ? "VALID" : "INVALID"));
        
        // Check if merged split file exists and has content
        Path mergedPath = Path.of(mergedSplitPath);
        if (Files.exists(mergedPath)) {
            long fileSize = Files.size(mergedPath);
            System.out.println("Merged split file size: " + fileSize + " bytes");
        } else {
            System.out.println("❌ ERROR: Merged split file does not exist!");
            return;
        }
        
        // CRITICAL TEST: Extract the merged split and verify it contains all original data
        System.out.println("\n🔍 CRITICAL VALIDATION: Verifying merged split contains all original data");
        
        // Extract merged split to temporary directory
        Path extractedMergedPath = tempDir.resolve("extracted_merged");
        QuickwitSplit.extractSplit(mergedSplitPath, extractedMergedPath.toString());
        System.out.println("Extracted merged split to: " + extractedMergedPath);
        
        // Search the extracted merged index to verify it contains data from all 3 original splits
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position");
            builder.addTextField("content", true, true, "default", "position");
            builder.addIntegerField("id", true, true, true);
            
            Schema schema = builder.build();
            Index mergedIndex = Index.open(extractedMergedPath.toString());
            
            try (Searcher searcher = mergedIndex.searcher()) {
                // Search for content from each original split
                System.out.println("\nSearching merged index for original content:");
                
                // Test 1: Search for "Split1" content
                Query query1 = Query.termQuery(schema, "content", "Split1");
                SearchResult result1 = searcher.search(query1, 10);
                System.out.println("  Split1 content found: " + result1.getHits().size() + " documents");
                validateSearchResult(result1, "Split1", searcher);
                
                // Test 2: Search for "Split2" content  
                Query query2 = Query.termQuery(schema, "content", "Split2");
                SearchResult result2 = searcher.search(query2, 10);
                System.out.println("  Split2 content found: " + result2.getHits().size() + " documents");
                validateSearchResult(result2, "Split2", searcher);
                
                // Test 3: Search for "Split3" content
                Query query3 = Query.termQuery(schema, "content", "Split3");
                SearchResult result3 = searcher.search(query3, 10);
                System.out.println("  Split3 content found: " + result3.getHits().size() + " documents");
                validateSearchResult(result3, "Split3", searcher);
                
                // Test 4: Count all documents
                Query allQuery = Query.allQuery();
                SearchResult allResult = searcher.search(allQuery, 100);
                System.out.println("  Total documents in merged split: " + allResult.getHits().size());
                
                // Verify we have exactly 3 documents (one from each original split)
                if (allResult.getHits().size() == 3) {
                    System.out.println("✅ SUCCESS: Merged split contains exactly 3 documents as expected!");
                } else {
                    System.out.println("❌ ERROR: Expected 3 documents but found " + allResult.getHits().size());
                }
                
                // Test 5: Verify each document has unique content
                System.out.println("\nVerifying document uniqueness:");
                Set<String> contentValues = new HashSet<>();
                for (var hit : allResult.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        String content = (String) doc.getFirst("content");
                        contentValues.add(content);
                        System.out.println("  Document content: " + content);
                    }
                }
                
                if (contentValues.size() == 3) {
                    System.out.println("✅ SUCCESS: All 3 documents have unique content!");
                } else {
                    System.out.println("❌ ERROR: Expected 3 unique content values but found " + contentValues.size());
                }
                
                // Cleanup queries and results
                query1.close();
                query2.close();
                query3.close();
                allQuery.close();
                result1.close();
                result2.close();
                result3.close();
                allResult.close();
                
            }
            
            mergedIndex.close();
        }
    }
    
    private static void validateSearchResult(SearchResult result, String expectedContent, Searcher searcher) throws Exception {
        if (result.getHits().size() > 0) {
            var hit = result.getHits().get(0);
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                String actualContent = (String) doc.getFirst("content");
                if (actualContent != null && actualContent.contains(expectedContent)) {
                    System.out.println("    ✅ Found expected content: " + actualContent);
                } else {
                    System.out.println("    ❌ Content mismatch: expected '" + expectedContent + "' but found '" + actualContent + "'");
                }
            }
        } else {
            System.out.println("    ❌ No documents found for content: " + expectedContent);
        }
    }
    
    private static void testErrorCases() throws Exception {
        System.out.println("\n❌ Test Case 4: Error Handling");
        
        try {
            // Test with empty split list
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
                "test-index", "test-source", "test-node");
            QuickwitSplit.mergeSplits(Arrays.asList(), "/tmp/output.split", config);
            System.out.println("❌ Should have failed with empty split list");
        } catch (IllegalArgumentException e) {
            System.out.println("✅ Correctly caught error for empty split list: " + e.getMessage());
        }
        
        try {
            // Test with single split (should require at least 2)
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
                "test-index", "test-source", "test-node");
            QuickwitSplit.mergeSplits(Arrays.asList("/path/to/single.split"), "/tmp/output.split", config);
            System.out.println("❌ Should have failed with single split");
        } catch (IllegalArgumentException e) {
            System.out.println("✅ Correctly caught error for single split: " + e.getMessage());
        }
        
        try {
            // Test with invalid output path
            QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
                "test-index", "test-source", "test-node");
            QuickwitSplit.mergeSplits(
                Arrays.asList("/path/to/split1.split", "/path/to/split2.split"), 
                "/tmp/output.txt", config);
            System.out.println("❌ Should have failed with invalid output path");
        } catch (IllegalArgumentException e) {
            System.out.println("✅ Correctly caught error for invalid output path: " + e.getMessage());
        }
    }
    
    private static void deleteDirectoryRecursively(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (Exception e) {
                        // Ignore deletion errors in test cleanup
                    }
                });
        }
    }
}