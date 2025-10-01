package io.indextables.tantivy4java.split.merge;

/**
 * Simple utility to inspect the created QuickwitSplit file
 */
public class QuickwitSplitInspector {
    
    public static void main(String[] args) {
        try {
            String splitPath = "/tmp/splitexample/example_index.split";
            
            System.out.println("🔍 Inspecting QuickwitSplit file: " + splitPath);
            System.out.println("==============================================");
            
            // Read split metadata
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.readSplitMetadata(splitPath);
            
            System.out.println("📊 Split Metadata:");
            System.out.println("  - Split ID: " + metadata.getSplitId());
            System.out.println("  - Document Count: " + metadata.getNumDocs());
            System.out.println("  - Uncompressed Size: " + metadata.getUncompressedSizeBytes() + " bytes");
            System.out.println("  - Time Range Start: " + metadata.getTimeRangeStart());
            System.out.println("  - Time Range End: " + metadata.getTimeRangeEnd());
            System.out.println("  - Tags: " + metadata.getTags());
            System.out.println("  - Delete Opstamp: " + metadata.getDeleteOpstamp());
            System.out.println("  - Merge Operations: " + metadata.getNumMergeOps());
            
            // List files in the split
            System.out.println("\n📂 Files in split:");
            var splitFiles = QuickwitSplit.listSplitFiles(splitPath);
            for (String file : splitFiles) {
                System.out.println("  - " + file);
            }
            
            // Validate the split
            boolean isValid = QuickwitSplit.validateSplit(splitPath);
            System.out.println("\n✅ Split validation: " + (isValid ? "PASSED" : "FAILED"));
            
            System.out.println("\n🎉 Split inspection completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Error inspecting split: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}