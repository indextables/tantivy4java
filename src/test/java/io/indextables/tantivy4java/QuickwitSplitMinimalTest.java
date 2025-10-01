package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Minimal test to isolate QuickwitSplit JVM crash issue
 */
public class QuickwitSplitMinimalTest {
    
    @Test
    void testBasicClassLoading() {
        // Test 1: Can we load the class without issues?
        System.out.println("Testing QuickwitSplit class loading...");
        try {
            Class<?> clazz = Class.forName("io.indextables.tantivy4java.split.merge.QuickwitSplit");
            System.out.println("✅ QuickwitSplit class loaded successfully: " + clazz.getName());
        } catch (Exception e) {
            System.out.println("❌ Failed to load QuickwitSplit class: " + e.getMessage());
            e.printStackTrace();
            fail("Class loading failed: " + e.getMessage());
        }
    }
    
    @Test
    void testSplitConfigCreation() {
        // Test 2: Can we create SplitConfig objects?
        System.out.println("Testing SplitConfig creation...");
        try {
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");
            System.out.println("✅ SplitConfig created successfully: " + config.getIndexUid());
        } catch (Exception e) {
            System.out.println("❌ Failed to create SplitConfig: " + e.getMessage());
            e.printStackTrace();
            fail("SplitConfig creation failed: " + e.getMessage());
        }
    }
    
    @Test 
    void testValidateSplitSafeMethod() {
        // Test 3: Test the simplest native method first
        System.out.println("Testing validateSplit method...");
        try {
            // This should return false for a non-existent file, but not crash
            boolean result = QuickwitSplit.validateSplit("/nonexistent/file.split");
            System.out.println("✅ validateSplit returned: " + result);
        } catch (Exception e) {
            System.out.println("❌ validateSplit failed: " + e.getMessage());
            e.printStackTrace();
            fail("validateSplit failed: " + e.getMessage());
        }
    }
    
    @Test
    void testIndexCreationWorksWithQuickwitSplit(@TempDir Path tempDir) {
        // Test 4: Test creating a basic index (same as other tests do)
        System.out.println("Testing basic index creation with QuickwitSplit loaded...");
        try {
            Schema schema = new SchemaBuilder()
                    .addTextField("title")
                    .build();
            System.out.println("✅ Schema created successfully");
            
            Path indexPath = tempDir.resolve("test_index");
            Index index = new Index(schema, indexPath.toString());
            System.out.println("✅ Index created successfully");
            
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "Test Document");
                writer.addDocument(doc);
                writer.commit();
                System.out.println("✅ Document added and committed successfully");
            }
            
            index.reload();
            System.out.println("✅ Index reload successful");
            
        } catch (Exception e) {
            System.out.println("❌ Index creation failed: " + e.getMessage());
            e.printStackTrace();
            fail("Index creation failed: " + e.getMessage());
        }
    }
}
