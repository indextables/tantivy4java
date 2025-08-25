import com.tantivy4java.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;

public class TestSparkTypeMismatch {
    public static void main(String[] args) throws Exception {
        Path tempDir = Files.createTempDirectory("spark_type_mismatch");
        
        System.out.println("=== Testing Spark Type Mismatch Issue ===");
        System.out.println("Reproducing: java.lang.Long is not a valid external type for schema of timestamp");
        
        // Test the exact scenario: field mixup between Long and Timestamp
        testLongTimestampMixup(tempDir);
        
        // Test schema introspection methods that might return wrong types
        testSchemaIntrospectionIssue(tempDir);
        
        // Test SplitSearcher schema metadata (if this is a split-related issue)
        testSplitSearcherSchemaBug(tempDir);
    }
    
    private static void testLongTimestampMixup(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 1: Long/Timestamp Type Mixup");
        Path indexPath = tempDir.resolve("long_timestamp_mixup");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create a schema where type confusion might occur
            builder.addIntegerField("id", true, true, true);                    // Should be Long
            builder.addDateField("timestamp", true, true, true);                // Should be Timestamp
            builder.addIntegerField("count", true, true, true);                 // Should be Long
            builder.addDateField("last_seen", true, true, true);                // Should be Timestamp
            
            Schema schema = builder.build();
            
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            // Add document with mixed types
            try (Document doc = new Document()) {
                doc.addInteger("id", 123L);
                doc.addDate("timestamp", LocalDateTime.now());
                doc.addInteger("count", 456L);
                doc.addDate("last_seen", LocalDateTime.now().minusHours(1));
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            // Now test field type detection and document retrieval
            index.reload();
            try (Searcher searcher = index.searcher()) {
                // Test schema field types
                System.out.println("📋 Schema Field Analysis:");
                Schema readSchema = index.getSchema();
                for (String fieldName : readSchema.getFieldNames()) {
                    System.out.println("  " + fieldName + ": hasField=" + readSchema.hasField(fieldName));
                }
                
                // Test document retrieval - this might trigger the type mismatch
                System.out.println("\n📄 Document Field Type Analysis:");
                Query query = Query.allQuery();
                SearchResult result = searcher.search(query, 1);
                
                if (result.getHits().size() > 0) {
                    var hit = result.getHits().get(0);
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        
                        // These calls might trigger the type mismatch error
                        testFieldRetrieval(doc, "id", "Expected Long");
                        testFieldRetrieval(doc, "timestamp", "Expected LocalDateTime");
                        testFieldRetrieval(doc, "count", "Expected Long");  
                        testFieldRetrieval(doc, "last_seen", "Expected LocalDateTime");
                    }
                }
                
                result.close();
                query.close();
            }
            
            index.close();
        }
    }
    
    private static void testSchemaIntrospectionIssue(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 2: Schema Introspection Type Reporting");
        Path indexPath = tempDir.resolve("schema_introspection");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Add fields that might get their types confused in introspection
            builder.addIntegerField("employee_id", true, true, true);           // Long/Integer
            builder.addDateField("hire_date", true, true, true);                // Date/Timestamp
            builder.addFloatField("salary", true, true, true);                  // Float/Double
            builder.addTextField("name", true, true, "default", "position");    // Text/String
            builder.addBooleanField("active", true, true, true);                // Boolean
            
            Schema schema = builder.build();
            
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            try (Document doc = new Document()) {
                doc.addInteger("employee_id", 12345L);
                doc.addDate("hire_date", LocalDateTime.of(2020, 1, 15, 9, 30));
                doc.addFloat("salary", 75000.0f);
                doc.addText("name", "John Smith");
                doc.addBoolean("active", true);
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            index.reload();
            
            // Test various schema introspection methods
            System.out.println("📊 Schema Introspection Methods:");
            Schema readSchema = index.getSchema();
            
            System.out.println("  Field names: " + readSchema.getFieldNames());
            System.out.println("  Field count: " + readSchema.getFieldCount());
            
            try {
                System.out.println("  Schema summary:");
                String summary = readSchema.getSchemaSummary();
                System.out.println("    " + summary);
                
                // Look for potential type mismatches in the summary
                if (summary.contains("Long") && summary.contains("timestamp")) {
                    System.out.println("  ⚠️  POTENTIAL BUG: Schema summary contains both Long and timestamp references");
                }
                if (summary.contains("java.lang.Long is not a valid external type")) {
                    System.out.println("  🚨 BUG REPRODUCED: Found the exact error message!");
                }
                
            } catch (Exception e) {
                System.out.println("  ❌ Schema introspection error: " + e.getMessage());
                if (e.getMessage().contains("java.lang.Long is not a valid external type")) {
                    System.out.println("  🚨 BUG REPRODUCED: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            index.close();
        }
    }
    
    private static void testSplitSearcherSchemaBug(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 3: SplitSearcher Schema Metadata Bug");
        
        // This test is only relevant if SplitSearcher is involved in the bug
        // For now, we'll test if the issue occurs when using split-like operations
        Path indexPath = tempDir.resolve("split_schema_bug");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create schema similar to what might be used in Spark integration
            builder.addIntegerField("id", true, true, true);
            builder.addTextField("content", true, true, "default", "position");
            builder.addDateField("created_at", true, true, true);
            builder.addFloatField("score", true, true, true);
            
            Schema schema = builder.build();
            
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            try (Document doc = new Document()) {
                doc.addInteger("id", 1L);
                doc.addText("content", "Test document content");
                doc.addDate("created_at", LocalDateTime.now());
                doc.addFloat("score", 0.95f);
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            index.reload();
            
            try (Searcher searcher = index.searcher()) {
                // Test schema operations that might be used in Spark integration
                Schema readSchema = index.getSchema();
                
                System.out.println("📋 Split-like Schema Operations:");
                System.out.println("  Field discovery:");
                for (String fieldName : readSchema.getFieldNames()) {
                    System.out.println("    " + fieldName + " -> hasField: " + readSchema.hasField(fieldName));
                }
                
                // Try to get detailed schema information that might trigger the bug
                try {
                    System.out.println("  Stored fields: " + readSchema.getStoredFieldNames());
                    System.out.println("  Indexed fields: " + readSchema.getIndexedFieldNames());
                    
                    // This operation might trigger type confusion
                    for (String fieldName : readSchema.getFieldNames()) {
                        try {
                            // Test field type detection methods
                            System.out.println("    Field " + fieldName + " analysis:");
                            
                            // These calls might expose the type mapping bug
                            boolean isStored = readSchema.getStoredFieldNames().contains(fieldName);
                            boolean isIndexed = readSchema.getIndexedFieldNames().contains(fieldName);
                            
                            System.out.println("      Stored: " + isStored + ", Indexed: " + isIndexed);
                            
                        } catch (Exception e) {
                            System.out.println("      ❌ Error analyzing field " + fieldName + ": " + e.getMessage());
                            if (e.getMessage().contains("java.lang.Long is not a valid external type")) {
                                System.out.println("      🚨 BUG REPRODUCED: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    System.out.println("  ❌ Schema metadata error: " + e.getMessage());
                    if (e.getMessage().contains("java.lang.Long is not a valid external type")) {
                        System.out.println("  🚨 BUG REPRODUCED: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
            
            index.close();
        }
    }
    
    private static void testFieldRetrieval(Document doc, String fieldName, String expectedType) {
        try {
            Object value = doc.getFirst(fieldName);
            String actualType = value != null ? value.getClass().getSimpleName() : "null";
            System.out.println("    " + fieldName + ": " + value + " (type: " + actualType + ") - " + expectedType);
            
        } catch (Exception e) {
            System.out.println("    " + fieldName + ": ❌ ERROR - " + e.getMessage());
            if (e.getMessage().contains("java.lang.Long is not a valid external type")) {
                System.out.println("      🚨 BUG REPRODUCED: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}