import com.tantivy4java.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

public class TestTypeMappingBug {
    public static void main(String[] args) throws Exception {
        Path tempDir = Files.createTempDirectory("type_mapping_bug");
        
        System.out.println("=== Investigating Type Mapping Bug ===");
        
        // Test Case 1: Basic Type Mapping Issue
        testBasicTypeMappingIssue(tempDir);
        
        // Test Case 2: Reproduce Exact Spark Integration Issue
        testSparkSchemaIntegration(tempDir);
        
        // Test Case 3: Field Order and Type Consistency
        testFieldOrderConsistency(tempDir);
    }
    
    private static void testBasicTypeMappingIssue(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 1: Basic Type Mapping Issue");
        Path indexPath = tempDir.resolve("basic_type_mapping");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Add different field types in a specific order
            builder.addIntegerField("id", true, true, true);           // Should be Integer/Long
            builder.addTextField("name", true, true, "default", "position");  // Should be Text
            builder.addDateField("last_updated", true, true, true);    // Should be Date/Timestamp
            builder.addFloatField("salary", true, true, true);         // Should be Float
            builder.addBooleanField("is_active", true, true, true);    // Should be Boolean
            
            Schema schema = builder.build();
            
            System.out.println("📝 Original Schema (during write):");
            printSchemaInfo(schema);
            
            // Create index and add document
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            try (Document doc = new Document()) {
                doc.addInteger("id", 12345L);
                doc.addText("name", "John Doe");
                doc.addDate("last_updated", LocalDateTime.ofInstant(Instant.ofEpochSecond(1234567890L), ZoneId.systemDefault()));
                doc.addFloat("salary", 75000.0f);
                doc.addBoolean("is_active", true);
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            // Reload index and check schema consistency
            index.reload();
            try (Searcher searcher = index.searcher()) {
                Schema readSchema = index.getSchema();
                
                System.out.println("\n📖 Read Schema (after read):");
                printSchemaInfo(readSchema);
                
                // Check for type mapping consistency
                System.out.println("\n🔍 Type Mapping Analysis:");
                checkTypeConsistency("id", schema, readSchema);
                checkTypeConsistency("name", schema, readSchema);
                checkTypeConsistency("last_updated", schema, readSchema);
                checkTypeConsistency("salary", schema, readSchema);
                checkTypeConsistency("is_active", schema, readSchema);
            }
            
            index.close();
        }
    }
    
    private static void testSparkSchemaIntegration(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 2: Spark Schema Integration Issue");
        Path indexPath = tempDir.resolve("spark_schema");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create schema that matches the failing Spark test
            builder.addIntegerField("id", true, true, true);                    // LongType -> Integer
            builder.addTextField("name", true, true, "default", "position");            // StringType -> Text
            builder.addIntegerField("age", true, true, true);                   // IntegerType -> Integer
            builder.addIntegerField("experience_years", true, true, true);      // IntegerType -> Integer
            builder.addTextField("department", true, true, "default", "position");     // StringType -> Text
            builder.addTextField("location", true, true, "default", "position");       // StringType -> Text
            builder.addTextField("title", true, true, "default", "position");          // StringType -> Text
            builder.addFloatField("salary", true, true, true);                  // DoubleType -> Float
            builder.addBooleanField("is_active", true, true, true);             // BooleanType -> Boolean
            builder.addTextField("bio", true, true, "default", "position");             // StringType -> Text
            builder.addDateField("last_updated", true, true, true);             // TimestampType -> Date (THIS IS THE SUSPECT!)
            builder.addDateField("created_date", true, true, true);             // DateType -> Date
            
            Schema schema = builder.build();
            
            System.out.println("📝 Spark-like Schema (during write):");
            printDetailedSchemaInfo(schema);
            
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            try (Document doc = new Document()) {
                doc.addInteger("id", 1L);
                doc.addText("name", "Employee1");
                doc.addInteger("age", 32L);
                doc.addInteger("experience_years", 10L);
                doc.addText("department", "Engineering");
                doc.addText("location", "Seattle");
                doc.addText("title", "Staff Engineer");
                doc.addFloat("salary", 87525.0f);
                doc.addBoolean("is_active", true);
                doc.addText("bio", "Experienced engineer...");
                doc.addDate("last_updated", LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault()));
                doc.addDate("created_date", LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault()));
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            // Read back and verify schema mapping
            index.reload();
            try (Searcher searcher = index.searcher()) {
                Schema readSchema = index.getSchema();
                
                System.out.println("\n📖 Spark-like Schema (after read):");
                printDetailedSchemaInfo(readSchema);
                
                // The bug: Check if field types got mixed up (especially id vs timestamp fields)
                System.out.println("\n🚨 Potential Bug Detection:");
                compareFieldTypes("id", schema, readSchema);
                compareFieldTypes("last_updated", schema, readSchema);
                compareFieldTypes("created_date", schema, readSchema);
                
                // Test document retrieval with potential type mismatch
                testDocumentRetrieval(searcher, readSchema);
            }
            
            index.close();
        }
    }
    
    private static void testFieldOrderConsistency(Path tempDir) throws Exception {
        System.out.println("\n🔍 Test Case 3: Field Order Consistency");
        Path indexPath = tempDir.resolve("field_order");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Add fields in specific order to test ordering preservation
            String[] expectedFieldOrder = {
                "field_a_int", "field_b_text", "field_c_date", 
                "field_d_float", "field_e_bool", "field_f_text2"
            };
            
            builder.addIntegerField("field_a_int", true, true, true);
            builder.addTextField("field_b_text", true, true, "default", "position");
            builder.addDateField("field_c_date", true, true, true);
            builder.addFloatField("field_d_float", true, true, true);
            builder.addBooleanField("field_e_bool", true, true, true);
            builder.addTextField("field_f_text2", true, true, "default", "position");
            
            Schema schema = builder.build();
            
            System.out.println("📝 Expected Field Order:");
            for (int i = 0; i < expectedFieldOrder.length; i++) {
                System.out.println("  " + i + ": " + expectedFieldOrder[i]);
            }
            
            Index index = new Index(schema, indexPath.toString(), false);
            IndexWriter writer = index.writer(50, 1);
            
            try (Document doc = new Document()) {
                doc.addInteger("field_a_int", 1L);
                doc.addText("field_b_text", "text1");
                doc.addDate("field_c_date", LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault()));
                doc.addFloat("field_d_float", 1.5f);
                doc.addBoolean("field_e_bool", true);
                doc.addText("field_f_text2", "text2");
                writer.addDocument(doc);
            }
            
            writer.commit();
            writer.close();
            
            index.reload();
            try (Searcher searcher = index.searcher()) {
                Schema readSchema = index.getSchema();
                
                System.out.println("\n📖 Actual Field Order After Read:");
                List<String> actualFieldNames = readSchema.getFieldNames();
                for (int i = 0; i < actualFieldNames.size(); i++) {
                    String fieldName = actualFieldNames.get(i);
                    System.out.println("  " + i + ": " + fieldName);
                    
                    if (i < expectedFieldOrder.length && !expectedFieldOrder[i].equals(fieldName)) {
                        System.out.println("    ❌ MISMATCH! Expected: " + expectedFieldOrder[i]);
                    }
                }
            }
            
            index.close();
        }
    }
    
    private static void printSchemaInfo(Schema schema) {
        List<String> fieldNames = schema.getFieldNames();
        System.out.println("  Field count: " + fieldNames.size());
        for (String fieldName : fieldNames) {
            System.out.println("  - " + fieldName);
        }
    }
    
    private static void printDetailedSchemaInfo(Schema schema) {
        List<String> fieldNames = schema.getFieldNames();
        System.out.println("  Field count: " + fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            System.out.println("  " + i + ": " + fieldName);
        }
    }
    
    private static void checkTypeConsistency(String fieldName, Schema originalSchema, Schema readSchema) {
        try {
            boolean originalHasField = originalSchema.hasField(fieldName);
            boolean readHasField = readSchema.hasField(fieldName);
            
            System.out.println("  " + fieldName + ":");
            System.out.println("    Original has field: " + originalHasField);
            System.out.println("    Read has field: " + readHasField);
            
            if (originalHasField != readHasField) {
                System.out.println("    ❌ FIELD EXISTENCE MISMATCH!");
            }
        } catch (Exception e) {
            System.out.println("  " + fieldName + ": ❌ ERROR - " + e.getMessage());
        }
    }
    
    private static void compareFieldTypes(String fieldName, Schema originalSchema, Schema readSchema) {
        System.out.println("  " + fieldName + ":");
        System.out.println("    Original has field: " + originalSchema.hasField(fieldName));
        System.out.println("    Read has field: " + readSchema.hasField(fieldName));
        
        if (originalSchema.hasField(fieldName) && readSchema.hasField(fieldName)) {
            System.out.println("    ✅ Field exists in both schemas");
        } else {
            System.out.println("    ❌ FIELD EXISTENCE MISMATCH!");
        }
    }
    
    private static void testDocumentRetrieval(Searcher searcher, Schema schema) {
        System.out.println("\n🔍 Testing Document Retrieval with Type Mapping:");
        
        try {
            // Search for documents
            Query query = Query.allQuery();
            SearchResult result = searcher.search(query, 1);
            
            if (result.getHits().size() > 0) {
                var hit = result.getHits().get(0);
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    System.out.println("  Retrieved document:");
                    
                    // Try to retrieve each field and check for type mismatches
                    for (String fieldName : schema.getFieldNames()) {
                        try {
                            Object value = doc.getFirst(fieldName);
                            System.out.println("    " + fieldName + ": " + value + " (type: " + 
                                             (value != null ? value.getClass().getSimpleName() : "null") + ")");
                        } catch (Exception e) {
                            System.out.println("    " + fieldName + ": ❌ ERROR retrieving - " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }
            
            result.close();
            query.close();
        } catch (Exception e) {
            System.out.println("  ❌ Document retrieval failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}