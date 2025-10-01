package io.indextables.tantivy4java.examples;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * Example program demonstrating creation of a Quickwit-compatible index with 50 documents.
 * Creates a Tantivy index (the search engine underlying Quickwit) with structured data
 * that follows Quickwit's indexing patterns.
 */
public class QuickwitIndexExample {
    
    public static void main(String[] args) {
        try {
            System.out.println("🚀 Creating Quickwit-Compatible Index with 50 Documents");
            System.out.println("======================================================");
            
            // Create the output directory
            Path indexDir = Paths.get("/tmp/exampleindex");
            Files.createDirectories(indexDir);
            System.out.println("📁 Created index directory: " + indexDir);
            
            // Build schema suitable for log/event data (typical Quickwit use case)
            try (SchemaBuilder builder = new SchemaBuilder()) {
                // Common fields for log/event data
                builder.addTextField("message", true, false, "default", "position")     // Log message
                       .addTextField("service", true, true, "default", "")              // Service name
                       .addTextField("level", true, true, "default", "")                // Log level
                       .addTextField("host", true, true, "default", "")                 // Hostname
                       .addTextField("environment", true, true, "default", "")          // Environment
                       .addIntegerField("status_code", true, true, true)                // HTTP status code
                       .addFloatField("response_time", true, true, true)                // Response time in ms
                       .addTextField("user_id", true, true, "default", "")              // User ID
                       .addTextField("request_path", true, true, "default", "")         // API path
                       .addBooleanField("is_error", true, true, true);                  // Error flag
                
                try (Schema schema = builder.build()) {
                    System.out.println("📝 Created schema with 10 fields suitable for log data");
                    
                    // Create Tantivy index (compatible with Quickwit)
                    try (Index index = new Index(schema, indexDir.toString(), false)) {
                        System.out.println("🗂️ Created Tantivy index at: " + indexDir);
                        
                        // Add 50 documents with realistic log/event data
                        try (IndexWriter writer = index.writer(100, 1)) {
                            Random random = new Random(12345); // Fixed seed for reproducibility
                            
                            // Sample data for realistic log entries
                            String[] services = {"user-service", "payment-service", "notification-service", "auth-service", "api-gateway"};
                            String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
                            String[] hosts = {"web-01", "web-02", "api-01", "api-02", "worker-01"};
                            String[] environments = {"production", "staging", "development"};
                            String[] paths = {"/api/users", "/api/payments", "/api/auth/login", "/api/notifications", "/health"};
                            String[] userIds = {"user_001", "user_002", "user_003", "user_004", "user_005", "anonymous"};
                            
                            String[] messageTemplates = {
                                "Successfully processed request for user %s",
                                "Failed to connect to database: timeout after 30s",
                                "User authentication successful",
                                "Payment processed successfully: $%.2f",
                                "Cache miss for key: %s",
                                "Rate limit exceeded for IP: %s",
                                "Service health check passed",
                                "Background job completed in %.1fms",
                                "Invalid request parameters received",
                                "Database connection pool exhausted"
                            };
                            
                            System.out.println("📄 Adding 50 documents to index...");
                            for (int i = 1; i <= 50; i++) {
                                // Generate realistic log entry data
                                String service = services[random.nextInt(services.length)];
                                String level = levels[random.nextInt(levels.length)];
                                String host = hosts[random.nextInt(hosts.length)];
                                String environment = environments[random.nextInt(environments.length)];
                                String path = paths[random.nextInt(paths.length)];
                                String userId = userIds[random.nextInt(userIds.length)];
                                
                                // Generate status codes based on level
                                int statusCode;
                                boolean isError;
                                switch (level) {
                                    case "ERROR":
                                        statusCode = 500 + random.nextInt(99); // 5xx errors
                                        isError = true;
                                        break;
                                    case "WARN":
                                        statusCode = random.nextBoolean() ? (400 + random.nextInt(99)) : 200; // 4xx or 200
                                        isError = statusCode >= 400;
                                        break;
                                    default:
                                        statusCode = random.nextBoolean() ? 200 : (200 + random.nextInt(99)); // 2xx
                                        isError = false;
                                        break;
                                }
                                
                                // Generate response time (errors tend to be slower)
                                float responseTime = isError ? 
                                    500f + random.nextFloat() * 2000f :  // 500-2500ms for errors
                                    10f + random.nextFloat() * 200f;     // 10-210ms for success
                                
                                // Generate message
                                String messageTemplate = messageTemplates[random.nextInt(messageTemplates.length)];
                                String message;
                                if (messageTemplate.contains("%s")) {
                                    message = String.format(messageTemplate, userId);
                                } else if (messageTemplate.contains("%.2f")) {
                                    message = String.format(messageTemplate, responseTime * 10); // Convert to dollar amount
                                } else if (messageTemplate.contains("%.1f")) {
                                    message = String.format(messageTemplate, responseTime);
                                } else {
                                    message = messageTemplate; // No formatting needed
                                }
                                
                                // Create JSON document
                                String jsonDoc = String.format(
                                    "{ \"message\": \"%s\", \"service\": \"%s\", \"level\": \"%s\", " +
                                    "\"host\": \"%s\", \"environment\": \"%s\", \"status_code\": %d, " +
                                    "\"response_time\": %.2f, \"user_id\": \"%s\", \"request_path\": \"%s\", " +
                                    "\"is_error\": %s }",
                                    message.replace("\"", "\\\""), service, level, host, environment, 
                                    statusCode, responseTime, userId, path, isError
                                );
                                
                                writer.addJson(jsonDoc);
                                
                                // Progress indicator
                                if (i % 10 == 0) {
                                    System.out.println("  ✅ Added " + i + " documents");
                                }
                            }
                            
                            // Commit all documents
                            writer.commit();
                            System.out.println("💾 Committed 50 documents to index");
                        }
                        
                        // Reload index to make all documents searchable
                        index.reload();
                        
                        // Verify index content and demonstrate search capabilities
                        try (Searcher searcher = index.searcher()) {
                            int numDocs = searcher.getNumDocs();
                            System.out.println("🔍 Index contains " + numDocs + " documents");
                            
                            // Example searches demonstrating Quickwit-like queries
                            System.out.println("\n📊 Sample search results:");
                            
                            // 1. Search for errors
                            try (Query errorQuery = Query.termQuery(schema, "is_error", true);
                                 SearchResult errorResult = searcher.search(errorQuery, 5)) {
                                
                                System.out.println("\n🔴 Error entries found: " + errorResult.getHits().size());
                                for (var hit : errorResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String message = (String) doc.get("message").get(0);
                                        String service = (String) doc.get("service").get(0);
                                        long statusCode = (Long) doc.get("status_code").get(0);
                                        System.out.println("  - [" + service + "] " + statusCode + ": " + 
                                                         (message.length() > 60 ? message.substring(0, 57) + "..." : message));
                                    }
                                }
                            }
                            
                            // 2. Search by service
                            try (Query serviceQuery = Query.termQuery(schema, "service", "user-service");
                                 SearchResult serviceResult = searcher.search(serviceQuery, 3)) {
                                
                                System.out.println("\n👤 User service entries found: " + serviceResult.getHits().size());
                                for (var hit : serviceResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String level = (String) doc.get("level").get(0);
                                        String message = (String) doc.get("message").get(0);
                                        System.out.println("  - [" + level + "] " + 
                                                         (message.length() > 50 ? message.substring(0, 47) + "..." : message));
                                    }
                                }
                            }
                            
                            // 3. Range query for slow requests
                            try (Query slowQuery = Query.rangeQuery(schema, "response_time", FieldType.FLOAT, 
                                                                   1000.0, Double.MAX_VALUE, true, false);
                                 SearchResult slowResult = searcher.search(slowQuery, 3)) {
                                
                                System.out.println("\n🐌 Slow requests (>1000ms) found: " + slowResult.getHits().size());
                                for (var hit : slowResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String service = (String) doc.get("service").get(0);
                                        double responseTime = (Double) doc.get("response_time").get(0);
                                        String path = (String) doc.get("request_path").get(0);
                                        System.out.println("  - " + service + " " + path + ": " + 
                                                         String.format("%.1fms", responseTime));
                                    }
                                }
                            }
                            
                            // Show index statistics
                            System.out.println("\n📈 Index Statistics:");
                            System.out.println("  - Total documents: " + numDocs);
                            System.out.println("  - Index directory: " + indexDir);
                            System.out.println("  - Index files created: " + Files.list(indexDir).count() + " files");
                            
                            // List index files
                            System.out.println("\n📂 Index files created:");
                            Files.list(indexDir).forEach(file -> 
                                System.out.println("  - " + file.getFileName() + 
                                                 " (" + getFileSize(file) + " bytes)"));
                        }
                        
                        // Convert the index to a QuickwitSplit
                        System.out.println("\n🔄 Converting Tantivy index to Quickwit split...");
                        
                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "example-logs-2024", 
                            "log-ingestion", 
                            "indexer-node-1"
                        );
                        
                        Path splitPath = indexDir.resolve("logs.split");
                        
                        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndex(
                            index, splitPath.toString(), config
                        );
                        
                        System.out.println("✅ Successfully converted to Quickwit split!");
                        System.out.println("📊 Split Metadata:");
                        System.out.println("  - Split ID: " + splitMetadata.getSplitId());
                        System.out.println("  - Document Count: " + splitMetadata.getNumDocs());
                        System.out.println("  - Uncompressed Size: " + splitMetadata.getUncompressedSizeBytes() + " bytes");
                        System.out.println("  - Delete Opstamp: " + splitMetadata.getDeleteOpstamp());
                        System.out.println("  - Merge Operations: " + splitMetadata.getNumMergeOps());
                        
                        // List files in the split
                        System.out.println("\n📂 Files contained in the split:");
                        var splitFiles = QuickwitSplit.listSplitFiles(splitPath.toString());
                        for (String file : splitFiles) {
                            System.out.println("  - " + file);
                        }
                        
                        // Validate the split
                        boolean isValid = QuickwitSplit.validateSplit(splitPath.toString());
                        System.out.println("\n✅ Split validation: " + (isValid ? "PASSED" : "FAILED"));
                        
                        // Show final file structure
                        System.out.println("\n📁 Final directory structure:");
                        Files.list(indexDir).forEach(file -> {
                            String fileName = file.getFileName().toString();
                            long fileSize = getFileSize(file);
                            String fileType = fileName.endsWith(".split") ? " (Quickwit Split)" : 
                                            fileName.startsWith(".") ? " (Lock/Meta)" :
                                            " (Tantivy Index)";
                            System.out.println("  - " + fileName + " (" + fileSize + " bytes)" + fileType);
                        });

                        System.out.println("\n🎉 Quickwit split creation completed successfully!");
                        System.out.println("💡 Index location: " + indexDir);
                        System.out.println("💡 Split file: " + splitPath);
                        System.out.println("💡 The split file contains all index data and can be used with Quickwit");
                        System.out.println("💡 Split file includes hotcache-compatible format ready for distributed search");
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("❌ Error creating Quickwit index: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Helper method to get file size safely
     */
    private static long getFileSize(Path file) {
        try {
            return Files.size(file);
        } catch (Exception e) {
            return 0;
        }
    }
}