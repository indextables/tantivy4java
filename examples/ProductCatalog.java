import com.tantivy4java.*;
import java.util.*;

/**
 * ProductCatalog - E-commerce search implementation
 * 
 * This example demonstrates:
 * - Product catalog schema design
 * - Category and brand filtering
 * - Price range searches
 * - Rating-based sorting and filtering  
 * - Availability and inventory queries
 * - Complex product search with multiple criteria
 * 
 * Perfect for understanding e-commerce search patterns.
 */
public class ProductCatalog {
    
    public static void main(String[] args) {
        System.out.println("=== Product Catalog Search Example ===\n");
        
        try {
            runProductCatalogDemo();
        } catch (Exception e) {
            System.err.println("Product catalog demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runProductCatalogDemo() throws Exception {
        
        // Create e-commerce product schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("product_id", true, true, true)
                   .addTextField("name", true, true, "default", "position")
                   .addTextField("description", false, true, "en_stem", "position")
                   .addTextField("brand", true, true, "raw", "")                    // Exact brand matching
                   .addTextField("category", true, true, "raw", "")                 // Exact category matching
                   .addTextField("tags", true, true, "default", "")                 // Product tags
                   .addFloatField("price", true, true, true)                        // For range queries
                   .addFloatField("rating", true, true, true)                       // Customer rating
                   .addIntegerField("review_count", true, true, true)               // Number of reviews
                   .addBooleanField("in_stock", true, true, true)                   // Availability
                   .addBooleanField("on_sale", true, true, true)                    // Sale status
                   .addIntegerField("inventory_count", true, true, true)            // Stock level
                   .addTextField("color", true, true, "raw", "")                    // Product color
                   .addTextField("size", true, true, "raw", "");                    // Product size
            
            try (Schema schema = builder.build()) {
                System.out.println("✓ Product catalog schema created");
                
                try (Index index = new Index(schema, "", true)) {
                    
                    // Load sample product data
                    indexProductCatalog(index);
                    
                    // Demonstrate e-commerce search patterns
                    demonstrateEcommerceSearch(index, schema);
                }
            }
        }
    }
    
    private static void indexProductCatalog(Index index) throws Exception {
        try (IndexWriter writer = index.writer(50_000_000, 1)) {
            
            String[] products = {
                "{ \"product_id\": 1001, \"name\": \"Wireless Bluetooth Headphones\", " +
                "\"description\": \"Premium noise-canceling over-ear headphones with 30-hour battery life\", " +
                "\"brand\": \"AudioTech\", \"category\": \"Electronics\", " +
                "\"tags\": \"wireless bluetooth noise-canceling premium\", " +
                "\"price\": 199.99, \"rating\": 4.5, \"review_count\": 1247, " +
                "\"in_stock\": true, \"on_sale\": false, \"inventory_count\": 45, " +
                "\"color\": \"Black\", \"size\": \"One Size\" }",
                
                "{ \"product_id\": 1002, \"name\": \"Gaming Mechanical Keyboard\", " +
                "\"description\": \"RGB backlit mechanical keyboard with blue switches for gaming\", " +
                "\"brand\": \"GamePro\", \"category\": \"Electronics\", " +
                "\"tags\": \"gaming mechanical rgb backlit switches\", " +
                "\"price\": 129.99, \"rating\": 4.8, \"review_count\": 892, " +
                "\"in_stock\": true, \"on_sale\": true, \"inventory_count\": 23, " +
                "\"color\": \"RGB\", \"size\": \"Full Size\" }",
                
                "{ \"product_id\": 1003, \"name\": \"Cotton T-Shirt Men's\", " +
                "\"description\": \"100% organic cotton comfortable casual t-shirt for everyday wear\", " +
                "\"brand\": \"EcoWear\", \"category\": \"Clothing\", " +
                "\"tags\": \"cotton organic casual comfortable men\", " +
                "\"price\": 24.99, \"rating\": 4.2, \"review_count\": 324, " +
                "\"in_stock\": true, \"on_sale\": false, \"inventory_count\": 156, " +
                "\"color\": \"Blue\", \"size\": \"Medium\" }",
                
                "{ \"product_id\": 1004, \"name\": \"Yoga Mat Premium\", " +
                "\"description\": \"Non-slip eco-friendly yoga mat with carrying strap and alignment lines\", " +
                "\"brand\": \"ZenFit\", \"category\": \"Sports\", " +
                "\"tags\": \"yoga fitness non-slip eco-friendly alignment\", " +
                "\"price\": 79.99, \"rating\": 4.6, \"review_count\": 567, " +
                "\"in_stock\": true, \"on_sale\": true, \"inventory_count\": 34, " +
                "\"color\": \"Purple\", \"size\": \"Standard\" }",
                
                "{ \"product_id\": 1005, \"name\": \"Coffee Maker Automatic\", " +
                "\"description\": \"Programmable drip coffee maker with thermal carafe and timer\", " +
                "\"brand\": \"BrewMaster\", \"category\": \"Kitchen\", " +
                "\"tags\": \"coffee automatic programmable thermal timer\", " +
                "\"price\": 89.99, \"rating\": 4.4, \"review_count\": 203, " +
                "\"in_stock\": false, \"on_sale\": false, \"inventory_count\": 0, " +
                "\"color\": \"Stainless Steel\", \"size\": \"12 Cup\" }",
                
                "{ \"product_id\": 1006, \"name\": \"Running Shoes Women's\", " +
                "\"description\": \"Lightweight breathable running shoes with cushioned sole and arch support\", " +
                "\"brand\": \"RunFast\", \"category\": \"Sports\", " +
                "\"tags\": \"running shoes lightweight breathable cushioned women\", " +
                "\"price\": 119.99, \"rating\": 4.3, \"review_count\": 445, " +
                "\"in_stock\": true, \"on_sale\": true, \"inventory_count\": 67, " +
                "\"color\": \"Pink\", \"size\": \"Size 8\" }",
                
                "{ \"product_id\": 1007, \"name\": \"Smartphone Case Protective\", " +
                "\"description\": \"Heavy-duty protective case with screen protector and card slots\", " +
                "\"brand\": \"GuardTech\", \"category\": \"Electronics\", " +
                "\"tags\": \"smartphone case protective heavy-duty screen card-slots\", " +
                "\"price\": 39.99, \"rating\": 4.1, \"review_count\": 156, " +
                "\"in_stock\": true, \"on_sale\": false, \"inventory_count\": 89, " +
                "\"color\": \"Clear\", \"size\": \"iPhone 14\" }",
                
                "{ \"product_id\": 1008, \"name\": \"Desk Lamp LED Adjustable\", " +
                "\"description\": \"Modern LED desk lamp with touch control and USB charging port\", " +
                "\"brand\": \"LightPro\", \"category\": \"Home\", " +
                "\"tags\": \"desk lamp led adjustable touch usb charging modern\", " +
                "\"price\": 64.99, \"rating\": 4.7, \"review_count\": 298, " +
                "\"in_stock\": true, \"on_sale\": false, \"inventory_count\": 12, " +
                "\"color\": \"White\", \"size\": \"Adjustable\" }"
            };
            
            for (String product : products) {
                writer.addJson(product);
            }
            
            writer.commit();
            System.out.println("✓ Indexed " + products.length + " products");
        }
        
        index.reload();
    }
    
    private static void demonstrateEcommerceSearch(Index index, Schema schema) throws Exception {
        try (Searcher searcher = index.searcher()) {
            
            // 1. Basic product search
            System.out.println("\n=== 1. Product Name Search ===");
            searchByProductName(searcher, schema, "headphones");
            
            // 2. Category filtering
            System.out.println("\n=== 2. Category-based Search ===");
            searchByCategory(searcher, schema, "Electronics");
            
            // 3. Price range filtering
            System.out.println("\n=== 3. Price Range Search ===");
            searchByPriceRange(searcher, schema, 50.0, 150.0);
            
            // 4. High-rated products
            System.out.println("\n=== 4. High-Rated Products ===");
            searchHighRatedProducts(searcher, schema);
            
            // 5. Sale items only
            System.out.println("\n=== 5. Products on Sale ===");
            searchSaleItems(searcher, schema);
            
            // 6. In-stock availability
            System.out.println("\n=== 6. Available Products ===");
            searchAvailableProducts(searcher, schema);
            
            // 7. Brand filtering
            System.out.println("\n=== 7. Brand-Specific Search ===");
            searchByBrand(searcher, schema, "RunFast");
            
            // 8. Complex product search
            System.out.println("\n=== 8. Complex Multi-Criteria Search ===");
            performComplexProductSearch(searcher, schema);
            
            // 9. Tag-based discovery
            System.out.println("\n=== 9. Tag-Based Product Discovery ===");
            searchByTags(searcher, schema, "wireless");
            
            // 10. Low inventory alert
            System.out.println("\n=== 10. Low Inventory Products ===");
            searchLowInventory(searcher, schema);
        }
    }
    
    private static void searchByProductName(Searcher searcher, Schema schema, String productName) 
            throws Exception {
        
        try (Query nameQuery = Query.termQuery(schema, "name", productName);
             SearchResult result = searcher.search(nameQuery, 10)) {
            
            System.out.println("Products matching '" + productName + "':");
            displayProductResults(searcher, result);
        }
    }
    
    private static void searchByCategory(Searcher searcher, Schema schema, String category) 
            throws Exception {
        
        try (Query categoryQuery = Query.termQuery(schema, "category", category);
             SearchResult result = searcher.search(categoryQuery, 10)) {
            
            System.out.println("Products in category '" + category + "':");
            displayProductResults(searcher, result);
        }
    }
    
    private static void searchByPriceRange(Searcher searcher, Schema schema, 
                                          double minPrice, double maxPrice) throws Exception {
        
        try (Query priceQuery = Query.rangeQuery(schema, "price", FieldType.FLOAT,
                 minPrice, maxPrice, true, true);
             SearchResult result = searcher.search(priceQuery, 10)) {
            
            System.out.printf("Products between $%.2f - $%.2f:%n", minPrice, maxPrice);
            displayProductResults(searcher, result);
        }
    }
    
    private static void searchHighRatedProducts(Searcher searcher, Schema schema) throws Exception {
        
        // Products with 4.5+ rating and at least 200 reviews
        try (Query ratingQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                 4.5, Double.MAX_VALUE, true, false);
             Query reviewQuery = Query.rangeQuery(schema, "review_count", FieldType.INTEGER,
                 200, Integer.MAX_VALUE, true, false);
             Query highRatedQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, ratingQuery),
                 new Query.OccurQuery(Occur.MUST, reviewQuery)
             ));
             SearchResult result = searcher.search(highRatedQuery, 10)) {
            
            System.out.println("High-rated products (4.5+ stars, 200+ reviews):");
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String name = (String) doc.get("name").get(0);
                    Double rating = (Double) doc.get("rating").get(0);
                    Long reviews = (Long) doc.get("review_count").get(0);
                    Double price = (Double) doc.get("price").get(0);
                    
                    System.out.printf("  • %s - $%.2f (%.1f stars, %,d reviews)%n", 
                                    name, price, rating, reviews);
                }
            }
        }
    }
    
    private static void searchSaleItems(Searcher searcher, Schema schema) throws Exception {
        
        try (Query saleQuery = Query.termQuery(schema, "on_sale", true);
             Query stockQuery = Query.termQuery(schema, "in_stock", true);
             Query availableSaleQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, saleQuery),
                 new Query.OccurQuery(Occur.MUST, stockQuery)
             ));
             SearchResult result = searcher.search(availableSaleQuery, 10)) {
            
            System.out.println("Available sale items:");
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String name = (String) doc.get("name").get(0);
                    Double price = (Double) doc.get("price").get(0);
                    Long inventory = (Long) doc.get("inventory_count").get(0);
                    
                    System.out.printf("  🏷️  %s - $%.2f (%d in stock)%n", 
                                    name, price, inventory);
                }
            }
        }
    }
    
    private static void searchAvailableProducts(Searcher searcher, Schema schema) throws Exception {
        
        try (Query stockQuery = Query.termQuery(schema, "in_stock", true);
             Query inventoryQuery = Query.rangeQuery(schema, "inventory_count", FieldType.INTEGER,
                 1, Integer.MAX_VALUE, true, false);
             Query availableQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, stockQuery),
                 new Query.OccurQuery(Occur.MUST, inventoryQuery)
             ));
             SearchResult result = searcher.search(availableQuery, 10)) {
            
            System.out.println("Available products (" + result.getHits().size() + " in stock):");
            System.out.println("  ✓ All listed products are currently available");
        }
    }
    
    private static void searchByBrand(Searcher searcher, Schema schema, String brand) 
            throws Exception {
        
        try (Query brandQuery = Query.termQuery(schema, "brand", brand);
             SearchResult result = searcher.search(brandQuery, 10)) {
            
            System.out.println("Products by brand '" + brand + "':");
            displayProductResults(searcher, result);
        }
    }
    
    private static void performComplexProductSearch(Searcher searcher, Schema schema) 
            throws Exception {
        
        // Complex search: Electronics under $200, in stock, highly rated, preferably on sale
        try (Query categoryQuery = Query.termQuery(schema, "category", "Electronics");
             Query priceQuery = Query.rangeQuery(schema, "price", FieldType.FLOAT,
                 0.0, 200.0, true, false);
             Query stockQuery = Query.termQuery(schema, "in_stock", true);
             Query ratingQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                 4.0, Double.MAX_VALUE, true, false);
             Query saleQuery = Query.termQuery(schema, "on_sale", true);
             
             Query complexQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, categoryQuery),        // Must be Electronics
                 new Query.OccurQuery(Occur.MUST, priceQuery),           // Must be under $200
                 new Query.OccurQuery(Occur.MUST, stockQuery),           // Must be in stock
                 new Query.OccurQuery(Occur.SHOULD, ratingQuery),        // Prefer highly rated
                 new Query.OccurQuery(Occur.SHOULD, saleQuery)           // Prefer on sale
             ));
             
             SearchResult result = searcher.search(complexQuery, 10)) {
            
            System.out.println("Electronics under $200, in stock, preferably highly rated/on sale:");
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String name = (String) doc.get("name").get(0);
                    Double price = (Double) doc.get("price").get(0);
                    Double rating = (Double) doc.get("rating").get(0);
                    Boolean onSale = (Boolean) doc.get("on_sale").get(0);
                    Long inventory = (Long) doc.get("inventory_count").get(0);
                    
                    String saleText = onSale ? " 🏷️ ON SALE" : "";
                    System.out.printf("  • %s - $%.2f (%.1f stars, %d stock)%s%n", 
                                    name, price, rating, inventory, saleText);
                }
            }
        }
    }
    
    private static void searchByTags(Searcher searcher, Schema schema, String tag) throws Exception {
        
        try (Query tagQuery = Query.termQuery(schema, "tags", tag);
             SearchResult result = searcher.search(tagQuery, 10)) {
            
            System.out.println("Products tagged with '" + tag + "':");
            displayProductResults(searcher, result);
        }
    }
    
    private static void searchLowInventory(Searcher searcher, Schema schema) throws Exception {
        
        // Products with low inventory (less than 25 units)
        try (Query lowInventoryQuery = Query.rangeQuery(schema, "inventory_count", FieldType.INTEGER,
                 1, 25, true, true);
             Query inStockQuery = Query.termQuery(schema, "in_stock", true);
             Query lowStockQuery = Query.booleanQuery(Arrays.asList(
                 new Query.OccurQuery(Occur.MUST, lowInventoryQuery),
                 new Query.OccurQuery(Occur.MUST, inStockQuery)
             ));
             SearchResult result = searcher.search(lowStockQuery, 10)) {
            
            System.out.println("Low inventory alert (< 25 units):");
            for (Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String name = (String) doc.get("name").get(0);
                    Long inventory = (Long) doc.get("inventory_count").get(0);
                    String category = (String) doc.get("category").get(0);
                    
                    System.out.printf("  ⚠️  %s (%s) - Only %d left%n", 
                                    name, category, inventory);
                }
            }
        }
    }
    
    private static void displayProductResults(Searcher searcher, SearchResult result) 
            throws Exception {
        
        if (result.getHits().isEmpty()) {
            System.out.println("  No products found");
            return;
        }
        
        for (Hit hit : result.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                Long productId = (Long) doc.get("product_id").get(0);
                String name = (String) doc.get("name").get(0);
                String brand = (String) doc.get("brand").get(0);
                Double price = (Double) doc.get("price").get(0);
                Boolean inStock = (Boolean) doc.get("in_stock").get(0);
                
                String stockText = inStock ? "✓" : "✗";
                System.out.printf("  [%d] %s by %s - $%.2f %s%n", 
                                productId, name, brand, price, stockText);
            }
        }
    }
}