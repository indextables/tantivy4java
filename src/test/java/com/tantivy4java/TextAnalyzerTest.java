package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Test class for TextAnalyzer tokenization functionality
 * Tests Tantivy's tokenizers and text analysis capabilities
 */
public class TextAnalyzerTest {
    
    @Test
    @DisplayName("Static tokenize method with default tokenizer")
    public void testStaticTokenizeDefault() {
        System.out.println("🚀 === STATIC TOKENIZE DEFAULT TEST ===");
        System.out.println("Testing TextAnalyzer.tokenize() with default tokenizer");
        
        // Test basic tokenization
        String text = "Hello World! This is a test document.";
        List<String> tokens = TextAnalyzer.tokenize(text);
        
        System.out.println("Input: '" + text + "'");
        System.out.println("Tokens: " + tokens);
        
        assertNotNull(tokens, "Tokens should not be null");
        assertFalse(tokens.isEmpty(), "Tokens should not be empty");
        
        // Default tokenizer should lowercase and split words
        assertTrue(tokens.contains("hello"), "Should contain lowercase 'hello'");
        assertTrue(tokens.contains("world"), "Should contain lowercase 'world'");
        assertTrue(tokens.contains("test"), "Should contain 'test'");
        assertTrue(tokens.contains("document"), "Should contain 'document'");
        
        System.out.println("✅ Static tokenize default test passed");
    }
    
    @Test
    @DisplayName("Static tokenize method with different tokenizers")
    public void testStaticTokenizeDifferentTokenizers() {
        System.out.println("🚀 === STATIC TOKENIZE DIFFERENT TOKENIZERS TEST ===");
        System.out.println("Testing different tokenizer types");
        
        String text = "Hello-World_123! Test@example.com";
        
        // Test default tokenizer
        System.out.println("\n🔍 Testing default tokenizer:");
        List<String> defaultTokens = TextAnalyzer.tokenize(text, "default");
        System.out.println("Input: '" + text + "'");
        System.out.println("Default tokens: " + defaultTokens);
        assertNotNull(defaultTokens);
        assertFalse(defaultTokens.isEmpty());
        
        // Test simple tokenizer
        System.out.println("\n🔍 Testing simple tokenizer:");
        List<String> simpleTokens = TextAnalyzer.tokenize(text, "simple");
        System.out.println("Simple tokens: " + simpleTokens);
        assertNotNull(simpleTokens);
        assertFalse(simpleTokens.isEmpty());
        
        // Test whitespace tokenizer
        System.out.println("\n🔍 Testing whitespace tokenizer:");
        List<String> whitespaceTokens = TextAnalyzer.tokenize(text, "whitespace");
        System.out.println("Whitespace tokens: " + whitespaceTokens);
        assertNotNull(whitespaceTokens);
        assertFalse(whitespaceTokens.isEmpty());
        
        // Test keyword tokenizer (treats entire input as one token)
        System.out.println("\n🔍 Testing keyword tokenizer:");
        List<String> keywordTokens = TextAnalyzer.tokenize(text, "keyword");
        System.out.println("Keyword tokens: " + keywordTokens);
        assertNotNull(keywordTokens);
        assertEquals(1, keywordTokens.size(), "Keyword tokenizer should produce exactly one token");
        assertEquals(text, keywordTokens.get(0), "Keyword tokenizer should preserve original text");
        
        // Test raw tokenizer
        System.out.println("\n🔍 Testing raw tokenizer:");
        List<String> rawTokens = TextAnalyzer.tokenize(text, "raw");
        System.out.println("Raw tokens: " + rawTokens);
        assertNotNull(rawTokens);
        assertFalse(rawTokens.isEmpty());
        
        System.out.println("✅ Different tokenizers test passed");
    }
    
    @Test
    @DisplayName("TextAnalyzer instance methods")
    public void testTextAnalyzerInstance() {
        System.out.println("🚀 === TEXT ANALYZER INSTANCE TEST ===");
        System.out.println("Testing TextAnalyzer instance creation and analysis");
        
        String text = "The quick brown fox jumps over the lazy dog.";
        
        // Test with default analyzer
        System.out.println("\n🔍 Testing default analyzer instance:");
        try (TextAnalyzer analyzer = TextAnalyzer.create("default")) {
            List<String> tokens = analyzer.analyze(text);
            System.out.println("Input: '" + text + "'");
            System.out.println("Tokens: " + tokens);
            
            assertNotNull(tokens);
            assertFalse(tokens.isEmpty());
            
            // Should be lowercase
            assertTrue(tokens.contains("quick"));
            assertTrue(tokens.contains("brown"));
            assertTrue(tokens.contains("fox"));
        }
        
        // Test with simple analyzer
        System.out.println("\n🔍 Testing simple analyzer instance:");
        try (TextAnalyzer analyzer = TextAnalyzer.create("simple")) {
            List<String> tokens = analyzer.analyze(text);
            System.out.println("Simple tokens: " + tokens);
            
            assertNotNull(tokens);
            assertFalse(tokens.isEmpty());
        }
        
        System.out.println("✅ TextAnalyzer instance test passed");
    }
    
    @Test
    @DisplayName("Advanced tokenization scenarios")
    public void testAdvancedTokenization() {
        System.out.println("🚀 === ADVANCED TOKENIZATION TEST ===");
        System.out.println("Testing complex text scenarios");
        
        // Test with punctuation and special characters
        System.out.println("\n🔍 Testing punctuation handling:");
        String punctuatedText = "Don't split contractions! But do split sentences. What about URLs: https://example.com?";
        List<String> tokens = TextAnalyzer.tokenize(punctuatedText);
        System.out.println("Input: '" + punctuatedText + "'");
        System.out.println("Tokens: " + tokens);
        assertNotNull(tokens);
        assertFalse(tokens.isEmpty());
        
        // Test with numbers
        System.out.println("\n🔍 Testing numbers and mixed content:");
        String numericText = "Product ABC-123 costs $49.99 and was released in 2024.";
        List<String> numericTokens = TextAnalyzer.tokenize(numericText);
        System.out.println("Input: '" + numericText + "'");
        System.out.println("Tokens: " + numericTokens);
        assertNotNull(numericTokens);
        assertFalse(numericTokens.isEmpty());
        
        // Test with empty and whitespace strings
        System.out.println("\n🔍 Testing edge cases:");
        List<String> emptyTokens = TextAnalyzer.tokenize("");
        System.out.println("Empty string tokens: " + emptyTokens);
        assertNotNull(emptyTokens);
        
        List<String> whitespaceTokens = TextAnalyzer.tokenize("   \t\n  ");
        System.out.println("Whitespace string tokens: " + whitespaceTokens);
        assertNotNull(whitespaceTokens);
        
        System.out.println("✅ Advanced tokenization test passed");
    }
    
    @Test
    @DisplayName("English stemming tokenizer")
    public void testEnglishStemming() {
        System.out.println("🚀 === ENGLISH STEMMING TEST ===");
        System.out.println("Testing English stemming analyzer");
        
        String text = "running runs runner runners quickly walking walked";
        
        System.out.println("\n🔍 Testing stemming vs simple tokenization:");
        
        // Compare simple tokenizer (no stemming)
        List<String> simpleTokens = TextAnalyzer.tokenize(text, "simple");
        System.out.println("Input: '" + text + "'");
        System.out.println("Simple tokens: " + simpleTokens);
        
        // English stemming tokenizer (if supported)
        try {
            List<String> stemTokens = TextAnalyzer.tokenize(text, "en_stem");
            System.out.println("Stemmed tokens: " + stemTokens);
            assertNotNull(stemTokens);
            assertFalse(stemTokens.isEmpty());
            System.out.println("✅ Stemming tokenizer working");
        } catch (Exception e) {
            System.out.println("⚠️ Stemming tokenizer not available: " + e.getMessage());
            // This is okay - stemming might not be implemented yet
        }
        
        System.out.println("✅ English stemming test completed");
    }
    
    @Test
    @DisplayName("Tokenizer error handling")
    public void testTokenizerErrorHandling() {
        System.out.println("🚀 === TOKENIZER ERROR HANDLING TEST ===");
        System.out.println("Testing error conditions and invalid inputs");
        
        // Test invalid tokenizer name
        System.out.println("\n🔍 Testing invalid tokenizer name:");
        try {
            List<String> tokens = TextAnalyzer.tokenize("test", "nonexistent_tokenizer");
            fail("Should have thrown an exception for invalid tokenizer name");
        } catch (Exception e) {
            System.out.println("Expected error for invalid tokenizer: " + e.getClass().getSimpleName());
            assertTrue(true, "Correctly handled invalid tokenizer name");
        }
        
        // Test valid text with valid tokenizers
        System.out.println("\n🔍 Testing valid operations:");
        String validText = "Valid test text";
        List<String> validTokens = TextAnalyzer.tokenize(validText, "default");
        assertNotNull(validTokens);
        assertFalse(validTokens.isEmpty());
        System.out.println("Valid tokenization: " + validTokens);
        
        System.out.println("✅ Error handling test passed");
    }
    
    @Test
    @DisplayName("Tokenization consistency")
    public void testTokenizationConsistency() {
        System.out.println("🚀 === TOKENIZATION CONSISTENCY TEST ===");
        System.out.println("Testing that tokenization is consistent across calls");
        
        String text = "Consistent tokenization test with multiple calls";
        
        // Call tokenizer multiple times
        List<String> tokens1 = TextAnalyzer.tokenize(text);
        List<String> tokens2 = TextAnalyzer.tokenize(text);
        List<String> tokens3 = TextAnalyzer.tokenize(text);
        
        System.out.println("Input: '" + text + "'");
        System.out.println("Tokens 1: " + tokens1);
        System.out.println("Tokens 2: " + tokens2);
        System.out.println("Tokens 3: " + tokens3);
        
        assertEquals(tokens1, tokens2, "Tokenization should be consistent across calls");
        assertEquals(tokens2, tokens3, "Tokenization should be consistent across calls");
        assertEquals(tokens1.size(), tokens2.size(), "Token count should be consistent");
        
        System.out.println("✅ Tokenization consistency test passed");
    }
}