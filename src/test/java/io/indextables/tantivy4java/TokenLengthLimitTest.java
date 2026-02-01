/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.util.TextAnalyzer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for configurable token length limits.
 *
 * <p>Validates that:
 * <ul>
 *   <li>Default 255-byte limit is applied (Quickwit-compatible)</li>
 *   <li>Custom limits work correctly</li>
 *   <li>Validation rejects invalid limits</li>
 *   <li>Tokens longer than the limit are filtered (not truncated)</li>
 *   <li>All tokenizers apply the limit correctly</li>
 *   <li>Legacy 40-byte behavior is reproducible</li>
 * </ul>
 */
public class TokenLengthLimitTest {

    @TempDir
    Path tempDir;

    @Test
    public void testTokenLengthConstants() {
        // Verify constant values
        assertEquals(65530, TokenLength.TANTIVY_MAX, "TANTIVY_MAX should be 65530");
        assertEquals(255, TokenLength.DEFAULT, "DEFAULT should be 255");
        assertEquals(40, TokenLength.LEGACY, "LEGACY should be 40");
        assertEquals(1, TokenLength.MIN, "MIN should be 1");
    }

    @Test
    public void testTokenLengthValidation() {
        // Valid values should not throw
        assertDoesNotThrow(() -> TokenLength.validate(1));
        assertDoesNotThrow(() -> TokenLength.validate(40));
        assertDoesNotThrow(() -> TokenLength.validate(255));
        assertDoesNotThrow(() -> TokenLength.validate(65530));

        // Invalid values should throw
        assertThrows(IllegalArgumentException.class, () -> TokenLength.validate(0));
        assertThrows(IllegalArgumentException.class, () -> TokenLength.validate(-1));
        assertThrows(IllegalArgumentException.class, () -> TokenLength.validate(65531));
        assertThrows(IllegalArgumentException.class, () -> TokenLength.validate(Integer.MAX_VALUE));
    }

    @Test
    public void testTextFieldIndexingWithMaxTokenLength() {
        TextFieldIndexing indexing = TextFieldIndexing.create()
            .withTokenizer("default")
            .withMaxTokenLength(100)
            .withPositions();

        assertEquals(100, indexing.getMaxTokenLength());
        assertEquals("default", indexing.getTokenizerName());
    }

    @Test
    public void testTextFieldIndexingDefaultMaxTokenLength() {
        TextFieldIndexing indexing = TextFieldIndexing.create();

        assertEquals(TokenLength.DEFAULT, indexing.getMaxTokenLength(),
            "Default max token length should be 255");
    }

    @Test
    public void testTextFieldIndexingValidation() {
        assertThrows(IllegalArgumentException.class, () ->
            TextFieldIndexing.create().withMaxTokenLength(0));

        assertThrows(IllegalArgumentException.class, () ->
            TextFieldIndexing.create().withMaxTokenLength(-1));

        assertThrows(IllegalArgumentException.class, () ->
            TextFieldIndexing.create().withMaxTokenLength(65531));
    }

    @Test
    public void testTokenizeWithDefaultLimit() {
        // Create a token that's 100 bytes (should be kept with 255 limit)
        String mediumToken = "a".repeat(100);
        String text = "hello " + mediumToken + " world";

        List<String> tokens = TextAnalyzer.tokenize(text, "default");

        assertTrue(tokens.contains("hello"), "Should contain 'hello'");
        assertTrue(tokens.contains(mediumToken.toLowerCase()), "Should contain medium token");
        assertTrue(tokens.contains("world"), "Should contain 'world'");
    }

    @Test
    public void testTokenizeWithLegacyLimit() {
        // Create a token that's 50 bytes (should be filtered with 40-byte limit)
        String longToken = "a".repeat(50);
        String text = "hello " + longToken + " world";

        List<String> tokensLegacy = TextAnalyzer.tokenize(text, "default", TokenLength.LEGACY);
        List<String> tokensDefault = TextAnalyzer.tokenize(text, "default", TokenLength.DEFAULT);

        // With legacy limit (40), the 50-byte token should be filtered
        assertTrue(tokensLegacy.contains("hello"), "Legacy: should contain 'hello'");
        assertFalse(tokensLegacy.contains(longToken.toLowerCase()),
            "Legacy: should NOT contain 50-byte token (filtered by 40-byte limit)");
        assertTrue(tokensLegacy.contains("world"), "Legacy: should contain 'world'");

        // With default limit (255), the 50-byte token should be kept
        assertTrue(tokensDefault.contains("hello"), "Default: should contain 'hello'");
        assertTrue(tokensDefault.contains(longToken.toLowerCase()),
            "Default: should contain 50-byte token");
        assertTrue(tokensDefault.contains("world"), "Default: should contain 'world'");
    }

    @Test
    public void testTokenizeWithCustomLimit() {
        String token30 = "a".repeat(30);
        String token50 = "b".repeat(50);
        String token100 = "c".repeat(100);
        String text = token30 + " " + token50 + " " + token100;

        // With 40-byte limit: only token30 should pass
        List<String> tokens40 = TextAnalyzer.tokenize(text, "default", 40);
        assertTrue(tokens40.contains(token30), "40-limit: should contain 30-byte token");
        assertFalse(tokens40.contains(token50), "40-limit: should NOT contain 50-byte token");
        assertFalse(tokens40.contains(token100), "40-limit: should NOT contain 100-byte token");

        // With 60-byte limit: token30 and token50 should pass
        List<String> tokens60 = TextAnalyzer.tokenize(text, "default", 60);
        assertTrue(tokens60.contains(token30), "60-limit: should contain 30-byte token");
        assertTrue(tokens60.contains(token50), "60-limit: should contain 50-byte token");
        assertFalse(tokens60.contains(token100), "60-limit: should NOT contain 100-byte token");

        // With 255-byte limit: all tokens should pass
        List<String> tokens255 = TextAnalyzer.tokenize(text, "default", 255);
        assertTrue(tokens255.contains(token30), "255-limit: should contain 30-byte token");
        assertTrue(tokens255.contains(token50), "255-limit: should contain 50-byte token");
        assertTrue(tokens255.contains(token100), "255-limit: should contain 100-byte token");
    }

    @Test
    public void testAllTokenizersApplyLimit() {
        String longToken = "a".repeat(50);
        String text = "hello " + longToken;

        // Test with legacy limit (40) - long token should be filtered
        List<String> defaultTokens = TextAnalyzer.tokenize(text, "default", 40);
        List<String> simpleTokens = TextAnalyzer.tokenize(text, "simple", 40);
        List<String> whitespaceTokens = TextAnalyzer.tokenize(text, "whitespace", 40);

        // All tokenizers should filter the 50-byte token with 40-byte limit
        assertFalse(defaultTokens.contains(longToken.toLowerCase()),
            "default tokenizer should filter 50-byte token with 40-byte limit");
        assertFalse(simpleTokens.contains(longToken.toLowerCase()),
            "simple tokenizer should filter 50-byte token with 40-byte limit");
        assertFalse(whitespaceTokens.contains(longToken.toLowerCase()),
            "whitespace tokenizer should filter 50-byte token with 40-byte limit");

        // keyword and raw tokenizers don't apply limits (single token semantics)
        List<String> keywordTokens = TextAnalyzer.tokenize(text, "keyword", 40);
        List<String> rawTokens = TextAnalyzer.tokenize(text, "raw", 40);

        // These tokenizers treat the whole input as one token
        assertEquals(1, keywordTokens.size(), "keyword tokenizer produces single token");
        assertEquals(1, rawTokens.size(), "raw tokenizer produces single token");
    }

    @Test
    public void testSchemaBuilderWithMaxTokenLength() throws Exception {
        String indexPath = tempDir.resolve("token_length_test").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Add fields with different token length limits
            builder.addTextField("content_default", true, false, "default", "position");
            builder.addTextField("content_legacy", true, false, "default", "position", TokenLength.LEGACY);
            builder.addTextField("content_custom", true, false, "default", "position", 100);

            try (Schema schema = builder.build()) {
                assertNotNull(schema, "Schema should be created");

                // Define tokens with varying lengths for testing
                String shortToken = "hello";
                String mediumToken = "a".repeat(50);  // 50 bytes
                String longToken = "b".repeat(150);   // 150 bytes

                // Create index and verify it works
                try (Index index = new Index(schema, indexPath, false)) {
                    assertNotNull(index, "Index should be created");

                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("content_default", shortToken + " " + mediumToken + " " + longToken);
                            doc.addText("content_legacy", shortToken + " " + mediumToken + " " + longToken);
                            doc.addText("content_custom", shortToken + " " + mediumToken + " " + longToken);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                    }

                    index.reload();

                    // Verify searches work
                    try (Searcher searcher = index.searcher()) {
                        assertNotNull(searcher, "Searcher should be created");

                        // Short token should be found in all fields
                        try (Query query = Query.termQuery(schema, "content_default", "hello")) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size(),
                                "Short token should be found in content_default");
                        }

                        try (Query query = Query.termQuery(schema, "content_legacy", "hello")) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size(),
                                "Short token should be found in content_legacy");
                        }

                        // Medium token (50 bytes) should be found in default (255) and custom (100)
                        // but not in legacy (40)
                        try (Query query = Query.termQuery(schema, "content_default", mediumToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size(),
                                "Medium token should be found in content_default (255-byte limit)");
                        }

                        try (Query query = Query.termQuery(schema, "content_custom", mediumToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size(),
                                "Medium token should be found in content_custom (100-byte limit)");
                        }

                        try (Query query = Query.termQuery(schema, "content_legacy", mediumToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(0, result.getHits().size(),
                                "Medium token should NOT be found in content_legacy (40-byte limit filters it)");
                        }

                        // Long token (150 bytes) should be found in default (255)
                        // but not in legacy (40) or custom (100)
                        try (Query query = Query.termQuery(schema, "content_default", longToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(1, result.getHits().size(),
                                "Long token should be found in content_default (255-byte limit)");
                        }

                        try (Query query = Query.termQuery(schema, "content_legacy", longToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(0, result.getHits().size(),
                                "Long token should NOT be found in content_legacy (40-byte limit)");
                        }

                        try (Query query = Query.termQuery(schema, "content_custom", longToken.toLowerCase())) {
                            SearchResult result = searcher.search(query, 10);
                            assertEquals(0, result.getHits().size(),
                                "Long token should NOT be found in content_custom (100-byte limit)");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testTextFieldIndexingWithSchemaBuilder() throws Exception {
        String indexPath = tempDir.resolve("text_indexing_test").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            TextFieldIndexing customIndexing = TextFieldIndexing.create()
                .withTokenizer("default")
                .withMaxTokenLength(60)
                .withPositions();

            builder.addTextField("content", true, false, customIndexing);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath, false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    String shortToken = "hello";
                    String mediumToken = "a".repeat(50);
                    String longToken = "b".repeat(100);

                    try (Document doc = new Document()) {
                        doc.addText("content", shortToken + " " + mediumToken + " " + longToken);
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }

                index.reload();

                try (Searcher searcher = index.searcher()) {
                    // 50-byte token should be found (under 60-byte limit)
                    try (Query query = Query.termQuery(schema, "content", "a".repeat(50))) {
                        SearchResult result = searcher.search(query, 10);
                        assertEquals(1, result.getHits().size(),
                            "50-byte token should be found with 60-byte limit");
                    }

                    // 100-byte token should NOT be found (exceeds 60-byte limit)
                    try (Query query = Query.termQuery(schema, "content", "b".repeat(100))) {
                        SearchResult result = searcher.search(query, 10);
                        assertEquals(0, result.getHits().size(),
                            "100-byte token should NOT be found with 60-byte limit");
                    }
                }
            }
        }
    }

    @Test
    public void testBreakingChangeFromLegacyBehavior() {
        // This test documents the breaking change from 40 to 255 bytes
        String token45 = "a".repeat(45);
        String text = "test " + token45;

        // With the OLD behavior (40-byte limit), this token would be filtered
        List<String> legacyTokens = TextAnalyzer.tokenize(text, "default", TokenLength.LEGACY);
        assertFalse(legacyTokens.contains(token45),
            "With legacy 40-byte limit, 45-byte token is filtered");

        // With the NEW default behavior (255-byte limit), this token is kept
        List<String> defaultTokens = TextAnalyzer.tokenize(text, "default");
        assertTrue(defaultTokens.contains(token45),
            "With default 255-byte limit, 45-byte token is kept");

        // Users can restore legacy behavior with explicit setting
        List<String> explicitLegacy = TextAnalyzer.tokenize(text, "default", 40);
        assertFalse(explicitLegacy.contains(token45),
            "Explicit 40-byte limit restores legacy behavior");
    }
}
