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

package io.indextables.tantivy4java.core;

/**
 * Constants for configurable token length limits.
 *
 * <p>During tokenization, tokens longer than the configured limit are filtered out
 * (not truncated). This prevents excessively long tokens from consuming index space
 * and degrading search performance.
 *
 * <p>Token length limits:
 * <ul>
 *   <li>{@link #TANTIVY_MAX} (65,530 bytes) - Maximum supported by Tantivy (u16::MAX - 5)</li>
 *   <li>{@link #DEFAULT} (255 bytes) - Quickwit-compatible default, balances flexibility and safety</li>
 *   <li>{@link #LEGACY} (40 bytes) - Original tantivy4java default, matches Tantivy's built-in default</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * // Use Quickwit-compatible default (255 bytes)
 * TextFieldIndexing indexing = TextFieldIndexing.create()
 *     .withTokenizer("default")
 *     .withPositions();
 *
 * // Use legacy 40-byte limit for backward compatibility
 * TextFieldIndexing legacyIndexing = TextFieldIndexing.create()
 *     .withTokenizer("default")
 *     .withMaxTokenLength(TokenLength.LEGACY)
 *     .withPositions();
 *
 * // Use maximum supported length
 * TextFieldIndexing maxIndexing = TextFieldIndexing.create()
 *     .withTokenizer("default")
 *     .withMaxTokenLength(TokenLength.TANTIVY_MAX)
 *     .withPositions();
 * </pre>
 *
 * <p><b>Breaking Change Notice:</b> The default token length limit changed from 40 to 255 bytes
 * in this version. Tokens between 41-255 bytes that were previously filtered out will now be indexed.
 * Use {@code withMaxTokenLength(TokenLength.LEGACY)} to restore the previous behavior.
 */
public final class TokenLength {

    /**
     * Maximum token length supported by Tantivy: 65,530 bytes (u16::MAX - 5).
     *
     * <p>This is the absolute maximum that Tantivy's term dictionary can handle.
     * Using this value allows indexing of very long tokens (e.g., base64-encoded data,
     * long URLs, or technical identifiers).
     */
    public static final int TANTIVY_MAX = 65530;

    /**
     * Default token length limit: 255 bytes.
     *
     * <p>This is the Quickwit-compatible default, applied to all tokenizers.
     * It provides a good balance between flexibility (allowing moderately long tokens)
     * and safety (preventing excessively long tokens from bloating the index).
     *
     * <p>This value matches Quickwit's DEFAULT_REMOVE_TOKEN_LENGTH constant.
     */
    public static final int DEFAULT = 255;

    /**
     * Legacy token length limit: 40 bytes.
     *
     * <p>This was the original tantivy4java default, matching Tantivy's built-in
     * "default" tokenizer behavior. Use this value for backward compatibility
     * with indices created using earlier versions of tantivy4java.
     */
    public static final int LEGACY = 40;

    /**
     * Minimum valid token length limit: 1 byte.
     *
     * <p>A limit of 0 would filter out all tokens, which is not useful.
     */
    public static final int MIN = 1;

    private TokenLength() {
        // Prevent instantiation
    }

    /**
     * Validate a token length limit value.
     *
     * @param maxTokenLength The token length limit to validate
     * @throws IllegalArgumentException if the value is outside the valid range [1, 65530]
     */
    public static void validate(int maxTokenLength) {
        if (maxTokenLength < MIN || maxTokenLength > TANTIVY_MAX) {
            throw new IllegalArgumentException(
                "maxTokenLength must be between " + MIN + " and " + TANTIVY_MAX +
                " (got " + maxTokenLength + "). " +
                "Consider using TokenLength.DEFAULT (" + DEFAULT + "), " +
                "TokenLength.LEGACY (" + LEGACY + "), or " +
                "TokenLength.TANTIVY_MAX (" + TANTIVY_MAX + ").");
        }
    }
}
