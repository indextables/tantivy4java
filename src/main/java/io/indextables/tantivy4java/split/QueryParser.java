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

package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.Schema;

/**
 * Common interface for components that can parse query strings into SplitQuery objects.
 *
 * <p>This interface is implemented by both {@link SplitSearcher} and
 * {@link io.indextables.tantivy4java.xref.XRefSearcher}, allowing the same
 * query objects to be used with both searchers.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Works with both SplitSearcher and XRefSearcher
 * QueryParser parser = searcher; // Either type
 * SplitQuery query = parser.parseQuery("content:hello AND status:active");
 *
 * // Use the parsed query
 * if (searcher instanceof XRefSearcher) {
 *     XRefSearchResult result = ((XRefSearcher) searcher).search(query, 1000);
 * } else {
 *     SearchResult result = ((SplitSearcher) searcher).search(query, 100);
 * }
 * }</pre>
 *
 * <h2>Polymorphic Search</h2>
 * <p>For common search operations, consider using the generic search methods
 * defined in this interface. Both SplitSearcher and XRefSearcher implement
 * these, though they return different result types internally.</p>
 */
public interface QueryParser extends AutoCloseable {

    /**
     * Parse a query string using Quickwit syntax.
     *
     * <p>Supported query syntax:</p>
     * <ul>
     *   <li>{@code field:value} - term query</li>
     *   <li>{@code field:value AND field2:value2} - boolean AND</li>
     *   <li>{@code field:value OR field2:value2} - boolean OR</li>
     *   <li>{@code NOT field:value} - boolean NOT</li>
     *   <li>{@code field:[min TO max]} - range query</li>
     *   <li>{@code *} - match all</li>
     *   <li>Unqualified terms search across default fields</li>
     * </ul>
     *
     * @param queryString Query string in Quickwit syntax
     * @return SplitQuery that can be used with the searcher
     * @throws RuntimeException if parsing fails
     */
    SplitQuery parseQuery(String queryString);

    /**
     * Get the schema for this searcher.
     *
     * <p>The schema provides field information including names, types,
     * and configuration that can be used for query construction and
     * validation.</p>
     *
     * @return The schema for this index/xref
     */
    Schema getSchema();
}
