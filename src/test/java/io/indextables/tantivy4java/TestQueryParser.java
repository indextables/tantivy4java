package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;

/**
 * Test class to debug query parsing using Quickwit's parser
 */
public class TestQueryParser {
    static {
        Tantivy.initialize();
    }
    
    // Native method to test range query parsing
    public static native void testRangeQueryParsing();
    
    public static void main(String[] args) {
        System.out.println("Testing Quickwit Query Parser...");
        testRangeQueryParsing();
    }
}
