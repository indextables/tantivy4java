package com.tantivy4java;

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