// Test module to parse range queries using Quickwit's query parser
// and compare with our generated AST

use quickwit_query::query_ast::{QueryAst, RangeQuery};
use quickwit_query::JsonLiteral;
use serde_json;
use jni::JNIEnv;
use jni::objects::JClass;
use std::ops::Bound;

#[no_mangle]
pub extern "system" fn test_quickwit_range_query_parser() {
    println!("\n=== Testing Quickwit Query Parser for Range Queries ===\n");
    
    // Note: Quickwit doesn't have a direct parse_query_to_ast function accessible
    // We'll skip the parsing test and focus on constructing the correct AST structure
    
    println!("Quickwit doesn't expose a direct query parser, but we can construct the correct AST structure.");
    
    // Now let's manually construct a range query AST and see its structure
    println!("\n=== Manually Constructed Range Query AST ===\n");
    
    // RangeBound is an enum in quickwit_query::query_ast
    
    // Create a range query for price:[30 TO 80]
    let range_query = RangeQuery {
        field: "price".to_string(),
        lower_bound: Bound::Included(JsonLiteral::Number(serde_json::Number::from(30))),
        upper_bound: Bound::Included(JsonLiteral::Number(serde_json::Number::from(80))),
    };
    
    let ast = QueryAst::Range(range_query);
    
    println!("Manually constructed AST: {:?}", ast);
    match serde_json::to_string_pretty(&ast) {
        Ok(json) => println!("Manual AST as JSON:\n{}\n", json),
        Err(e) => println!("Failed to serialize manual AST: {}", e),
    }
    
    // Test with string bounds (which might be the issue)
    println!("\n=== Range Query with String Bounds ===\n");
    
    let range_query_str = RangeQuery {
        field: "price".to_string(),
        lower_bound: Bound::Included(JsonLiteral::String("30".to_string())),
        upper_bound: Bound::Included(JsonLiteral::String("80".to_string())),
    };
    
    let ast_str = QueryAst::Range(range_query_str);
    
    println!("String bounds AST: {:?}", ast_str);
    match serde_json::to_string_pretty(&ast_str) {
        Ok(json) => println!("String bounds AST as JSON:\n{}\n", json),
        Err(e) => println!("Failed to serialize string bounds AST: {}", e),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_test_TestQueryParser_testRangeQueryParsing(
    _env: JNIEnv,
    _class: JClass,
) {
    test_quickwit_range_query_parser();
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_range_query_parsing() {
        test_quickwit_range_query_parser();
    }
}