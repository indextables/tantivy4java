// query_conversion.rs - Utility for converting Tantivy Query to Quickwit QueryAst

use std::any::Any;
use anyhow::{Result, anyhow};
use tantivy::query::{Query as TantivyQuery, TermQuery, BooleanQuery, RangeQuery, PhraseQuery, FuzzyTermQuery, AllQuery, BoostQuery, ConstScoreQuery};
use tantivy::schema::Schema;
use quickwit_query::query_ast::{QueryAst, TermQuery as QwTermQuery, BoolQuery, RangeQuery as QwRangeQuery};

/// Recursively convert a Tantivy Query to Quickwit QueryAst
pub fn convert_tantivy_query_to_ast(
    query: &dyn TantivyQuery, 
    schema: &Schema
) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting Tantivy query to QueryAst...");
    
    // Try to downcast to specific query types
    // Note: This is tricky because TantivyQuery doesn't provide type information
    // We need to use Any trait for downcasting
    
    // First, convert to Any for downcasting
    let query_any = query as &dyn Any;
    
    // Try each query type we support
    if let Some(term_query) = query_any.downcast_ref::<TermQuery>() {
        eprintln!("RUST DEBUG: Found TermQuery");
        return convert_term_query(term_query, schema);
    }
    
    if let Some(boolean_query) = query_any.downcast_ref::<BooleanQuery>() {
        eprintln!("RUST DEBUG: Found BooleanQuery");
        return convert_boolean_query(boolean_query, schema);
    }
    
    if let Some(range_query) = query_any.downcast_ref::<RangeQuery>() {
        eprintln!("RUST DEBUG: Found RangeQuery");
        return convert_range_query(range_query, schema);
    }
    
    if let Some(phrase_query) = query_any.downcast_ref::<PhraseQuery>() {
        eprintln!("RUST DEBUG: Found PhraseQuery");
        return convert_phrase_query(phrase_query, schema);
    }
    
    if let Some(fuzzy_query) = query_any.downcast_ref::<FuzzyTermQuery>() {
        eprintln!("RUST DEBUG: Found FuzzyTermQuery");
        return convert_fuzzy_query(fuzzy_query, schema);
    }
    
    if let Some(all_query) = query_any.downcast_ref::<AllQuery>() {
        eprintln!("RUST DEBUG: Found AllQuery");
        return convert_all_query(all_query, schema);
    }
    
    if let Some(boost_query) = query_any.downcast_ref::<BoostQuery>() {
        eprintln!("RUST DEBUG: Found BoostQuery");
        return convert_boost_query(boost_query, schema);
    }
    
    if let Some(const_query) = query_any.downcast_ref::<ConstScoreQuery>() {
        eprintln!("RUST DEBUG: Found ConstScoreQuery");
        return convert_const_score_query(const_query, schema);
    }
    
    eprintln!("RUST DEBUG: Unsupported query type");
    Err(anyhow!("Unsupported query type for conversion to QueryAst"))
}

fn convert_term_query(query: &TermQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting TermQuery...");
    
    // Extract the Term from TermQuery using the public accessor
    let term = query.term();
    
    // Get the field from the term
    let field = term.field();
    let field_name = schema.get_field_name(field);
    
    // Get the value from the term 
    let value_str = match term.typ() {
        tantivy::schema::Type::Str => {
            // For string terms, get the text value
            term.value().as_str().unwrap_or("").to_string()
        }
        tantivy::schema::Type::U64 => {
            // For numeric terms, convert to string
            term.value().as_u64().map(|v| v.to_string()).unwrap_or("0".to_string())
        }
        tantivy::schema::Type::I64 => {
            term.value().as_i64().map(|v| v.to_string()).unwrap_or("0".to_string())
        }
        tantivy::schema::Type::F64 => {
            term.value().as_f64().map(|v| v.to_string()).unwrap_or("0.0".to_string())
        }
        tantivy::schema::Type::Bool => {
            term.value().as_bool().map(|v| v.to_string()).unwrap_or("false".to_string())
        }
        tantivy::schema::Type::Date => {
            // Handle date terms
            term.value().as_date().map(|date| date.to_rfc3339()).unwrap_or_default()
        }
        tantivy::schema::Type::Facet => {
            // Handle facet terms
            term.value().as_facet().map(|facet| facet.to_string()).unwrap_or_default()
        }
        tantivy::schema::Type::Bytes => {
            // Handle byte terms - convert to base64 or hex
            let bytes = term.value().as_bytes().unwrap_or(&[]);
            base64::encode(bytes)
        }
        tantivy::schema::Type::IpAddr => {
            // Handle IP address terms
            term.value().as_ip_addr().map(|ip| ip.to_string()).unwrap_or_default()
        }
        _ => "unknown_value".to_string(),
    };
    
    eprintln!("RUST DEBUG: Converted TermQuery - field: {}, value: {}", field_name, value_str);
    
    Ok(QueryAst::Term(QwTermQuery {
        field: field_name.to_string(),
        value: value_str,
    }))
}

fn convert_boolean_query(query: &BooleanQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting BooleanQuery...");
    
    // Extract clauses using the public accessor
    let clauses = query.clauses();
    
    let mut must_clauses = Vec::new();
    let mut should_clauses = Vec::new();
    let mut must_not_clauses = Vec::new();
    
    // Process each clause and recursively convert subqueries
    for (occur, subquery) in clauses {
        let converted_subquery = convert_tantivy_query_to_ast(subquery.as_ref(), schema)?;
        
        match occur {
            tantivy::query::Occur::Must => must_clauses.push(converted_subquery),
            tantivy::query::Occur::Should => should_clauses.push(converted_subquery),
            tantivy::query::Occur::MustNot => must_not_clauses.push(converted_subquery),
        }
    }
    
    eprintln!("RUST DEBUG: BooleanQuery converted - must: {}, should: {}, must_not: {}", 
              must_clauses.len(), should_clauses.len(), must_not_clauses.len());
    
    Ok(QueryAst::Bool(BoolQuery {
        must: must_clauses,
        should: should_clauses,
        must_not: must_not_clauses,
        filter: Vec::new(), // BooleanQuery doesn't have separate filter clauses
        minimum_should_match: None, // Could be extracted from get_minimum_number_should_match() if needed
    }))
}

fn convert_range_query(query: &RangeQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting RangeQuery...");
    
    // Get field information using public accessors
    let field = query.field();
    let field_name = schema.get_field_name(field);
    let value_type = query.value_type();
    
    // Unfortunately, RangeQuery doesn't provide public access to its bounds directly
    // We can only access one term via get_term(), but this is marked as pub(crate)
    // For now, we'll create a placeholder range query
    // TODO: Find a way to extract bounds or add public accessor to RangeQuery
    
    eprintln!("RUST DEBUG: RangeQuery field: {}, type: {:?}", field_name, value_type);
    
    // Create a placeholder range query - this is incomplete
    // In a real implementation, we would need access to the bounds
    Ok(QueryAst::Range(QwRangeQuery {
        field: field_name.to_string(),
        lower_bound: quickwit_query::query_ast::RangeBound::Unbounded,
        upper_bound: quickwit_query::query_ast::RangeBound::Unbounded,
        format: None, // For date formatting
    }))
}

fn convert_phrase_query(query: &PhraseQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting PhraseQuery...");
    
    // Extract field and terms using public accessors
    let field = query.field();
    let field_name = schema.get_field_name(field);
    let terms = query.phrase_terms();
    
    // Convert terms to text
    let phrase_text = terms.iter()
        .map(|term| {
            match term.typ() {
                tantivy::schema::Type::Str => {
                    term.value().as_str().unwrap_or("").to_string()
                }
                _ => term.value().to_string(), // For non-string terms
            }
        })
        .collect::<Vec<String>>()
        .join(" ");
    
    eprintln!("RUST DEBUG: PhraseQuery field: {}, phrase: \"{}\"", field_name, phrase_text);
    
    // Convert to a FullTextQuery - this is the closest equivalent in Quickwit
    // PhraseQuery becomes a FullTextQuery with phrase mode
    Ok(QueryAst::FullText(quickwit_query::query_ast::FullTextQuery {
        field: field_name.to_string(),
        text: format!("\"{}\"", phrase_text), // Wrap in quotes to indicate phrase
        params: quickwit_query::query_ast::FullTextParams {
            mode: quickwit_query::query_ast::FullTextMode::Phrase, // Use phrase mode
            tokenizer: None, // Use default tokenizer
            zero_terms_query: quickwit_query::ZeroTermsQuery::None,
        },
    }))
}

fn convert_fuzzy_query(query: &FuzzyTermQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting FuzzyTermQuery...");
    // TODO: Convert to appropriate fuzzy query type in QueryAst
    Ok(QueryAst::MatchAll) // Placeholder
}

fn convert_all_query(query: &AllQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting AllQuery...");
    Ok(QueryAst::MatchAll)
}

fn convert_boost_query(query: &BoostQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting BoostQuery...");
    // TODO: Extract underlying query and boost value, recursively convert underlying
    Ok(QueryAst::MatchAll) // Placeholder
}

fn convert_const_score_query(query: &ConstScoreQuery, schema: &Schema) -> Result<QueryAst> {
    eprintln!("RUST DEBUG: Converting ConstScoreQuery...");
    // TODO: Extract underlying query and score, recursively convert underlying
    Ok(QueryAst::MatchAll) // Placeholder
}