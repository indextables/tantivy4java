use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jlong, jobject};
use jni::JNIEnv;
use tantivy::tokenizer::{
    SimpleTokenizer, WhitespaceTokenizer, RawTokenizer,
    LowerCaser, RemoveLongFilter, TextAnalyzer as TantivyAnalyzer
};

use crate::utils::{handle_error, register_object, remove_object, with_object_mut};

/// Create a text analyzer with the specified tokenizer
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeCreateAnalyzer(
    mut env: JNIEnv,
    _class: JClass,
    tokenizer_name: JString,
) -> jlong {
    let tokenizer_name_str: String = match env.get_string(&tokenizer_name) {
        Ok(s) => s.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to get tokenizer name: {}", e));
            return 0;
        }
    };

    let analyzer = match create_analyzer(&tokenizer_name_str) {
        Ok(analyzer) => analyzer,
        Err(e) => {
            handle_error(&mut env, &format!("Failed to create analyzer: {}", e));
            return 0;
        }
    };

    register_object(Box::new(analyzer)) as jlong
}

/// Tokenize text using the specified tokenizer (static method)
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeTokenize(
    mut env: JNIEnv,
    _class: JClass,
    text: JString,
    tokenizer_name: JString,
) -> jobject {
    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to get text: {}", e));
            return std::ptr::null_mut();
        }
    };

    let tokenizer_name_str: String = match env.get_string(&tokenizer_name) {
        Ok(s) => s.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to get tokenizer name: {}", e));
            return std::ptr::null_mut();
        }
    };

    let mut analyzer = match create_analyzer(&tokenizer_name_str) {
        Ok(analyzer) => analyzer,
        Err(e) => {
            handle_error(&mut env, &format!("Failed to create analyzer: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Tokenize the text
    let tokens = tokenize_text(&mut analyzer, &text_str);

    // Convert tokens to Java List<String>
    match create_string_list(&mut env, tokens) {
        Ok(list) => list.into_raw(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to create token list: {}", e));
            std::ptr::null_mut()
        }
    }
}

/// Analyze text using the analyzer instance
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeAnalyze(
    mut env: JNIEnv,
    _class: JClass,
    analyzer_ptr: jlong,
    text: JString,
) -> jobject {
    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to get text: {}", e));
            return std::ptr::null_mut();
        }
    };

    let result = with_object_mut::<Box<TantivyAnalyzer>, Option<Vec<String>>>(
        analyzer_ptr as u64, 
        |analyzer| {
            Some(tokenize_text(analyzer, &text_str))
        }
    );

    match result {
        Some(Some(tokens)) => {
            match create_string_list(&mut env, tokens) {
                Ok(list) => list.into_raw(),
                Err(e) => {
                    handle_error(&mut env, &format!("Failed to create token list: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        _ => {
            handle_error(&mut env, "Invalid TextAnalyzer pointer");
            std::ptr::null_mut()
        }
    }
}

/// Close the text analyzer
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_TextAnalyzer_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    analyzer_ptr: jlong,
) {
    remove_object(analyzer_ptr as u64);
}

/// Create a text analyzer based on the tokenizer name
fn create_analyzer(tokenizer_name: &str) -> Result<TantivyAnalyzer, String> {
    match tokenizer_name {
        "default" => {
            // Default analyzer: simple tokenizer + lowercase + stop word removal
            Ok(TantivyAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(RemoveLongFilter::limit(40))
                .build())
        },
        "simple" => {
            // Simple tokenizer with lowercase
            Ok(TantivyAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build())
        },
        "keyword" => {
            // Keyword tokenizer (treats entire input as single token)
            Ok(TantivyAnalyzer::builder(RawTokenizer::default())
                .build())
        },
        "whitespace" => {
            // Whitespace tokenizer with lowercase
            Ok(TantivyAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build())
        },
        "raw" => {
            // Raw tokenizer (no processing)
            Ok(TantivyAnalyzer::builder(RawTokenizer::default())
                .build())
        },
        _ => Err(format!("Unknown tokenizer: {}", tokenizer_name)),
    }
}

/// Tokenize text using the given analyzer
fn tokenize_text(analyzer: &mut TantivyAnalyzer, text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token_stream = analyzer.token_stream(text);
    
    // Use the token stream properly - next() returns Option<&Token>
    while let Some(token) = token_stream.next() {
        tokens.push(token.text.clone());
    }
    
    tokens
}

/// Create a Java List<String> from a Vec<String>
fn create_string_list<'local>(env: &mut JNIEnv<'local>, tokens: Vec<String>) -> Result<JObject<'local>, String> {
    // Create ArrayList
    let arraylist_class = env.find_class("java/util/ArrayList").map_err(|e| format!("Failed to find ArrayList class: {}", e))?;
    let arraylist = env.new_object(&arraylist_class, "(I)V", &[JValue::Int(tokens.len() as i32)]).map_err(|e| format!("Failed to create ArrayList: {}", e))?;
    
    // Add each token to the list
    for token in tokens {
        let java_string = env.new_string(&token).map_err(|e| format!("Failed to create Java string: {}", e))?;
        env.call_method(&arraylist, "add", "(Ljava/lang/Object;)Z", &[JValue::Object(&java_string)])
            .map_err(|e| format!("Failed to add token to list: {}", e))?;
    }
    
    Ok(arraylist)
}