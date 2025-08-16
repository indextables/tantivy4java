use jni::objects::JClass;
use jni::sys::{jlong, jint, jfloat};
use jni::JNIEnv;
use tantivy::{Searcher, collector::TopDocs, query::Query, TantivyDocument, DocAddress};

use crate::{drop_handle, ptr_to_handle, handle_to_ptr};

pub struct SearchResults {
    pub results: Vec<(f32, DocAddress)>,
}

pub struct SearchResult {
    pub score: f32,
    pub doc_address: DocAddress,
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeSearch(
    _env: JNIEnv,
    _class: JClass,
    searcher_handle: jlong,
    query_handle: jlong,
    limit: jint,
) -> jlong {
    if let (Some(searcher), Some(query)) = (
        ptr_to_handle::<Searcher>(searcher_handle),
        ptr_to_handle::<Box<dyn Query>>(query_handle),
    ) {
        let collector = TopDocs::with_limit(limit as usize);
        if let Ok(top_docs) = searcher.search(query.as_ref(), &collector) {
            let results = SearchResults {
                results: top_docs,
            };
            let results_box = Box::new(results);
            return handle_to_ptr(results_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetDocument(
    _env: JNIEnv,
    _class: JClass,
    searcher_handle: jlong,
    doc_id: jint,
) -> jlong {
    if let Some(searcher) = ptr_to_handle::<Searcher>(searcher_handle) {
        let doc_address = DocAddress::new(0, doc_id as u32);
        if let Ok(document) = searcher.doc(doc_address) {
            let document_box = Box::new(document);
            return handle_to_ptr(document_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResults_nativeSize(
    _env: JNIEnv,
    _class: JClass,
    results_handle: jlong,
) -> jint {
    if let Some(results) = ptr_to_handle::<SearchResults>(results_handle) {
        return results.results.len() as jint;
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResults_nativeGet(
    _env: JNIEnv,
    _class: JClass,
    results_handle: jlong,
    index: jint,
) -> jlong {
    if let Some(results) = ptr_to_handle::<SearchResults>(results_handle) {
        if let Some((score, doc_address)) = results.results.get(index as usize) {
            let result = SearchResult {
                score: *score,
                doc_address: *doc_address,
            };
            let result_box = Box::new(result);
            return handle_to_ptr(result_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResults_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    results_handle: jlong,
) {
    drop_handle::<SearchResults>(results_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetScore(
    _env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) -> jfloat {
    if let Some(result) = ptr_to_handle::<SearchResult>(result_handle) {
        return result.score;
    }
    0.0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetDocId(
    _env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) -> jint {
    if let Some(result) = ptr_to_handle::<SearchResult>(result_handle) {
        return result.doc_address.doc_id as jint;
    }
    0
}