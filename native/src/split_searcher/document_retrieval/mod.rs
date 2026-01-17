// document_retrieval.rs - Document retrieval module re-exports
// Refactored into submodules during P3 refactoring phase

mod batch_doc_retrieval;
pub mod batch_serialization;
mod doc_retrieval_jni;
mod single_doc_retrieval;

// Re-export JNI entry points (must be public for JNI linkage)
pub use doc_retrieval_jni::{
    Java_io_indextables_tantivy4java_split_SplitSearcher_docBatchNative,
    Java_io_indextables_tantivy4java_split_SplitSearcher_docNative,
    Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative,
    Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNativeWithFields,
};

// Re-export internal functions for use by other modules
pub use batch_doc_retrieval::{
    retrieve_documents_batch_from_split, retrieve_documents_batch_from_split_optimized,
};
pub use single_doc_retrieval::{
    retrieve_document_from_split, retrieve_document_from_split_optimized,
};
