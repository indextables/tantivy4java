use jni::objects::{JClass, JString};
use jni::sys::{jlong, jint};
use jni::JNIEnv;
use tantivy::{Index, IndexWriter, IndexReader};
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use std::path::Path;
use std::sync::Arc;

use crate::{drop_handle, ptr_to_handle, handle_to_ptr};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeOpen(
    env: JNIEnv,
    _class: JClass,
    index_path: JString,
) -> jlong {
    if let Ok(path_str) = env.get_string(index_path) {
        let path_str: String = path_str.into();
        let path = Path::new(&path_str);
        
        if let Ok(mmap_directory) = MmapDirectory::open(path) {
            if let Ok(index) = Index::open(mmap_directory) {
                let index_box = Box::new(index);
                return handle_to_ptr(index_box);
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeCreate(
    env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
    index_path: JString,
) -> jlong {
    if let Some(schema) = ptr_to_handle::<Schema>(schema_handle) {
        if let Ok(path_str) = env.get_string(index_path) {
            let path_str: String = path_str.into();
            let path = Path::new(&path_str);
            
            if let Ok(mmap_directory) = MmapDirectory::open(path) {
                if let Ok(index) = Index::create(mmap_directory, schema.clone()) {
                    let index_box = Box::new(index);
                    return handle_to_ptr(index_box);
                }
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetWriter(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    heap_size_mb: jint,
) -> jlong {
    if let Some(index) = ptr_to_handle::<Index>(index_handle) {
        let heap_size = (heap_size_mb as usize) * 1024 * 1024;
        if let Ok(writer) = index.writer(heap_size) {
            let writer_box = Box::new(writer);
            return handle_to_ptr(writer_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetReader(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jlong {
    if let Some(index) = ptr_to_handle::<Index>(index_handle) {
        if let Ok(reader) = index.reader() {
            let reader_box = Box::new(reader);
            return handle_to_ptr(reader_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetSchema(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jlong {
    if let Some(index) = ptr_to_handle::<Index>(index_handle) {
        let schema = index.schema();
        let schema_box = Box::new(schema);
        handle_to_ptr(schema_box)
    } else {
        0
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) {
    drop_handle::<Index>(index_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeAddDocument(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
    document_handle: jlong,
) {
    if let Some(writer) = ptr_to_handle::<IndexWriter>(writer_handle) {
        if let Some(document) = ptr_to_handle::<tantivy::TantivyDocument>(document_handle) {
            let _ = writer.add_document(document.clone());
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocuments(
    env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
    field: JString,
    value: JString,
) {
    if let Some(_writer) = ptr_to_handle::<IndexWriter>(writer_handle) {
        if let (Ok(_field_str), Ok(_value_str)) = (env.get_string(field), env.get_string(value)) {
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeCommit(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
) {
    if let Some(writer) = ptr_to_handle::<IndexWriter>(writer_handle) {
        let _ = writer.commit();
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeRollback(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
) {
    if let Some(writer) = ptr_to_handle::<IndexWriter>(writer_handle) {
        let _ = writer.rollback();
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
) {
    drop_handle::<IndexWriter>(writer_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeGetSearcher(
    _env: JNIEnv,
    _class: JClass,
    reader_handle: jlong,
) -> jlong {
    if let Some(reader) = ptr_to_handle::<IndexReader>(reader_handle) {
        let searcher = reader.searcher();
        let searcher_box = Box::new(searcher);
        handle_to_ptr(searcher_box)
    } else {
        0
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeReload(
    _env: JNIEnv,
    _class: JClass,
    reader_handle: jlong,
) {
    if let Some(reader) = ptr_to_handle::<IndexReader>(reader_handle) {
        let _ = reader.reload();
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    reader_handle: jlong,
) {
    drop_handle::<IndexReader>(reader_handle);
}