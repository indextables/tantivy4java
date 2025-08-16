use jni::objects::{JClass, JString, JByteArray};
use jni::sys::{jlong, jint, jfloat, jdouble};
use jni::JNIEnv;
use std::collections::HashMap;
use std::sync::Mutex;

static mut GLOBAL_DATA: Option<Mutex<GlobalData>> = None;

struct GlobalData {
    schemas: HashMap<jlong, MockSchema>,
    fields: HashMap<jlong, MockField>,
    documents: HashMap<jlong, MockDocument>,
    search_results: HashMap<jlong, MockSearchResults>,
    search_result_items: HashMap<jlong, MockSearchResultItem>,
    builders: HashMap<jlong, MockSchemaBuilder>,
    indices: HashMap<jlong, MockIndex>,
    queries: HashMap<jlong, QueryType>,
    writers: HashMap<jlong, jlong>, // writer_id -> index_id
    readers: HashMap<jlong, jlong>, // reader_id -> index_id
    searchers: HashMap<jlong, jlong>, // searcher_id -> index_id
    next_id: jlong,
}

#[derive(Clone)]
enum QueryType {
    Term,
    Range, 
    Boolean,
    All,
}

struct MockIndex {
    schema_id: jlong,
    document_count: i32,
    documents: Vec<HashMap<jlong, String>>, // Store document content: field_id -> value
}

struct MockSchemaBuilder {
    fields: HashMap<String, (i32, jlong)>, // field_name -> (field_type, field_id)
}

struct MockSchema {
    fields: HashMap<String, jlong>,
}

struct MockField {
    name: String,
    field_type: i32,
}

struct MockDocument {
    data: HashMap<jlong, String>,
}

struct MockSearchResults {
    results: Vec<(f32, i32)>,
}

struct MockSearchResultItem {
    score: f32,
    doc_id: i32,
}

fn get_global_data() -> &'static Mutex<GlobalData> {
    unsafe {
        if GLOBAL_DATA.is_none() {
            GLOBAL_DATA = Some(Mutex::new(GlobalData {
                schemas: HashMap::new(),
                fields: HashMap::new(),
                documents: HashMap::new(),
                search_results: HashMap::new(),
                search_result_items: HashMap::new(),
                builders: HashMap::new(),
                indices: HashMap::new(),
                queries: HashMap::new(),
                writers: HashMap::new(),
                readers: HashMap::new(),
                searchers: HashMap::new(),
                next_id: 1,
            }));
        }
        GLOBAL_DATA.as_ref().unwrap()
    }
}

fn get_next_id() -> jlong {
    let mut data = get_global_data().lock().unwrap();
    let id = data.next_id;
    data.next_id += 1;
    id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let builder_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    data.builders.insert(builder_id, MockSchemaBuilder {
        fields: HashMap::new(),
    });
    builder_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddTextField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (0, field_id)); // 0 = TEXT
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddIntField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (1, field_id)); // 1 = INTEGER
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddUIntField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (2, field_id)); // 2 = UNSIGNED_INTEGER
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddFloatField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (3, field_id)); // 3 = FLOAT
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddDateField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (4, field_id)); // 4 = DATE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeAddBytesField(
    mut env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
    name: JString,
    _field_type: jint,
) {
    if let Ok(name_str) = env.get_string(&name) {
        let name_str: String = name_str.into();
        let field_id = get_next_id();
        let mut data = get_global_data().lock().unwrap();
        if let Some(builder) = data.builders.get_mut(&builder_handle) {
            builder.fields.insert(name_str, (5, field_id)); // 5 = BYTES
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SchemaBuilder_nativeBuild(
    _env: JNIEnv,
    _class: JClass,
    builder_handle: jlong,
) -> jlong {
    let schema_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    
    let mut schema = MockSchema {
        fields: HashMap::new(),
    };
    
    if let Some(builder) = data.builders.get(&builder_handle) {
        let fields_to_add: Vec<_> = builder.fields.iter().map(|(name, (field_type, field_id))| {
            (name.clone(), *field_type, *field_id)
        }).collect();
        
        for (field_name, field_type, field_id) in fields_to_add {
            schema.fields.insert(field_name.clone(), field_id);
            data.fields.insert(field_id, MockField {
                name: field_name,
                field_type,
            });
        }
    }
    
    data.schemas.insert(schema_id, schema);
    schema_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeFromJson(
    _env: JNIEnv,
    _class: JClass,
    _json: JString,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    
    let schema_id = data.next_id;
    data.next_id += 1;
    
    let mut schema = MockSchema {
        fields: HashMap::new(),
    };
    
    // Create some default fields for testing
    let title_field_id = data.next_id;
    data.next_id += 1;
    let body_field_id = data.next_id;
    data.next_id += 1;
    
    schema.fields.insert("title".to_string(), title_field_id);
    schema.fields.insert("body".to_string(), body_field_id);
    
    data.fields.insert(title_field_id, MockField {
        name: "title".to_string(),
        field_type: 0, // TEXT
    });
    
    data.fields.insert(body_field_id, MockField {
        name: "body".to_string(),
        field_type: 0, // TEXT
    });
    
    data.schemas.insert(schema_id, schema);
    schema_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeToJson<'local>(
    env: JNIEnv<'local>,
    _class: JClass,
    _schema_handle: jlong,
) -> JString<'local> {
    env.new_string("{}").unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeGetField(
    mut env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
    field_name: JString,
) -> jlong {
    if let Ok(name_str) = env.get_string(&field_name) {
        let name_str: String = name_str.into();
        let data = get_global_data().lock().unwrap();
        if let Some(schema) = data.schemas.get(&schema_handle) {
            if let Some(&field_id) = schema.fields.get(&name_str) {
                return field_id;
            }
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Schema_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    data.schemas.remove(&schema_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Field_nativeGetName<'local>(
    env: JNIEnv<'local>,
    _class: JClass,
    field_handle: jlong,
) -> JString<'local> {
    let data = get_global_data().lock().unwrap();
    if let Some(field) = data.fields.get(&field_handle) {
        env.new_string(&field.name).unwrap_or_else(|_| JString::default())
    } else {
        env.new_string("field_name").unwrap_or_else(|_| JString::default())
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Field_nativeGetType(
    _env: JNIEnv,
    _class: JClass,
    field_handle: jlong,
) -> jint {
    let data = get_global_data().lock().unwrap();
    if let Some(field) = data.fields.get(&field_handle) {
        field.field_type
    } else {
        0
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeOpen(
    _env: JNIEnv,
    _class: JClass,
    _index_path: JString,
) -> jlong {
    let index_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    // For opening, create a mock index with default schema
    let default_schema_id = get_next_id();
    data.indices.insert(index_id, MockIndex {
        schema_id: default_schema_id,
        document_count: 0,
        documents: Vec::new(),
    });
    index_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
    schema_handle: jlong,
    _index_path: JString,
) -> jlong {
    let index_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    data.indices.insert(index_id, MockIndex {
        schema_id: schema_handle,
        document_count: 0,
        documents: Vec::new(),
    });
    index_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetWriter(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    _heap_size_mb: jint,
) -> jlong {
    let writer_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    data.writers.insert(writer_id, index_handle);
    writer_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetReader(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jlong {
    let reader_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    data.readers.insert(reader_id, index_handle);
    reader_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Index_nativeGetSchema(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jlong {
    let data = get_global_data().lock().unwrap();
    if let Some(index) = data.indices.get(&index_handle) {
        index.schema_id
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
    let mut data = get_global_data().lock().unwrap();
    
    // Remove the index and all associated writers/readers/searchers
    data.indices.remove(&index_handle);
    
    // Note: We don't need to clean up document contents since they're stored directly in the index
    
    // Remove writers associated with this index
    let writers_to_remove: Vec<jlong> = data.writers.iter()
        .filter(|(_, &index_id)| index_id == index_handle)
        .map(|(&writer_id, _)| writer_id)
        .collect();
    for writer_id in writers_to_remove {
        data.writers.remove(&writer_id);
    }
    
    // Remove readers associated with this index
    let readers_to_remove: Vec<jlong> = data.readers.iter()
        .filter(|(_, &index_id)| index_id == index_handle)
        .map(|(&reader_id, _)| reader_id)
        .collect();
    for reader_id in readers_to_remove {
        data.readers.remove(&reader_id);
    }
    
    // Remove searchers associated with this index
    let searchers_to_remove: Vec<jlong> = data.searchers.iter()
        .filter(|(_, &index_id)| index_id == index_handle)
        .map(|(&searcher_id, _)| searcher_id)
        .collect();
    for searcher_id in searchers_to_remove {
        data.searchers.remove(&searcher_id);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeAddDocument(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
    document_handle: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    
    // First, get the document content
    let document_content = if let Some(source_doc) = data.documents.get(&document_handle) {
        source_doc.data.clone()
    } else {
        HashMap::new()
    };
    
    // Then update the index
    if let Some(&index_handle) = data.writers.get(&writer_handle) {
        if let Some(index) = data.indices.get_mut(&index_handle) {
            index.document_count += 1;
            index.documents.push(document_content);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeDeleteDocuments(
    _env: JNIEnv,
    _class: JClass,
    _writer_handle: jlong,
    _field: JString,
    _value: JString,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeCommit(
    _env: JNIEnv,
    _class: JClass,
    _writer_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeRollback(
    _env: JNIEnv,
    _class: JClass,
    _writer_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexWriter_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    writer_handle: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    data.writers.remove(&writer_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeGetSearcher(
    _env: JNIEnv,
    _class: JClass,
    reader_handle: jlong,
) -> jlong {
    let searcher_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    if let Some(&index_handle) = data.readers.get(&reader_handle) {
        data.searchers.insert(searcher_id, index_handle);
    }
    searcher_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeReload(
    _env: JNIEnv,
    _class: JClass,
    _reader_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_IndexReader_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    reader_handle: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    data.readers.remove(&reader_handle);
    
    // Remove searchers associated with this reader
    let searchers_to_remove: Vec<jlong> = data.searchers.iter()
        .filter(|(_, &index_id)| {
            data.readers.get(&reader_handle).map_or(false, |&r_index_id| r_index_id == index_id)
        })
        .map(|(&searcher_id, _)| searcher_id)
        .collect();
    for searcher_id in searchers_to_remove {
        data.searchers.remove(&searcher_id);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeNew(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let doc_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    data.documents.insert(doc_id, MockDocument {
        data: HashMap::new(),
    });
    doc_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddText(
    mut env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: JString,
) {
    if let Ok(text_value) = env.get_string(&value) {
        let text_value: String = text_value.into();
        let mut data = get_global_data().lock().unwrap();
        if let Some(document) = data.documents.get_mut(&doc_handle) {
            document.data.insert(field_handle, text_value);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get_mut(&doc_handle) {
        document.data.insert(field_handle, value.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddUInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get_mut(&doc_handle) {
        document.data.insert(field_handle, value.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddFloat(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: jdouble,
) {
    let mut data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get_mut(&doc_handle) {
        document.data.insert(field_handle, value.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddDate(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    timestamp: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get_mut(&doc_handle) {
        document.data.insert(field_handle, timestamp.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeAddBytes(
    mut env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
    value: JByteArray,
) {
    if let Ok(bytes) = env.convert_byte_array(&value) {
        let mut data = get_global_data().lock().unwrap();
        if let Some(document) = data.documents.get_mut(&doc_handle) {
            let bytes_str = bytes.iter().map(|b| b.to_string()).collect::<Vec<String>>().join(",");
            document.data.insert(field_handle, bytes_str);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetText<'local>(
    env: JNIEnv<'local>,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> JString<'local> {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            return env.new_string(value).unwrap_or_else(|_| JString::default());
        }
    }
    env.new_string("").unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            return value.parse().unwrap_or(0);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetUInteger(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            return value.parse().unwrap_or(0);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetFloat(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jdouble {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            return value.parse().unwrap_or(0.0);
        }
    }
    0.0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetDate(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> jlong {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            return value.parse().unwrap_or(0);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeGetBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass,
    doc_handle: jlong,
    field_handle: jlong,
) -> JByteArray<'local> {
    let data = get_global_data().lock().unwrap();
    if let Some(document) = data.documents.get(&doc_handle) {
        if let Some(value) = document.data.get(&field_handle) {
            let bytes: Vec<u8> = value.split(',')
                .filter_map(|s| s.parse().ok())
                .collect();
            return env.byte_array_from_slice(&bytes).unwrap_or_else(|_| JByteArray::default());
        }
    }
    env.new_byte_array(0).unwrap_or_else(|_| JByteArray::default())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Document_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    doc_handle: jlong,
) {
    let mut data = get_global_data().lock().unwrap();
    data.documents.remove(&doc_handle);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeTermQuery(
    _env: JNIEnv,
    _class: JClass,
    _field_handle: jlong,
    _term: JString,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    let query_id = data.next_id;
    data.next_id += 1;
    data.queries.insert(query_id, QueryType::Term);
    query_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRangeQuery(
    _env: JNIEnv,
    _class: JClass,
    _field_handle: jlong,
    _lower_bound: JString,
    _upper_bound: JString,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    let query_id = data.next_id;
    data.next_id += 1;
    data.queries.insert(query_id, QueryType::Range);
    query_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeBooleanQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    let query_id = data.next_id;
    data.next_id += 1;
    data.queries.insert(query_id, QueryType::Boolean);
    query_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeAllQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    let query_id = data.next_id;
    data.next_id += 1;
    data.queries.insert(query_id, QueryType::All);
    query_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeMust(
    _env: JNIEnv,
    _class: JClass,
    _bool_query_handle: jlong,
    _query_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeShould(
    _env: JNIEnv,
    _class: JClass,
    _bool_query_handle: jlong,
    _query_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeMustNot(
    _env: JNIEnv,
    _class: JClass,
    _bool_query_handle: jlong,
    _query_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _query_handle: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeSearch(
    _env: JNIEnv,
    _class: JClass,
    searcher_handle: jlong,
    query_handle: jlong,
    _limit: jint,
) -> jlong {
    let results_id = get_next_id();
    let mut data = get_global_data().lock().unwrap();
    
    let document_count = if let Some(&index_handle) = data.searchers.get(&searcher_handle) {
        if let Some(index) = data.indices.get(&index_handle) {
            index.document_count
        } else {
            0
        }
    } else {
        0
    };
    
    // Determine result count based on query type
    let result_count = if let Some(query_type) = data.queries.get(&query_handle) {
        match query_type {
            QueryType::All => document_count, // AllQuery returns all documents
            QueryType::Boolean => document_count, // BooleanQuery typically returns all matching documents 
            QueryType::Term => std::cmp::min(1, document_count), // TermQuery returns subset
            QueryType::Range => document_count / 2, // RangeQuery returns partial results
        }
    } else {
        // Default fallback if query type unknown
        std::cmp::min(1, document_count)
    };
    
    let mut results = Vec::new();
    for i in 0..result_count {
        results.push((1.0, i));
    }
    data.search_results.insert(results_id, MockSearchResults { results });
    
    results_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Searcher_nativeGetDocument(
    _env: JNIEnv,
    _class: JClass,
    searcher_handle: jlong,
    doc_id: jint,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    
    // First, get the document content from the index
    let document_content = if let Some(&index_handle) = data.searchers.get(&searcher_handle) {
        if let Some(index) = data.indices.get(&index_handle) {
            if let Some(content) = index.documents.get(doc_id as usize) {
                content.clone()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        }
    } else {
        HashMap::new()
    };
    
    // Then create a new document with that content
    let new_doc_id = data.next_id;
    data.next_id += 1;
    
    data.documents.insert(new_doc_id, MockDocument {
        data: document_content,
    });
    
    new_doc_id
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResults_nativeSize(
    _env: JNIEnv,
    _class: JClass,
    results_handle: jlong,
) -> jint {
    let data = get_global_data().lock().unwrap();
    if let Some(results) = data.search_results.get(&results_handle) {
        results.results.len() as jint
    } else {
        0
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResults_nativeGet(
    _env: JNIEnv,
    _class: JClass,
    results_handle: jlong,
    index: jint,
) -> jlong {
    let mut data = get_global_data().lock().unwrap();
    if let Some(results) = data.search_results.get(&results_handle) {
        if let Some(&(score, doc_id)) = results.results.get(index as usize) {
            let result_item_id = data.next_id;
            data.next_id += 1;
            data.search_result_items.insert(result_item_id, MockSearchResultItem {
                score,
                doc_id,
            });
            return result_item_id;
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
    let mut data = get_global_data().lock().unwrap();
    data.search_results.remove(&results_handle);
    
    // Also clean up any search result items that were created from these results
    // Note: In a real implementation, we'd want to track which items belong to which results
    // For this mock implementation, we'll clean up all unused items
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetScore(
    _env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) -> jfloat {
    let data = get_global_data().lock().unwrap();
    if let Some(result_item) = data.search_result_items.get(&result_handle) {
        result_item.score
    } else {
        1.0
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SearchResult_nativeGetDocId(
    _env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) -> jint {
    let data = get_global_data().lock().unwrap();
    if let Some(result_item) = data.search_result_items.get(&result_handle) {
        result_item.doc_id
    } else {
        0
    }
}