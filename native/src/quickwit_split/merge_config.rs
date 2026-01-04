// merge_config.rs - Configuration structures and extraction for merge operations
// Extracted from mod.rs during refactoring
// Contains: InternalMergeConfig, InternalAwsConfig, InternalAzureConfig, and extract functions

use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use anyhow::Result;

use crate::utils::jstring_to_string;

/// Configuration for split merging operations
#[derive(Debug, Clone)]
pub struct InternalMergeConfig {
    pub index_uid: String,
    pub source_id: String,
    pub node_id: String,
    pub doc_mapping_uid: String,
    pub partition_id: u64,
    pub delete_queries: Option<Vec<String>>,
    pub aws_config: Option<InternalAwsConfig>,
    pub azure_config: Option<InternalAzureConfig>,
    pub temp_directory_path: Option<String>,
    /// Maximum number of splits to process concurrently (memory optimization)
    /// Default: 0 (unlimited - use global semaphore limit)
    /// Set to a lower value (e.g., 3-5) for very large splits to reduce peak memory
    pub max_concurrent_splits: usize,
}

/// AWS configuration for S3-compatible storage
#[derive(Clone, Debug)]
pub struct InternalAwsConfig {
    pub access_key: Option<String>,  // Made optional to support default credential chain
    pub secret_key: Option<String>,  // Made optional to support default credential chain
    pub session_token: Option<String>,
    pub region: String,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
}

/// Azure Blob Storage configuration for merge operations
#[derive(Clone, Debug)]
pub struct InternalAzureConfig {
    pub account_name: String,
    pub account_key: Option<String>,      // Optional: for Storage Account Key auth
    pub bearer_token: Option<String>,     // Optional: for OAuth token auth
}

/// Extract AWS configuration from Java AwsConfig object
pub fn extract_aws_config(env: &mut JNIEnv, aws_obj: JObject) -> anyhow::Result<InternalAwsConfig> {
    let access_key = get_nullable_string_field_value(env, &aws_obj, "getAccessKey")?;
    let secret_key = get_nullable_string_field_value(env, &aws_obj, "getSecretKey")?;
    let region = get_string_field_value(env, &aws_obj, "getRegion")?;

    // Extract session token (optional - for STS/temporary credentials)
    let session_token = match env.call_method(&aws_obj, "getSessionToken", "()Ljava/lang/String;", &[]) {
        Ok(session_result) => {
            let session_obj = session_result.l()?;
            if env.is_same_object(&session_obj, JObject::null())? {
                None
            } else {
                Some(jstring_to_string(env, &session_obj.into())?)
            }
        },
        Err(_) => None,
    };

    // Extract endpoint (optional - for S3-compatible storage)
    let endpoint = match env.call_method(&aws_obj, "getEndpoint", "()Ljava/lang/String;", &[]) {
        Ok(endpoint_result) => {
            let endpoint_obj = endpoint_result.l()?;
            if env.is_same_object(&endpoint_obj, JObject::null())? {
                None
            } else {
                Some(jstring_to_string(env, &endpoint_obj.into())?)
            }
        },
        Err(_) => None,
    };

    // Extract force_path_style (optional)
    let force_path_style = match env.call_method(&aws_obj, "isForcePathStyle", "()Z", &[]) {
        Ok(result) => result.z().unwrap_or(false),
        Err(_) => false,
    };

    Ok(InternalAwsConfig {
        access_key,
        secret_key,
        session_token,
        region,
        endpoint,
        force_path_style,
    })
}

/// Extract Azure configuration from Java AzureConfig object
pub fn extract_azure_config(env: &mut JNIEnv, azure_obj: JObject) -> anyhow::Result<InternalAzureConfig> {
    let account_name = get_string_field_value(env, &azure_obj, "getAccountName")?;

    // Account key is optional (may use bearer token instead)
    let account_key = get_nullable_string_field_value(env, &azure_obj, "getAccountKey")?;

    // Bearer token is optional (may use account key instead)
    let bearer_token = get_nullable_string_field_value(env, &azure_obj, "getBearerToken")?;

    Ok(InternalAzureConfig {
        account_name,
        account_key,
        bearer_token,
    })
}

/// Extract merge configuration from Java MergeConfig object
pub fn extract_merge_config(env: &mut JNIEnv, config_obj: &JObject) -> Result<InternalMergeConfig> {
    let index_uid = get_string_field_value(env, config_obj, "getIndexUid")?;
    let source_id = get_string_field_value(env, config_obj, "getSourceId")?;
    let node_id = get_string_field_value(env, config_obj, "getNodeId")?;
    let doc_mapping_uid = get_string_field_value(env, config_obj, "getDocMappingUid")?;

    let partition_id = env.call_method(config_obj, "getPartitionId", "()J", &[])?.j()? as u64;

    // Extract delete queries (optional)
    let delete_queries: Option<Vec<String>> = None;  // TODO: implement if needed

    // Extract AWS config (optional)
    let aws_config = match env.call_method(config_obj, "getAwsConfig", "()Lio/indextables/tantivy4java/split/merge/QuickwitSplit$AwsConfig;", &[]) {
        Ok(aws_result) => {
            let aws_obj = aws_result.l()?;
            if env.is_same_object(&aws_obj, JObject::null())? {
                None
            } else {
                Some(extract_aws_config(env, aws_obj)?)
            }
        },
        Err(_) => None,
    };

    // Extract Azure config (optional)
    let azure_config = match env.call_method(config_obj, "getAzureConfig", "()Lio/indextables/tantivy4java/split/merge/QuickwitSplit$AzureConfig;", &[]) {
        Ok(azure_result) => {
            let azure_obj = azure_result.l()?;
            if env.is_same_object(&azure_obj, JObject::null())? {
                None
            } else {
                Some(extract_azure_config(env, azure_obj)?)
            }
        },
        Err(_) => None,
    };

    // Extract temp directory path (optional)
    let temp_directory_path = get_nullable_string_field_value(env, config_obj, "getTempDirectoryPath")?;

    Ok(InternalMergeConfig {
        index_uid,
        source_id,
        node_id,
        doc_mapping_uid,
        partition_id,
        delete_queries,
        aws_config,
        azure_config,
        temp_directory_path,
        max_concurrent_splits: 0,  // Default: unlimited (use global semaphore)
    })
}

/// Extract a string list from a Java List object
pub fn extract_string_list_from_jobject(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>> {
    let list_size = env.call_method(list_obj, "size", "()I", &[])?.i()?;
    let mut strings = Vec::with_capacity(list_size as usize);

    for i in 0..list_size {
        let element = env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[JValue::Int(i)])?.l()?;
        let java_string = JString::from(element);
        let rust_string = jstring_to_string(env, &java_string)?;
        strings.push(rust_string);
    }

    Ok(strings)
}

/// Helper function to get string field value from Java object
pub fn get_string_field_value(env: &mut JNIEnv, obj: &JObject, method_name: &str) -> Result<String> {
    let string_obj = env.call_method(obj, method_name, "()Ljava/lang/String;", &[])?.l()?;
    let java_string = JString::from(string_obj);
    jstring_to_string(env, &java_string)
}

/// Get a string field value that can be null (for nullable credentials)
pub fn get_nullable_string_field_value(env: &mut JNIEnv, obj: &JObject, method_name: &str) -> Result<Option<String>> {
    let string_obj = env.call_method(obj, method_name, "()Ljava/lang/String;", &[])?.l()?;
    if env.is_same_object(&string_obj, JObject::null())? {
        Ok(None)
    } else {
        let java_string = JString::from(string_obj);
        let string_value = jstring_to_string(env, &java_string)?;
        Ok(Some(string_value))
    }
}
