// iceberg_reader/catalog.rs - Catalog creation from config HashMap
//
// Creates an Iceberg catalog from a flat HashMap<String, String> config.
// A required `catalog_type` key selects the backend (rest, glue, hms).
// Remaining keys are passed directly to the catalog builder as properties.
//
// Supported catalog types:
//
// REST catalog (including Databricks Unity Catalog):
//   - `uri` (required) — REST endpoint URL
//     For Databricks: `https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest`
//   - `warehouse` — storage location or Unity Catalog name
//   - `credential` — OAuth2 `client_id:client_secret`
//   - `token` — bearer token (Databricks PAT or OAuth token)
//   - `oauth2-server-uri` — OAuth2 token endpoint
//     For Databricks: `https://<workspace>.cloud.databricks.com/oidc/v1/token`
//   - `header.<name>` — custom HTTP headers
//
// Glue catalog:
//   - `warehouse` (required) — S3 path
//   - `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`
//   - `region_name` — AWS region
//   - `profile_name` — AWS profile
//   - `catalog_id` — AWS account ID for cross-account
//
// HMS (Hive Metastore) catalog:
//   - `uri` (required) — thrift://host:port
//   - `warehouse` (required) — storage root
//   - `thrift_transport` — "framed" or "buffered"
//
// Storage credentials (FileIO, passed through to all catalogs):
//   AWS S3: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`,
//           `s3.region`, `s3.endpoint`, `s3.path-style-access`
//   Azure ADLS: `adls.account-name`, `adls.account-key`,
//               `adls.sas-token`, `adls.bearer-token`

use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use iceberg::Catalog;

use crate::debug_println;

/// Create an Iceberg catalog from a config HashMap.
///
/// The `catalog_type` key is required and selects the backend.
/// All remaining keys are passed as properties to the catalog builder,
/// which routes them internally to the appropriate configuration
/// (catalog settings, FileIO credentials, etc.).
pub async fn create_catalog(
    catalog_name: &str,
    config: &HashMap<String, String>,
) -> Result<Arc<dyn Catalog>> {
    let catalog_type = config
        .get("catalog_type")
        .ok_or_else(|| anyhow::anyhow!("Missing required config key: 'catalog_type'. Supported values: rest, glue, hms"))?;

    // Clone config and remove catalog_type — remaining keys are catalog properties
    let mut props = config.clone();
    props.remove("catalog_type");

    debug_println!(
        "🔧 ICEBERG_CATALOG: Creating '{}' catalog '{}' with {} properties",
        catalog_type,
        catalog_name,
        props.len()
    );

    match catalog_type.as_str() {
        "rest" => {
            use iceberg::CatalogBuilder;
            let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
                .load(catalog_name, props)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create REST catalog '{}': {}", catalog_name, e))?;
            Ok(Arc::new(catalog))
        }
        "glue" => {
            use iceberg::CatalogBuilder;
            let catalog = iceberg_catalog_glue::GlueCatalogBuilder::default()
                .load(catalog_name, props)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create Glue catalog '{}': {}", catalog_name, e))?;
            Ok(Arc::new(catalog))
        }
        "hms" => {
            use iceberg::CatalogBuilder;
            let catalog = iceberg_catalog_hms::HmsCatalogBuilder::default()
                .load(catalog_name, props)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create HMS catalog '{}': {}", catalog_name, e))?;
            Ok(Arc::new(catalog))
        }
        other => Err(anyhow::anyhow!(
            "Unsupported catalog_type '{}'. Supported values: rest, glue, hms",
            other
        )),
    }
}
