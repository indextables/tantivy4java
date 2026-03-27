// txlog/avro/state_reader.rs - Read state-v<N>/ directory (manifest list + parts)

use crate::debug_println;
use crate::txlog::actions::{FileEntry, StateManifest};
use crate::txlog::error::{TxLogError, Result};
use crate::txlog::schema_dedup;
use crate::txlog::storage::TxLogStorage;

use super::manifest_reader;

/// Read the StateManifest from a state directory.
pub async fn read_state_manifest(
    storage: &TxLogStorage,
    state_dir: &str,
) -> Result<StateManifest> {
    let manifest_path = format!("{}/_manifest", state_dir);
    let data = storage.get(&manifest_path).await?;
    let manifest: StateManifest = serde_json::from_slice(&data)?;
    debug_println!("📖 STATE_READER: Read state manifest at {} with {} manifests, {} total files",
        state_dir, manifest.manifests.len(), manifest.total_file_count);
    Ok(manifest)
}

/// Read all manifests in a state directory and return all FileEntry records.
/// Reads manifests concurrently with a bounded concurrency limit to avoid
/// overwhelming S3 with hundreds of concurrent GETs.
pub async fn read_all_manifests(
    storage: &TxLogStorage,
    state_dir: &str,
    state_manifest: &StateManifest,
    metadata_config: &std::collections::HashMap<String, String>,
) -> Result<Vec<FileEntry>> {
    use futures::stream::{self, StreamExt};

    const MAX_CONCURRENT_MANIFEST_READS: usize = 16;

    let manifest_futures = state_manifest.manifests.iter().map(|info| {
        let manifest_path = format!("{}/{}", state_dir, info.path);
        let storage_ref = storage;
        async move {
            let data = storage_ref.get(&manifest_path).await?;
            manifest_reader::read_manifest_bytes(&data)
        }
    });

    let results: Vec<Result<Vec<FileEntry>>> = stream::iter(manifest_futures)
        .buffer_unordered(MAX_CONCURRENT_MANIFEST_READS)
        .collect()
        .await;

    let mut all_entries = Vec::with_capacity(state_manifest.total_file_count as usize);
    for result in results {
        let mut entries = result?;
        all_entries.append(&mut entries);
    }

    // Restore schemas from dedup refs
    schema_dedup::restore_schemas(&mut all_entries, metadata_config);

    debug_println!("📖 STATE_READER: Read {} total entries from {} manifests",
        all_entries.len(), state_manifest.manifests.len());
    Ok(all_entries)
}

/// Read a single manifest from a state directory.
pub async fn read_single_manifest(
    storage: &TxLogStorage,
    state_dir: &str,
    manifest_path: &str,
    metadata_config: &std::collections::HashMap<String, String>,
) -> Result<Vec<FileEntry>> {
    let full_path = format!("{}/{}", state_dir, manifest_path);
    let data = storage.get(&full_path).await?;
    let mut entries = manifest_reader::read_manifest_bytes(&data)?;
    schema_dedup::restore_schemas(&mut entries, metadata_config);
    Ok(entries)
}
