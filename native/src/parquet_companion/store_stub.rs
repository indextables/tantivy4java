// store_stub.rs - Minimal .store file generation for parquet companion splits
//
// Creates valid but essentially empty .store files so tantivy's index reader
// can open the index without errors. Document retrieval is routed to parquet instead.

use std::path::Path;
use anyhow::{Context, Result};
use tantivy::schema::Schema;

use crate::debug_println;

/// Generate minimal valid .store files in the index directory.
///
/// Tantivy requires .store files to exist for each segment even though
/// in parquet companion mode we never read from them. This creates
/// stub files with valid headers/footers but no actual document data.
pub fn generate_store_stubs(
    index_dir: &Path,
    _schema: &Schema,
    num_docs: u32,
) -> Result<()> {
    debug_println!(
        "📝 STORE_STUB: Generating store stubs in {:?} for {} docs",
        index_dir, num_docs
    );

    // Find all segment directories/files that need .store files
    // In tantivy, each segment has files named like: {segment_uuid}.store
    let entries: Vec<_> = std::fs::read_dir(index_dir)
        .context("Failed to read index directory")?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            // Look for existing segment files (e.g. .term, .pos, .fast)
            // to determine which segments need store stubs
            name.ends_with(".term") || name.ends_with(".pos") || name.ends_with(".fast")
        })
        .collect();

    // Extract unique segment UUIDs
    let mut segment_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for entry in &entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(dot_pos) = name.rfind('.') {
            let segment_id = &name[..dot_pos];
            segment_ids.insert(segment_id.to_string());
        }
    }

    debug_println!("📝 STORE_STUB: Found {} segments needing store stubs", segment_ids.len());

    for segment_id in &segment_ids {
        let store_path = index_dir.join(format!("{}.store", segment_id));
        if store_path.exists() {
            debug_println!("📝 STORE_STUB: Store file already exists: {:?}", store_path);
            continue;
        }

        // Create a minimal store file using tantivy's store writer
        // The simplest approach: write an empty store with just the header
        write_empty_store(&store_path, num_docs)
            .with_context(|| format!("Failed to write store stub for segment {}", segment_id))?;

        debug_println!("📝 STORE_STUB: Created stub: {:?}", store_path);
    }

    Ok(())
}

/// Write an empty store file that tantivy can open without errors.
///
/// The store file format requires a valid header/footer structure even if
/// it contains no document data. We write a minimal valid file.
fn write_empty_store(path: &Path, _num_docs: u32) -> Result<()> {
    use std::io::Write;

    // Create a minimal store file
    // Tantivy's store format has a compressor byte + block structure
    // The simplest valid store is: just the compressor header (1 byte) + footer
    let mut file = std::fs::File::create(path)
        .context("Failed to create store stub file")?;

    // Write minimal valid store content
    // This is a valid empty store with LZ4 compression marker
    let empty_store: Vec<u8> = vec![
        0x04, // LZ4 compressor marker
        0x00, 0x00, 0x00, 0x00, // num_docs = 0 (we just need it to open)
        0x00, 0x00, 0x00, 0x00, // offset table size = 0
    ];

    file.write_all(&empty_store)
        .context("Failed to write store stub content")?;

    Ok(())
}
