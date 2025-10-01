# Plan: Replace Custom Merge with Quickwit's Implementation (Minimal Fork Changes)

## Overview

This plan addresses the `FileDoesNotExist("meta.json")` error in our merge operations by using Quickwit's actual merge implementation. Since the existing `merge_split_directories` method is tightly coupled to the actor system, we need to add ONE new standalone function to Quickwit that extracts the existing merge logic without actor dependencies.

## Root Cause Analysis

**Current Issue:**
- Our `perform_segment_merge` creates a UnionDirectory with merged segments
- The output directory contains only segment files, missing `meta.json`
- `Index::open` fails because there's no metadata file

**Quickwit's Complete Solution:**
- Uses `create_shadowing_meta_json_directory` to create a RamDirectory with meta.json
- Builds directory stack with meta.json as the second layer
- UnionDirectory reads meta.json from the shadowing directory
- After merge, `Index::open` succeeds because meta.json is available

**Why We Need One New Function:**
- Existing `merge_split_directories` is a method on `MergeExecutor` (requires `&self`)
- Takes `ActorContext<MergeExecutor>` parameter for progress tracking
- Uses `self.io_controls` from the actor instance
- Cannot be called standalone without actor system

**Minimal Solution:**
- Add ONE new function to Quickwit that extracts existing merge logic
- Remove actor dependencies (replace with parameters)
- Use Quickwit's exact merge implementation
- Make helper functions public

## Phase 1: Quickwit Fork Changes (Minimal)

### Required Function Visibility Changes

The following functions in `quickwit-indexing/src/actors/merge_executor.rs` need to be made public:

```rust
// Currently private, need to make public:
pub fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta>

pub fn open_split_directories(
    tantivy_dirs: &[Box<dyn Directory>],
    tokenizer_manager: &TokenizerManager
) -> anyhow::Result<(IndexMeta, Vec<Box<dyn Directory>>)>

pub fn create_shadowing_meta_json_directory(index_meta: IndexMeta) -> anyhow::Result<RamDirectory>

pub fn open_index<T: Into<Box<dyn Directory>>>(
    directory: T,
    tokenizer_manager: &TokenizerManager
) -> tantivy::Result<Index>
```

### Add ONE New Standalone Function

Add this new function to `quickwit-indexing/src/actors/merge_executor.rs` (extracts existing logic):

```rust
/// Standalone version of merge_split_directories for external use
/// This is a copy-paste of the existing merge_split_directories logic
/// but without actor system dependencies
pub async fn merge_split_directories_standalone(
    union_index_meta: IndexMeta,
    split_directories: Vec<Box<dyn Directory>>,
    delete_tasks: Vec<DeleteTask>,
    doc_mapper_opt: Option<Arc<DocMapper>>,
    output_path: &Path,
    io_controls: IoControls,
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<ControlledDirectory> {
    // This is literally copy-paste from existing merge_split_directories method
    let shadowing_meta_json_directory = create_shadowing_meta_json_directory(union_index_meta)?;

    // This directory is here to receive the merged split, as well as the final meta.json file.
    let output_directory = ControlledDirectory::new(
        Box::new(MmapDirectory::open_with_madvice(
            output_path,
            Advice::Sequential,
        )?),
        io_controls, // Parameter instead of self.io_controls
    );
    let mut directory_stack: Vec<Box<dyn Directory>> = vec![
        output_directory.box_clone(),
        Box::new(shadowing_meta_json_directory),
    ];
    directory_stack.extend(split_directories.into_iter());
    let union_directory = UnionDirectory::union_of(directory_stack);
    let union_index = open_index(union_directory, tokenizer_manager)?;

    let mut index_writer: IndexWriter = union_index.writer_with_num_threads(1, 15_000_000)?;
    let num_delete_tasks = delete_tasks.len();
    if num_delete_tasks > 0 {
        let doc_mapper = doc_mapper_opt
            .ok_or_else(|| anyhow!("doc mapper must be present if there are delete tasks"))?;
        for delete_task in delete_tasks {
            let delete_query = delete_task
                .delete_query
                .expect("A delete task must have a delete query.");
            let query_ast: QueryAst = serde_json::from_str(&delete_query.query_ast)
                .context("invalid query_ast json")?;
            let parsed_query_ast = query_ast.parse_user_query(&[]).context("invalid query")?;
            let (query, _) = doc_mapper.query(union_index.schema(), &parsed_query_ast, false)?;
            index_writer.delete_query(query)?;
        }
        index_writer.commit()?;
    }
    let segment_ids: Vec<SegmentId> = union_index
        .searchable_segment_metas()?
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect();
    if num_delete_tasks == 0 && segment_ids.len() <= 1 {
        return Ok(output_directory);
    }
    if num_delete_tasks != 0 && segment_ids.is_empty() {
        return Ok(output_directory);
    }
    // This is the key line - Quickwit's actual merge logic
    index_writer.merge(&segment_ids).await?;
    Ok(output_directory)
}
```

### Required Module Exports

Update `quickwit-indexing/src/lib.rs` to export the merge utilities:

```rust
pub use crate::actors::merge_executor::{
    combine_index_meta,
    open_split_directories,
    create_shadowing_meta_json_directory,
    open_index,
    merge_split_directories_standalone,
};
```

## Phase 2: tantivy4java Implementation Using Quickwit's Merge Logic

### 1. Update Function Visibility in Quickwit

Change the existing private functions to public:

```rust
// Change from private to public
pub fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta> {
    let mut union_index_meta = index_metas.pop().with_context(|| "only one IndexMeta")?;
    for index_meta in index_metas {
        union_index_meta.segments.extend(index_meta.segments);
    }
    Ok(union_index_meta)
}

pub fn open_split_directories(
    tantivy_dirs: &[Box<dyn Directory>],
    tokenizer_manager: &TokenizerManager
) -> anyhow::Result<(IndexMeta, Vec<Box<dyn Directory>>)> {
    let mut directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas = Vec::new();
    for tantivy_dir in tantivy_dirs {
        directories.push(tantivy_dir.clone());
        let index_meta = open_index(tantivy_dir.clone(), tokenizer_manager)?.load_metas()?;
        index_metas.push(index_meta);
    }
    let union_index_meta = combine_index_meta(index_metas)?;
    Ok((union_index_meta, directories))
}

pub fn create_shadowing_meta_json_directory(index_meta: IndexMeta) -> anyhow::Result<RamDirectory> {
    let union_index_meta_json = serde_json::to_string_pretty(&index_meta)?;
    let ram_directory = RamDirectory::default();
    ram_directory.atomic_write(Path::new("meta.json"), union_index_meta_json.as_bytes())?;
    Ok(ram_directory)
}

pub fn open_index<T: Into<Box<dyn Directory>>>(
    directory: T,
    tokenizer_manager: &TokenizerManager
) -> tantivy::Result<Index> {
    let mut index = Index::open(directory)?;
    index.set_tokenizers(tokenizer_manager.clone());
    index.set_fast_field_tokenizers(
        get_quickwit_fastfield_normalizer_manager()
            .tantivy_manager()
            .clone(),
    );
    Ok(index)
}
```

## Phase 3: tantivy4java Implementation Updates

### 1. Verify Cargo.toml Dependencies

The current dependencies are already correct since we have:
```toml
quickwit-indexing = { path = "../../quickwit/quickwit/quickwit-indexing" }
quickwit-doc-mapper = { path = "../../quickwit/quickwit/quickwit-doc-mapper" }
quickwit-query = { path = "../../quickwit/quickwit/quickwit-query" }
quickwit-common = { path = "../../quickwit/quickwit/quickwit-common" }
```

### 2. Replace Custom Merge Implementation

In `quickwit_split.rs`, add imports and replace the `perform_segment_merge` function:

```rust
// Add imports
use quickwit_indexing::{
    combine_index_meta,
    open_split_directories,
    merge_split_directories_standalone,
    open_index,
};
use quickwit_common::io::IoControls;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;

// Replace perform_segment_merge with this:
async fn perform_quickwit_merge(
    split_directories: Vec<Box<dyn Directory>>,
    output_path: &Path,
) -> Result<usize> {
    debug_log!("Performing Quickwit merge with {} directories", split_directories.len());

    // Step 1: Use Quickwit's helper to combine metadata
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let (union_index_meta, directories) = open_split_directories(&split_directories, tokenizer_manager)?;
    debug_log!("Combined metadata from {} splits", directories.len());

    // Step 2: Create IO controls for the merge operation
    let io_controls = IoControls::default();

    // Step 3: Use Quickwit's exact merge implementation
    let controlled_directory = merge_split_directories_standalone(
        union_index_meta,
        directories,
        Vec::new(), // No delete tasks for split merging
        None,       // No doc mapper needed for split merging
        output_path,
        io_controls,
        tokenizer_manager,
    ).await?;

    // Step 4: Open the merged index to get document count
    let merged_index = open_index(controlled_directory.clone(), tokenizer_manager)?;
    let reader = merged_index.reader()?;
    let searcher = reader.searcher();
    let doc_count = searcher.num_docs();

    debug_log!("Quickwit merge completed with {} documents", doc_count);
    Ok(doc_count as usize)
}
```

### 3. Update Main Merge Function

Replace the current merge logic in `merge_splits_impl_async`:

```rust
// Replace this section:
// let merged_docs = runtime.block_on(perform_segment_merge(&union_index))?;

// With this:
let merged_docs = runtime.block_on(perform_quickwit_merge(
    extracted_directories,
    &output_temp_dir,
))?;
```

### 4. Simplify Index Opening

Update the `create_merged_split_file` function to use Quickwit's `open_index`:

```rust
fn create_merged_split_file(merged_index_path: &Path, output_path: &str, metadata: &QuickwitSplitMetadata) -> Result<FooterOffsets> {
    debug_log!("Creating merged split file at {} from index {:?}", output_path, merged_index_path);

    // Use Quickwit's open_index function with proper tokenizer setup
    let merged_directory = MmapDirectory::open(merged_index_path)?;
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let merged_index = open_index(Box::new(merged_directory), tokenizer_manager)
        .map_err(|e| {
            let error_msg = e.to_string();
            if is_data_corruption_error(&error_msg) {
                anyhow!("Merged index file appears to be corrupted during split creation. This may indicate a problem with the merge process. Error: {}", error_msg)
            } else {
                anyhow!("Failed to open merged index for split creation: {}", error_msg)
            }
        })?;

    // Rest of the function remains the same...
}
```

## Phase 4: Testing and Validation

### 1. Update Test Cases

The existing test cases should work without modification since we're maintaining the same external API, just using Quickwit's implementation internally.

### 2. Validation Steps

1. **meta.json Creation**: Verify that merged indices have proper meta.json files
2. **Index::open Success**: Confirm that Index::open no longer fails with FileDoesNotExist
3. **Document Count Accuracy**: Ensure merged document counts match expectations
4. **S3 Compatibility**: Test S3 upload/download with Quickwit-merged splits
5. **Error Handling**: Verify graceful handling of corrupted splits

### 3. Test Commands

```bash
# Test basic merge functionality
TANTIVY4JAVA_DEBUG=1 mvn test -Dtest="RealS3EndToEndTest#step3_mergeSplitsFromS3" -q

# Test with various split configurations
TANTIVY4JAVA_DEBUG=1 mvn test -Dtest="S3MergeMockTest" -q

# Validate meta.json creation
TANTIVY4JAVA_DEBUG=1 mvn test -Dtest="MergeResilienceValidationTest" -q
```

## Phase 5: Benefits and Improvements

### Immediate Benefits

1. **✅ Fixes meta.json Issue**: Quickwit's `create_shadowing_meta_json_directory` creates proper metadata
2. **✅ Production-Tested Logic**: Uses Quickwit's battle-tested merge implementation
3. **✅ Consistent Behavior**: Matches Quickwit's merge behavior exactly
4. **✅ Simplified Maintenance**: Reduces custom code that needs maintenance

### Code Reduction

- **Remove**: `perform_segment_merge` function (~80 lines of custom merge logic)
- **Remove**: Custom union directory creation
- **Remove**: Manual segment merging code
- **Add**: One call to Quickwit's merge function (~30 lines)
- **Add**: One new function to Quickwit (~50 lines, but copy-paste of existing logic)
- **Net Result**: ~50 lines of code reduction, using proven Quickwit merge implementation

### Future-Proofing

- **Automatic Updates**: Benefits from Quickwit merge improvements
- **Bug Fixes**: Inherits Quickwit bug fixes automatically
- **Performance**: Gets Quickwit performance optimizations
- **Compatibility**: Ensures perfect compatibility with Quickwit ecosystem

## Implementation Order

1. **First**: Update Quickwit fork with visibility changes (Phase 1-2)
2. **Second**: Update tantivy4java to use Quickwit functions (Phase 3)
3. **Third**: Test and validate the integration (Phase 4)
4. **Fourth**: Document improvements and cleanup (Phase 5)

## Risk Mitigation

- **Minimal Changes**: Only making targeted visibility changes to Quickwit
- **Backward Compatibility**: External API remains unchanged
- **Incremental Testing**: Test each phase independently
- **Rollback Plan**: Keep original implementation available until validation complete

## Success Criteria

- [ ] No more `FileDoesNotExist("meta.json")` errors
- [ ] All existing tests pass with new implementation
- [ ] Merged indices can be opened successfully with `Index::open`
- [ ] Document counts remain accurate after merge
- [ ] S3 operations continue to work correctly
- [ ] Performance is maintained or improved

This plan provides a systematic approach to replacing our custom merge logic with Quickwit's proven implementation, eliminating the meta.json issue while improving code maintainability and reliability.