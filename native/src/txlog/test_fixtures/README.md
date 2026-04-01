# Transaction Log Test Fixtures

## Generating Scala-written fixtures

To generate true Scala-written transaction log fixtures for cross-compatibility testing:

1. Build the indextables_spark project:
   ```bash
   cd /Users/schenksj/tmp/x/it_native_refactor/indextables_spark
   git checkout v0.5.4
   mvn clean package -DskipTests
   ```

2. Run the fixture generator:
   ```bash
   java -cp target/indextables_spark-*.jar \
     io.indextables.spark.test.TxlogFixtureGenerator \
     /path/to/output/fixtures
   ```

3. Copy the generated fixtures here and commit them.

## Fixture types needed

- `simple/` — 1 version file + 1 checkpoint, 3 files, no partitions
- `multi_manifest/` — checkpoint with >50k entries (multiple manifest files)
- `tombstones/` — checkpoint with tombstones from file removes
- `shared_manifests/` — multiple state versions referencing shared manifests/
- `schema_dedup/` — entries with docMappingRef + schemaRegistry
- `incremental/` — multiple incremental checkpoints
- `compacted/` — compacted checkpoint after tombstone cleanup
- `partitioned/` — table with partition columns and partition bounds
- `post_truncate/` — checkpoint with cached metadata (version 0 deleted)

## Current status

Protocol regression tests in `protocol_regression_tests.rs` use Rust-generated
Avro binary fixtures (self-roundtrip). These validate our writer produces
Scala-compatible output.

True Scala-generated fixtures are TODO — requires building the Scala project.
