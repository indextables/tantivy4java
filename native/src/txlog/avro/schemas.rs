// txlog/avro/schemas.rs - Avro schema definitions for FileEntry
//
// Field IDs match Scala's AvroSchemas.scala for backward compatibility:
//   100-109: Basic file info
//   110-119: Statistics
//   120-129: Footer offsets
//   130-139: Split metadata
//   140-149: Streaming info
//   150-159: Companion mode

/// Raw JSON schema for FileEntry Avro records.
/// Matches the Scala AvroSchemas.FileEntrySchema exactly — same namespace,
/// field order, field-ids, and types.
pub const FILE_ENTRY_SCHEMA_JSON: &str = r#"{
  "type": "record",
  "name": "FileEntry",
  "namespace": "io.indextables.state",
  "fields": [
    {"name": "path",              "type": "string",            "field-id": 100},
    {"name": "partitionValues",   "type": {"type": "map", "values": "string"}, "field-id": 101},
    {"name": "size",              "type": "long",              "field-id": 102},
    {"name": "modificationTime",  "type": "long",              "field-id": 103},
    {"name": "dataChange",        "type": "boolean",           "field-id": 104},
    {"name": "stats",             "type": ["null", "string"],  "default": null,      "field-id": 110},
    {"name": "minValues",         "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 111},
    {"name": "maxValues",         "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 112},
    {"name": "numRecords",        "type": ["null", "long"],    "default": null,      "field-id": 113},
    {"name": "footerStartOffset", "type": ["null", "long"],    "default": null,      "field-id": 120},
    {"name": "footerEndOffset",   "type": ["null", "long"],    "default": null,      "field-id": 121},
    {"name": "hasFooterOffsets",  "type": "boolean",           "default": false,     "field-id": 124},
    {"name": "splitTags",         "type": ["null", {"type": "array", "items": "string"}], "default": null, "field-id": 132},
    {"name": "numMergeOps",       "type": ["null", "int"],     "default": null,      "field-id": 134},
    {"name": "docMappingRef",     "type": ["null", "string"],  "default": null,      "field-id": 135},
    {"name": "uncompressedSizeBytes", "type": ["null", "long"], "default": null,     "field-id": 136},
    {"name": "addedAtVersion",    "type": "long",              "default": 0,         "field-id": 140},
    {"name": "addedAtTimestamp",  "type": "long",              "default": 0,         "field-id": 141},
    {"name": "companionSourceFiles",    "type": ["null", {"type": "array", "items": "string"}], "default": null, "field-id": 150},
    {"name": "companionDeltaVersion",   "type": ["null", "long"],    "default": null, "field-id": 151},
    {"name": "companionFastFieldMode",  "type": ["null", "string"],  "default": null, "field-id": 152}
  ]
}"#;

/// Parse and return the FileEntry Avro schema.
pub fn file_entry_schema() -> apache_avro::Schema {
    apache_avro::Schema::parse_str(FILE_ENTRY_SCHEMA_JSON)
        .expect("FileEntry Avro schema must parse")
}

/// Raw JSON schema for StateManifest Avro records.
/// Matches the Scala StateManifest schema exactly.
pub const STATE_MANIFEST_SCHEMA_JSON: &str = r#"{
  "type": "record",
  "name": "StateManifest",
  "namespace": "io.indextables.state",
  "fields": [
    {"name": "formatVersion", "type": "int"},
    {"name": "stateVersion", "type": "long"},
    {"name": "createdAt", "type": "long"},
    {"name": "numFiles", "type": "long"},
    {"name": "totalBytes", "type": "long"},
    {"name": "manifests", "type": {"type": "array", "items": {"type": "record", "name": "ManifestInfoItem", "fields": [
      {"name": "path", "type": "string"},
      {"name": "numEntries", "type": "long"},
      {"name": "minAddedAtVersion", "type": "long"},
      {"name": "maxAddedAtVersion", "type": "long"},
      {"name": "partitionBounds", "type": ["null", {"type": "map", "values": {"type": "record", "name": "PartitionBoundsItem", "fields": [
        {"name": "min", "type": ["null", "string"], "default": null},
        {"name": "max", "type": ["null", "string"], "default": null}
      ]}}], "default": null},
      {"name": "tombstoneCount", "type": "long", "default": 0},
      {"name": "liveEntryCount", "type": "long", "default": -1}
    ]}}},
    {"name": "tombstones", "type": {"type": "array", "items": "string"}, "default": []},
    {"name": "schemaRegistry", "type": {"type": "map", "values": "string"}, "default": {}},
    {"name": "protocolVersion", "type": "int", "default": 4},
    {"name": "metadata", "type": ["null", "string"], "default": null}
  ]
}"#;

/// Parse and return the StateManifest Avro schema.
pub fn state_manifest_schema() -> apache_avro::Schema {
    apache_avro::Schema::parse_str(STATE_MANIFEST_SCHEMA_JSON)
        .expect("StateManifest Avro schema must parse")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_entry_schema_parses() {
        let schema = file_entry_schema();
        match &schema {
            apache_avro::Schema::Record(r) => {
                assert_eq!(r.name.fullname(None), "io.indextables.state.FileEntry");
                assert_eq!(r.fields.len(), 21);
            }
            other => panic!("expected Record schema, got {:?}", other),
        }
    }

    #[test]
    fn test_state_manifest_schema_parses() {
        let schema = state_manifest_schema();
        match &schema {
            apache_avro::Schema::Record(r) => {
                assert_eq!(r.name.fullname(None), "io.indextables.state.StateManifest");
                assert_eq!(r.fields.len(), 10);
            }
            other => panic!("expected Record schema, got {:?}", other),
        }
    }
}
