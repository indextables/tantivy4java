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
/// Matches the Scala AvroSchemas.FileEntrySchema exactly.
pub const FILE_ENTRY_SCHEMA_JSON: &str = r#"{
  "type": "record",
  "name": "FileEntry",
  "namespace": "io.indextables.txlog",
  "fields": [
    {"name": "path",              "type": "string",            "order": "ascending", "field-id": 100},
    {"name": "size",              "type": "long",              "order": "ascending", "field-id": 101},
    {"name": "modificationTime",  "type": "long",              "order": "ascending", "field-id": 102},
    {"name": "dataChange",        "type": "boolean",           "default": true,      "field-id": 103},
    {"name": "partitionValues",   "type": {"type": "map", "values": "string"}, "default": {}, "field-id": 104},
    {"name": "stats",             "type": ["null", "string"],  "default": null,      "field-id": 110},
    {"name": "minValues",         "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 111},
    {"name": "maxValues",         "type": ["null", {"type": "map", "values": "string"}], "default": null, "field-id": 112},
    {"name": "numRecords",        "type": ["null", "long"],    "default": null,      "field-id": 113},
    {"name": "footerStartOffset", "type": ["null", "long"],    "default": null,      "field-id": 120},
    {"name": "footerEndOffset",   "type": ["null", "long"],    "default": null,      "field-id": 121},
    {"name": "hasFooterOffsets",  "type": ["null", "boolean"], "default": null,      "field-id": 122},
    {"name": "deleteOpstamp",    "type": ["null", "long"],    "default": null,      "field-id": 123},
    {"name": "splitTags",         "type": ["null", {"type": "array", "items": "string"}], "default": null, "field-id": 130},
    {"name": "numMergeOps",       "type": ["null", "int"],     "default": null,      "field-id": 131},
    {"name": "docMappingJson",    "type": ["null", "string"],  "default": null,      "field-id": 132},
    {"name": "docMappingRef",     "type": ["null", "string"],  "default": null,      "field-id": 133},
    {"name": "uncompressedSizeBytes", "type": ["null", "long"], "default": null,     "field-id": 134},
    {"name": "timeRangeStart",    "type": ["null", "long"],    "default": null,      "field-id": 135},
    {"name": "timeRangeEnd",      "type": ["null", "long"],    "default": null,      "field-id": 136},
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_parses() {
        let schema = file_entry_schema();
        // Should be a Record type
        match &schema {
            apache_avro::Schema::Record(r) => {
                assert_eq!(r.name.fullname(None), "io.indextables.txlog.FileEntry");
                assert_eq!(r.fields.len(), 25);
            }
            other => panic!("expected Record schema, got {:?}", other),
        }
    }
}
