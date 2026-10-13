//! A fast append: new data files committed as one `append` snapshot on `main`
//! (§49 §6.1 step 3).
//!
//! HS1 Task 11 needs snapshots to read, so this is the smallest append that
//! is valid Iceberg: one new manifest holding the files, a manifest list with
//! the parent's manifests and the new one, the snapshot with its summary totals,
//! and the commit's requirement that `main` is still the parent. HS1 Task 12
//! builds the House's write path on it (column statistics from Parquet
//! footers, group commit, rebase and retry).
//!
//! Nothing here touches a store: the manifest and the manifest list come back as
//! bytes with their locations, for the caller to write before it commits.
//! iceberg's in-memory `FileIO` is only the writers' scratch space.

use std::collections::HashMap;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, ManifestList,
    ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot, SnapshotReference,
    SnapshotRetention, Struct, Summary, TableMetadata,
};
use iceberg::{TableRequirement, TableUpdate};

use crate::metadata::LakeError;

/// The branch the House reads and writes.
pub const MAIN: &str = "main";

/// A data file already written to the table's location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDataFile {
    /// Its absolute location (`s3://bucket/…/data/<uuid>.parquet`).
    pub location: String,
    /// Its rows.
    pub record_count: u64,
    /// Its size.
    pub size_bytes: u64,
    /// Its partition tuple under the table's default spec (`Struct::empty()`
    /// for an unpartitioned table).
    pub partition: Struct,
}

/// An append ready to commit.
#[derive(Debug, Clone)]
pub struct Append {
    /// The new snapshot's id.
    pub snapshot_id: i64,
    /// What to write before the commit: the manifest and the manifest list, by
    /// absolute location.
    pub files: Vec<(String, Bytes)>,
    /// `main` is still the parent snapshot (`assert-ref-snapshot-id`).
    pub requirements: Vec<TableRequirement>,
    /// The snapshot, and `main` moved to it.
    pub updates: Vec<TableUpdate>,
}

fn invalid(what: &str, err: impl std::fmt::Display) -> LakeError {
    LakeError::Invalid(format!("{what}: {err}"))
}

/// A snapshot id the table does not have yet: positive, random.
fn fresh_snapshot_id(metadata: &TableMetadata) -> i64 {
    loop {
        let bits = uuid::Uuid::new_v4().as_u128();
        let id = ((bits as u64) & (i64::MAX as u64)) as i64;
        if id != 0 && metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

/// The summary key's value in the parent, as a number.
fn total(parent: Option<&Summary>, key: &str) -> u64 {
    parent
        .and_then(|s| s.additional_properties.get(key))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Builds the append of `files` onto the table's current snapshot.
/// `parent_manifest_list` is the bytes of the current snapshot's manifest list
/// (`None` when the table has no snapshot). The snapshot's time is
/// `timestamp_ms`, or the table's last update when that is later (Iceberg
/// refuses a snapshot older than its table's last change). Format v3 tables
/// need row lineage, which HS1 Task 12 adds; they are refused here.
pub async fn fast_append(
    metadata: &TableMetadata,
    parent_manifest_list: Option<&[u8]>,
    files: Vec<NewDataFile>,
    timestamp_ms: i64,
) -> Result<Append, LakeError> {
    let version = metadata.format_version();
    if version != FormatVersion::V2 {
        return Err(LakeError::Invalid(format!(
            "appends to a format {version} table are not supported yet (HS1 Task 12)"
        )));
    }
    let parent = metadata.current_snapshot();
    if parent.is_some() != parent_manifest_list.is_some() {
        return Err(LakeError::Invalid(
            "the parent's manifest list is needed exactly when the table has a snapshot"
                .to_string(),
        ));
    }
    let snapshot_id = fresh_snapshot_id(metadata);
    let sequence_number = metadata.next_sequence_number();
    let schema = metadata.current_schema().clone();
    let spec = metadata.default_partition_spec().as_ref().clone();
    let base = format!("{}/metadata", metadata.location().trim_end_matches('/'));
    let token = uuid::Uuid::new_v4();
    let manifest_location = format!("{base}/{token}-m0.avro");
    let list_location = format!("{base}/snap-{snapshot_id}-1-{token}.avro");
    let io = FileIO::new_with_memory();

    let (mut added_records, mut added_bytes) = (0u64, 0u64);
    let output = io
        .new_output(&manifest_location)
        .map_err(|e| invalid("the manifest", e))?;
    let mut writer =
        ManifestWriterBuilder::new(output, Some(snapshot_id), schema.clone(), spec.clone())
            .build_v2_data();
    let added_files = files.len() as u64;
    for file in files {
        added_records += file.record_count;
        added_bytes += file.size_bytes;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file.location)
            .file_format(DataFileFormat::Parquet)
            .partition(file.partition)
            .partition_spec_id(spec.spec_id())
            .record_count(file.record_count)
            .file_size_in_bytes(file.size_bytes)
            .build()
            .map_err(|e| invalid("a data file", e))?;
        writer
            .add_file(data_file, sequence_number)
            .map_err(|e| invalid("a data file", e))?;
    }
    let manifest = writer
        .write_manifest_file()
        .await
        .map_err(|e| invalid("the manifest", e))?;
    let manifest_bytes = io
        .new_input(&manifest_location)
        .map_err(|e| invalid("the manifest", e))?
        .read()
        .await
        .map_err(|e| invalid("the manifest", e))?;

    let list_output = io
        .new_output(&list_location)
        .map_err(|e| invalid("the manifest list", e))?;
    let mut list = ManifestListWriter::v2(
        list_output
            .writer()
            .await
            .map_err(|e| invalid("the manifest list", e))?,
        snapshot_id,
        parent.map(|p| p.snapshot_id()),
        sequence_number,
    );
    if let Some(bytes) = parent_manifest_list {
        let previous = ManifestList::parse_with_version(bytes, version)
            .map_err(|e| invalid("the parent's manifest list", e))?;
        list.add_manifests(previous.consume_entries().into_iter())
            .map_err(|e| invalid("the manifest list", e))?;
    }
    list.add_manifests(std::iter::once(manifest))
        .map_err(|e| invalid("the manifest list", e))?;
    list.close()
        .await
        .map_err(|e| invalid("the manifest list", e))?;
    let list_bytes = io
        .new_input(&list_location)
        .map_err(|e| invalid("the manifest list", e))?
        .read()
        .await
        .map_err(|e| invalid("the manifest list", e))?;

    let previous = parent.map(|p| p.summary());
    let mut properties = HashMap::new();
    let mut put = |key: &str, value: u64| {
        properties.insert(key.to_string(), value.to_string());
    };
    put("added-data-files", added_files);
    put("added-records", added_records);
    put("added-files-size", added_bytes);
    put(
        "total-data-files",
        total(previous, "total-data-files") + added_files,
    );
    put(
        "total-records",
        total(previous, "total-records") + added_records,
    );
    put(
        "total-files-size",
        total(previous, "total-files-size") + added_bytes,
    );
    for key in [
        "total-delete-files",
        "total-position-deletes",
        "total-equality-deletes",
    ] {
        put(key, total(previous, key));
    }
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent.map(|p| p.snapshot_id()))
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(timestamp_ms.max(metadata.last_updated_ms()))
        .with_manifest_list(list_location.clone())
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: properties,
        })
        .with_schema_id(schema.schema_id())
        .build();
    Ok(Append {
        snapshot_id,
        files: vec![
            (manifest_location, manifest_bytes),
            (list_location, list_bytes),
        ],
        requirements: vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN.to_string(),
            snapshot_id: parent.map(|p| p.snapshot_id()),
        }],
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ],
    })
}
