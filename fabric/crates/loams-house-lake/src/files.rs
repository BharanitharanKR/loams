//! The data files of a snapshot, read from its manifest list and manifests
//! (HS1 Task 11): `system.parts` has one row per file, and the `FINAL` views of
//! Replacing and Summing tables order rows by their file's sequence number.
//!
//! The caller reads the Avro files (through `loams-store`); this module only
//! parses them.

use iceberg::spec::{
    DataContentType, FormatVersion, Literal, Manifest, ManifestContentType, ManifestList,
    ManifestStatus, PrimitiveLiteral, Struct,
};

use crate::metadata::LakeError;

/// A manifest a snapshot's list names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRef {
    /// Its absolute location.
    pub location: String,
    /// The data sequence number its added files inherit.
    pub sequence_number: i64,
    /// Whether it lists delete files rather than data files.
    pub deletes: bool,
}

/// A live file of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFile {
    /// Its absolute location.
    pub location: String,
    /// Its rows.
    pub record_count: u64,
    /// Its size.
    pub size_bytes: u64,
    /// Its partition tuple, as ClickHouse's `system.parts.partition` prints one
    /// (`tuple()` when unpartitioned, the value alone for one field).
    pub partition: String,
    /// Its data sequence number.
    pub sequence_number: i64,
    /// A data file (not a position or equality delete file).
    pub data: bool,
}

/// The manifests of a snapshot's manifest list.
pub fn manifests(list: &[u8], version: FormatVersion) -> Result<Vec<ManifestRef>, LakeError> {
    let list = ManifestList::parse_with_version(list, version)
        .map_err(|e| LakeError::Invalid(format!("a manifest list: {e}")))?;
    Ok(list
        .entries()
        .iter()
        .map(|m| ManifestRef {
            location: m.manifest_path.clone(),
            sequence_number: m.sequence_number,
            deletes: m.content == ManifestContentType::Deletes,
        })
        .collect())
}

/// The live files of one manifest (added or existing; deleted entries are
/// history).
pub fn live_files(manifest: &[u8], of: &ManifestRef) -> Result<Vec<LiveFile>, LakeError> {
    let manifest = Manifest::parse_avro(manifest)
        .map_err(|e| LakeError::Invalid(format!("manifest {}: {e}", of.location)))?;
    Ok(manifest
        .entries()
        .iter()
        .filter(|entry| entry.is_alive())
        .map(|entry| {
            let file = entry.data_file();
            LiveFile {
                location: file.file_path().to_string(),
                record_count: file.record_count(),
                size_bytes: file.file_size_in_bytes(),
                partition: partition_text(file.partition()),
                sequence_number: entry.sequence_number().unwrap_or(of.sequence_number),
                data: file.content_type() == DataContentType::Data,
            }
        })
        .collect())
}

/// The data manifests of a snapshot's list that the snapshot itself added
/// (HS1 Task 17: the files one commit brought).
pub fn manifests_added_by(
    list: &[u8],
    version: FormatVersion,
    snapshot_id: i64,
) -> Result<Vec<ManifestRef>, LakeError> {
    let list = ManifestList::parse_with_version(list, version)
        .map_err(|e| LakeError::Invalid(format!("a manifest list: {e}")))?;
    Ok(list
        .entries()
        .iter()
        .filter(|m| m.added_snapshot_id == snapshot_id && m.content == ManifestContentType::Data)
        .map(|m| ManifestRef {
            location: m.manifest_path.clone(),
            sequence_number: m.sequence_number,
            deletes: false,
        })
        .collect())
}

/// The data files a manifest lists as added by `snapshot_id`.
pub fn added_files(
    manifest: &[u8],
    of: &ManifestRef,
    snapshot_id: i64,
) -> Result<Vec<LiveFile>, LakeError> {
    let manifest = Manifest::parse_avro(manifest)
        .map_err(|e| LakeError::Invalid(format!("manifest {}: {e}", of.location)))?;
    Ok(manifest
        .entries()
        .iter()
        .filter(|entry| {
            entry.status() == ManifestStatus::Added
                && entry.snapshot_id().is_none_or(|id| id == snapshot_id)
                && entry.data_file().content_type() == DataContentType::Data
        })
        .map(|entry| {
            let file = entry.data_file();
            LiveFile {
                location: file.file_path().to_string(),
                record_count: file.record_count(),
                size_bytes: file.file_size_in_bytes(),
                partition: partition_text(file.partition()),
                sequence_number: entry.sequence_number().unwrap_or(of.sequence_number),
                data: true,
            }
        })
        .collect())
}

/// A partition tuple as ClickHouse prints a partition: `tuple()`, the value of
/// a single field, or `(a, b)`.
fn partition_text(partition: &Struct) -> String {
    let values: Vec<String> = partition
        .iter()
        .map(|value| value.map_or_else(|| "NULL".to_string(), literal_text))
        .collect();
    match values.len() {
        0 => "tuple()".to_string(),
        1 => values[0].clone(),
        _ => format!("({})", values.join(", ")),
    }
}

fn literal_text(literal: &Literal) -> String {
    match literal {
        Literal::Primitive(PrimitiveLiteral::String(s)) => {
            format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        Literal::Primitive(PrimitiveLiteral::Int(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::Long(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::Boolean(v)) => u8::from(*v).to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, TableMetadata, Type, UnboundPartitionSpec,
    };
    use iceberg::{TableCreation, TableUpdate};

    use super::*;
    use crate::append::{NewDataFile, fast_append};

    fn table() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "k", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("s3://local/ns/1/house/default/t-x".to_string())
            .schema(schema)
            .partition_spec(UnboundPartitionSpec::builder().build())
            .properties(HashMap::new())
            .build();
        crate::metadata::create(creation).expect("metadata")
    }

    fn file(n: u64) -> NewDataFile {
        NewDataFile {
            location: format!("s3://local/ns/1/house/default/t-x/data/{n}.parquet"),
            record_count: n,
            size_bytes: 100 * n,
            partition: Struct::empty(),
        }
    }

    /// Two appends: the second snapshot's list holds both manifests, the totals
    /// add up, and each file keeps the sequence number it was added with.
    #[tokio::test]
    async fn appends_chain_and_files_read_back() {
        let mut metadata = table();
        let mut file_name = crate::MetadataFile::first(metadata.location());
        let mut list: Option<Vec<u8>> = None;
        let mut manifests_by_location: HashMap<String, Vec<u8>> = HashMap::new();
        for (round, files) in [vec![file(1), file(2)], vec![file(3)]]
            .into_iter()
            .enumerate()
        {
            let append = fast_append(&metadata, list.as_deref(), files, 1_000 + round as i64)
                .await
                .expect("append");
            for (location, bytes) in &append.files {
                manifests_by_location.insert(location.clone(), bytes.to_vec());
            }
            list = Some(append.files[1].1.to_vec());
            assert!(matches!(append.updates[0], TableUpdate::AddSnapshot { .. }));
            metadata = crate::metadata::commit(
                &metadata,
                &file_name,
                &append.requirements,
                append.updates,
            )
            .expect("commit");
            file_name = file_name.next();
            assert_eq!(
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                Some(append.snapshot_id)
            );
        }
        let current = metadata.current_snapshot().expect("snapshot");
        assert_eq!(
            current.summary().additional_properties["total-records"],
            "6"
        );
        assert_eq!(
            current.summary().additional_properties["total-data-files"],
            "3"
        );
        let refs = manifests(list.as_deref().expect("list"), FormatVersion::V2).expect("list");
        assert_eq!(refs.len(), 2);
        let mut files = Vec::new();
        for r in &refs {
            files.extend(live_files(&manifests_by_location[&r.location], r).expect("manifest"));
        }
        files.sort_by_key(|f| f.record_count);
        let seqs: Vec<(u64, i64)> = files
            .iter()
            .map(|f| (f.record_count, f.sequence_number))
            .collect();
        assert_eq!(seqs, vec![(1, 1), (2, 1), (3, 2)]);
        assert!(files.iter().all(|f| f.data && f.partition == "tuple()"));

        // The second snapshot added one manifest, holding file 3 alone.
        let second = metadata.current_snapshot().expect("snapshot").snapshot_id();
        let added = manifests_added_by(list.as_deref().expect("list"), FormatVersion::V2, second)
            .expect("added");
        assert_eq!(added.len(), 1);
        let files_added = added_files(
            &manifests_by_location[&added[0].location],
            &added[0],
            second,
        )
        .expect("files");
        assert_eq!(
            files_added
                .iter()
                .map(|f| f.record_count)
                .collect::<Vec<_>>(),
            vec![3]
        );

        // A stale parent is a conflict at commit.
        let stale = fast_append(&table(), None, vec![file(9)], 5)
            .await
            .expect("append");
        assert!(
            crate::metadata::commit(&metadata, &file_name, &stale.requirements, stale.updates)
                .is_err()
        );
    }
}
