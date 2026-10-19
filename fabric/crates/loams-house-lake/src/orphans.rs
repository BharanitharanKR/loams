//! Orphan files (HS1 Task 15, §49 §11): files under a table's location that no
//! retained snapshot and no metadata file of the table names.
//!
//! They are the leftovers of writes that never committed (a failed or lost
//! commit, an abandoned compaction, a crash between upload and commit) and of
//! metadata files that fell out of the metadata log. A file is an orphan only
//! when all of these hold:
//!
//! * it is under the table's `data/` or `metadata/` directory (anything else
//!   under the location is not the House's to judge);
//! * no kept snapshot lists it (its manifest list, a manifest, a live data or
//!   delete file), and it is not the current metadata file, a metadata file of
//!   the log, or a statistics file;
//! * it is older than the cutoff (three days by default): a write that has
//!   uploaded its files and not yet committed them is never that old.
//!
//! A metadata file whose version is newer than the catalog's pointer means the
//! catalog is behind the bucket (a catalog restored to an earlier point, §49
//! §11 disaster recovery). Then nothing of the table is judged at all: the
//! newer metadata names files the pointer does not, and they must survive
//! until the table is reconciled.

use std::collections::HashSet;

use iceberg::spec::TableMetadata;

use crate::metadata::MetadataFile;

/// A file the store listed under a table's location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    /// Its absolute location (`s3://bucket/…`), as the table names files.
    pub location: String,
    /// When it was last written, in Unix milliseconds.
    pub last_modified_ms: u64,
}

/// What [`orphans`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Orphans {
    /// These files may be deleted.
    Files(Vec<String>),
    /// The bucket holds this metadata file, newer than the catalog's pointer:
    /// nothing is judged until the table is reconciled.
    NewerMetadata(String),
}

/// The files `metadata` (read from `current`) names itself: the current
/// metadata file, the metadata log, every snapshot's manifest list and the
/// statistics files. The manifests and data files are the caller's to add,
/// from the manifest lists of the snapshots it keeps.
pub fn metadata_references(metadata: &TableMetadata, current: &str) -> HashSet<String> {
    let mut named: HashSet<String> = HashSet::new();
    named.insert(current.to_string());
    named.extend(
        metadata
            .metadata_log()
            .iter()
            .map(|l| l.metadata_file.clone()),
    );
    named.extend(metadata.snapshots().map(|s| s.manifest_list().to_string()));
    named.extend(
        metadata
            .statistics_iter()
            .map(|s| s.statistics_path.clone()),
    );
    named.extend(
        metadata
            .partition_statistics_iter()
            .map(|s| s.statistics_path.clone()),
    );
    named
}

/// The orphans among `listed`, the files under `table_location` (see the
/// module docs): `referenced` is every file a kept snapshot or the metadata
/// names, `current` the catalog's metadata file, and only files written before
/// `cutoff_ms` may be orphans.
pub fn orphans(
    table_location: &str,
    listed: &[StoredFile],
    referenced: &HashSet<String>,
    current: &MetadataFile,
    cutoff_ms: u64,
) -> Orphans {
    let base = table_location.trim_end_matches('/');
    let data = format!("{base}/data/");
    let meta = format!("{base}/metadata/");
    for file in listed {
        if file.location.starts_with(&meta)
            && file.location.ends_with(".metadata.json")
            && let Ok(name) = MetadataFile::parse(&file.location)
            && name.version() > current.version()
        {
            return Orphans::NewerMetadata(file.location.clone());
        }
    }
    let mut out: Vec<String> = listed
        .iter()
        .filter(|f| f.location.starts_with(&data) || f.location.starts_with(&meta))
        .filter(|f| f.last_modified_ms < cutoff_ms)
        .filter(|f| !referenced.contains(&f.location))
        .map(|f| f.location.clone())
        .collect();
    out.sort();
    Orphans::Files(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOC: &str = "s3://local/ns/1/house/default/t-x";

    fn stored(path: &str, at: u64) -> StoredFile {
        StoredFile {
            location: format!("{LOC}/{path}"),
            last_modified_ms: at,
        }
    }

    #[test]
    fn only_old_unreferenced_files_in_the_tables_directories() {
        let current = MetadataFile::parse(&format!(
            "{LOC}/metadata/00003-6f1c1c43-4f1b-4c1a-9d43-7c8a1f6d2b10.metadata.json"
        ))
        .expect("name");
        let referenced: HashSet<String> = [format!("{LOC}/data/kept.parquet"), current.to_string()]
            .into_iter()
            .collect();
        let listed = vec![
            stored("data/kept.parquet", 10),
            stored("data/old.parquet", 10),
            stored("data/new.parquet", 5_000),
            stored("metadata/snap-1-x.avro", 10),
            stored(
                "metadata/00003-6f1c1c43-4f1b-4c1a-9d43-7c8a1f6d2b10.metadata.json",
                10,
            ),
            stored(
                "metadata/00002-11111111-1111-1111-1111-111111111111.metadata.json",
                10,
            ),
            stored("other/readme.txt", 10),
        ];
        assert_eq!(
            orphans(LOC, &listed, &referenced, &current, 1_000),
            Orphans::Files(vec![
                format!("{LOC}/data/old.parquet"),
                format!("{LOC}/metadata/00002-11111111-1111-1111-1111-111111111111.metadata.json"),
                format!("{LOC}/metadata/snap-1-x.avro"),
            ])
        );

        // A newer metadata file than the pointer: nothing is judged.
        let mut ahead = listed;
        ahead.push(stored(
            "metadata/00004-22222222-2222-2222-2222-222222222222.metadata.json",
            10,
        ));
        assert!(matches!(
            orphans(LOC, &ahead, &referenced, &current, 1_000),
            Orphans::NewerMetadata(_)
        ));
    }
}
