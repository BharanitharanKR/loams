//! Table metadata files: a new table's first one, a commit applied to the current
//! one, and their names.
//!
//! A metadata file is `<table location>/metadata/<version>-<uuid>.metadata.json`
//! (`00000-…` for a new table), uncompressed JSON, as the Java reference and
//! iceberg-rust's catalogs name them. A commit writes a **new** file and the
//! catalog then swaps its pointer from the old one to the new one with
//! compare-and-set; files are never rewritten, so a reader pinned to one file
//! (HS1 Task 11) keeps reading the same bytes.

use std::fmt;

use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::{TableCreation, TableRequirement, TableUpdate};

/// A metadata operation failed.
#[derive(Debug, thiserror::Error)]
pub enum LakeError {
    /// A requirement of the commit does not hold: someone else committed first.
    /// The caller reloads and tries again, or gives up.
    #[error("commit conflict: {0}")]
    Conflict(String),
    /// The creation or an update is not valid Iceberg.
    #[error("invalid table metadata: {0}")]
    Invalid(String),
    /// Rows cannot be written as the table's data: a value its column cannot
    /// hold, or an Arrow stream that is not whole (HS1 Task 12).
    #[error("{0}")]
    Data(String),
    /// A write's input is larger than its cap, in bytes (HS1 Task 12).
    #[error("the rows are more than the {0} bytes one statement may write")]
    TooLarge(u64),
    /// A metadata file could not be read.
    #[error("unreadable metadata file {location}: {message}")]
    Unreadable {
        /// The file.
        location: String,
        /// What was wrong with it.
        message: String,
    },
}

impl LakeError {
    pub(crate) fn from_iceberg(err: iceberg::Error) -> Self {
        match err.kind() {
            iceberg::ErrorKind::CatalogCommitConflicts => Self::Conflict(err.to_string()),
            _ => Self::Invalid(err.to_string()),
        }
    }
}

/// The name of a metadata file: its table location, version and uuid.
#[derive(Clone, PartialEq, Eq)]
pub struct MetadataFile {
    table_location: String,
    version: u64,
    id: uuid::Uuid,
}

impl fmt::Debug for MetadataFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetadataFile({self})")
    }
}

impl fmt::Display for MetadataFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/metadata/{:05}-{}.metadata.json",
            self.table_location.trim_end_matches('/'),
            self.version,
            self.id
        )
    }
}

impl MetadataFile {
    /// The first metadata file of a table at `table_location`.
    pub fn first(table_location: &str) -> Self {
        Self {
            table_location: table_location.trim_end_matches('/').to_string(),
            version: 0,
            id: uuid::Uuid::new_v4(),
        }
    }

    /// The file after this one.
    pub fn next(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: uuid::Uuid::new_v4(),
        }
    }

    /// Reads a metadata file's name. A name that is not `<version>-<uuid>`
    /// (written by another engine) still parses, as version 0, so the file after
    /// it gets a fresh, ordered name.
    pub fn parse(location: &str) -> Result<Self, LakeError> {
        let unreadable = |message: &str| LakeError::Unreadable {
            location: location.to_string(),
            message: message.to_string(),
        };
        let (dir, file) = location
            .rsplit_once('/')
            .ok_or_else(|| unreadable("not a path"))?;
        let table_location = dir
            .strip_suffix("/metadata")
            .ok_or_else(|| unreadable("not in a metadata/ directory"))?
            .to_string();
        let stem = file
            .strip_suffix(".metadata.json")
            .or_else(|| file.strip_suffix(".gz.metadata.json"))
            .ok_or_else(|| unreadable("not a .metadata.json file"))?;
        let (version, id) = match stem.split_once('-') {
            Some((version, id)) => match (version.parse::<u64>(), uuid::Uuid::parse_str(id)) {
                (Ok(version), Ok(id)) => (version, id),
                _ => (0, uuid::Uuid::new_v4()),
            },
            None => (0, uuid::Uuid::new_v4()),
        };
        Ok(Self {
            table_location,
            version,
            id,
        })
    }

    /// The table's location.
    pub fn table_location(&self) -> &str {
        &self.table_location
    }

    /// The version in the name.
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// A new table's first metadata. The creation must name its location. Iceberg
/// assigns fresh field ids (from 1, in schema order) and the partition spec's and
/// sort order's sources follow them, so a caller reads ids back by name.
pub fn create(creation: TableCreation) -> Result<TableMetadata, LakeError> {
    let built = TableMetadataBuilder::from_table_creation(creation)
        .and_then(TableMetadataBuilder::build)
        .map_err(LakeError::from_iceberg)?;
    Ok(built.metadata)
}

/// A commit applied to `current` (read from `current_file`): every requirement is
/// checked against it (`Conflict` when one fails), then the updates are applied in
/// order, and the old file joins the metadata log. The caller writes the result to
/// `current_file.next()` and swaps its pointer with compare-and-set.
pub fn commit(
    current: &TableMetadata,
    current_file: &MetadataFile,
    requirements: &[TableRequirement],
    updates: Vec<TableUpdate>,
) -> Result<TableMetadata, LakeError> {
    for requirement in requirements {
        requirement
            .check(Some(current))
            .map_err(|err| LakeError::Conflict(err.to_string()))?;
    }
    let mut builder =
        TableMetadataBuilder::new_from_metadata(current.clone(), Some(current_file.to_string()));
    for update in updates {
        builder = update.apply(builder).map_err(LakeError::from_iceberg)?;
    }
    let built = builder.build().map_err(LakeError::from_iceberg)?;
    Ok(built.metadata)
}

/// A metadata file's bytes.
pub fn to_json(metadata: &TableMetadata) -> Result<Vec<u8>, LakeError> {
    serde_json::to_vec(metadata).map_err(|err| LakeError::Invalid(err.to_string()))
}

/// A metadata file read back.
pub fn from_json(location: &str, bytes: &[u8]) -> Result<TableMetadata, LakeError> {
    serde_json::from_slice(bytes).map_err(|err| LakeError::Unreadable {
        location: location.to_string(),
        message: err.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;

    fn creation(location: &str) -> TableCreation {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableCreation::builder()
            .name("t".to_string())
            .location(location.to_string())
            .schema(schema)
            .properties(HashMap::from([("k".to_string(), "v".to_string())]))
            .build()
    }

    #[test]
    fn file_names_are_versioned_and_parse_back() {
        let first = MetadataFile::first("s3://b/p/ns/1/house/default/t-x/");
        let text = first.to_string();
        assert!(text.starts_with("s3://b/p/ns/1/house/default/t-x/metadata/00000-"));
        assert!(text.ends_with(".metadata.json"));
        let parsed = MetadataFile::parse(&text).expect("parses");
        assert_eq!(parsed, first);
        let next = parsed.next();
        assert_eq!(next.version(), 1);
        assert_eq!(next.table_location(), "s3://b/p/ns/1/house/default/t-x");
        // Another engine's name still parses, as version 0.
        let foreign = MetadataFile::parse("s3://b/t/metadata/v3.metadata.json").expect("parses");
        assert_eq!(foreign.version(), 0);
        assert!(MetadataFile::parse("s3://b/t/v3.json").is_err());
    }

    #[test]
    fn commit_checks_requirements_and_logs_the_old_file() {
        let metadata = create(creation("memory://t")).expect("creates");
        let file = MetadataFile::first("memory://t");
        let schema_id = metadata.current_schema_id();
        let updated = commit(
            &metadata,
            &file,
            &[TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: schema_id,
            }],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("k".to_string(), "w".to_string())]),
            }],
        )
        .expect("commits");
        assert_eq!(updated.properties().get("k").map(String::as_str), Some("w"));
        assert_eq!(
            updated
                .metadata_log()
                .last()
                .map(|l| l.metadata_file.clone()),
            Some(file.to_string())
        );
        let stale = commit(
            &updated,
            &file.next(),
            &[TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: schema_id + 1,
            }],
            Vec::new(),
        );
        assert!(matches!(stale, Err(LakeError::Conflict(_))), "{stale:?}");
        let bytes = to_json(&updated).expect("json");
        assert_eq!(from_json("x", &bytes).expect("reads"), updated);
    }
}
