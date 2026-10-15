//! UP1 Task 1: every collection format's reader checks the registry's
//! readable range, and its constant matches the registry.

use bytes::Bytes;
use loams_common::CollectionId;
use loams_common::formats::{self, FormatId, FormatSpec};

use crate::codec::{self, CODEC_VERSION};
use crate::deadletter::{self, DEAD_LETTERS_VERSION, DeadLetter};
use crate::doc::DocOp;
use crate::manifest::{self, CollectionManifest, MANIFEST_FORMAT_VERSION};
use crate::pk::PrimaryKey;
use crate::pkindex::{self, PK_DELTA_VERSION, PkWatermark, VALUE_VERSION};

/// `f` with `id`'s `min_read` lowered to `current - 1`.
fn allowing_previous<T>(id: FormatId, f: impl FnOnce() -> T) -> T {
    let spec = formats::spec_of(id);
    formats::with_override(
        FormatSpec {
            min_read: spec.current - 1,
            ..spec
        },
        f,
    )
}

/// An envelope (`magic ‖ u16 LE version ‖ body ‖ crc32c LE`) re-sealed at
/// `version`.
fn envelope_at(bytes: &[u8], version: u16) -> Vec<u8> {
    let mut out = bytes[..bytes.len() - 4].to_vec();
    out[4..6].copy_from_slice(&version.to_le_bytes());
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// A one-byte value version replaced by `version`.
fn byte_at(bytes: &[u8], version: u8) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[0] = version;
    out
}

#[test]
fn format_constants_match_registry() {
    let current = |id| formats::spec_of(id).current;
    assert_eq!(
        MANIFEST_FORMAT_VERSION,
        current(formats::COLLECTION_MANIFEST)
    );
    assert_eq!(PK_DELTA_VERSION, current(formats::COLLECTION_PK_DELTA));
    assert_eq!(
        DEAD_LETTERS_VERSION,
        current(formats::COLLECTION_DEAD_LETTERS)
    );
    assert_eq!(
        u16::from(CODEC_VERSION),
        current(formats::COLLECTION_RECORD)
    );
    assert_eq!(
        u16::from(VALUE_VERSION),
        current(formats::COLLECTION_PK_VALUE)
    );
}

mod manifest_reader {
    use super::*;

    fn object(version: u16) -> Vec<u8> {
        let bytes = manifest::encode_manifest(&CollectionManifest::empty(CollectionId(7)));
        envelope_at(&bytes, version)
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = object(MANIFEST_FORMAT_VERSION - 1);
        assert!(manifest::decode_manifest(&older).is_err());
        allowing_previous(formats::COLLECTION_MANIFEST, || {
            manifest::decode_manifest(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let err = manifest::decode_manifest(&object(MANIFEST_FORMAT_VERSION + 1)).unwrap_err();
        assert!(err.to_string().contains("format_unreadable"), "{err}");
    }
}

mod pk_delta_reader {
    use super::*;

    fn object(version: u16) -> Vec<u8> {
        let bytes = pkindex::encode_pk_delta(&[(b"k".to_vec(), Some(1))]);
        envelope_at(&bytes, version)
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = object(PK_DELTA_VERSION - 1);
        assert!(pkindex::decode_pk_delta(&older).is_err());
        allowing_previous(formats::COLLECTION_PK_DELTA, || {
            pkindex::decode_pk_delta(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let err = pkindex::decode_pk_delta(&object(PK_DELTA_VERSION + 1)).unwrap_err();
        assert!(err.to_string().contains("format_unreadable"), "{err}");
    }
}

mod dead_letters_reader {
    use super::*;

    fn object(version: u16) -> Vec<u8> {
        let bytes = deadletter::encode_dead_letters(&[DeadLetter {
            partition: 0,
            offset: 1,
            key: None,
            value: None,
            reason: "test".to_string(),
        }]);
        envelope_at(&bytes, version)
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = object(DEAD_LETTERS_VERSION - 1);
        assert!(deadletter::decode_dead_letters(&older).is_err());
        allowing_previous(formats::COLLECTION_DEAD_LETTERS, || {
            deadletter::decode_dead_letters(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let err = deadletter::decode_dead_letters(&object(DEAD_LETTERS_VERSION + 1)).unwrap_err();
        assert!(err.to_string().contains("format_unreadable"), "{err}");
    }
}

mod record_reader {
    use super::*;

    fn record(version: u8) -> loams_log::Record {
        let mut record = codec::encode(&DocOp::Delete(PrimaryKey::U64(1))).expect("encode");
        let value = record.value.take().expect("a value");
        record.value = Some(Bytes::from(byte_at(&value, version)));
        record
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = record(CODEC_VERSION - 1);
        assert!(codec::decode(&older).is_err());
        allowing_previous(formats::COLLECTION_RECORD, || {
            codec::decode(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        assert!(codec::decode(&record(CODEC_VERSION + 1)).is_err());
    }
}

mod pk_value_reader {
    use super::*;

    fn value(version: u8) -> Vec<u8> {
        byte_at(&PkWatermark::default().encode(), version)
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = value(VALUE_VERSION - 1);
        assert!(PkWatermark::decode(&older).is_err());
        allowing_previous(formats::COLLECTION_PK_VALUE, || {
            PkWatermark::decode(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        assert!(PkWatermark::decode(&value(VALUE_VERSION + 1)).is_err());
    }
}
