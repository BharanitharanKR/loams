//! UP1 Task 1: the hot tier's readers check the registry's readable range.

use loams_common::formats::{self, FormatId, FormatSpec};
use loams_hot::artifact::{ARTIFACT_FORMAT_VERSION, decode_covered, encode_covered};
use loams_hot::registry::NodeDescriptor;
use roaring::RoaringTreemap;

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

#[test]
fn format_constants_match_registry() {
    assert_eq!(
        ARTIFACT_FORMAT_VERSION,
        formats::spec_of(formats::HOT_ARTIFACT).current
    );
    assert_eq!(
        formats::spec_of(formats::HOT_OWNER).current,
        1,
        "the owner string is v1"
    );
}

mod artifact_reader {
    use super::*;

    fn object(version: u16) -> Vec<u8> {
        let bytes = encode_covered(&RoaringTreemap::from_iter([1u64, 5]));
        let mut out = bytes[..bytes.len() - 4].to_vec();
        out[4..6].copy_from_slice(&version.to_le_bytes());
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = object(ARTIFACT_FORMAT_VERSION - 1);
        assert!(decode_covered(&older).is_err());
        allowing_previous(formats::HOT_ARTIFACT, || {
            decode_covered(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let err = decode_covered(&object(ARTIFACT_FORMAT_VERSION + 1)).unwrap_err();
        assert!(err.to_string().contains("format_unreadable"), "{err}");
    }
}

mod owner_reader {
    use super::*;

    const OWNER: &str = "01J00000000000000000000000;10.0.0.1:7000;query;z1";

    #[test]
    fn reader_accepts_previous_version() {
        let older = format!("v0;{OWNER}");
        assert!(NodeDescriptor::decode(1, &older).is_err());
        allowing_previous(formats::HOT_OWNER, || {
            NodeDescriptor::decode(1, &older).expect("N-1 opens once min_read allows it");
        });
        let current = NodeDescriptor::decode(1, &format!("v1;{OWNER}")).unwrap();
        assert!(current.encode().starts_with("v1;"));
    }

    #[test]
    fn reader_refuses_next_version() {
        assert!(NodeDescriptor::decode(1, &format!("v2;{OWNER}")).is_err());
    }
}
