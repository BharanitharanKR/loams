//! UP1 Task 1: Graph's readers check the registry's readable range, and its
//! constants match the registry.

use loams_common::formats::{self, FormatId, FormatSpec};
use loams_graph::changeset::{CodecError, PropMap, RECORD_VERSION, decode_props, encode_props};
use loams_graph::snapshot::manifest::{MANIFEST_FORMAT, manifest_format_readable};
use loams_graph::snapshot::portable::{PORTABLE_VERSION, portable_version_readable};

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
    let current = |id| formats::spec_of(id).current;
    assert_eq!(u16::from(RECORD_VERSION), current(formats::GRAPH_CHANGESET));
    assert_eq!(MANIFEST_FORMAT, u32::from(current(formats::GRAPH_MANIFEST)));
    assert_eq!(
        PORTABLE_VERSION,
        u32::from(current(formats::GRAPH_PORTABLE))
    );
}

mod changeset_reader {
    use super::*;

    fn props_at(version: u8) -> Vec<u8> {
        let mut bytes = encode_props(&PropMap::new()).expect("encode").to_vec();
        bytes[0] = version;
        bytes
    }

    #[test]
    fn reader_accepts_previous_version() {
        let older = props_at(RECORD_VERSION - 1);
        assert!(matches!(
            decode_props(&older),
            Err(CodecError::UnknownVersion(0))
        ));
        allowing_previous(formats::GRAPH_CHANGESET, || {
            decode_props(&older).expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let newer = props_at(RECORD_VERSION + 1);
        assert!(matches!(
            decode_props(&newer),
            Err(CodecError::UnknownVersion(_))
        ));
    }
}

mod manifest_reader {
    use super::*;

    #[test]
    fn reader_accepts_previous_version() {
        assert!(manifest_format_readable(MANIFEST_FORMAT));
        assert!(!manifest_format_readable(MANIFEST_FORMAT - 1));
        allowing_previous(formats::GRAPH_MANIFEST, || {
            assert!(manifest_format_readable(MANIFEST_FORMAT - 1));
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        assert!(!manifest_format_readable(MANIFEST_FORMAT + 1));
    }
}

mod portable_reader {
    use super::*;

    #[test]
    fn reader_accepts_previous_version() {
        assert!(portable_version_readable(PORTABLE_VERSION));
        assert!(!portable_version_readable(PORTABLE_VERSION - 1));
        allowing_previous(formats::GRAPH_PORTABLE, || {
            assert!(portable_version_readable(PORTABLE_VERSION - 1));
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        assert!(!portable_version_readable(PORTABLE_VERSION + 1));
    }
}
