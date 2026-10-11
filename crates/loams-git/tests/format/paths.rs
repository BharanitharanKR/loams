//! Object paths: the layout of §36 §4.1 and §19.4.1, and parsing back.

use loams_git::{
    MountId, NamespaceId, ObjectId, Principal, RepoId, RepoName, RepoPath, RepoPaths, Seq, SnapN,
    WsId, WsName,
};
use proptest::prelude::*;
use ulid::Ulid;

fn oid(byte: u8) -> ObjectId {
    ObjectId::from_hex(hex_of(byte).as_bytes()).expect("40 hex digits")
}

fn hex_of(byte: u8) -> String {
    format!("{byte:02x}").repeat(20)
}

#[test]
fn layout_matches_the_design() {
    let ns = NamespaceId(7);
    let repo = RepoId::parse("r01h0000000000000000000000").unwrap();
    let ws = WsId::from_ulid(Ulid::from_parts(1, 1));
    let mount = MountId::from_ulid(Ulid::from_parts(2, 2));
    let p = RepoPaths::new(ns, &repo);
    let root = "ns/7/repos/r01h0000000000000000000000";
    let a = hex_of(0xab);
    assert_eq!(p.root(), root);
    assert_eq!(
        RepoPaths::names_record(ns, &RepoName::parse("loams").unwrap()),
        "ns/7/repos/_names/loams.json"
    );
    assert_eq!(p.head(), format!("{root}/head"));
    assert_eq!(
        p.segment(Seq(1)),
        format!("{root}/wal/00000000000000000001.lgw")
    );
    assert_eq!(
        p.checkpoint(Seq(0)),
        format!("{root}/checkpoints/00000000000000000000.lgc")
    );
    assert_eq!(p.pack(&oid(0xab)), format!("{root}/packs/{a}.lpk"));
    let claim = Ulid::from_parts(3, 3);
    assert_eq!(
        p.gc_claim(claim),
        format!(
            "{root}/gc/claims/{}.json",
            claim.to_string().to_ascii_lowercase()
        )
    );
    assert_eq!(
        p.ws_snapshot(&ws, SnapN(2)),
        format!("{root}/ws/{ws}/snap/00000000000000000002.lws")
    );
    assert_eq!(p.ws_head(&ws), format!("{root}/ws/{ws}/head"));
    assert_eq!(
        p.ws_pack(&ws, &oid(0xab)),
        format!("{root}/ws/{ws}/packs/{a}.lpk")
    );
    let salvage = Ulid::from_parts(4, 4);
    assert_eq!(
        p.ws_salvage(&ws, &mount, salvage),
        format!(
            "{root}/ws/{ws}/salvage/{mount}/{}.lws",
            salvage.to_string().to_ascii_lowercase()
        )
    );
    assert_eq!(
        p.ws_name(
            &Principal::parse("alice").unwrap(),
            &WsName::parse("fix-1").unwrap()
        ),
        format!("{root}/_ws_names/alice/fix-1.json")
    );
    assert_eq!(
        p.derived_tree_aux(&oid(0xab)),
        format!("{root}/derived/tree-aux/{a}.lta")
    );
    assert_eq!(
        p.derived_graph(Seq(9)),
        format!("{root}/derived/graph/00000000000000000009.graph")
    );
    // sha256("src/lib.rs")
    assert_eq!(
        p.derived_blame(&oid(0xab), &RepoPath::parse("src/lib.rs").unwrap()),
        format!(
            "{root}/derived/blame/{a}/{}.lbl",
            "b1a35a68f14e696205874893c07fd24fdb88882b47c23cc0e0c80a30c7d53759"
        )
    );
}

#[test]
fn parsing_refuses_other_spellings() {
    assert_eq!(RepoPaths::parse_segment("wal/7.lgw"), None);
    assert_eq!(
        RepoPaths::parse_segment("wal/00000000000000000007.lgc"),
        None
    );
    assert_eq!(
        RepoPaths::parse_checkpoint("checkpoints/0000000000000000000x.lgc"),
        None
    );
    assert_eq!(
        RepoPaths::parse_pack(&format!("packs/{}.lpk", "AB".repeat(20))),
        None
    );
    assert_eq!(RepoPaths::parse_pack("packs/abc.lpk"), None);
    assert_eq!(
        RepoPaths::parse_ws_snapshot("ws/wnope/snap/00000000000000000001.lws"),
        None
    );
}

proptest! {
    #[test]
    fn paths_round_trip(
        ns in any::<u64>(),
        seq in any::<u64>(),
        n in any::<u64>(),
        repo in "[a-z0-9][a-z0-9._-]{0,20}",
        ws_bits in any::<u128>(),
        bytes in any::<[u8; 20]>(),
    ) {
        let repo = RepoId::parse(&repo).unwrap();
        let p = RepoPaths::new(NamespaceId(ns), &repo);
        let ws = WsId::from_ulid(Ulid(ws_bits));
        let checksum = ObjectId::try_from(&bytes[..]).unwrap();
        let root = format!("ns/{ns}/repos/{repo}/");

        for path in [
            p.segment(Seq(seq)), p.checkpoint(Seq(seq)), p.pack(&checksum),
            p.ws_snapshot(&ws, SnapN(n)), p.ws_pack(&ws, &checksum),
        ] {
            prop_assert!(path.starts_with(&root), "{} under {}", path, root);
        }
        prop_assert_eq!(RepoPaths::parse_segment(&p.segment(Seq(seq))), Some(Seq(seq)));
        prop_assert_eq!(RepoPaths::parse_checkpoint(&p.checkpoint(Seq(seq))), Some(Seq(seq)));
        prop_assert_eq!(RepoPaths::parse_pack(&p.pack(&checksum)), Some(checksum));
        prop_assert_eq!(RepoPaths::parse_pack(&p.ws_pack(&ws, &checksum)), Some(checksum));
        prop_assert_eq!(
            RepoPaths::parse_ws_snapshot(&p.ws_snapshot(&ws, SnapN(n))),
            Some((ws.clone(), SnapN(n)))
        );
        // A segment's name is its seq's 20 digits, so a listing sorts by seq.
        prop_assert!(p.segment(Seq(seq)) < p.segment(Seq(seq.saturating_add(1))) || seq == u64::MAX);
    }
}
