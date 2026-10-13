//! Object paths (§36 §4.1, §19.4.1; plan "Object layout", Rulings 21–22).
//!
//! ```text
//! ns/<ns_id>/repos/_names/<name>.json                 catalog record
//! ns/<ns_id>/repos/_deleting/<repo_id>.json           deletion marker (T13.1)
//! ns/<ns_id>/repos/<repo_id>/
//!   head  wal/<seq:020>.lgw  checkpoints/<seq:020>.lgc  packs/<checksum>.lpk  gc/claims/<ulid>.json
//!   ws/<ws_id>/{snap/<n:020>.lws, head, packs/<checksum>.lpk, salvage/<mount_id>/<ulid>.lws}
//!   _ws_names/<owner>/<name>.json
//!   derived/{tree-aux/<tree_oid>.lta, graph/<seq:020>.graph, blame/<commit>/<sha256(path)>.lbl}
//! ```
//!
//! Every component comes from a validated id, so no path escapes its
//! repository's root.

use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::ids::{
    MountId, NamespaceId, ObjectId, Principal, RepoId, RepoName, RepoPath, Seq, SnapN, WsId,
    WsName, lowercase_ulid,
};

/// The object paths of one repository.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RepoPaths {
    /// `ns/<ns_id>/repos/<repo_id>`.
    root: String,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// `<20 digits><ext>` as the number it names.
fn twenty_digits(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)?.parse::<Seq>().ok().map(|seq| seq.0)
}

impl RepoPaths {
    /// The paths of `repo` in namespace `ns`.
    pub fn new(ns: NamespaceId, repo: &RepoId) -> Self {
        Self {
            root: format!("ns/{ns}/repos/{repo}"),
        }
    }

    /// `ns/<ns_id>/repos/<repo_id>`, without a trailing slash.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The catalog record of the repository named `name` (Ruling 21). It is
    /// read before the id is known, so it needs only the namespace.
    pub fn names_record(ns: NamespaceId, name: &RepoName) -> String {
        format!("{}{name}.json", Self::names_prefix(ns))
    }

    /// The prefix of every catalog record in `ns`, for listings.
    pub fn names_prefix(ns: NamespaceId) -> String {
        format!("ns/{ns}/repos/_names/")
    }

    /// The name a catalog record path `…_names/<name>.json` holds.
    pub fn parse_names_record(path: &str) -> Option<RepoName> {
        let name = path.rsplit_once("_names/")?.1.strip_suffix(".json")?;
        RepoName::parse(name).ok()
    }

    /// The deletion marker of repository `repo` in `ns` (GT1 Task 13, ruling
    /// T13.1): create-only, keyed by the id so it never names a later
    /// repository of the same name.
    pub fn deleting_marker(ns: NamespaceId, repo: &RepoId) -> String {
        format!("{}{repo}.json", Self::deleting_prefix(ns))
    }

    /// The prefix of every deletion marker in `ns`.
    pub fn deleting_prefix(ns: NamespaceId) -> String {
        format!("ns/{ns}/repos/_deleting/")
    }

    /// The repository a deletion marker path names.
    pub fn parse_deleting_marker(path: &str) -> Option<RepoId> {
        let id = path.rsplit_once("_deleting/")?.1.strip_suffix(".json")?;
        RepoId::parse(id).ok()
    }

    /// The `head` hint (§36 §4.1).
    pub fn head(&self) -> String {
        format!("{}/head", self.root)
    }

    /// Segment `seq` (§36 §4.3).
    pub fn segment(&self, seq: Seq) -> String {
        format!("{}{seq}.lgw", self.wal_prefix())
    }

    /// Checkpoint `seq` (§36 §4.3).
    pub fn checkpoint(&self, seq: Seq) -> String {
        format!("{}{seq}.lgc", self.checkpoint_prefix())
    }

    /// The pack object named by its pack checksum (§36 §4.3).
    pub fn pack(&self, checksum: &ObjectId) -> String {
        format!("{}{}.lpk", self.pack_prefix(), checksum.to_hex())
    }

    /// `…/wal/`.
    pub fn wal_prefix(&self) -> String {
        format!("{}/wal/", self.root)
    }

    /// `…/checkpoints/`.
    pub fn checkpoint_prefix(&self) -> String {
        format!("{}/checkpoints/", self.root)
    }

    /// `…/packs/`.
    pub fn pack_prefix(&self) -> String {
        format!("{}/packs/", self.root)
    }

    /// The seq of a segment path (or name) `…wal/<seq:020>.lgw`.
    pub fn parse_segment(path: &str) -> Option<Seq> {
        let name = path.rsplit_once("wal/").map_or(path, |(_, name)| name);
        twenty_digits(name, ".lgw").map(Seq)
    }

    /// The seq of a checkpoint path (or name) `…checkpoints/<seq:020>.lgc`.
    pub fn parse_checkpoint(path: &str) -> Option<Seq> {
        let name = path
            .rsplit_once("checkpoints/")
            .map_or(path, |(_, name)| name);
        twenty_digits(name, ".lgc").map(Seq)
    }

    /// The checksum of a pack path (or name) `…packs/<hex>.lpk`, in either
    /// the repository's or a workspace's pack set.
    pub fn parse_pack(path: &str) -> Option<ObjectId> {
        let name = path.rsplit_once("packs/").map_or(path, |(_, name)| name);
        let hex = name.strip_suffix(".lpk")?;
        // Only the lowercase spelling `to_hex` writes.
        if hex.bytes().any(|b| b.is_ascii_uppercase()) {
            return None;
        }
        ObjectId::from_hex(hex.as_bytes()).ok()
    }

    /// A GC claim (§03 §7).
    pub fn gc_claim(&self, claim: Ulid) -> String {
        format!("{}/gc/claims/{}.json", self.root, lowercase_ulid(claim))
    }

    /// `…/ws/<ws_id>/`.
    pub fn ws_prefix(&self, ws: &WsId) -> String {
        format!("{}/ws/{ws}/", self.root)
    }

    /// `…/ws/<ws_id>/snap/`.
    pub fn ws_snapshot_prefix(&self, ws: &WsId) -> String {
        format!("{}snap/", self.ws_prefix(ws))
    }

    /// Snapshot `n` of workspace `ws` (§19.4.1).
    pub fn ws_snapshot(&self, ws: &WsId, n: SnapN) -> String {
        format!("{}{n}.lws", self.ws_snapshot_prefix(ws))
    }

    /// The workspace and snapshot number of a snapshot path
    /// `…ws/<ws_id>/snap/<n:020>.lws`.
    pub fn parse_ws_snapshot(path: &str) -> Option<(WsId, SnapN)> {
        let (front, name) = path.rsplit_once("/snap/")?;
        let ws = front.rsplit_once("ws/")?.1;
        let ws = WsId::parse(ws).ok()?;
        let n = twenty_digits(name, ".lws")?;
        Some((ws, SnapN(n)))
    }

    /// The workspace's `head` hint (§19.4.1).
    pub fn ws_head(&self, ws: &WsId) -> String {
        format!("{}head", self.ws_prefix(ws))
    }

    /// A workspace pack (§19.4.1).
    pub fn ws_pack(&self, ws: &WsId, checksum: &ObjectId) -> String {
        format!("{}packs/{}.lpk", self.ws_prefix(ws), checksum.to_hex())
    }

    /// A fenced writer's unsaved state (§19.4.3).
    pub fn ws_salvage(&self, ws: &WsId, mount: &MountId, salvage: Ulid) -> String {
        format!(
            "{}salvage/{mount}/{}.lws",
            self.ws_prefix(ws),
            lowercase_ulid(salvage)
        )
    }

    /// The name record of `owner`'s workspace `name` (§19.4.1).
    pub fn ws_name(&self, owner: &Principal, name: &WsName) -> String {
        format!("{}{name}.json", self.ws_names_prefix(owner))
    }

    /// `…/_ws_names/<owner>/`, for listings.
    pub fn ws_names_prefix(&self, owner: &Principal) -> String {
        format!("{}/_ws_names/{owner}/", self.root)
    }

    /// The tree aux record (entry sizes) of a tree (§19.5, D830).
    pub fn derived_tree_aux(&self, tree: &ObjectId) -> String {
        format!("{}/derived/tree-aux/{}.lta", self.root, tree.to_hex())
    }

    /// A commit-graph layer for commits since the last compaction (§19.8, D833).
    pub fn derived_graph(&self, seq: Seq) -> String {
        format!("{}/derived/graph/{seq}.graph", self.root)
    }

    /// The blame of `path` at `commit` (§19.8): the path is hashed, so any
    /// path length gives one bounded name.
    pub fn derived_blame(&self, commit: &ObjectId, path: &RepoPath) -> String {
        let digest = Sha256::digest(path.as_str().as_bytes());
        format!(
            "{}/derived/blame/{}/{}.lbl",
            self.root,
            commit.to_hex(),
            hex(&digest)
        )
    }
}
