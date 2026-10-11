//! Identifiers (§36 §4–§5, §19.4, §19.13; plan GT1 Task 4, Rulings 21–22).
//!
//! Every identifier that appears in an object path has exactly one
//! spelling: parsing refuses anything a constructor would not produce, so a
//! path names one object and a parsed id prints back to the same string.

use std::fmt;
use std::str::FromStr;

use ulid::Ulid;

use crate::error::IdError;

/// A git object id. SHA-1 only for now (Ruling 8).
pub use gix_hash::ObjectId;
/// The namespace in `ns/<ns_id>/…` paths: the numeric metastore id, printed
/// in decimal (Ruling 22). URLs and the API carry the namespace's name.
pub use loams_common::NamespaceId;

/// The length of a ULID in Crockford base32.
const ULID_CHARS: usize = 26;

/// Whether `s` is a ULID in lowercase Crockford base32, the only spelling
/// Loams writes (no `i`, `l`, `o` or `u`, and at most `7` first).
fn is_lowercase_ulid(s: &str) -> bool {
    s.len() == ULID_CHARS
        && s.bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
        && Ulid::from_string(s).is_ok_and(|ulid| ulid.to_string().to_ascii_lowercase() == s)
}

/// A ULID's lowercase spelling, as Loams writes it in ids and paths.
pub fn lowercase_ulid(ulid: Ulid) -> String {
    ulid.to_string().to_ascii_lowercase()
}

/// `[a-z0-9][a-z0-9._-]{0,99}`: repository names, workspace names and
/// legacy repository ids.
fn is_name(s: &str) -> bool {
    let bytes = s.as_bytes();
    (1..=100).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|&b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// The id's string form.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, IdError> {
                Self::parse(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

macro_rules! ulid_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal, $prefix:literal) => {
        string_id!($(#[$doc])* $name, $kind);

        impl $name {
            /// The one-letter prefix before the ULID.
            pub const PREFIX: char = $prefix;

            /// A new id from a fresh ULID.
            pub fn generate() -> Self {
                Self::from_ulid(Ulid::generate())
            }

            /// The id of `ulid`.
            pub fn from_ulid(ulid: Ulid) -> Self {
                Self(format!("{}{}", $prefix, lowercase_ulid(ulid)))
            }

            /// Parses `<prefix>` followed by 26 lowercase ULID characters.
            pub fn parse(s: &str) -> Result<Self, IdError> {
                match s.strip_prefix($prefix) {
                    Some(ulid) if is_lowercase_ulid(ulid) => Ok(Self(s.to_string())),
                    _ => Err(IdError::new(
                        $kind,
                        s,
                        concat!("expected '", $prefix, "' and 26 lowercase ULID characters"),
                    )),
                }
            }
        }
    };
}

string_id!(
    /// A repository's immutable id (Ruling 21). New repositories get `r` and
    /// a lowercase ULID ([`RepoId::generate`]); `parse` also accepts §36's
    /// `[a-z0-9][a-z0-9._-]{0,99}`, which every generated id satisfies.
    /// Bucket paths use the id, never the name, so a rename or a delete and
    /// re-create never aliases data.
    RepoId,
    "repository id"
);

impl RepoId {
    /// The prefix of generated ids.
    pub const PREFIX: char = 'r';

    /// A new id: `r` and a fresh lowercase ULID.
    pub fn generate() -> Self {
        Self::from_ulid(Ulid::generate())
    }

    /// The generated id of `ulid`.
    pub fn from_ulid(ulid: Ulid) -> Self {
        Self(format!("r{}", lowercase_ulid(ulid)))
    }

    /// Parses `[a-z0-9][a-z0-9._-]{0,99}`.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        if is_name(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "repository id",
                s,
                "expected [a-z0-9][a-z0-9._-]{0,99}",
            ))
        }
    }
}

string_id!(
    /// A repository's name in its namespace: `[a-z0-9][a-z0-9._-]{0,99}`,
    /// not ending in `.git` (which URLs add). The catalog maps it to a
    /// [`RepoId`] (`_names/<name>.json`).
    RepoName,
    "repository name"
);

impl RepoName {
    /// Parses a repository name.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        if !is_name(s) {
            return Err(IdError::new(
                "repository name",
                s,
                "expected [a-z0-9][a-z0-9._-]{0,99}",
            ));
        }
        if s.ends_with(".git") {
            return Err(IdError::new("repository name", s, "must not end in '.git'"));
        }
        Ok(Self(s.to_string()))
    }
}

string_id!(
    /// A named workspace's name, `[a-z0-9][a-z0-9._-]{0,99}`, unique per
    /// owner (`_ws_names/<owner>/<name>.json`, §19.4.4). Unnamed (agent)
    /// workspaces have none.
    WsName,
    "workspace name"
);

impl WsName {
    /// Parses a workspace name.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        if is_name(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "workspace name",
                s,
                "expected [a-z0-9][a-z0-9._-]{0,99}",
            ))
        }
    }
}

ulid_id!(
    /// A workspace: `w` and 26 lowercase ULID characters (§19.4.1).
    WsId,
    "workspace id",
    'w'
);

ulid_id!(
    /// One vfsd mount of a workspace: `m` and a lowercase ULID, persisted in
    /// vfsd's `meta.sqlite`. Snapshots name their writer by it (§19.4.3).
    MountId,
    "mount id",
    'm'
);

ulid_id!(
    /// A sealed env value's secret: `s` and a lowercase ULID, allocated on the
    /// first seal of (repository, file, key) and reused after (§19.13.2).
    SecretId,
    "secret id",
    's'
);

/// The longest idempotency key, in bytes.
pub const MAX_IDEM_KEY_BYTES: usize = 128;

string_id!(
    /// A transaction's idempotency key (§36 §4.5): 1 to 128 bytes of
    /// `[A-Za-z0-9._:-]`. It is the CloudEvents `id` of its event.
    IdemKey,
    "idempotency key"
);

impl IdemKey {
    /// Parses an idempotency key.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let ok = (1..=MAX_IDEM_KEY_BYTES).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'));
        if ok {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "idempotency key",
                s,
                "expected 1 to 128 bytes of [A-Za-z0-9._:-]",
            ))
        }
    }
}

/// The longest principal, in bytes.
pub const MAX_PRINCIPAL_BYTES: usize = 256;

string_id!(
    /// Who acted: the credential's stable subject (§36 §4.5, Ruling 6). It is
    /// one path segment of `_ws_names/<owner>/`, so it is 1 to 256 bytes of
    /// printable ASCII without `/` or `\`, and not `.` or `..`.
    Principal,
    "principal"
);

impl Principal {
    /// Parses a principal.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let ok = (1..=MAX_PRINCIPAL_BYTES).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_graphic() && b != b'/' && b != b'\\')
            && s != "."
            && s != "..";
        if ok {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "principal",
                s,
                "expected 1 to 256 bytes of printable ASCII without '/' or '\\', not '.' or '..'",
            ))
        }
    }
}

string_id!(
    /// A full ref name under `refs/`, valid for `git check-ref-format`
    /// (through `gix-validate`). `HEAD` is not a `RefName`: it is handled as
    /// a symbolic ref only.
    RefName,
    "ref name"
);

impl RefName {
    /// Parses a ref name.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        if !s.starts_with("refs/") {
            return Err(IdError::new("ref name", s, "must start with 'refs/'"));
        }
        gix_validate::reference::name(s.as_bytes().into())
            .map_err(|_| IdError::new("ref name", s, "not a valid git ref name"))?;
        Ok(Self(s.to_string()))
    }
}

/// The longest repository path, in bytes.
pub const MAX_REPO_PATH_BYTES: usize = 4096;

string_id!(
    /// A path inside a repository's tree: `/`-separated, relative, with no
    /// empty, `.` or `..` segment, no `.git` segment (in any case) and no
    /// NUL, at most 4096 bytes. The empty path is the root.
    RepoPath,
    "repository path"
);

impl RepoPath {
    /// The root of the tree.
    pub fn root() -> Self {
        Self(String::new())
    }

    /// Parses a repository path.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let bad = |reason| Err(IdError::new("repository path", s, reason));
        if s.is_empty() {
            return Ok(Self::root());
        }
        if s.len() > MAX_REPO_PATH_BYTES {
            return bad("longer than 4096 bytes");
        }
        if s.contains('\0') {
            return bad("contains NUL");
        }
        for segment in s.split('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                return bad("empty, '.' or '..' segment");
            }
            if segment.eq_ignore_ascii_case(".git") {
                return bad("'.git' segment");
            }
        }
        Ok(Self(s.to_string()))
    }
}

/// A WAL sequence number (§36 §5.1). `Seq(0)` is the empty log; segments
/// start at 1. It prints as 20 zero-padded digits, the form object names use,
/// so names sort in sequence order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(pub u64);

impl Seq {
    /// The empty log.
    pub const ZERO: Seq = Seq(0);

    /// The next sequence number.
    ///
    /// # Panics
    /// At `u64::MAX`, which a log never reaches.
    pub fn next(self) -> Seq {
        Seq(self.0.checked_add(1).expect("sequence number overflow"))
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

impl FromStr for Seq {
    type Err = IdError;

    /// Parses the 20-digit form only.
    fn from_str(s: &str) -> Result<Self, IdError> {
        parse_twenty_digits(s)
            .map(Seq)
            .ok_or_else(|| IdError::new("sequence number", s, "expected 20 decimal digits"))
    }
}

/// A workspace snapshot number (§19.4.1); snapshot 0 is written by
/// `CreateWorkspace`. Prints as 20 zero-padded digits, as [`Seq`] does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapN(pub u64);

impl SnapN {
    /// The first snapshot.
    pub const ZERO: SnapN = SnapN(0);

    /// The next snapshot number.
    ///
    /// # Panics
    /// At `u64::MAX`, which a chain never reaches.
    pub fn next(self) -> SnapN {
        SnapN(self.0.checked_add(1).expect("snapshot number overflow"))
    }
}

impl fmt::Display for SnapN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

impl FromStr for SnapN {
    type Err = IdError;

    /// Parses the 20-digit form only.
    fn from_str(s: &str) -> Result<Self, IdError> {
        parse_twenty_digits(s)
            .map(SnapN)
            .ok_or_else(|| IdError::new("snapshot number", s, "expected 20 decimal digits"))
    }
}

fn parse_twenty_digits(s: &str) -> Option<u64> {
    (s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

string_id!(
    /// A W3C `traceparent` (Trace Context Level 1): `00-<32 hex>-<16 hex>-<2
    /// hex>` in lowercase, with a trace id and a parent id that are not all
    /// zeros. A later version (not `ff`) may append `-…` fields.
    TraceParent,
    "traceparent"
);

impl TraceParent {
    /// Parses a `traceparent` header value.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        if valid_traceparent(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "traceparent",
                s,
                "expected version-traceid-parentid-flags in lowercase hex",
            ))
        }
    }
}

fn valid_traceparent(s: &str) -> bool {
    let hex = |part: &str, len: usize| {
        part.len() == len
            && part
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let not_zero = |part: &str| part.bytes().any(|b| b != b'0');
    if s.len() < 55 {
        return false;
    }
    let (head, rest) = s.split_at(55);
    let parts: Vec<&str> = head.split('-').collect();
    let [version, trace, parent, flags] = parts[..] else {
        return false;
    };
    let fields = hex(version, 2)
        && version != "ff"
        && hex(trace, 32)
        && not_zero(trace)
        && hex(parent, 16)
        && not_zero(parent)
        && hex(flags, 2);
    let tail = if version == "00" {
        rest.is_empty()
    } else {
        rest.is_empty() || rest.starts_with('-')
    };
    fields && tail
}
