//! Binlog positions: MySQL GTID sets, MariaDB GTID lists, and `file:pos`
//! when GTIDs are off (CN2 Task 11; R0.7, R0.8).
//!
//! MySQL resumes with `COM_BINLOG_DUMP_GTID` and the set of transactions the
//! Fabric already has; the server sends every transaction outside it.
//! MariaDB's GTID is `domain-server-sequence`, and a replica resumes by
//! setting `@slave_connect_state` to the last GTID it applied in each domain
//! before a plain `COM_BINLOG_DUMP`. `mysql_common` 0.37 knows neither that
//! nor MariaDB's GTID events (`MARIADB_GTID_EVENT` 0xA2,
//! `MARIADB_GTID_LIST_EVENT` 0xA3), which `Event::read_data` reports as
//! unknown; [`MariaGtidEvent`] and [`parse_gtid_list_event`] read them from
//! the raw event body.

use std::collections::BTreeMap;
use std::fmt;

use mysql_common::packets::{GnoInterval, Sid, Tag};
use serde::{Deserialize, Serialize};

/// `MARIADB_ANNOTATE_ROWS_EVENT`: the statement behind the next rows events.
pub const MARIADB_ANNOTATE_ROWS: u8 = 0xa0;
/// `MARIADB_BINLOG_CHECKPOINT_EVENT`.
pub const MARIADB_BINLOG_CHECKPOINT: u8 = 0xa1;
/// `MARIADB_GTID_EVENT`: starts an event group (a transaction or a DDL).
pub const MARIADB_GTID: u8 = 0xa2;
/// `MARIADB_GTID_LIST_EVENT`: the binlog state at the start of a file.
pub const MARIADB_GTID_LIST: u8 = 0xa3;
/// `MARIADB_START_ENCRYPTION_EVENT`.
pub const MARIADB_START_ENCRYPTION: u8 = 0xa4;
/// MariaDB's compressed query and rows events (`log_bin_compress`).
pub const MARIADB_COMPRESSED: std::ops::RangeInclusive<u8> = 0xa5..=0xaa;

/// A MySQL server UUID as text (`3e11fa47-71ca-11e1-9e33-c80aa9429562`).
pub fn uuid_text(u: &[u8; 16]) -> String {
    let h: String = u.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

fn uuid_bytes(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// A MySQL GTID set: per source UUID (and tag, MySQL 8.4+), the disjoint,
/// sorted intervals `[start, end)` of transaction numbers. Serialised as the
/// server's text form.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct GtidSet {
    sets: BTreeMap<(String, String), Vec<(u64, u64)>>,
}

impl From<GtidSet> for String {
    fn from(g: GtidSet) -> String {
        g.to_string()
    }
}

impl TryFrom<String> for GtidSet {
    type Error = String;
    fn try_from(s: String) -> Result<Self, String> {
        GtidSet::parse(&s)
    }
}

impl GtidSet {
    /// The server's text form: `uuid:1-5:7,uuid2:tag:1-3` (whitespace and
    /// newlines, which `@@gtid_executed` carries, are ignored).
    pub fn parse(s: &str) -> Result<Self, String> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let mut out = GtidSet::default();
        for entry in s.split(',').filter(|e| !e.is_empty()) {
            let mut parts = entry.split(':');
            let uuid = parts.next().unwrap_or_default().to_ascii_lowercase();
            if uuid_bytes(&uuid).is_none() {
                return Err(format!("not a GTID set: bad UUID in {entry:?}"));
            }
            let mut tag = String::new();
            for part in parts {
                let interval = match part.split_once('-') {
                    Some((a, b)) => a.parse::<u64>().ok().zip(b.parse::<u64>().ok()),
                    None => part.parse::<u64>().ok().map(|a| (a, a)),
                };
                match interval {
                    Some((a, b)) if a >= 1 && b >= a => out.insert_range(&uuid, &tag, a, b + 1),
                    Some(_) => return Err(format!("not a GTID set: bad interval in {entry:?}")),
                    None if !part.is_empty()
                        && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
                    {
                        tag = part.to_ascii_lowercase();
                    }
                    None => return Err(format!("not a GTID set: {entry:?}")),
                }
            }
        }
        Ok(out)
    }

    fn insert_range(&mut self, uuid: &str, tag: &str, start: u64, end: u64) {
        let v = self
            .sets
            .entry((uuid.to_owned(), tag.to_owned()))
            .or_default();
        v.push((start, end));
        v.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(v.len());
        for (a, b) in v.drain(..) {
            match merged.last_mut() {
                Some(last) if a <= last.1 => last.1 = last.1.max(b),
                _ => merged.push((a, b)),
            }
        }
        *v = merged;
    }

    /// Add one transaction.
    pub fn add(&mut self, uuid: &[u8; 16], tag: Option<&str>, gno: u64) {
        self.insert_range(
            &uuid_text(uuid),
            &tag.unwrap_or_default().to_ascii_lowercase(),
            gno,
            gno + 1,
        );
    }

    pub fn contains(&self, uuid: &[u8; 16], tag: Option<&str>, gno: u64) -> bool {
        self.sets
            .get(&(
                uuid_text(uuid),
                tag.unwrap_or_default().to_ascii_lowercase(),
            ))
            .is_some_and(|v| v.iter().any(|(a, b)| *a <= gno && gno < *b))
    }

    /// Whether every transaction of `other` is in `self`.
    pub fn contains_set(&self, other: &GtidSet) -> bool {
        other.sets.iter().all(|(k, ranges)| {
            let mine = self.sets.get(k).map(Vec::as_slice).unwrap_or_default();
            ranges
                .iter()
                .all(|(a, b)| mine.iter().any(|(x, y)| x <= a && b <= y))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// The set as `COM_BINLOG_DUMP_GTID`'s SID blocks.
    pub fn sids(&self) -> Result<Vec<Sid<'static>>, String> {
        self.sets
            .iter()
            .map(|((uuid, tag), ranges)| {
                let bytes = uuid_bytes(uuid).ok_or_else(|| format!("bad UUID {uuid}"))?;
                let mut sid = Sid::new(bytes).with_intervals(
                    ranges
                        .iter()
                        .map(|(a, b)| GnoInterval::new(*a, *b))
                        .collect(),
                );
                if !tag.is_empty() {
                    let t = Tag::new(tag.clone()).map_err(|_| format!("bad GTID tag {tag}"))?;
                    sid = sid.with_tag(t);
                }
                Ok(sid)
            })
            .collect()
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for ((uuid, tag), ranges) in &self.sets {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            f.write_str(uuid)?;
            if !tag.is_empty() {
                write!(f, ":{tag}")?;
            }
            for (a, b) in ranges {
                if b - a == 1 {
                    write!(f, ":{a}")?;
                } else {
                    write!(f, ":{a}-{}", b - 1)?;
                }
            }
        }
        Ok(())
    }
}

/// A MariaDB GTID, `domain-server-sequence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MariaGtid {
    pub domain: u32,
    pub server: u32,
    pub seq: u64,
}

impl MariaGtid {
    pub fn parse(s: &str) -> Option<Self> {
        let mut p = s.trim().split('-');
        let g = MariaGtid {
            domain: p.next()?.parse().ok()?,
            server: p.next()?.parse().ok()?,
            seq: p.next()?.parse().ok()?,
        };
        p.next().is_none().then_some(g)
    }
}

impl fmt::Display for MariaGtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.domain, self.server, self.seq)
    }
}

/// A MariaDB GTID position: the last GTID per domain, as
/// `@@gtid_binlog_pos` and `@slave_connect_state` write it
/// (`0-1-100,1-2-7`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct GtidList {
    domains: BTreeMap<u32, MariaGtid>,
}

impl From<GtidList> for String {
    fn from(g: GtidList) -> String {
        g.to_string()
    }
}

impl TryFrom<String> for GtidList {
    type Error = String;
    fn try_from(s: String) -> Result<Self, String> {
        GtidList::parse(&s)
    }
}

impl GtidList {
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut out = GtidList::default();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let g =
                MariaGtid::parse(part).ok_or_else(|| format!("not a MariaDB GTID: {part:?}"))?;
            out.domains.insert(g.domain, g);
        }
        Ok(out)
    }

    /// Record `g` as the last GTID of its domain.
    pub fn update(&mut self, g: MariaGtid) {
        self.domains.insert(g.domain, g);
    }

    /// Whether `g` is at or before this list's position in its domain.
    pub fn covers(&self, g: &MariaGtid) -> bool {
        self.domains
            .get(&g.domain)
            .is_some_and(|last| last.seq >= g.seq)
    }

    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &MariaGtid> {
        self.domains.values()
    }
}

impl fmt::Display for GtidList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.domains.values().map(ToString::to_string).collect();
        f.write_str(&parts.join(","))
    }
}

/// `MARIADB_GTID_EVENT`'s body (checksum removed): sequence number (8 LE),
/// domain id (4 LE), flags (1), then optional fields this crate does not
/// need. The server id comes from the event header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaGtidEvent {
    pub gtid: MariaGtid,
    /// `FL_STANDALONE`: the group is one statement with no BEGIN/COMMIT
    /// (DDL, or a non-transactional statement).
    pub standalone: bool,
}

impl MariaGtidEvent {
    pub const FL_STANDALONE: u8 = 1;

    pub fn parse(body: &[u8], server_id: u32) -> Option<Self> {
        let seq = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
        let domain = u32::from_le_bytes(body.get(8..12)?.try_into().ok()?);
        let flags = *body.get(12)?;
        Some(Self {
            gtid: MariaGtid {
                domain,
                server: server_id,
                seq,
            },
            standalone: flags & Self::FL_STANDALONE != 0,
        })
    }
}

/// `MARIADB_GTID_LIST_EVENT`'s body: a count (4 LE; the low 28 bits) and
/// that many `(domain 4, server 4, sequence 8)`.
pub fn parse_gtid_list_event(body: &[u8]) -> Option<Vec<MariaGtid>> {
    let count = u32::from_le_bytes(body.get(0..4)?.try_into().ok()?) & 0x0fff_ffff;
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let at = 4 + 16 * i;
        let e = body.get(at..at + 16)?;
        out.push(MariaGtid {
            domain: u32::from_le_bytes(e[0..4].try_into().ok()?),
            server: u32::from_le_bytes(e[4..8].try_into().ok()?),
            seq: u64::from_le_bytes(e[8..16].try_into().ok()?),
        });
    }
    Some(out)
}

/// Where the binlog reader is: after the last whole transaction it has
/// delivered. `file`/`pos` follow the stream in every flavour (for
/// `loamslsn` and humans); the GTID state is what a restart resumes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "flavor")]
pub enum BinlogPos {
    Mysql {
        executed: GtidSet,
        file: String,
        pos: u64,
    },
    Mariadb {
        state: GtidList,
        file: String,
        pos: u64,
    },
    /// GTIDs off: a binlog file and the offset of the next transaction.
    File { file: String, pos: u64 },
}

impl BinlogPos {
    pub fn file_pos(&self) -> (&str, u64) {
        match self {
            BinlogPos::Mysql { file, pos, .. }
            | BinlogPos::Mariadb { file, pos, .. }
            | BinlogPos::File { file, pos } => (file, *pos),
        }
    }

    pub fn set_file_pos(&mut self, f: &str, p: u64) {
        match self {
            BinlogPos::Mysql { file, pos, .. }
            | BinlogPos::Mariadb { file, pos, .. }
            | BinlogPos::File { file, pos } => {
                if !f.is_empty() {
                    f.clone_into(file);
                }
                *pos = p;
            }
        }
    }
}

impl fmt::Display for BinlogPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinlogPos::Mysql { executed, .. } => write!(f, "gtid {executed}"),
            BinlogPos::Mariadb { state, .. } => write!(f, "gtid {state}"),
            BinlogPos::File { file, pos } => write!(f, "{file}:{pos}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    #[test]
    fn gtid_sets_parse_merge_and_print() {
        let s = GtidSet::parse(&format!("{U1}:1-5:7,\n{}:3", U1.to_uppercase())).expect("set");
        assert_eq!(s.to_string(), format!("{U1}:1-5:7"));
        let mut s = s;
        let u = uuid_bytes(U1).expect("uuid");
        s.add(&u, None, 6);
        assert_eq!(s.to_string(), format!("{U1}:1-7"));
        assert!(s.contains(&u, None, 7) && !s.contains(&u, None, 8));
        s.add(&u, Some("Loams"), 1);
        assert_eq!(s.to_string(), format!("{U1}:1-7,{U1}:loams:1"));
        let back = GtidSet::parse(&s.to_string()).expect("roundtrip");
        assert_eq!(back, s);
        assert!(s.contains_set(&GtidSet::parse(&format!("{U1}:2-4")).expect("sub")));
        assert!(!s.contains_set(&GtidSet::parse(&format!("{U1}:5-9")).expect("not sub")));
        assert!(GtidSet::parse("").expect("empty").is_empty());
        assert!(GtidSet::parse("nope:1").is_err());
        assert!(GtidSet::parse(&format!("{U1}:5-2")).is_err());
        assert_eq!(s.sids().expect("sids").len(), 2);
        let json = serde_json::to_string(&s).expect("json");
        assert_eq!(serde_json::from_str::<GtidSet>(&json).expect("json"), s);
    }

    #[test]
    fn mariadb_gtid_position_roundtrip() {
        let mut l = GtidList::parse("0-1-100, 1-2-7").expect("list");
        assert_eq!(l.to_string(), "0-1-100,1-2-7");
        l.update(MariaGtid {
            domain: 0,
            server: 3,
            seq: 101,
        });
        assert_eq!(l.to_string(), "0-3-101,1-2-7");
        assert!(l.covers(&MariaGtid::parse("1-2-7").expect("gtid")));
        assert!(!l.covers(&MariaGtid::parse("1-2-8").expect("gtid")));
        assert!(!l.covers(&MariaGtid::parse("5-1-1").expect("gtid")));
        assert!(GtidList::parse("0-1").is_err());
        let pos = BinlogPos::Mariadb {
            state: l.clone(),
            file: "mariadb-bin.000002".into(),
            pos: 1234,
        };
        let json = serde_json::to_string(&pos).expect("json");
        assert_eq!(serde_json::from_str::<BinlogPos>(&json).expect("back"), pos);
        assert_eq!(pos.to_string(), "gtid 0-3-101,1-2-7");
    }

    #[test]
    fn mariadb_events_parse_from_raw_bodies() {
        // seq 42, domain 1, flags FL_STANDALONE | FL_GROUP_COMMIT_ID, commit id.
        let mut body = 42u64.to_le_bytes().to_vec();
        body.extend(1u32.to_le_bytes());
        body.push(0x03);
        body.extend(9u64.to_le_bytes());
        let e = MariaGtidEvent::parse(&body, 7).expect("gtid event");
        assert_eq!(e.gtid.to_string(), "1-7-42");
        assert!(e.standalone);
        assert!(MariaGtidEvent::parse(&body[..10], 7).is_none());

        let mut list = 2u32.to_le_bytes().to_vec();
        for (d, s, n) in [(0u32, 1u32, 100u64), (1, 2, 7)] {
            list.extend(d.to_le_bytes());
            list.extend(s.to_le_bytes());
            list.extend(n.to_le_bytes());
        }
        let gtids = parse_gtid_list_event(&list).expect("list event");
        assert_eq!(gtids.len(), 2);
        assert_eq!(gtids[1].to_string(), "1-2-7");
        assert!(parse_gtid_list_event(&list[..20]).is_none());
    }

    #[test]
    fn uuids_render() {
        let u = uuid_bytes(U1).expect("uuid");
        assert_eq!(uuid_text(&u), U1);
    }
}
