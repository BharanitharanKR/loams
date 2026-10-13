//! Synthetic binlog events for the unit tests: built byte by byte from the
//! binlog format (v4 headers, no checksum) and read back through
//! `mysql_common` exactly as the stream reads them.

use mysql_common::binlog::EventStreamReader;
use mysql_common::binlog::consts::{BinlogVersion, EventType};
use mysql_common::constants::ColumnType;

use crate::binlog::{RawEvent, decode_packet};
use crate::gtid::MARIADB_GTID;

pub(crate) const UUID: [u8; 16] = [
    0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62,
];
pub(crate) const UUID_TEXT: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
pub(crate) const BIGINT: u8 = ColumnType::MYSQL_TYPE_LONGLONG as u8;
pub(crate) const VARCHAR: u8 = ColumnType::MYSQL_TYPE_VARCHAR as u8;

pub(crate) struct Synth {
    esr: EventStreamReader,
    pub(crate) pos: u32,
    /// The last event's bytes.
    last: Vec<u8>,
}

/// A column of a synthetic table: its binlog type and metadata.
#[derive(Clone, Copy)]
pub(crate) struct Col(pub(crate) u8, pub(crate) &'static [u8]);

pub(crate) const ID: Col = Col(BIGINT, &[]);
pub(crate) const V40: Col = Col(VARCHAR, &[40, 0]);

pub(crate) enum Val<'a> {
    Null,
    Int(i64),
    Str(&'a [u8]),
}

impl Synth {
    pub(crate) fn new() -> Self {
        Self {
            esr: EventStreamReader::new(BinlogVersion::Version4),
            pos: 100,
            last: Vec::new(),
        }
    }

    /// An event's bytes, as a binlog dump sends them after its `0x00`.
    pub(crate) fn bytes(&mut self, ty: u8, body: &[u8]) -> Vec<u8> {
        self.pos += 100;
        let size = 19 + body.len();
        let mut v = Vec::new();
        v.extend(1_700_000_000u32.to_le_bytes());
        v.push(ty);
        v.extend(1u32.to_le_bytes());
        v.extend((size as u32).to_le_bytes());
        v.extend(self.pos.to_le_bytes());
        v.extend(0u16.to_le_bytes());
        v.extend(body);
        self.last.clone_from(&v);
        v
    }

    /// The last event as a binlog dump packet (`0x00` and the event).
    pub(crate) fn packet(&self) -> Vec<u8> {
        let mut p = vec![0u8];
        p.extend(&self.last);
        p
    }

    pub(crate) fn event(&mut self, ty: u8, body: &[u8]) -> RawEvent {
        let v = self.bytes(ty, body);
        decode_packet(&mut self.esr, &v)
            .expect("a well-formed event")
            .remove(0)
    }

    pub(crate) fn rotate(&mut self, file: &str) -> RawEvent {
        let mut b = 4u64.to_le_bytes().to_vec();
        b.extend(file.as_bytes());
        self.event(EventType::ROTATE_EVENT as u8, &b)
    }

    pub(crate) fn gtid(&mut self, gno: u64) -> RawEvent {
        let mut b = vec![1u8];
        b.extend(UUID);
        b.extend(gno.to_le_bytes());
        self.event(EventType::GTID_EVENT as u8, &b)
    }

    pub(crate) fn anonymous(&mut self) -> RawEvent {
        let mut b = vec![1u8];
        b.extend([0u8; 16]);
        b.extend(0u64.to_le_bytes());
        self.event(EventType::ANONYMOUS_GTID_EVENT as u8, &b)
    }

    pub(crate) fn maria_gtid(&mut self, seq: u64, domain: u32, standalone: bool) -> RawEvent {
        let mut b = seq.to_le_bytes().to_vec();
        b.extend(domain.to_le_bytes());
        b.push(u8::from(standalone));
        b.extend([0u8; 6]);
        self.event(MARIADB_GTID, &b)
    }

    pub(crate) fn query(&mut self, schema: &str, sql: &str) -> RawEvent {
        let mut b = Vec::new();
        b.extend(1u32.to_le_bytes());
        b.extend(0u32.to_le_bytes());
        b.push(schema.len() as u8);
        b.extend(0u16.to_le_bytes());
        b.extend(0u16.to_le_bytes());
        b.extend(schema.as_bytes());
        b.push(0);
        b.extend(sql.as_bytes());
        self.event(EventType::QUERY_EVENT as u8, &b)
    }

    pub(crate) fn xid(&mut self) -> RawEvent {
        self.event(EventType::XID_EVENT as u8, &9u64.to_le_bytes())
    }

    /// `names`: `binlog_row_metadata=FULL`'s column names, or none.
    pub(crate) fn table_map(
        &mut self,
        id: u64,
        db: &str,
        table: &str,
        cols: &[Col],
        names: &[&str],
    ) -> RawEvent {
        let mut b = id.to_le_bytes()[..6].to_vec();
        b.extend(0u16.to_le_bytes());
        b.push(db.len() as u8);
        b.extend(db.as_bytes());
        b.push(0);
        b.push(table.len() as u8);
        b.extend(table.as_bytes());
        b.push(0);
        b.push(cols.len() as u8);
        b.extend(cols.iter().map(|c| c.0));
        let meta: Vec<u8> = cols.iter().flat_map(|c| c.1.iter().copied()).collect();
        b.push(meta.len() as u8);
        b.extend(meta);
        b.extend(vec![0xffu8; cols.len().div_ceil(8)]);
        if !names.is_empty() {
            // COLUMN_NAME (4): length-encoded names.
            let mut v = Vec::new();
            for n in names {
                v.push(n.len() as u8);
                v.extend(n.as_bytes());
            }
            b.push(4);
            b.push(v.len() as u8);
            b.extend(v);
        }
        self.event(EventType::TABLE_MAP_EVENT as u8, &b)
    }

    pub(crate) fn image(cols: &[Col], row: &[Val<'_>]) -> Vec<u8> {
        let mut nulls = vec![0u8; cols.len().div_ceil(8)];
        let mut vals = Vec::new();
        for (i, v) in row.iter().enumerate() {
            match v {
                Val::Null => nulls[i / 8] |= 1 << (i % 8),
                Val::Int(n) => vals.extend(n.to_le_bytes()),
                Val::Str(s) => {
                    vals.push(s.len() as u8);
                    vals.extend(*s);
                }
            }
        }
        nulls.extend(vals);
        nulls
    }

    pub(crate) fn rows(
        &mut self,
        ty: EventType,
        id: u64,
        cols: &[Col],
        rows: &[&[Val<'_>]],
    ) -> RawEvent {
        let mut b = id.to_le_bytes()[..6].to_vec();
        b.extend(0u16.to_le_bytes());
        let v1 = matches!(
            ty,
            EventType::WRITE_ROWS_EVENT_V1
                | EventType::UPDATE_ROWS_EVENT_V1
                | EventType::DELETE_ROWS_EVENT_V1
        );
        if !v1 {
            // v2's extra data: just its own length.
            b.extend(2u16.to_le_bytes());
        }
        b.push(cols.len() as u8);
        let present = vec![0xffu8; cols.len().div_ceil(8)];
        b.extend(&present);
        if matches!(
            ty,
            EventType::UPDATE_ROWS_EVENT | EventType::UPDATE_ROWS_EVENT_V1
        ) {
            b.extend(&present);
        }
        for r in rows {
            b.extend(Self::image(cols, r));
        }
        self.event(ty as u8, &b)
    }

    /// A watermark row `(owner, mark)` written to the table mapped as `id`
    /// (columns [`WM_COLS`]).
    pub(crate) fn watermark(&mut self, id: u64, owner: &[u8], mark: &[u8]) -> RawEvent {
        let long = |v: &[u8]| -> Vec<u8> {
            let mut b = (v.len() as u16).to_le_bytes().to_vec();
            b.extend(v);
            b
        };
        let mut body = id.to_le_bytes()[..6].to_vec();
        body.extend(0u16.to_le_bytes());
        body.extend(2u16.to_le_bytes());
        body.push(2);
        body.push(0xff);
        body.push(0);
        body.extend(long(owner));
        body.extend(long(mark));
        self.event(EventType::WRITE_ROWS_EVENT as u8, &body)
    }
}

/// The watermark table's columns as the binlog has them: `varchar(255)` and
/// `varchar(64)` in utf8mb4, each with a 2-byte length.
pub(crate) const WM_COLS: [Col; 2] = [Col(VARCHAR, &[0xfc, 0x03]), Col(VARCHAR, &[0, 1])];
