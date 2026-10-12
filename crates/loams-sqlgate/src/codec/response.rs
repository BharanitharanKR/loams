//! Where a relayed session stands between TiDB's responses (plan SQ1 Task
//! 6): sans-I/O, from the first bytes of each client command and of each
//! upstream frame.
//!
//! The gate still never parses SQL (D731). It follows only the response
//! framing of the MySQL protocol, which depends on the command byte: a
//! result set's column count, column definitions, optional EOF, rows and
//! terminator; a prepare's parameter and column definitions; single-packet
//! answers. From the terminators it keeps TiDB's latest server status, so
//! the migration knows whether the session is idle, inside a transaction or
//! holding a cursor open.
//!
//! The tracker is a cheap pre-check, never the authority. TiDB refuses
//! `SHOW SESSION_STATES` (ERR 8146) for every state it cannot move (an
//! open transaction, a cursor with unfetched rows, bound long data, table
//! or advisory locks, temporary tables), and the migration honours that.

use std::collections::VecDeque;

use super::Reader;

/// `SERVER_STATUS_IN_TRANS`.
pub const STATUS_IN_TRANS: u16 = 0x0001;
/// `SERVER_MORE_RESULTS_EXISTS`: another result of a multi-statement query
/// or a procedure follows.
pub const STATUS_MORE_RESULTS: u16 = 0x0008;
/// `SERVER_STATUS_CURSOR_EXISTS`.
pub const STATUS_CURSOR_EXISTS: u16 = 0x0040;
/// `SERVER_STATUS_IN_TRANS_READONLY`.
pub const STATUS_IN_TRANS_READONLY: u16 = 0x2000;

/// The largest frame payload; a frame this long continues in the next.
const MAX_FRAME: usize = 0xff_ffff;

/// How many leading bytes of a frame [`ResponseTracker::frame`] reads (an
/// OK's status lies within 1 + 9 + 9 + 2 bytes).
pub const FRAME_HEAD: usize = 32;

/// `COM_SET_OPTION`'s options.
const MULTI_STATEMENTS_ON: u16 = 0;
const MULTI_STATEMENTS_OFF: u16 = 1;

/// Why a session cannot move now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotMovable {
    /// A command is in flight, or a response is not finished.
    Busy,
    /// TiDB's last status says a transaction is open.
    InTransaction,
    /// TiDB's last status says a cursor is open.
    CursorOpen,
    /// The tracker lost the response framing (an unexpected packet, a
    /// `LOAD DATA LOCAL` request): the session never moves.
    Untracked,
}

/// What the response to one command looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// One packet (OK, ERR, EOF or a string). `status`: the OK or EOF
    /// carries the session's status. `set_option`: a `COM_SET_OPTION`
    /// turning multi-statements on (`Some(true)`) or off.
    Single {
        status: bool,
        set_option: Option<bool>,
    },
    /// The first packet of a result: OK, ERR, `LOCAL INFILE` or a column
    /// count (`COM_QUERY`, `COM_STMT_EXECUTE`).
    Result,
    /// `left` column definitions of a result set.
    Columns { left: u64 },
    /// The EOF after the column definitions (no `CLIENT_DEPRECATE_EOF`). A
    /// cursor's response ends here.
    ColumnsEof,
    /// Rows until the terminator (also `COM_STMT_FETCH`'s whole answer).
    Rows,
    /// `COM_FIELD_LIST`: column definitions until EOF.
    FieldList,
    /// `COM_STMT_PREPARE`: the prepare OK or ERR.
    Prepare,
    /// The prepare's parameter and column definitions and their EOFs.
    Definitions { left: u64 },
}

/// The response framing of one relayed session.
#[derive(Debug, Clone)]
pub struct ResponseTracker {
    deprecate_eof: bool,
    /// One entry per command still awaiting (part of) its response; the
    /// front is the response being read.
    pending: VecDeque<Expect>,
    /// The previous frame was `MAX_FRAME` long: this one continues it.
    continued: bool,
    status: u16,
    multi_statements: bool,
    untracked: bool,
    commands: u64,
}

impl ResponseTracker {
    /// A session just logged in (no transaction), with
    /// `CLIENT_DEPRECATE_EOF` agreed or not, and multi-statements on or
    /// off as agreed at login.
    pub fn new(deprecate_eof: bool, multi_statements: bool) -> Self {
        Self {
            deprecate_eof,
            pending: VecDeque::new(),
            continued: false,
            status: 0,
            multi_statements,
            untracked: false,
            commands: 0,
        }
    }

    /// A command relayed to TiDB: `first` is its payload's first bytes
    /// (the command byte, and for `COM_SET_OPTION` the two option bytes).
    pub fn command(&mut self, first: &[u8]) {
        let Some(&byte) = first.first() else {
            self.untracked = true;
            return;
        };
        self.commands += 1;
        let single = |status| Expect::Single {
            status,
            set_option: None,
        };
        let expect = match byte {
            // COM_QUIT, COM_STMT_SEND_LONG_DATA, COM_STMT_CLOSE: no answer.
            0x01 | 0x18 | 0x19 => return,
            // COM_INIT_DB, COM_REFRESH, COM_PING, COM_STMT_RESET,
            // COM_RESET_CONNECTION: OK or ERR.
            0x02 | 0x07 | 0x0e | 0x1a | 0x1f => single(true),
            // COM_STATISTICS: a string.
            0x09 => single(false),
            0x1b => Expect::Single {
                status: false,
                set_option: match first.get(1..3) {
                    Some([lo, hi]) => match u16::from_le_bytes([*lo, *hi]) {
                        MULTI_STATEMENTS_ON => Some(true),
                        MULTI_STATEMENTS_OFF => Some(false),
                        _ => None,
                    },
                    _ => None,
                },
            },
            0x03 | 0x17 => Expect::Result,
            0x1c => Expect::Rows,
            0x04 => Expect::FieldList,
            0x16 => Expect::Prepare,
            _ => {
                self.untracked = true;
                return;
            }
        };
        self.pending.push_back(expect);
    }

    /// One frame from TiDB: its payload length and its first bytes (at
    /// least `min(len, FRAME_HEAD)` of them).
    pub fn frame(&mut self, len: usize, head: &[u8]) {
        let continuation = self.continued;
        self.continued = len == MAX_FRAME;
        if continuation {
            // The rest of a packet already counted.
            return;
        }
        let Some(expect) = self.pending.front().copied() else {
            // Nothing was asked: an unsolicited packet.
            self.untracked = true;
            return;
        };
        let first = head.first().copied();
        let terminator = first == Some(0xfe) && len < MAX_FRAME;
        let next = match expect {
            Expect::Single { status, set_option } => {
                match first {
                    Some(0x00 | 0xfe) => {
                        if status {
                            self.take_status(head);
                        }
                        if let Some(on) = set_option {
                            self.multi_statements = on;
                        }
                    }
                    Some(0xff) => {}
                    _ if status => self.untracked = true,
                    _ => {}
                }
                None
            }
            Expect::Result => match first {
                Some(0x00) => self.after_terminator(head, false),
                Some(0xff) => None,
                Some(0xfb) => {
                    // LOAD DATA LOCAL: the client answers with file data
                    // the relay does not frame. Never moved again.
                    self.untracked = true;
                    None
                }
                _ => match Reader::new(head).lenenc("column count") {
                    Ok(n) if n > 0 => Some(Expect::Columns { left: n }),
                    _ => {
                        self.untracked = true;
                        None
                    }
                },
            },
            Expect::Columns { left } => {
                if left > 1 {
                    Some(Expect::Columns { left: left - 1 })
                } else if self.deprecate_eof {
                    Some(Expect::Rows)
                } else {
                    Some(Expect::ColumnsEof)
                }
            }
            Expect::ColumnsEof => match first {
                Some(0xfe) if terminator => {
                    self.take_eof_status(head);
                    if self.status & STATUS_CURSOR_EXISTS != 0 {
                        None
                    } else {
                        Some(Expect::Rows)
                    }
                }
                Some(0xff) => None,
                _ => {
                    self.untracked = true;
                    None
                }
            },
            Expect::Rows => match first {
                Some(0xfe) if terminator => self.after_terminator(head, true),
                Some(0xff) => None,
                _ => Some(Expect::Rows),
            },
            Expect::FieldList => match first {
                Some(0xfe) if terminator => None,
                Some(0xff) => None,
                _ => Some(Expect::FieldList),
            },
            Expect::Prepare => match first {
                Some(0x00) => {
                    let mut r = Reader::new(head);
                    let counts = (|| {
                        r.u8("header")?;
                        r.u32("statement id")?;
                        let columns = u64::from(r.u16("columns")?);
                        let params = u64::from(r.u16("params")?);
                        Ok::<_, super::DecodeError>((columns, params))
                    })();
                    match counts {
                        Ok((columns, params)) => {
                            let eof = |n: u64| u64::from(n > 0 && !self.deprecate_eof);
                            let left = columns + eof(columns) + params + eof(params);
                            (left > 0).then_some(Expect::Definitions { left })
                        }
                        Err(_) => {
                            self.untracked = true;
                            None
                        }
                    }
                }
                Some(0xff) => None,
                _ => {
                    self.untracked = true;
                    None
                }
            },
            Expect::Definitions { left } => {
                (left > 1).then_some(Expect::Definitions { left: left - 1 })
            }
        };
        match next {
            Some(e) => {
                if let Some(front) = self.pending.front_mut() {
                    *front = e;
                }
            }
            None => {
                self.pending.pop_front();
            }
        }
    }

    /// After a result's OK or terminator: the next result when TiDB says
    /// more follow, otherwise the response is done.
    fn after_terminator(&mut self, head: &[u8], terminator: bool) -> Option<Expect> {
        if terminator && !self.deprecate_eof {
            self.take_eof_status(head);
        } else {
            self.take_status(head);
        }
        (self.status & STATUS_MORE_RESULTS != 0).then_some(Expect::Result)
    }

    /// The status of an OK packet (header 0x00, or 0xfe in place of an EOF).
    fn take_status(&mut self, head: &[u8]) {
        let mut r = Reader::new(head);
        let status = (|| {
            r.u8("header")?;
            r.lenenc("affected rows")?;
            r.lenenc("last insert id")?;
            r.u16("status")
        })();
        match status {
            Ok(s) => self.status = s,
            Err(_) => self.untracked = true,
        }
    }

    /// The status of an EOF packet (0xfe, warnings, status).
    fn take_eof_status(&mut self, head: &[u8]) {
        match head.get(3..5) {
            Some([lo, hi]) => self.status = u16::from_le_bytes([*lo, *hi]),
            _ => self.untracked = true,
        }
    }

    /// No command awaits (part of) its answer.
    pub fn idle(&self) -> bool {
        self.pending.is_empty() && !self.continued
    }

    /// TiDB's latest server status.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Commands relayed so far.
    pub fn commands(&self) -> u64 {
        self.commands
    }

    /// Whether multi-statements are on (the login's choice, then the last
    /// successful `COM_SET_OPTION`).
    pub fn multi_statements(&self) -> bool {
        self.multi_statements
    }

    /// Whether the session may move now, as far as the gate can tell.
    pub fn movable(&self) -> Result<(), NotMovable> {
        if self.untracked {
            Err(NotMovable::Untracked)
        } else if !self.idle() {
            Err(NotMovable::Busy)
        } else if self.status & (STATUS_IN_TRANS | STATUS_IN_TRANS_READONLY) != 0 {
            Err(NotMovable::InTransaction)
        } else if self.status & STATUS_CURSOR_EXISTS != 0 {
            Err(NotMovable::CursorOpen)
        } else {
            Ok(())
        }
    }
}
