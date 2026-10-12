//! The response tracker the migration relies on (SQ1 Task 6).
use loams_sqlgate::codec::command::{ErrPacket, OkPacket};
use loams_sqlgate::codec::handshake::Capabilities as C;
use loams_sqlgate::codec::response::{
    FRAME_HEAD, NotMovable, ResponseTracker, STATUS_CURSOR_EXISTS, STATUS_IN_TRANS,
    STATUS_MORE_RESULTS,
};
use proptest::prelude::*;

const AUTOCOMMIT: u16 = 0x0002;

fn ok(status: u16) -> Vec<u8> {
    OkPacket {
        affected_rows: 1,
        last_insert_id: 300,
        status,
        warnings: 0,
        info: Vec::new(),
    }
    .encode(C::PROTOCOL_41)
}

/// The OK that ends a result set under `CLIENT_DEPRECATE_EOF` (0xfe header).
fn ok_eof(status: u16) -> Vec<u8> {
    let mut p = ok(status);
    p[0] = 0xfe;
    p
}

fn eof(status: u16) -> Vec<u8> {
    let mut p = vec![0xfe, 0, 0];
    p.extend_from_slice(&status.to_le_bytes());
    p
}

fn err() -> Vec<u8> {
    ErrPacket::new(1064, *b"42000", "syntax").encode()
}

fn coldef() -> Vec<u8> {
    b"\x03def\x00\x00\x00\x01x\x00\x0c\x3f\x00\x0b\x00\x00\x00\x08\x00\x00\x00\x00\x00".to_vec()
}

fn text_row(v: &[u8]) -> Vec<u8> {
    let mut p = vec![v.len() as u8];
    p.extend_from_slice(v);
    p
}

fn feed(t: &mut ResponseTracker, packets: &[Vec<u8>]) {
    for p in packets {
        t.frame(p.len(), &p[..p.len().min(FRAME_HEAD)]);
    }
}

#[test]
fn a_new_session_is_movable_and_a_command_makes_it_busy() {
    let mut t = ResponseTracker::new(true, true);
    assert_eq!(t.movable(), Ok(()));
    t.command(b"\x03SELECT 1");
    assert_eq!(t.movable(), Err(NotMovable::Busy));
    assert_eq!(t.commands(), 1);
}

#[test]
fn result_set_with_deprecate_eof_ends_at_the_ok_terminator() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03SELECT x");
    feed(&mut t, &[vec![1], coldef(), text_row(b"7")]);
    assert!(!t.idle());
    feed(&mut t, &[ok_eof(AUTOCOMMIT)]);
    assert!(t.idle());
    assert_eq!(t.movable(), Ok(()));
}

#[test]
fn result_set_without_deprecate_eof_has_two_eofs() {
    let mut t = ResponseTracker::new(false, true);
    t.command(b"\x03SELECT x");
    feed(
        &mut t,
        &[vec![1], coldef(), eof(AUTOCOMMIT), text_row(b"7")],
    );
    assert!(!t.idle(), "the EOF after the columns does not end it");
    feed(&mut t, &[eof(AUTOCOMMIT | STATUS_IN_TRANS)]);
    assert!(t.idle());
    assert_eq!(t.movable(), Err(NotMovable::InTransaction));
}

#[test]
fn begin_and_commit_follow_the_in_transaction_flag() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03BEGIN");
    feed(&mut t, &[ok(AUTOCOMMIT | STATUS_IN_TRANS)]);
    assert_eq!(t.movable(), Err(NotMovable::InTransaction));
    // An error inside the transaction leaves it open.
    t.command(b"\x03SELEC");
    feed(&mut t, &[err()]);
    assert_eq!(t.movable(), Err(NotMovable::InTransaction));
    t.command(b"\x03COMMIT");
    feed(&mut t, &[ok(AUTOCOMMIT)]);
    assert_eq!(t.movable(), Ok(()));
}

#[test]
fn multi_statement_results_are_followed_to_the_last() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03SELECT 1; INSERT ...; SELECT 2");
    feed(
        &mut t,
        &[
            vec![1],
            coldef(),
            text_row(b"1"),
            ok_eof(AUTOCOMMIT | STATUS_MORE_RESULTS),
            ok(AUTOCOMMIT | STATUS_MORE_RESULTS),
        ],
    );
    assert!(!t.idle());
    feed(
        &mut t,
        &[vec![1], coldef(), text_row(b"2"), ok_eof(AUTOCOMMIT)],
    );
    assert!(t.idle());
}

#[test]
fn a_cursor_response_ends_after_the_columns_and_holds_the_session() {
    for deprecate_eof in [true, false] {
        let mut t = ResponseTracker::new(deprecate_eof, true);
        t.command(b"\x17\x01\x00\x00\x00\x01\x01\x00\x00\x00");
        let end = if deprecate_eof { ok_eof } else { eof };
        feed(
            &mut t,
            &[vec![1], coldef(), end(AUTOCOMMIT | STATUS_CURSOR_EXISTS)],
        );
        assert!(t.idle(), "{deprecate_eof}");
        assert_eq!(t.movable(), Err(NotMovable::CursorOpen));
        // COM_STMT_FETCH: binary rows (0x00 header), then the last row.
        t.command(b"\x1c\x01\x00\x00\x00\x0a\x00\x00\x00");
        feed(&mut t, &[vec![0x00, 0x00, 0x07]]);
        assert!(!t.idle());
        feed(&mut t, &[end(AUTOCOMMIT | 0x0080)]);
        assert_eq!(t.movable(), Ok(()), "{deprecate_eof}");
    }
}

#[test]
fn prepare_reads_its_parameter_and_column_definitions() {
    for (deprecate_eof, defs) in [(true, 3), (false, 5)] {
        let mut t = ResponseTracker::new(deprecate_eof, true);
        t.command(b"\x16SELECT ?, ?");
        // id 1, two columns, one parameter.
        let prepare_ok = vec![0x00, 1, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0];
        feed(&mut t, &[prepare_ok]);
        for _ in 0..defs - 1 {
            feed(&mut t, &[coldef()]);
            assert!(!t.idle());
        }
        feed(&mut t, &[coldef()]);
        assert!(t.idle(), "{deprecate_eof}");
    }
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x16DO 1");
    feed(&mut t, &[vec![0x00, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]);
    assert!(t.idle(), "no definitions");
}

#[test]
fn commands_without_answers_and_single_packet_answers() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x19\x01\x00\x00\x00");
    t.command(b"\x18\x01\x00\x00\x00\x00\x00data");
    assert!(t.idle(), "COM_STMT_CLOSE and SEND_LONG_DATA get no answer");
    t.command(b"\x09");
    feed(&mut t, &[b"Uptime: 7".to_vec()]);
    assert!(t.idle());
    t.command(b"\x0e");
    feed(&mut t, &[ok(AUTOCOMMIT)]);
    assert_eq!(t.movable(), Ok(()));
}

#[test]
fn set_option_tracks_multi_statements() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x1b\x01\x00");
    feed(&mut t, &[ok_eof(0)]);
    assert!(!t.multi_statements());
    assert_eq!(
        t.movable(),
        Ok(()),
        "SET_OPTION's status is not the session's"
    );
    t.command(b"\x1b\x00\x00");
    feed(&mut t, &[err()]);
    assert!(!t.multi_statements(), "a refused option changes nothing");
    t.command(b"\x1b\x00\x00");
    feed(&mut t, &[eof(0)]);
    assert!(t.multi_statements());
}

#[test]
fn pipelined_commands_are_answered_in_order() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03SELECT 1");
    t.command(b"\x03BEGIN");
    feed(
        &mut t,
        &[vec![1], coldef(), text_row(b"1"), ok_eof(AUTOCOMMIT)],
    );
    assert!(!t.idle());
    feed(&mut t, &[ok(AUTOCOMMIT | STATUS_IN_TRANS)]);
    assert_eq!(t.movable(), Err(NotMovable::InTransaction));
}

#[test]
fn a_row_over_16_mib_spans_frames() {
    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03SELECT big");
    feed(&mut t, &[vec![1], coldef()]);
    // The row's first frame is full; its continuation starts with 0xfe
    // bytes of data and is not a terminator.
    t.frame(0xff_ffff, &[0xfe, 1, 0, 0, 1, 0, 0, 0, 0]);
    t.frame(5, &[0xfe, 0, 0, 2, 0]);
    assert!(!t.idle());
    feed(&mut t, &[ok_eof(AUTOCOMMIT)]);
    assert!(t.idle());
}

#[test]
fn unexpected_packets_and_local_infile_stop_migration_for_good() {
    let mut t = ResponseTracker::new(true, true);
    feed(&mut t, &[err()]);
    assert_eq!(t.movable(), Err(NotMovable::Untracked));

    let mut t = ResponseTracker::new(true, true);
    t.command(b"\x03LOAD DATA LOCAL INFILE 'x' INTO TABLE t");
    feed(&mut t, &[b"\xfbx".to_vec()]);
    assert_eq!(t.movable(), Err(NotMovable::Untracked));
}

proptest! {
    /// Untrusted upstream bytes never panic the tracker.
    #[test]
    fn tracker_never_panics(
        deprecate_eof: bool,
        steps in proptest::collection::vec(
            (any::<bool>(), proptest::collection::vec(any::<u8>(), 0..40), 0usize..0x100_0000),
            0..40,
        ),
    ) {
        let mut t = ResponseTracker::new(deprecate_eof, true);
        for (is_command, bytes, len) in steps {
            if is_command {
                t.command(&bytes);
            } else {
                t.frame(len, &bytes);
            }
            let _ = t.movable();
        }
    }
}
