//! Pack objects (§36 §4.3, D390).

use std::process::{Command, Stdio};

use loams_git::ObjectId;
use loams_git::format::FormatError;
use loams_git::format::lpk::{
    LPK_FOOTER_LEN, LPK_MAGIC, decode_footer, encode_footer, pack_checksum,
};

use crate::fixtures::golden;

#[test]
fn lpk_footer_round_trip() {
    let pack = b"PACK\x00\x00\x00\x02\x00\x00\x00\x00pack bytes and a trailer";
    let idx = b"\xfftOc\x00\x00\x00\x02idx bytes";
    let footer = encode_footer(pack, idx);
    assert_eq!(&footer[..8], LPK_MAGIC);
    let decoded = decode_footer(&footer).expect("decodes");
    assert_eq!(decoded.pack_len, pack.len() as u64);
    assert_eq!(decoded.idx_len, idx.len() as u64);
    assert_eq!(decoded.idx_offset(), pack.len() as u64);
    assert_eq!(
        decoded.object_len(),
        (pack.len() + idx.len() + LPK_FOOTER_LEN) as u64
    );
    assert_eq!(
        decoded.crc32c,
        crc32c::crc32c(&[&pack[..], &idx[..]].concat())
    );

    let golden = golden("lpk_v1_footer.bin", &footer);
    assert_eq!(golden, footer, "the footer layout is pinned");

    let mut bad = footer;
    bad[0] = b'X';
    assert!(matches!(
        decode_footer(&bad),
        Err(FormatError::Corrupt { .. })
    ));
    let mut reserved = footer;
    reserved[31] = 1;
    assert!(matches!(
        decode_footer(&reserved),
        Err(FormatError::Corrupt { .. })
    ));
    let mut overflow = footer;
    overflow[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(matches!(
        decode_footer(&overflow),
        Err(FormatError::Corrupt { .. })
    ));
}

/// Runs git in `dir`; `None` when git is not installed.
fn git(dir: &std::path::Path, args: &[&str], stdin: Option<&[u8]>) -> Option<Vec<u8>> {
    use std::io::Write;
    let mut child = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input)
            .expect("write stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("git runs");
    assert!(out.status.success(), "git {args:?} failed");
    Some(out.stdout)
}

#[test]
fn pack_checksum_reads_trailer() {
    let dir = tempfile::Builder::new()
        .prefix("lpk-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let path = dir.path();
    if git(path, &["init", "-q", "."], None).is_none() {
        eprintln!("git not found: pack_checksum_reads_trailer skipped");
        return;
    }
    std::fs::write(path.join("a.txt"), "hello\n").expect("write");
    git(path, &["add", "a.txt"], None);
    git(
        path,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "commit",
            "-q",
            "-m",
            "one",
        ],
        None,
    );
    let head = git(path, &["rev-parse", "HEAD"], None).expect("rev-parse");
    // `pack-objects <base>` writes base-<checksum>.pack and prints the checksum.
    let printed = git(path, &["pack-objects", "--revs", "out"], Some(&head)).expect("pack-objects");
    let printed = String::from_utf8(printed).expect("utf-8");
    let printed = printed.trim();
    let pack = std::fs::read(path.join(format!("out-{printed}.pack"))).expect("pack file");
    let checksum = pack_checksum(&pack).expect("a pack");
    assert_eq!(checksum.to_string(), printed);
    assert_eq!(
        checksum,
        ObjectId::from_hex(printed.as_bytes()).expect("hex")
    );

    assert!(matches!(
        pack_checksum(b"PACK"),
        Err(FormatError::Corrupt { .. })
    ));
    let mut not_a_pack = pack;
    not_a_pack[0] = b'K';
    assert!(matches!(
        pack_checksum(&not_a_pack),
        Err(FormatError::Corrupt { .. })
    ));
}
