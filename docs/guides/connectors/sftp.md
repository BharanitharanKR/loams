# SFTP

Native, `loams_connector_objects::sftp`, over SSH 2 and SFTP v3 (`russh`
0.63.3 with `ring`, `russh-sftp` 3.0.1). Connections go through the SDK's
egress-checked `net::connect`.

```yaml
host: sftp.example.net
port: 22
username: loams
root: upload                  # the directory the source reads and the sink writes under
recursive: false              # the source descends into subdirectories
host_key: "ssh-ed25519 AAAA…"  # required: an unknown host key is refused
auth:
  password: sftp-password     # secret names: a password, or
  private_key: sftp-key       # an OpenSSH private key (Ed25519 or ECDSA)
  private_key_passphrase: null
prefix: out                   # the sink's directory under root
source: { decode: bytes }     # as S3's `source`
sink: { format: ndjson }      # as S3's `sink`
```

| | |
|---|---|
| Source | polling: the directory is listed and sorted by path, read after a high-water path; ids `"<host>/<path>@<mtime>-<size>"`; dot files are skipped |
| Sink | `at_least_once`: one file per Fabric batch, written as `.<name>.tmp` and published with `RENAME`, which refuses an existing destination, so no reader sees a partial file |
| Keys | Ed25519 and ECDSA only: RSA needs the `rsa` crate, which carries RUSTSEC-2023-0071 with no fix (CN2 R13.6) |
| Tested against | `atmoz/sftp` (OpenSSH) in PR CI |

A file written later under a path below the high-water path is not seen
(as with S3's listing).
