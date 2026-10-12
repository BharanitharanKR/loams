#!/usr/bin/env python3
"""NF1 Task 1: list the Rust packages compiled into an image, and flag forbidden ones.

    audit_image.py [--offline] [--licences FILE] <platform>=<image> ...

Each argument is one platform of an image: either a `skopeo copy ... dir:<path>`
directory, or `<oci layout>@sha256:<manifest digest>` for one manifest in an OCI
image layout (audit-image.sh stages the whole index as one). The layers are read in order as tar streams, with OCI
whiteouts applied; every ELF file that has a `.dep-v0` section (cargo-auditable) is
written out and read with `rust-audit-info`.

Prints {"<platform>:<path>": [{"name", "version", "source"}, ...]} on stdout
(`"kind": "build"` is added for build-only dependencies, which are not linked).
An ELF file without `.dep-v0` (built with plain `cargo build`) is listed with the
crates its Cargo source paths name (`registry/src/<index>/<crate>-<version>/`,
`git/checkouts/<repo>-<hash>/<rev>/`), each marked `"evidence": "strings"`;
that is a lower bound, since a stripped binary may name none.

A finding, printed on stderr, makes the exit code 3:
  - CDDL: the crate `inferno`, or any crate whose licence on crates.io names CDDL;
  - a private source: a `subzero*` crate that is not a local path (Neon's public
    tree links a path stub; the real crate is in the private neondatabase/subzero).
Build-only dependencies are listed but are not findings.

--licences is a JSON cache {"name@version": "SPDX expression"}; missing crates.io
entries are fetched from the crates.io API (one request a second) and written back,
unless --offline. Exit 1 on errors, 2 on usage.
"""

import argparse
import json
import re
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath

FORBIDDEN_CRATES = {"inferno": "CDDL-1.0"}
USER_AGENT = "loams-nf1-audit (https://github.com/ostrium-labs/loams)"


def dep_v0_present(data: bytes) -> bool:
    """True if the ELF image in `data` has a section named `.dep-v0`."""
    if len(data) < 0x40 or data[:4] != b"\x7fELF":
        return False
    cls, enc = data[4], data[5]
    if cls not in (1, 2) or enc not in (1, 2):
        return False
    e = "<" if enc == 1 else ">"
    try:
        if cls == 2:
            shoff, = struct.unpack_from(e + "Q", data, 0x28)
            shentsize, shnum, shstrndx = struct.unpack_from(e + "HHH", data, 0x3A)
            off_fmt, off_at, size_at = e + "Q", 0x18, 0x20
        else:
            shoff, = struct.unpack_from(e + "I", data, 0x20)
            shentsize, shnum, shstrndx = struct.unpack_from(e + "HHH", data, 0x2E)
            off_fmt, off_at, size_at = e + "I", 0x10, 0x14
        if shoff == 0 or shnum == 0 or shstrndx >= shnum:
            return False

        def header(i):
            base = shoff + i * shentsize
            name, = struct.unpack_from(e + "I", data, base)
            off, = struct.unpack_from(off_fmt, data, base + off_at)
            size, = struct.unpack_from(off_fmt, data, base + size_at)
            return name, off, size

        _, str_off, str_size = header(shstrndx)
        strtab = data[str_off:str_off + str_size]
        for i in range(shnum):
            name, _, _ = header(i)
            end = strtab.find(b"\0", name)
            if strtab[name:end] == b".dep-v0":
                return True
    except struct.error:
        return False
    return False


REGISTRY_PATH = re.compile(
    rb"registry/src/(?:index\.crates\.io|github\.com)-[0-9a-f]{16}/([A-Za-z0-9_-]+?)-(\d+\.\d+\.\d+[A-Za-z0-9.+-]*?)/")
CHECKOUT_PATH = re.compile(rb"git/checkouts/([A-Za-z0-9_.-]+)-[0-9a-f]{16}/([0-9a-f]{7,40})/")


def crates_from_strings(data: bytes) -> list:
    """Crates named by the Cargo source paths left in a binary built without
    cargo-auditable (panic locations, debug info): a lower bound, not a full list."""
    found = {(m[1].decode(), m[2].decode(), "crates.io") for m in REGISTRY_PATH.finditer(data)}
    found |= {(m[1].decode(), m[2].decode(), "git") for m in CHECKOUT_PATH.finditer(data)}
    return [{"name": n, "version": v, "source": s, "evidence": "strings"} for n, v, s in found]


def audit_info(data: bytes) -> list:
    with tempfile.NamedTemporaryFile(prefix="nf1-audit-", suffix=".elf") as f:
        f.write(data)
        f.flush()
        try:
            r = subprocess.run(["rust-audit-info", f.name], capture_output=True, text=True,
                               timeout=300)
        except subprocess.TimeoutExpired as err:
            raise RuntimeError(f"rust-audit-info timed out on {f.name}") from err
    if r.returncode != 0:
        raise RuntimeError(f"rust-audit-info failed: {r.stderr.strip()}")
    return json.loads(r.stdout)["packages"]


def open_layer(path: Path):
    with path.open("rb") as f:
        head = f.read(4)
    if head.startswith(b"\x28\xb5\x2f\xfd"):
        from compression import zstd  # Python 3.14+

        return tarfile.open(fileobj=zstd.open(path, "rb"), mode="r|")
    return tarfile.open(path, mode="r|*")  # gzip, or an uncompressed tar


def norm(name: str) -> str:
    return "/" + str(PurePosixPath("/" + name)).lstrip("/")


def layer_blobs(spec: str) -> list[Path]:
    """The layer files, in order, of `<skopeo dir>` or `<oci layout>@<digest>`."""
    layout, at, digest = spec.rpartition("@")
    if at and digest.startswith("sha256:"):
        blobs = Path(layout) / "blobs" / "sha256"
        manifest = json.loads((blobs / digest.split(":", 1)[1]).read_text())
    else:
        blobs = Path(spec)
        manifest = json.loads((blobs / "manifest.json").read_text())
    return [blobs / layer["digest"].split(":", 1)[1] for layer in manifest["layers"]]


def scan_image(layers: list[Path]) -> dict:
    """{path: packages} of the final filesystem of one platform."""
    state: dict[str, list] = {}
    for blob in layers:
        added: dict[str, list] = {}
        replaced: set[str] = set()
        removed: set[str] = set()
        opaque: set[str] = set()
        links: list[tuple[str, str]] = []
        with open_layer(blob) as tar:
            for m in tar:
                path = norm(m.name)
                base = PurePosixPath(path).name
                parent = str(PurePosixPath(path).parent)
                if base == ".wh..wh..opq":
                    opaque.add(parent.rstrip("/") + "/")
                    continue
                if base.startswith(".wh."):
                    removed.add(str(PurePosixPath(parent) / base[4:]))
                    continue
                replaced.add(path)
                if m.islnk():
                    links.append((path, norm(m.linkname)))
                    continue
                if not m.isfile() or m.size < 0x40:
                    continue
                f = tar.extractfile(m)
                if f.read(4) != b"\x7fELF":
                    continue
                data = b"\x7fELF" + f.read()
                if dep_v0_present(data):
                    added[path] = audit_info(data)
                else:
                    found = crates_from_strings(data)
                    if found:
                        added[path] = found
        for path in list(state):
            if (path in replaced or path in removed
                    or any(path.startswith(d + "/") for d in removed)
                    or any(path.startswith(d) for d in opaque)):
                del state[path]
        state.update(added)
        for path, target in links:
            if target in state:
                state[path] = state[target]
    return state


def load_licences(path):
    if path and Path(path).exists():
        return json.loads(Path(path).read_text())
    return {}


def fetch_licences(keys, cache: dict, path) -> None:
    missing = sorted(k for k in keys if k not in cache)
    for n, key in enumerate(missing):
        name, version = key.rsplit("@", 1)
        req = urllib.request.Request(
            f"https://crates.io/api/v1/crates/{name}/{version}",
            headers={"User-Agent": USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                cache[key] = json.load(resp)["version"].get("license") or "unknown"
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise
            cache[key] = "unknown"
        if path and (n % 25 == 24 or n == len(missing) - 1):
            Path(path).write_text(json.dumps(cache, indent=0, sort_keys=True) + "\n")
        time.sleep(1)


def entry(p: dict) -> dict:
    e = {"name": p["name"], "version": p["version"], "source": p.get("source", "unknown")}
    if p.get("kind") == "build":
        e["kind"] = "build"
    if p.get("evidence"):
        e["evidence"] = p["evidence"]
    return e


def findings(report: dict, licences: dict) -> list[str]:
    out = []
    for binary, pkgs in sorted(report.items()):
        for p in pkgs:
            if p.get("kind") == "build":
                continue
            name, version, source = p["name"], p["version"], p["source"]
            lic = licences.get(f"{name}@{version}", "")
            if name in FORBIDDEN_CRATES:
                out.append(f"{binary}: {name} {version} ({FORBIDDEN_CRATES[name]}, forbidden crate)")
            elif "CDDL" in lic.upper():
                out.append(f"{binary}: {name} {version} (licence {lic}: CDDL)")
            if name.replace("_", "-").startswith("subzero") and source != "local":
                out.append(f"{binary}: {name} {version} (source {source}: private neondatabase/subzero)")
    return out


def dump(report: dict) -> str:
    """One package per line, so the audit files diff well."""
    if not report:
        return "{}\n"
    parts = []
    for binary in sorted(report):
        rows = ",\n".join("    " + json.dumps(p, sort_keys=False) for p in report[binary])
        parts.append(f"  {json.dumps(binary)}: [\n{rows}\n  ]")
    return "{\n" + ",\n".join(parts) + "\n}\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--offline", action="store_true", help="do not query crates.io")
    ap.add_argument("--licences", help="JSON cache of crates.io licences")
    ap.add_argument("platforms", nargs="+", metavar="PLATFORM=IMAGE")
    args = ap.parse_args(argv)

    report: dict[str, list] = {}
    for spec in args.platforms:
        platform, sep, image = spec.partition("=")
        if not sep or not image:
            print(f"error: expected <platform>=<image>, got {spec}", file=sys.stderr)
            return 2
        for path, pkgs in scan_image(layer_blobs(image)).items():
            uniq = {(p["name"], p["version"], p.get("source")): entry(p) for p in pkgs}
            report[f"{platform}:{path}"] = sorted(
                uniq.values(), key=lambda e: (e["name"], e["version"], e["source"]))

    licences = load_licences(args.licences)
    crates_io = {f"{p['name']}@{p['version']}" for ps in report.values() for p in ps
                 if p["source"] == "crates.io"}
    if not args.offline:
        fetch_licences(crates_io, licences, args.licences)
    unknown = sorted(k for k in crates_io if licences.get(k, "unknown") == "unknown")
    if unknown:
        print(f"note: no crates.io licence for {len(unknown)} crates: {' '.join(unknown[:20])}",
              file=sys.stderr)

    sys.stdout.write(dump(report))
    found = findings(report, licences)
    for f in found:
        print(f"FINDING: {f}", file=sys.stderr)
    print(f"audit: {len(report)} binaries with Rust package data, "
          f"{len({(p['name'], p['version']) for ps in report.values() for p in ps})} packages, "
          f"{len(found)} findings", file=sys.stderr)
    return 3 if found else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, tarfile.TarError) as err:
        print(f"error: {err}", file=sys.stderr)
        sys.exit(1)
