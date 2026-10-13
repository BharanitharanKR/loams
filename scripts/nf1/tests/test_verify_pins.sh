#!/usr/bin/env bash
# NF1 Task 12: tests for scripts/nf1/verify-pins.sh, with stub cosign, gh and
# oras that answer from fixtures (no network, no registry):
#
#   unsigned_digest_fails_verify     a digest without a signature fails
#   wrong_identity_fails_verify      signed from a branch other than main, or by the
#                                    old repository name ostrium-labs/neon: fails
#   sbom_has_no_forbidden_licence    CDDL anywhere, or a non-allowlisted licence outside
#                                    Debian's packages, fails; Debian's are listed
#   provenance_bound_to_the_fork_commit  an attestation for another commit (or none) fails
#   one_sbom_per_platform            an index with one SBOM where it has two platforms fails
#   gpl_sources_attached             a compute pin without the sources artifact, or with an
#                                    unsigned one, fails
#   mirror_pin_skipped_with_notice   a mirror: pin is skipped, and no tool is called
#   tool_timeout_is_a_failure        a tool that hangs fails the image after TOOL_TIMEOUT
#   signed_pins_pass                 a correctly signed, attested pin set passes
#
# The stub cosign implements what the test needs of `cosign verify`: it fails
# when the image has no signer in the fixture, or when the signer does not match
# --certificate-identity-regexp (grep -E), so the script's identity is tested.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
verify="$here/../verify-pins.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0
fail() { echo "FAIL $current: $*" >&2; failures=$((failures + 1)); }

stubs="$work/bin"
mkdir -p "$stubs"
cat > "$stubs/cosign" <<'EOF'
#!/usr/bin/env bash
# cosign verify --certificate-oidc-issuer I --certificate-identity-regexp R <ref>
[[ $1 == verify ]] || exit 9
issuer="" regexp="" ref=""
shift
while (($#)); do
  case $1 in
    --certificate-oidc-issuer) issuer=$2; shift 2 ;;
    --certificate-identity-regexp) regexp=$2; shift 2 ;;
    *) ref=$1; shift ;;
  esac
done
[[ $issuer == https://token.actions.githubusercontent.com ]] || { echo "wrong issuer $issuer" >&2; exit 1; }
signer=$(awk -v r="$ref" '$1 == r { print $2 }' "$FIXTURE/signers")
[[ -n $signer ]] || { echo "no signatures found for $ref" >&2; exit 1; }
grep -Eq -- "$regexp" <<<"$signer" || { echo "none of the expected identities matched: $signer" >&2; exit 1; }
echo '[{"critical":{}}]'
EOF
cat > "$stubs/gh" <<'EOF'
#!/usr/bin/env bash
# gh attestation verify oci://<ref> --repo R --source-digest C --source-ref F
#   [--predicate-type T --format json]; $FIXTURE/attested has "<ref> <commit>".
[[ $1 == attestation && $2 == verify ]] || exit 9
ref=${3#oci://} predicate="" digest=""
[[ " $* " == *" --repo ostrium-labs/loams-postgres "* ]] || { echo "not scoped to the fork" >&2; exit 1; }
[[ " $* " == *" --source-ref refs/heads/main "* ]] || { echo "not scoped to main" >&2; exit 1; }
[[ " $* " == *" --predicate-type "* ]] && predicate=sbom
args=("$@")
for ((i = 0; i < $#; i++)); do [[ ${args[i]} == --source-digest ]] && digest=${args[i + 1]}; done
attested=$(awk -v r="$ref" '$1 == r { print $2 }' "$FIXTURE/attested")
[[ -n $attested ]] || { echo "no attestations for $ref" >&2; exit 1; }
[[ $attested == "$digest" ]] || { echo "source digest $attested, not $digest" >&2; exit 1; }
if [[ $predicate == sbom ]]; then
  shopt -s nullglob
  files=("$FIXTURE"/sbom-"${ref##*:}"-*.json)
  ((${#files[@]})) || { echo "no SBOM attestation" >&2; exit 1; }
  sep="" out="["
  for f in "${files[@]}"; do
    out+="$sep{\"verificationResult\":{\"statement\":{\"predicateType\":\"https://spdx.dev/Document/v2.3\",\"predicate\":$(cat "$f")}}}"
    sep=","
  done
  echo "$out]"
fi
EOF
cat > "$stubs/oras" <<'EOF'
#!/usr/bin/env bash
# oras discover --artifact-type T --format json <ref>
[[ $1 == discover ]] || exit 9
ref=${*: -1}
if grep -Fxq -- "$ref" "$FIXTURE/sources"; then
  echo '{"manifests":[{"artifactType":"application/vnd.loams.sources.v1","digest":"sha256:5555555555555555555555555555555555555555555555555555555555555555"}]}'
else
  echo '{"manifests":[]}'
fi
EOF
cat > "$stubs/hang" <<'EOF'
#!/usr/bin/env bash
sleep 30
EOF
cat > "$stubs/refuse" <<'EOF'
#!/usr/bin/env bash
echo "called for a mirror pin: $*" >&2
exit 7
EOF
chmod +x "$stubs"/*

GOOD='https://github.com/ostrium-labs/loams-postgres/.github/workflows/loams-sign.yml@refs/heads/main'
REL='https://github.com/ostrium-labs/loams-postgres/.github/workflows/loams-release.yml@refs/heads/main'
D1=sha256:$(printf '1%.0s' {1..64})
D2=sha256:$(printf '2%.0s' {1..64})
D5=sha256:$(printf '5%.0s' {1..64})  # the sources artifact the stub oras reports
SHA=0123456789abcdef0123456789abcdef01234567
STORAGE=ghcr.io/ostrium-labs/loams-postgres-storage
COMPUTE=ghcr.io/ostrium-labs/loams-postgres-compute-v17

sbom() {
  # sbom <digest> <name|licence|purl> ...: the same packages for both platforms.
  local d=${1#sha256:} p
  shift
  for p in amd64 arm64; do sbom_one "$FIXTURE/sbom-$d-$p.json" "$p" "$@"; done
}

sbom_one() {
  # sbom_one <file> <platform> <name|licence|purl> ...
  local out=$1 platform=$2 first=1
  shift 2
  {
    printf '{"spdxVersion":"SPDX-2.3","documentNamespace":"https://example.test/%s","packages":[' "$out-$platform"
    for p in "$@"; do
      IFS='|' read -r name lic purl <<<"$p"
      ((first)) || printf ','
      first=0
      printf '{"name":"%s","versionInfo":"1.0","licenseConcluded":"NOASSERTION","licenseDeclared":"%s","externalRefs":[{"referenceType":"purl","referenceLocator":"%s"}]}' \
        "$name" "$lic" "$purl"
    done
    printf ']}'
  } > "$out"
}

setup() {
  # A pin file with a storage and a compute image built by the fork, signed,
  # attested, with clean SBOMs and the compute sources attached.
  FIXTURE="$work/$current"
  export FIXTURE
  mkdir -p "$FIXTURE"
  cat > "$FIXTURE/pins.toml" <<EOF
schema = 1
fork_release = "lp-2026.10.0"
fork_commit = "$SHA"
[majors]
supported = [17]
beta = []
maintenance = []
default = 17
[images.storage]
repository = "$STORAGE"
digest = "$D1"
source = "ostrium-labs/loams-postgres@$SHA"
pg_minors = { "17" = "17.5" }
[images.compute-v17]
repository = "$COMPUTE"
digest = "$D2"
source = "ostrium-labs/loams-postgres@$SHA"
pg_minor = "17.5"
EOF
  printf '%s %s\n%s %s\n%s %s\n' "$STORAGE@$D1" "$GOOD" "$COMPUTE@$D2" "$REL" "$COMPUTE@$D5" "$GOOD" > "$FIXTURE/signers"
  printf '%s %s\n%s %s\n' "$STORAGE@$D1" "$SHA" "$COMPUTE@$D2" "$SHA" > "$FIXTURE/attested"
  printf '%s\n' "$COMPUTE@$D2" > "$FIXTURE/sources"
  sbom "$D1" 'tokio|MIT|pkg:cargo/tokio@1.0' 'ring|Apache-2.0 AND ISC|pkg:cargo/ring@0.17' \
    'serde|MIT OR Apache-2.0|pkg:cargo/serde@1.0' 'libc6|NOASSERTION|pkg:deb/debian/libc6@2.36'
  sbom "$D2" 'postgis|GPL-2.0-or-later|pkg:generic/postgis@3.3' \
    'libreadline8|GPL-3.0-only|pkg:deb/debian/libreadline8@8.2' 'pgvector|PostgreSQL|pkg:generic/pgvector@0.8.0'
}

check() {
  # check <pass|fail> [expected text in the output]
  local expected=$1 want=${2:-} rc=0
  PATH="$stubs:$PATH" "$verify" "$FIXTURE/pins.toml" > "$FIXTURE/out" 2>&1 || rc=$?
  if [[ $expected == pass && $rc != 0 || $expected == fail && $rc != 1 ]]; then
    cat "$FIXTURE/out" >&2
    fail "expected $expected, exit $rc"
  elif [[ -n $want ]] && ! grep -qF -- "$want" "$FIXTURE/out"; then
    cat "$FIXTURE/out" >&2
    fail "no '$want' in the output"
  fi
}

signed_pins_pass() {
  setup
  check pass "compute-v17: verified $COMPUTE@$D2"
  grep -qF 'libreadline8 1.0: GPL-3.0-only' "$FIXTURE/out" || fail "Debian's licences are not listed"
}

unsigned_digest_fails_verify() {
  setup
  sed -i "\|^$STORAGE@|d" "$FIXTURE/signers"
  check fail "storage: cosign verify failed: no signatures found"
}

wrong_identity_fails_verify() {
  setup
  sed -i "s|^$STORAGE@$D1 .*|$STORAGE@$D1 ${GOOD%main}dev|" "$FIXTURE/signers"
  check fail "storage: cosign verify failed: none of the expected identities matched"
  setup
  sed -i "s|^$STORAGE@$D1 .*|$STORAGE@$D1 ${GOOD/loams-postgres/neon}|" "$FIXTURE/signers"
  check fail "storage: cosign verify failed"
  setup
  sed -i "s|^$STORAGE@$D1 .*|$STORAGE@$D1 ${GOOD/loams-sign/loams-build-storage}|" "$FIXTURE/signers"
  check fail "storage: cosign verify failed"
}

sbom_has_no_forbidden_licence() {
  setup
  sbom "$D1" 'inferno|CDDL-1.0|pkg:cargo/inferno@0.11.21' 'tokio|MIT|pkg:cargo/tokio@1.0'
  check fail "storage: SBOM: inferno 1.0: CDDL"
  setup
  sbom "$D1" 'mongo|SSPL-1.0|pkg:generic/mongo@1' 'either|MIT OR SSPL-1.0|pkg:cargo/either@1'
  check fail "storage: SBOM: mongo 1.0: not on the allowlist (SSPL-1.0)"
  grep -qF 'either' "$FIXTURE/out" && fail "an OR with an allowed side was refused"
  setup
  sbom "$D2" 'libfoo|CDDL-1.1|pkg:deb/debian/libfoo@1'
  check fail "compute-v17: SBOM: libfoo 1.0: CDDL"   # CDDL fails even in a Debian package
  setup
  rm "$FIXTURE"/sbom-"${D1#sha256:}"-*.json
  check fail "storage: no SPDX SBOM attestation"
}

provenance_bound_to_the_fork_commit() {
  setup
  sed -i "s|^$STORAGE@$D1 .*|$STORAGE@$D1 ffffffffffffffffffffffffffffffffffffffff|" "$FIXTURE/attested"
  check fail "storage: no build provenance attestation: source digest ffff"
}

one_sbom_per_platform() {
  setup
  rm "$FIXTURE/sbom-${D2#sha256:}-arm64.json"
  check fail "compute-v17: 1 SPDX SBOM attestation(s); one per platform (2) is required"
}

gpl_sources_attached() {
  setup
  : > "$FIXTURE/sources"
  check fail "compute-v17: no application/vnd.loams.sources.v1 artifact attached"
  setup
  sed -i "\|^$COMPUTE@$D5 |d" "$FIXTURE/signers"
  check fail "compute-v17: the application/vnd.loams.sources.v1 artifact is not signed"
}

mirror_pin_skipped_with_notice() {
  setup
  FIXTURE_PINS=$(sed -e "s|^source = \"ostrium-labs/loams-postgres@$SHA\"|source = \"mirror:ghcr.io/neondatabase/neon@$D1\"|" "$FIXTURE/pins.toml")
  echo "$FIXTURE_PINS" > "$FIXTURE/pins.toml"
  local rc=0
  COSIGN="$stubs/refuse" GH="$stubs/refuse" ORAS="$stubs/refuse" "$verify" "$FIXTURE/pins.toml" > "$FIXTURE/out" 2>&1 || rc=$?
  [[ $rc == 0 ]] || { cat "$FIXTURE/out" >&2; fail "exit $rc"; }
  [[ $(grep -c '^::notice::.* is a mirror' "$FIXTURE/out") == 2 ]] || fail "no notice for each mirror pin"
}

tool_timeout_is_a_failure() {
  setup
  local rc=0
  PATH="$stubs:$PATH" COSIGN="$stubs/hang" TOOL_TIMEOUT=1 "$verify" "$FIXTURE/pins.toml" > "$FIXTURE/out" 2>&1 || rc=$?
  if [[ $rc != 1 ]] || ! grep -qF 'cosign verify failed: timed out after 1 s' "$FIXTURE/out"; then
    cat "$FIXTURE/out" >&2
    fail "exit $rc"
  fi
}

all=(signed_pins_pass unsigned_digest_fails_verify wrong_identity_fails_verify sbom_has_no_forbidden_licence
  provenance_bound_to_the_fork_commit one_sbom_per_platform gpl_sources_attached mirror_pin_skipped_with_notice
  tool_timeout_is_a_failure)
for current in "${@:-${all[@]}}"; do
  echo "--- $current"
  "$current"
done
if ((failures)); then
  echo "test_verify_pins: $failures failure(s)" >&2
  exit 1
fi
echo "test_verify_pins: ok"
