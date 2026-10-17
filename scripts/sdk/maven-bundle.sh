#!/usr/bin/env bash
# Turn the files `sdks/{java,kotlin}/build.sh package DIR` wrote into a Maven
# Central Portal bundle: the repository layout dev/loams/<artifact>/<version>/,
# a detached ASCII-armoured GPG signature (.asc) and .md5/.sha1 checksums for
# every file, zipped. sdk-release.yml uploads the zip to
# https://central.sonatype.com/api/v1/publisher/upload.
#
#   maven-bundle.sh ARTIFACT_DIR BUNDLE.zip [GPG_KEY_ID]
#
# Signing uses the default secret key in the current GNUPGHOME, or GPG_KEY_ID.
# Every signature is verified before the zip is written.
set -euo pipefail

dir="${1:?usage: maven-bundle.sh ARTIFACT_DIR BUNDLE.zip [GPG_KEY_ID]}"
zip_out="${2:?usage: maven-bundle.sh ARTIFACT_DIR BUNDLE.zip [GPG_KEY_ID]}"
key="${3:-}"

pom=$(find "$dir" -maxdepth 1 -name '*.pom' | head -1)
[ -n "$pom" ] || { echo "maven-bundle: no .pom in $dir" >&2; exit 1; }
field() { sed -n "s|^  <$1>\\(.*\\)</$1>\$|\\1|p" "$pom" | head -1; }
group=$(field groupId); artifact=$(field artifactId); version=$(field version)
[ -n "$group" ] && [ -n "$artifact" ] && [ -n "$version" ] || { echo "maven-bundle: $pom has no groupId/artifactId/version" >&2; exit 1; }

for want in "$artifact-$version.pom" "$artifact-$version.jar" "$artifact-$version-sources.jar" "$artifact-$version-javadoc.jar"; do
  [ -f "$dir/$want" ] || { echo "maven-bundle: $dir/$want is missing; Central requires it" >&2; exit 1; }
done

stage=$(mktemp -d "${RUNNER_TEMP:-$PWD}/maven-bundle.XXXXXX")
trap 'rm -rf "$stage"' EXIT
repo_dir="$stage/${group//.//}/$artifact/$version"
mkdir -p "$repo_dir"
cp "$dir/$artifact-$version"* "$repo_dir/"

sign_args=(--batch --yes --armor --detach-sign)
[ -n "$key" ] && sign_args+=(--local-user "$key")
for file in "$repo_dir"/*; do
  case "$file" in *.asc|*.md5|*.sha1) continue ;; esac
  gpg "${sign_args[@]}" --output "$file.asc" "$file"
  gpg --batch --verify "$file.asc" "$file" 2>/dev/null \
    || { echo "maven-bundle: the signature on $(basename "$file") does not verify" >&2; exit 1; }
  md5sum "$file" | cut -d' ' -f1 | tr -d '\n' >"$file.md5"
  sha1sum "$file" | cut -d' ' -f1 | tr -d '\n' >"$file.sha1"
done

rm -f "$zip_out"
zip_abs="$(cd "$(dirname "$zip_out")" && pwd)/$(basename "$zip_out")"
(cd "$stage" && zip -qr "$zip_abs" .)
echo "maven-bundle: $group:$artifact:$version -> $zip_out"
unzip -l "$zip_abs"
