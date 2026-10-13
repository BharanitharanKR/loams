#!/usr/bin/env bash
# The C++ SDK's release artifact: a versioned source archive of sdks/cpp, plus its
# SHA-256, proven to build, install and be found with find_package(loams CONFIG).
#
#   sdks/cpp/packaging/release.sh <semver> <out-dir>
#
# Writes <out-dir>/loams-cpp-<semver>.tar.gz and <out-dir>/loams-cpp-<semver>.tar.gz.sha256.
# The archive is `git archive` of the committed tree at HEAD, so it is reproducible
# from the tag and holds nothing the working tree has that git does not.
#
# The build needs what CMakeLists.txt needs: CMake >= 3.20, a C++17 compiler,
# libcurl, pkg-config and the protobuf runtime the stubs were generated for (36.1).
set -euo pipefail

version=${1:?usage: release.sh <semver> <out-dir>}
out=${2:?usage: release.sh <semver> <out-dir>}
version=${version#v}
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
name="loams-cpp-$version"

mkdir -p "$out"
out=$(cd "$out" && pwd)
work="$out/.work-$name"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# The manifest must already say this version; the archive is not where a version
# gets decided.
python3 "$repo/scripts/sdk/sdk-version.py" check "$version" >/dev/null

# Everything a consumer builds from: no ctest output, no packaging helpers.
git -C "$repo" archive --format=tar --prefix="$name/" HEAD:sdks/cpp \
  | tar -x -C "$work"
rm -rf "$work/$name/packaging" "$work/$name/Testing"
# A fixed mtime and owner so the same commit gives the same bytes.
mtime=$(git -C "$repo" log -1 --format=%ct HEAD)
tar --sort=name --mtime="@$mtime" --owner=0 --group=0 --numeric-owner \
  -C "$work" -cf - "$name" | gzip -n -9 > "$out/$name.tar.gz"
(cd "$out" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")

# Build it the way a consumer would: from the archive, tests off (the archive
# carries no fixture corpus), installed to a prefix.
tar -xzf "$out/$name.tar.gz" -C "$work"
mv "$work/$name" "$work/src"
cmake -S "$work/src" -B "$work/build" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$work/prefix" >/dev/null
cmake --build "$work/build" -j "${LOAMS_BUILD_JOBS:-4}" >/dev/null
cmake --install "$work/build" >/dev/null

# A separate project finds it with find_package and links it.
cmake -S "$here/consumer" -B "$work/consumer" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$work/prefix${CMAKE_PREFIX_PATH:+;$CMAKE_PREFIX_PATH}" >/dev/null
cmake --build "$work/consumer" -j "${LOAMS_BUILD_JOBS:-4}" >/dev/null
reported=$("$work/consumer/consumer" | head -1)
if [ "$reported" != "$version" ]; then
  echo "the installed SDK reports version '$reported', the release is $version" >&2
  exit 1
fi

echo "$out/$name.tar.gz"
cat "$out/$name.tar.gz.sha256"
echo "installed, found with find_package(loams CONFIG) and linked: version $reported"
