#!/usr/bin/env bash
# Downloads the public datasets the seed scripts load, each into its own new
# directory under $E2E_DATA (default /mnt/Projects/loams-e2e/data; the
# worktree's ../local-e2e-data links there). Downloads are data, never run.
# Idempotent: a file that is there is kept. SHA256SUMS records what was read.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
mkdir -p "$E2E_DATA"
get() { # dir url [name]
  local dir="$E2E_DATA/$1" name="${3:-${2##*/}}"
  mkdir -p "$dir"
  [[ -s "$dir/$name" ]] && { echo "have $1/$name"; return; }
  echo "get  $1/$name"
  curl -q -fsSL --retry 3 -o "$dir/$name.part" "$2" && mv "$dir/$name.part" "$dir/$name"
}
# Collections: DBpedia entities with OpenAI text-embedding-3-small 512-d vectors (100k rows, 364 MB).
get dbpedia https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K/resolve/main/data/train-00000-of-00001.parquet
# Streams, links, Graph, Live: one hour of GH Archive (136 MB gz).
get gharchive https://data.gharchive.org/2024-01-15-15.json.gz
# House: NYC TLC yellow taxi trips, January 2024 (50 MB parquet, 2.96M rows).
get nyc-taxi https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
# Loams SQL and Loams Postgres: Chinook 1.4.5.
get chinook https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql
get chinook https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql
# Graph: SNAP email-Eu-core.
get snap-email https://snap.stanford.edu/data/email-Eu-core.txt.gz
get snap-email https://snap.stanford.edu/data/email-Eu-core-department-labels.txt.gz
(cd "$E2E_DATA" && sha256sum */* > SHA256SUMS && cat SHA256SUMS)
