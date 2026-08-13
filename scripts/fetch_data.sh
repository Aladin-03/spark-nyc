#!/usr/bin/env bash
#
# Fetch NYC TLC yellow taxi data. This script is the reproducibility contract:
# the parquet is not in git, but anyone who clones this repo runs one command
# and gets a byte-identical dataset.
#
#   bash scripts/fetch_data.sh                    # default: 2024-01..03
#   bash scripts/fetch_data.sh 2024-04 2024-05    # specific months
#   bash scripts/fetch_data.sh --year 2024        # all twelve

set -euo pipefail

BASE="https://d37ci6vzurychx.cloudfront.net"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
YELLOW="$ROOT/data/yellow"
REF="$ROOT/data/reference"

if [ "${1:-}" = "--year" ]; then
  MONTHS=(); for m in $(seq -w 1 12); do MONTHS+=("${2}-${m}"); done
elif [ $# -gt 0 ]; then
  MONTHS=("$@")
else
  MONTHS=(2024-01 2024-02 2024-03)
fi

mkdir -p "$YELLOW" "$REF"

for m in "${MONTHS[@]}"; do
  f="yellow_tripdata_${m}.parquet"
  if [ -f "$YELLOW/$f" ]; then
    echo "have  $f"
  else
    echo "get   $f"
    curl -fSL --retry 3 --progress-bar -o "$YELLOW/$f.part" "$BASE/trip-data/$f"
    mv "$YELLOW/$f.part" "$YELLOW/$f"
  fi
done

# Zone lookup: ~12KB, 265 rows. This is the broadcast-join side on day 3.
if [ ! -f "$REF/taxi_zone_lookup.csv" ]; then
  echo "get   taxi_zone_lookup.csv"
  curl -fSL --retry 3 -o "$REF/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"
fi

echo
du -sh "$YELLOW" "$REF"
