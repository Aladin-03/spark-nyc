"""Paths, table names and constants. Defined once, imported everywhere.

Nothing here knows about Spark. That is on purpose: a path is a path, and
keeping it out of the pipeline modules means you can change the layout in one
place instead of grepping for string literals.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# --- raw, as downloaded by scripts/fetch_data.sh ---------------------------
DATA = ROOT / "data"
RAW_YELLOW = DATA / "yellow"
ZONES_CSV = DATA / "reference" / "taxi_zone_lookup.csv"

# --- the three layers ------------------------------------------------------
BRONZE = DATA / "bronze" / "yellow"
SILVER = DATA / "silver" / "trips"
SILVER_REJECTS = DATA / "silver" / "rejects"
GOLD = DATA / "gold"

# Columns the pipeline adds. Underscore-prefixed so they never collide with a
# column the TLC might add to the source one day.
COL_SOURCE_FILE = "_source_file"
COL_INGESTED_AT = "_ingested_at"

_MONTH_FILE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def raw_month_path(year: int, month: int) -> Path:
    """Path to one month of raw data."""
    return RAW_YELLOW / f"yellow_tripdata_{year}-{month:02d}.parquet"


def available_months() -> list[tuple[int, int]]:
    """Every (year, month) currently sitting in data/yellow/, sorted.

    The filename is the contract. If fetch_data.sh downloaded it, this finds it,
    which is what makes the pipeline pick up new months without a code change.
    """
    found = []
    for p in sorted(RAW_YELLOW.glob("yellow_tripdata_*.parquet")):
        m = _MONTH_FILE.search(p.name)
        if m:
            found.append((int(m.group(1)), int(m.group(2))))
    return found
