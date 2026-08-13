# NYC Taxi: a Spark pipeline built to be tuned

A batch pipeline over NYC yellow taxi trip records, built to be tuned rather than
just to run. Monthly Parquet is validated and landed as Iceberg tables, reshaped
into fact and dimension tables, then aggregated. Alongside the pipeline, a
benchmark suite isolates one Spark tuning variable at a time (partition count,
join strategy, key skew, AQE) and records the measured effect.

The dataset is small on purpose, sized for fast iteration on a laptop. The tuning
behaviour it demonstrates does not depend on that size.

## Results

> Filled in as each benchmark runs. The numbers are the point of this repo.

| Change | Before | After |
|---|---|---|
| | | |

Supporting Spark UI screenshots and output samples live in [`docs/`](docs/).

## Run it

Requires a JDK 17 on `PATH`. Check with `java -version`.

```bash
python3 -m venv .venv && source .venv/bin/activate
make setup       # install pinned dependencies
make data        # fetch three months, ~150MB
```

`make help` lists every target.

## Recreating this on another machine

The dataset is deliberately not in git. Three months is 153MB and twelve is around
700MB, which would make the repo slow to clone and is pointless besides, since the
files are public and byte-identical wherever you fetch them.

The Makefile is what makes that safe. Everything needed to get from a bare clone to
a working copy is a target, so nothing depends on remembering a command:

```bash
git clone <this repo> && cd spark-nyc
python3 -m venv .venv && source .venv/bin/activate
make setup       # exact pinned versions from requirements.txt
make data        # re-fetches the same three months
```

That is the full sequence on a new laptop. `make data-full` pulls all twelve months
of 2024 instead, which is what the skew benchmarks need. `make clean` drops Spark's
scratch output (`warehouse/`, `metastore_db/`, event logs) without touching the
downloaded data, so a run can be repeated from a clean state.

Committed: source, config, the fetch script, pinned requirements, and the findings
in `docs/`. Regenerated: the data and every table built from it.

## Layout

```
scripts/         fetch_data.sh, the reproducibility contract for the dataset
src/
  config.py      paths and constants, defined once
  session.py     the SparkSession builder, so every run is comparable
  bronze/        raw Parquet to validated Iceberg tables
  silver/        cleaned, conformed, zone-enriched
  gold/          aggregates and marts
  quality/       validation rules, testable independently of Spark
  io/            Iceberg read and write helpers
benchmarks/      tuning experiments, one variable each
docs/            findings, Spark UI screenshots, output samples
data/            fetched and generated, gitignored
```

`data/` mirrors the code layers (`data/bronze/`, `data/silver/`, `data/gold/`) so
it is obvious which module produced which table.
