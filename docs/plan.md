# Project plan

> The map. Read this before starting any layer.
>
> Each layer has its own document with the detail and the decisions made inside it:
> Layers: [`bronze.md`](bronze.md) · `silver.md` · `gold.md`
>
> Vocabulary and mechanics: [`concepts.md`](concepts.md)

---

## 1. What this is

A batch pipeline that turns monthly NYC yellow taxi files into tables an analyst
can query, plus a benchmark suite that measures the cost of the decisions made
along the way.

**The questions the finished product answers:**

1. **Which pickup zones make the most money, and at what times?**
2. **How does demand move through the day and the week?** Rush hours, weekends,
   late nights.
3. **How do the boroughs compare?** Manhattan against Queens against Brooklyn on
   trip volume, average fare and tipping.
4. **How much of the source data is unusable, and is that fraction steady?** If
   February is 0.4% bad and March is 11% bad, something changed at the source and
   we want to know that day, not three months later.
5. **How does demand look on a spatial grid rather than by zone?** Taxi zones are
   irregular administrative polygons of wildly different sizes. H3 gives equal-area
   hexagons, which is what you need to compare places fairly or to join against
   any other spatial dataset.

**Who it is for:** someone who wants to query tables, not read pipeline code.

**What makes it a portfolio piece rather than a tutorial:**

1. It is re-runnable, and re-running it does not corrupt anything.
2. It survives new months arriving without a code change.
3. Every performance decision in it has a measured number behind it.

Points 1 and 2 are where tutorial pipelines fail. Point 3 is what gets asked about.

---

## 2. The shape

```
   SOURCE                     NYC TLC website. Monthly Parquet, zone lookup CSV,
   layer 0                    zone shapefile. Not ours: columns and types change
      |                       without warning.
      |  scripts/fetch_data.sh
      v
   LANDING                    data/yellow/                            [STORED]
   layer 1                    The files exactly as downloaded. Immutable.
      |                       Never written to, so we can always rebuild.
      |
      |  read one month
      v
   [ schema check ]--FAIL-->  STOP. Job fails loudly, nothing is written.
      |                       Missing column or changed type must not proceed.
      | pass
      v
   BRONZE                     data/bronze/yellow/                     [STORED]
   layer 2                    Same rows, same types, plus provenance.
      |                       Nothing dropped, nothing reshaped.
      |
      |  apply src/quality/ rules
      v
   [ quality rules ]--FAIL--> QUARANTINE   data/silver/rejects/       [STORED]
      |                       The row, kept, with the reason it failed.
      | pass                       |
      |                            |  reject rate counted every run
      |                            v
      |                       [ reject rate > threshold? ]--YES--> ALERT, and
      |                                                            look upstream
      v
   SILVER                     data/silver/trips/                      [STORED]
   layer 3                    One row per trip. Trustworthy.
      |    ^                  Cleaned, conformed, zone-enriched, H3-indexed.
      |    |
      |    |  joined from
      |    +---------------   DIMENSIONS   data/silver/dim_zone/      [STORED]
      |                       zone name, borough, and the H3 cells the
      |                       zone polygon covers
      |
      |  aggregate
      v
   GOLD                       data/gold/<table>/                      [STORED]
   layer 4                    Many tables, one per question above.
      |                       No cleaning here. If gold needs to filter for
      |                       quality, silver failed.
      v
   CONSUMPTION                Queries, benchmarks/, README results table.
   layer 5                    What an interviewer opens first.
```

---

## 3. Every layer is stored, and why that matters

A medallion layer is **not** a transformation you run on the way past. It is a
table you write, and then read from.

| Layer | Path | Written by | Read by |
|---|---|---|---|
| Landing | `data/yellow/` | `scripts/fetch_data.sh` | the bronze job |
| Bronze | `data/bronze/yellow/` | `src/bronze/` | the silver job |
| Silver | `data/silver/trips/` | `src/silver/` | the gold job |
| Rejects | `data/silver/rejects/` | `src/silver/` | quality reporting |
| Dimensions | `data/silver/dim_zone/` | `src/silver/` | the silver join |
| Gold | `data/gold/<table>/` | `src/gold/` | analysts, dashboards |

**Why they must be materialised**, and this is the part that makes it click:

In Airflow, bronze and silver are **separate tasks, separate processes, separate
Spark sessions.** The silver task cannot see a DataFrame that lived in the bronze
task's memory. The only thing two tasks can share is storage.

It also means you can rerun silver without touching the source, and debug gold
without recomputing everything above it.

---

## 4. Where Iceberg fits

Iceberg replaces "a directory of Parquet files" with "a table". The same Parquet
files sit underneath, plus a metadata layer tracking snapshots, schema history and
which files belong to which version of the table.

**It is not a layer. It is the storage format the layers switch to.**

Sequence, deliberately:

**Phase 1.** Build bronze, silver and gold as plain Parquet directories. Run them.
Hit the walls.

**Phase 2.** Switch to Iceberg, because of the walls:

| The wall you hit | What Iceberg does about it |
|---|---|
| Re-running a month duplicates every row | `MERGE INTO`: a re-run updates rather than appends |
| A new source column breaks the write | Schema evolution, tracked as a version |
| "The number changed, what was it before?" | Snapshots and time travel |
| A failed job leaves half-written files behind | Atomic commits: readers see the old snapshot until the new one lands |

Doing it in this order means you can say *"I moved to Iceberg because re-running a
month duplicated 3M rows and there was no way to fix it except deleting
directories by hand"* rather than *"Iceberg was on the job description."*

Detail lives in [`bronze.md`](bronze.md).

---

## 5. The layers

### Layer 0 · Source

Monthly Parquet from the NYC TLC, `taxi_zone_lookup.csv` (265 zones, 12 KB), and
the taxi zone **shapefile** for the H3 work.

**We do not control it.** Columns get added between years, types change, files get
silently republished. Every design decision downstream assumes the source will
surprise us.

### Layer 1 · Landing

`data/yellow/`, built by `scripts/fetch_data.sh`.

**The rule: never write here, never modify anything in it.** It exists so that when
something downstream is wrong, we can rebuild and prove what the source said.

### Layer 2 · Bronze

**Answers: what did the source say, and when did we hear it?**

**The rule: do not reshape anything.** Same rows, same columns, same types.

Full detail and decisions: [`bronze.md`](bronze.md).

### Layer 3 · Silver

**Answers: what is true?**

**The rule: still one row per trip.** No aggregation anywhere in this layer.

**Clean.** Reject rows that cannot be real: negative fares, zero-distance trips
with a fare, dropoff before pickup, pickup timestamps outside the file's own month
including trips dated 2002 and 2009.

**Conform.** One set of types, one timezone, one naming convention.

**Enrich.** Join the zone dimension so `PULocationID = 132` becomes
`JFK Airport, Queens`, and carry the H3 cell for that zone.

**Why the join is here and not in gold:** every gold table would otherwise repeat
it. Enrichment that is universally useful happens once.

**Why quarantine instead of delete:** a rejects table lets you count what was
thrown away and check the fraction is stable.

### Layer 4 · Gold

**Answers: the specific question somebody asked.**

One silver table, several gold tables, one per question in section 1.

**Why not just serve from silver?** At three months you could. Gold earns its place
for three reasons:

1. **The shape is different.** Silver is one row per trip. A dashboard wants one
   row per zone per day. Somebody has to aggregate, and doing it on every dashboard
   load means paying for it every time.
2. **It decouples consumers from silver.** Change a silver column and every
   dashboard breaks. Gold is a contract that absorbs that change.
3. **Cost.** Aggregating 9.5M rows per page refresh is fine. 900M is not.

Being straight: at this data size gold is partly pedagogical. It is also where all
the interesting Spark problems live, because it is where the shuffles are.

### Layer 5 · Consumption

Queries proving the tables answer section 1, the benchmark suite, and the README
results table.

---

## 6. H3, and where it comes in

H3 is Uber's hexagonal spatial index. It cuts the world into equal-area hexagons at
16 resolutions, each with an ID.

**Why this project needs it.** Taxi zones are administrative polygons of wildly
different sizes and shapes. "Trips per zone" cannot be compared fairly between a
tiny midtown block and half of Staten Island. Hexagons are uniform, so they can.

The second reason is joins. **H3 is a shared key between datasets that have no
natural join.** Trip records are tabular point events. Weather is a gridded array.
Nothing links them until both are indexed to the same hexagon.

**⚠️ The complication.** The 2024 files have no `pickup_latitude` or
`pickup_longitude`, only `PULocationID`. So we cannot index points directly. Instead:

1. Take the taxi zone **shapefile** (polygons, not points).
2. Polyfill each zone polygon into the set of H3 cells covering it.
3. That mapping becomes a **dimension table**: `LocationID → [h3_cell, ...]`.
4. Silver joins to it, so every trip carries the H3 cells of its pickup zone.
5. Gold can then aggregate by hexagon instead of by zone.

**Where it lives:** the zone-to-H3 mapping is built once and stored as
`data/silver/dim_zone/`. It is a dimension, not a pipeline stage.

**Phase 2, later:** join gridded weather (NetCDF or Zarr) on the same H3 cell. That
is where H3 stops being decoration and becomes the mechanism.

---

## 7. Predictions, not planted mistakes

**Build every layer as correctly as you know how.** Nothing here is sabotage.

Deliberately planting a bug teaches you only that a mistake you already knew about
produced the outcome you were already told to expect. That is theatre. The learning
lives in the gap between what you predicted and what the Spark UI actually shows,
and that gap only exists if you were honestly trying to get it right.

So: build it properly, write the prediction down, run three months, scale to twelve,
watch what degrades, diagnose it in the UI, then fix it and record the number. Some
of these will happen anyway despite your best effort, which is the point.

| Layer | Prediction | What it teaches if it happens |
|---|---|---|
| Bronze | Output file count grows with nothing controlling it | Becomes a small-file problem at twelve months |
| Bronze | The schema check reads more than it needs to | Reading a footer is not reading a file |
| Silver | The zone-lookup join plans as sort-merge, not broadcast | When the optimiser needs telling, and how to see which it chose |
| Silver | Reject counting costs a second full pass | Doing quality checks in one pass instead of two |
| Gold | Default 200 shuffle partitions produce tiny output files | You already have the tuning table from day 2 |
| Gold | Aggregating on pickup zone is skewed | One task at 30x the median. ⭐ The war story |
| Gold | Silver gets read repeatedly with no caching | When caching pays and when it costs |

Record the outcome next to each. **A prediction that turned out wrong is the most
valuable row in this table**, because it is the only one that told you something you
did not already believe.

---

## 8. How each layer gets built

Same three steps every time:

1. **Notebook.** `notebooks/NN_<layer>.ipynb`. Play with the data, get the syntax
   wrong, watch the Spark UI. Nothing reusable yet.
2. **Module.** Wrap what worked into `src/<layer>/`, with a `main()` so it runs
   from the command line and so Airflow can call it later.
3. **Record.** The decisions go in that layer's markdown file. Numbers go in the
   README results table.

Notebooks are scratch space. `src/` is the product. Do not let the notebook become
the pipeline.

**Every decision made inside a layer gets written down in that layer's document**,
with the reasoning and the alternative that was rejected. Six months from now the
reasoning is the part you will have forgotten.

---

## 9. Spark skills, and where each one shows up

Meet these where they occur rather than learning them as topics.

| Skill | Where it appears |
|---|---|
| Schemas and data types | Bronze: validating the source has not drifted |
| Transformations vs actions | Everywhere. Name which one every line is |
| Jobs, stages, tasks | Every run. Read the UI before moving on |
| Nulls | Silver: `dropna`, `fillna`, and NULL as a real business value |
| Strings | Silver: conforming `store_and_fwd_flag` and zone names |
| Timestamps | Silver: timezone, truncation, extracting hour and weekday. The biggest one here |
| Filtering and sorting | Silver cleaning rules |
| Joins | Silver: the zone dimension. Broadcast vs sort-merge |
| Aggregations | Gold: every table |
| Window functions | Gold: ranking zones, running totals |
| Partitioning | Bronze and gold writes |
| Shuffles and skew | Gold, on a genuinely skewed key |
| Caching | Gold, when silver gets read repeatedly |
| UDFs | Deliberately avoided. Know **why**: a Python UDF is opaque to Catalyst and pays serialization per row. The answer is "I use built-ins, and reach for a UDF only when there is no built-in" |
| Unit testing | `src/quality/rules.py`. Pure functions, which is exactly why quality is a separate module |

---

## 10. Orchestration, at the end

Once the pipeline is correct, wrap it in an Airflow DAG: one task per layer with
dependencies between them.

```
fetch_data  >>  bronze_ingest  >>  silver_build  >>  [gold_zone_daily,
                                                      gold_hourly_demand,
                                                      gold_borough_summary]
```

Worth doing because Airflow is already claimed on the CV from the Dentsu work, and
a public repo showing it beats a line claiming it. **Not before the pipeline is
correct.** Orchestrating a broken pipeline just schedules the breakage.

---

## 11. Out of scope

Streaming. Cluster operations. Cost tuning. Scala. A UI or dashboard. Real-time
serving. Machine learning on the trips.

The line for when it comes up: *"I have built and tuned jobs, I have not operated
Spark in production. What I know is the engine, not the ops."*
