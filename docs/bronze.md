# Bronze layer

> Parent: [`plan.md`](plan.md) · Next: `silver.md`
>
> Every decision made in this layer is recorded here, with the reasoning and the
> option that was rejected. Six months from now the reasoning is the part you will
> have forgotten.

---

## 1. The mental model

**Bronze is the boundary between the world you do not control and the world you
do.**

Upstream of it, anything can happen. The TLC can add a column, change a type,
republish a file, or ship a month with 11% garbage in it. Downstream of it,
everything has to be predictable, because dashboards and models depend on it.

Bronze exists to absorb that boundary. Its entire job is to take an uncontrolled
input and make it **reproducible**, without judging it.

Three properties define a good bronze layer, in any company, on any stack:

| Property | Means | Test |
|---|---|---|
| **Faithful** | You can prove what the source said, later | Can you rebuild a report from six months ago exactly? |
| **Idempotent** | Running it twice equals running it once | Run the job twice. Did the row count change? |
| **Observable** | You know what arrived, when, and from where | Can you answer "which file did this row come from"? |

Almost every real bronze bug is one of those three failing.

### The names you will hear for it

Bronze, raw, staging, landing zone, ODS (older warehouses), L0. Not identical in
every shop, but the intent is the same: **the first place the data lands inside
your system, unjudged.**

Worth recognising all of them, because an interviewer will use whichever one their
company uses.

### The instinct to fight

Every engineer's first instinct in bronze is to clean something. A negative fare is
obviously wrong, so why keep it?

Because **the moment you drop a row in bronze, you have destroyed evidence.** Six
months later someone asks why January revenue moved and the answer is in the rows
you deleted. Rejecting data is a decision, and decisions belong in a layer where
they are recorded, counted and reversible. That is silver.

Bronze does not have opinions. Bronze has a tape recorder.

---

## 2. The rule

**Do not reshape anything.** Same rows, same column names, same types.

| Belongs in bronze | Does not, and goes to |
|---|---|
| Every row from the source, including bad ones | Filtering impossible values → **silver** |
| Original column names, spelling and case | Renaming, casing conventions → **silver** |
| Original types, even the awkward ones | Casting, timezone conversion → **silver** |
| Which file each row came from | Derived business columns → **silver** |
| When it was loaded | The zone lookup join → **silver** |
| A check that the schema is as expected | Any aggregation at all → **gold** |

### Why provenance columns are not "reshaping"

They describe the **load**, not the trip. Nothing about the taxi ride changes. They
are metadata about the row's arrival, and they are the difference between "the
numbers look wrong" and "the numbers look wrong for rows loaded on the 14th from
file X".

---

## 3. Physical shape

### Where

```
data/bronze/yellow/
```

One directory per source dataset. When a green-taxi or FHV feed is added later it
becomes `data/bronze/green/`, not a column in the same table.

### Format

**Phase 1: Parquet.** Columnar, compressed, splittable, and what the source already
is, so nothing is lost in conversion.

**Phase 2: Iceberg.** Section 8.

### Partitioning

```
data/bronze/yellow/year=2024/month=1/part-00000.parquet
                            month=2/part-00000.parquet
                            month=3/part-00000.parquet
```

**Decision: partition by `year` and `month`.**

Why those two:

- A query for March reads one directory instead of twelve. That is **partition
  pruning**, and it is free once the layout is right.
- Reloading one month can target one directory rather than the whole table.
- Cardinality is right. Twelve directories a year is nothing.

Why **not** by day: 365 directories a year, each a few MB, and you have built the
small files problem on purpose.

Why **not** by `PULocationID`: 265 values times months is thousands of tiny
directories. High-cardinality partition columns are the classic way to make a table
slower while believing you optimised it.

Why **not** unpartitioned: every query reads every file. No pruning at all.

> ⚠️ **A detail that will bite.** Writing `month` as an integer gives you `month=1`,
> not `month=01`. Directory listings then sort as 1, 10, 11, 12, 2, 3. It works
> correctly, it just reads badly. If you want zero-padding, the column has to be a
> string, and then filters have to compare strings. Pick one and be consistent.

### Where the partition values come from

**From the filename, not from the row's pickup timestamp.**

`yellow_tripdata_2024-03.parquet` produces `year=2024, month=3` for every row in it,
including the ones whose `tpep_pickup_datetime` says 2002.

That is deliberate. Bronze records **what the source claimed**. Checking whether the
rows agree with that claim is silver's job, and it is one of the more interesting
quality rules because the disagreement is real: NYC files genuinely contain trips
dated years outside their own month.

---

## 4. Provenance columns

Three, all underscore-prefixed so they can never collide with a column the TLC
might add.

| Column | Source | Answers |
|---|---|---|
| `_source_file` | `input_file_name()` | Which file did this row come from? |
| `_ingested_at` | `current_timestamp()` | When did we load it? |
| `_batch_id` | one value per pipeline run | Which run produced this? |

**Why `_source_file` matters:** when the TLC republishes a month with corrected
data, this is how you find every row that came from the old version.

**Why `_ingested_at` matters:** it separates "when the trip happened" from "when we
learned about it". Those are different questions and business people conflate them
constantly.

**Why `_batch_id` matters:** it is the handle for undoing one bad run. Without it,
"delete everything the broken job wrote" has no `WHERE` clause.

> `input_file_name()` works on file-based sources in Spark 3.5. It returns the full
> URI, so it is long and repetitive, but Parquet dictionary-encodes it and the cost
> is close to nothing.

---

## 5. Schema drift: what to do and why

**The principle: accept changes that cannot corrupt existing data, fail on the ones
that can.**

| Change at the source | What to do | Why |
|---|---|---|
| **New column appears** | Accept, log it loudly | Additive. Nothing existing breaks, and bronze keeps everything anyway |
| **Expected column missing** | **Fail the job** | It becomes NULL downstream, and in this domain NULL is a real business value, so the corruption is invisible |
| **Type changed** (int → string) | **Fail the job** | Spark will coerce silently. `"12"` and `12` compare differently and no error is raised |
| **Column renamed** | **Fail the job** | Reads as a delete plus an add. Needs a human decision |
| **Column order changed** | Ignore | Parquet is name-addressed, not position-addressed |
| **Nullability changed** | Accept, log it | Cannot corrupt values. Worth knowing about |

### Why "fail loudly" beats "handle it gracefully"

A pipeline that keeps running through a schema change produces **wrong numbers that
look right**. Nobody investigates a dashboard that looks plausible. A pipeline that
stops produces an angry Slack message within the hour, and an angry Slack message is
the cheapest possible failure mode.

This is the same reasoning as the fail-fast ingestion contract on the Dentsu
platform, and it generalises: **the cost of a failure is not how loud it is, it is
how long it takes to notice.**

### How the check works

Compare the set of columns in the file against a declared expected list. Not the
full Spark `StructType`, because that also carries nullability and metadata that
changes for uninteresting reasons. Names and types are what matter.

---

## 6. Idempotency: the central problem of this layer

**The question every scheduled pipeline must answer: what is here that I have not
already processed?**

Get it wrong and re-running the job duplicates data. There is no error, no warning,
and every downstream number is silently inflated.

Four approaches, in increasing order of sophistication.

### A. Append, no check ⚠️ what we build first

```python
df.write.mode("append").partitionBy("year", "month").parquet(path)
```

Run March twice and every March row exists twice. No way to fix it except deleting
directories by hand.

**We build this on purpose.** Doing it and then watching your row count double is
worth more than reading about it. Planted mistake number one.

### B. Dynamic partition overwrite

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
```

Replaces **only the partitions present in the DataFrame** and leaves the others
alone. Re-running March overwrites March and does not touch January.

- Cheap, no extra state to maintain, and standard practice
- Requires the unit of reprocessing to line up exactly with the partition key
- Not atomic: a crash mid-write can leave a partition half-replaced

### C. A processed manifest

Keep a small record of which source files have been loaded. Check it at the start of
every run and skip what is already there.

- Works even when the reprocessing unit does not match the partitioning
- Handles the case where the same file is republished with new content, via a hash
- Extra state you now have to keep correct, and it can drift from reality

### D. Iceberg `MERGE INTO` ⭐ the real answer

```sql
MERGE INTO bronze.yellow t
USING updates s
ON t.trip_id = s.trip_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

- Genuinely idempotent, at row level
- **Atomic.** A failed job commits nothing; readers keep seeing the previous snapshot
- Needs a real key, which this dataset does not have out of the box

> ⚠️ **NYC trip records have no primary key.** No trip ID, no medallion number in
> the 2024 files. So a row-level MERGE needs a synthetic key, usually a hash of the
> columns that together identify a trip. That is itself a design decision, and it is
> in silver's document because it is a reshaping choice.

**Our sequence: A, then B, then D.** Each step because the previous one hurt.

---

## 7. One table or one table per month

**Decision: one table, partitioned by year and month.**

The instinct to keep months separate is right. The mechanism of separate tables is
wrong.

| | One table, partitioned | One table per month |
|---|---|---|
| Query all of 2024 | one read | twelve-way UNION |
| Add month 13 | nothing to change | edit every query |
| Physically separated on disk | ✅ yes | ✅ yes |
| Reload one month | overwrite one partition | drop one table |
| Single thing to point at | ✅ yes | ❌ no |
| Schema changes | one place | thirteen places |

**Partitioning gives you the physical separation you wanted with none of the
downsides.** That is the reason partitioning exists as a feature: it is separation
without fragmentation.

Table-per-period is a pattern you will still meet in older warehouses, usually with
a view stitching them together. Recognise it and understand why it was replaced.

---

## 8. Iceberg in bronze

### What it actually is

**Iceberg is a specification for describing a table, not a storage engine and not a
file format.** The data stays as ordinary Parquet files. Iceberg adds metadata:

```
data/bronze/yellow/
    data/                          the Parquet files, unchanged
    metadata/
        v1.metadata.json           table schema, partition spec, snapshot list
        v2.metadata.json           after the next write
        snap-*.avro                manifest lists: which files are in which snapshot
        *.avro                     manifests: file paths, row counts, column stats
```

Because it is a spec rather than an engine, **Trino, DuckDB, Flink and Snowflake can
all read the same table.** That is the argument for it over a vendor format.

### The four things it gives this layer

| Feature | What it fixes here |
|---|---|
| **Snapshots** | Every write creates a version. "What did this look like last Tuesday" becomes a query |
| **Atomic commits** | A failed job commits nothing. Readers never see a half-written table |
| **Schema evolution** | Adding a column is a metadata change, tracked with a version. No rewrite |
| **`MERGE INTO`** | Real idempotency, at row level |

### Time travel, once it is in

```sql
SELECT * FROM bronze.yellow VERSION AS OF 12345;
SELECT * FROM bronze.yellow TIMESTAMP AS OF '2026-08-14 10:00:00';
```

And the metadata tables, which answer questions that otherwise need a filesystem
walk:

```sql
SELECT * FROM bronze.yellow.snapshots;
SELECT * FROM bronze.yellow.history;
SELECT COUNT(*), SUM(file_size_in_bytes)/1024/1024 AS mb FROM bronze.yellow.files;
```

> ⚠️ **On the CV, put the capability, not the feature name.** "Schema evolution" is a
> competency. "Time travel" is a feature name, and listing feature names invites
> *"when did you use that in production?"* Keep it for conversation.

### Setup, when we get there

Iceberg ships as a runtime jar matched to the Spark minor version. On Spark 3.5 that
is `iceberg-spark-runtime-3.5_2.12`. Check Maven Central for the current 1.x release
rather than pinning from memory.

```python
.config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:<version>")
.config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local.type", "hadoop")
.config("spark.sql.catalog.local.warehouse", str(ROOT / "warehouse"))
```

A `hadoop` catalog keeps the metadata in the same directory tree as the data, which
needs no external service. Production usually uses a REST catalog, Glue or Nessie
instead, because a hadoop catalog cannot safely handle concurrent writers.

**That last sentence is the honest limitation to state out loud in an interview.**

---

## 9. Decisions recorded

| # | Decision | Chosen | Rejected, and why |
|---|---|---|---|
| 1 | Idempotency | Append first, then dynamic partition overwrite, then Iceberg MERGE | Going straight to MERGE. The failure has to happen once to mean anything |
| 2 | Partitioning | `year` + `month` | By day (too many small files), by zone (high cardinality), none (no pruning) |
| 3 | Partition values from | The filename | The pickup timestamp: that is a reshaping decision and belongs to silver |
| 4 | Schema drift | Accept additive, fail on missing / retyped / renamed | Auto-merging everything: it hides corruption |
| 5 | Table layout | One table, partitioned | One table per month: needs a UNION per query and fragments the schema |
| 6 | Format | Parquet first, Iceberg second | Iceberg first: then you never learn what it is solving |
| 7 | Cleaning | None here at all | Dropping obvious garbage early: destroys the evidence |
| 8 | Provenance | `_source_file`, `_ingested_at`, `_batch_id` | None: makes every later question unanswerable |

---

## 10. Mistakes planted in this layer

| Mistake | When it will bite | What fixes it |
|---|---|---|
| Append with no idempotency | The first time you rerun a month | Dynamic partition overwrite, then Iceberg MERGE |
| No control over output file count | At twelve months, and worse in gold | `coalesce` before write, or shuffle partition tuning |
| `count()` before every write | When ingestion becomes the slow part | Take the count from write metrics instead of a separate pass |

---

## 11. How this layer gets built

**Step 1, notebook** (`notebooks/03_bronze.ipynb`)

Work out, with the data in front of you and the Spark UI open:

- Read one month. How many partitions, how many jobs, how many stages?
- What are the 19 columns and their exact types?
- Add the three provenance columns. Which of those calls is a transformation and
  which forces a job?
- Write it partitioned. Look at the directory tree that comes out.
- Count the output files. Does the number match what the partition formula predicts?
- Read it back and confirm the row count survived.
- **Then run the whole thing twice and watch the row count double.**

**Step 2, module** (`src/bronze/ingest.py`)

Wrap what worked into functions, with a `main()` so it runs from the command line
and Airflow can call it later. Roughly: read one month, check the schema, add
provenance, write, report.

**Step 3, record**

Update section 9 with anything you decided differently, and put the numbers in the
README results table.

---

## 12. Done when

- [ ] `python -m src.bronze.ingest` lands every month found in `data/yellow/`
- [ ] Adding a new file to `data/yellow/` gets picked up with no code change
- [ ] The row count in bronze equals the row count in the source, exactly
- [ ] A missing or retyped column fails the job with a readable message
- [ ] Every row can be traced to its source file and its load time
- [ ] **You have run it twice and seen the duplication with your own eyes**
- [ ] Section 9 matches what you actually built

---

## 13. Answers this layer produces

**"How do you handle a source schema that changes?"**
> Accept what cannot corrupt existing data, fail on what can. A new column is
> additive so I log it and carry on. A missing column, a changed type or a rename
> fails the job, because those turn into silently wrong numbers rather than errors.
> A pipeline that keeps running through a schema change is more expensive than one
> that stops.

**"How do you make a pipeline idempotent?"**
> Depends what the storage supports. With plain Parquet, dynamic partition overwrite
> gets you there when the reprocessing unit matches the partition key. With Iceberg,
> `MERGE INTO` gives you row-level idempotency and atomic commits, so a failed run
> commits nothing. I started with a naive append, watched a rerun double the row
> count, and that is why I moved.

**"Why keep a raw layer at all? It is duplicated data."**
> Because the source is not under my control and it is not reproducible. Files get
> republished, columns get added, and if I only keep the cleaned version I cannot
> prove what the source said or rebuild after a bug in my own cleaning logic.
> Storage is cheaper than an unanswerable question.

**"What is Iceberg, in one sentence?"**
> A specification for describing a table on top of ordinary Parquet files: which
> files belong to which snapshot, what the schema was at each version, and column
> statistics. Because it is a spec and not an engine, Trino or DuckDB can read the
> same table Spark wrote.
