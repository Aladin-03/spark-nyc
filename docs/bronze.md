# Bronze layer

> Parent: [`plan.md`](plan.md) · Next: `silver.md`
>
> **The order this layer gets built in is the general one: [`method.md`](method.md).**
> This document is the bronze-specific content that fills it in.
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

> **Idempotency is a property of the result, not of the run.** A job that succeeds
> twice and doubles the row count succeeded twice and is not idempotent.
>
> The related but separate property is **atomicity**: either the whole write lands or
> none of it does. A crash halfway through a Parquet write leaves half the files
> behind, which is a failure of atomicity, not of idempotency. Parquet directories do
> not give you atomicity. Iceberg does, because nothing is visible until the commit.
> Keep the two words apart.

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

**First, four different things get called a "partition".** They are at four
different levels and confusing them is the most common way to misread a Spark job.

```
  1. TABLE PARTITION           data/bronze/yellow/year=2024/month=03/
     directories on disk          ^ a FOLDER. This is what "partition by" means.

  2. FILES in that partition      part-00000.parquet   part-00001.parquet
     one per writing task         ^ just files. NOT separate partitions.

  3. ROW GROUPS inside a file     [rows 0-1M][rows 1M-2M][rows 2M-3M]
     ~128 MB blocks, each with    ^ a logical boundary INSIDE one file.
     min/max stats per column

  4. SPARK PARTITION              the slice one task holds in memory
     the unit of parallelism      ^ exists only while the job runs.
                                    This is df.rdd.getNumPartitions().
```

Two different skipping mechanisms come out of this, one per level:

| Name | Level | How it works |
|---|---|---|
| **Partition pruning** | 1, folders | `WHERE month = 3` and the planner never lists the other folders. Decided before a single file is opened |
| **Predicate pushdown** | 3, row groups | `WHERE fare > 100` reads the Parquet footer stats and skips row groups whose maximum fare is 50 |

Pruning only fires if you filter **on the partition column**. Filtering on
`tpep_pickup_datetime` will not prune a `month=` partition, because Spark has no idea
the two are related.

"Physical separation" means **folders**. Different months are different directories.
Multiple `part-*` files in one directory are parallel write output, not separation.

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

Mechanically, a partition value **always comes from a column**. `partitionBy("year",
"month")` requires those columns to exist. Spark builds the folder name from the
value and then **strips the column out of the file contents**, because the path
already encodes it, and reconstructs it from the path on read.

```
  SOURCE FILENAME                  →   COLUMN        →   DIRECTORY
  yellow_tripdata_2024-03.parquet      year  = 2024      year=2024/
                                       month = 3         month=3/
```

The design decision is where those columns get their values.

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

> ⚠️ **A batch is not a Spark job.** Spark's job id is internal and dies with the
> session, so job 7 today is a different job 7 tomorrow and nothing outside the UI
> can use it. A **batch** is one logical run of *your* pipeline over a defined input,
> and you generate its id yourself at the start of the run and write it into the
> data. Here, a UTC timestamp. In production, the orchestrator's run id, which for
> Airflow is the `run_id` of the DAG run.
>
> `_ingested_at` is close but not equal: a run that takes forty minutes writes many
> different timestamps while sharing one batch id.

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
| **Column order changed** | Ignore, **but see below** | Parquet is name-addressed, not position-addressed |
| **Nullability changed** | Accept, log it | Cannot corrupt values. Worth knowing about |

> ⚠️ **"Ignore reorder" is only safe if nothing in the pipeline reads by position.**
> `df1.union(df2)` matches columns **by position** and will silently swap your data
> when a source reorders. `df1.unionByName(df2)` matches by name. Same for
> `toDF(*names)`, which renames positionally. **Ban positional operations in this
> project.** This is the Spark version of the ordinal breakage you get feeding a
> reordered Postgres table into a BI tool, and the fix is the same: address by name.

**Why not just read everything as a string?** Because it depends on the source:

| Source | Right answer |
|---|---|
| CSV, JSON, an API | String in bronze, cast in silver. There were no real types to begin with, so parsing is already an interpretation, and interpretations belong in silver |
| **Parquet, ours** | Keep the source types. They came with the file |

Forcing our Parquet to string **would be reshaping**, and it breaks the one rule of
this layer. Nulls are not a reason to do it: Parquet represents null natively, it
never needs the string `"NULL"`.

**On nullability.** If you assert a column is non-null and the source sends null,
yes, the job must fail. But two caveats. First, Spark's Parquet reader marks almost
everything nullable on read regardless of what the file says, so you cannot rely on
comparing the flags, it has to be an explicit check you write. Second, and more
important: **null checks mostly do not belong here at all.** Bronze judges structure,
does the column exist and is it the right type. Silver judges values. The difference
matters because bronze can only crash, whereas silver can quarantine forty bad rows
and keep three million good ones.

> **Bronze fails on shape. Silver quarantines on content.**

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

### A. Append, no check ⚠️ the broken one

```python
df.write.mode("append").partitionBy("year", "month").parquet(path)
```

Run March twice and every March row exists twice. No error, no warning, and the only
fix is deleting directories by hand.

**Do this once in the notebook, not in the pipeline.** Write March, count, write
March again, count again. Seeing 5.9 million where 3.0 million belongs is worth two
minutes. Then delete it and build B properly. The pipeline itself is built correctly
from the first commit.

### B. Dynamic partition overwrite ⭐ what we build

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
```

Replaces **only the partitions present in the DataFrame** and leaves the others
alone. Re-running March overwrites March and does not touch January.

- Cheap, no extra state to maintain, and standard practice
- Requires the unit of reprocessing to line up exactly with the partition key
- Not atomic: a crash mid-write can leave a partition half-replaced

> ⚠️ **Without that config line, `mode("overwrite")` deletes the entire table** and
> leaves only March behind. The default is `static`. This is the most destructive
> default in Spark and it is worth remembering on its own.

### C. A processed manifest

Keep a small record of which source files have been loaded. Check it at the start of
every run and skip what is already there.

- Works even when the reprocessing unit does not match the partitioning
- Handles the case where the same file is republished with new content, via a hash
- Extra state you now have to keep correct, and it can drift from reality

### D. Iceberg atomic partition replacement ⭐ phase 2

What bronze wants from Iceberg is **not** row-level `MERGE INTO`. It is B, made
atomic and given a memory:

```sql
REPLACE PARTITIONS  -- one commit. Either all of March is replaced, or none of it is.
```

- **Atomic.** A failed job commits nothing, readers keep seeing the previous snapshot
- **Historied.** Every write is a snapshot you can inspect, diff and roll back to
- No extra state to maintain, because the table maintains it

> ⚠️ **`MERGE INTO` is a different tool and this dataset cannot use it cleanly.**
> MERGE matches incoming rows to existing rows **on a key**, and NYC trip records
> have no key: no trip ID, no medallion number in the 2024 files. Inventing one from
> a column hash is a judgement call, so it lives in silver. MERGE is the right answer
> for CDC and for slowly changing dimensions, which is where you should reach for it
> in an interview, not here.

**Our sequence: B on plain Parquet, then D on Iceberg.** A gets demonstrated once in
the notebook. C is understood and skipped, because our filenames already tell us
which month a file holds.

---

## 7. One table or one table per month

**Decision: one table, partitioned by year and month.**

The instinct to keep months separate is right. The mechanism of separate tables is
wrong.

**One partitioned table does not mean one file.** This is the fear worth killing
before it takes hold:

```
  ONE TABLE, PARTITIONED              ONE TABLE PER MONTH
  data/bronze/yellow/                 bronze.yellow_2024_01
    year=2024/                        bronze.yellow_2024_02
      month=01/  part-00000.parquet   bronze.yellow_2024_03
      month=02/  part-00000.parquet    ^ three names to register,
      month=03/  part-00000.parquet      three schemas that can drift,
   ^ ONE name. THREE folders.            a UNION in every query.
```

All of 2024 stays in twelve separate folders. You get the separation *and* the single
table. The per-month option gives you the separation and takes the table away.

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
| **Partition replacement** | Replace all of March in one commit, atomically |

### What lives where, and what a snapshot actually is

You declare two things once, at `CREATE TABLE`: the **schema** and the **partition
spec**. Iceberg maintains everything below from then on. You never hand-write a
manifest.

| File | Holds |
|---|---|
| `v3.metadata.json` | Current table state: **every schema version**, every partition spec, the **full list of snapshots**, and a pointer to the current one |
| `snap-<id>-...avro` | A **manifest list**: which manifest files make up that one snapshot |
| `<uuid>-m0.avro` | A **manifest**: which data files, their partition values, row counts, per-column min/max |
| `data/**.parquet` | The rows |

**A snapshot is not one line in a log.** It is an entry in the snapshot list inside
`metadata.json` (id, parent id, timestamp, operation such as `append` or `overwrite`,
and summary counts) *plus* the `snap-*.avro` manifest list enumerating every file the
table consisted of at that instant.

**Time travel is not stored anywhere separately.** That is the part worth
internalising. Reading an old snapshot means reading its old manifest list, which
still points at data files nobody deleted. Time travel is free because nothing was
thrown away. The cost is storage, and the operation that reclaims it is **expiring
snapshots**, which deletes the orphaned files and ends your ability to travel back
past that point. Schema history lives in `metadata.json` too, as a list of schemas
with ids, and each snapshot records which schema id it was written under. That is how
one table holds files with different columns and still reads as one thing.

Open `v1.metadata.json` in a text editor the first afternoon you have a real table.
It is plain JSON and it teaches more than this section does.

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
| 1 | Idempotency | Dynamic partition overwrite, then Iceberg atomic replace | Naive append (duplicates silently); row-level `MERGE INTO` (needs a key this data has not got); a processed manifest (state to maintain, and the filename already tells us the month) |
| 2 | Partitioning | `year` + `month` | By day (too many small files), by zone (high cardinality), none (no pruning) |
| 3 | Partition values from | The filename | The pickup timestamp: that is a reshaping decision and belongs to silver |
| 4 | Schema drift | Accept additive, fail on missing / retyped / renamed | Auto-merging everything: it hides corruption |
| 5 | Table layout | One table, partitioned | One table per month: needs a UNION per query and fragments the schema |
| 6 | Format | Parquet first, Iceberg second | Iceberg first: then you never learn what it is solving. ORC and Avro: equivalent or row-based, no advantage here |
| 6b | Positional operations | Banned. `unionByName`, never `union` | `union` matches by column position, so a reordered source silently swaps your data |
| 7 | Cleaning | None here at all | Dropping obvious garbage early: destroys the evidence |
| 8 | Provenance | `_source_file`, `_ingested_at`, `_batch_id` | None: makes every later question unanswerable |

---

## 10. Where this will break, predicted in advance

**Nothing here is sabotage.** Build this layer as correctly as you know how. The
point of the table is that you write down what you expect to go wrong *before* you
scale up, and then find out whether you were right.

Deliberately planting a bug only teaches you that a mistake you already knew about
produced the outcome you were already told to expect. The learning is in the gap
between what you predicted and what the Spark UI actually shows, and that gap only
exists if you were honestly trying to get it right.

| Prediction | When it should bite | What you think will fix it |
|---|---|---|
| Output file count grows with no control | At twelve months, worse in gold | `coalesce` before write, or shuffle partition tuning |
| A separate `count()` before every write doubles the work | When ingestion becomes the slow part | Take the count from write metrics instead of a second pass |
| Schema check reads the whole file instead of the footer | When a month is 5 GB rather than 50 MB | Read the schema without reading the data |

Fill in what actually happened next to each one. A wrong prediction is the most
useful row in this table.

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
- **One throwaway cell: append the same month twice and count.** See 5.9 million
  where 3.0 million belongs, then delete the directory. That is the whole reason
  the module gets built with dynamic partition overwrite from its first line.

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
- [ ] **Running `python -m src.bronze.ingest` twice leaves the row count unchanged**
- [ ] You have seen the duplicate-append failure once, in the notebook, and deleted it
- [ ] Section 9 matches what you actually built, and section 10 has real outcomes in it

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
