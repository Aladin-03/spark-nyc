# Concepts and vocabulary

> Parent: [`plan.md`](plan.md) · Layer detail: [`bronze.md`](bronze.md)
>
> The words that come up in interviews, what each one actually means, and whether it
> lands in this project. Written to be re-read before an interview, not once.

---

## 1. Storage layouts

### Row-oriented vs columnar

The whole distinction is **what sits next to what on disk**.

```
  THREE ROWS
  id  zone  fare
   1   132  55.0
   2   161  12.5
   3   132  38.0

  ROW-ORIENTED                       COLUMNAR
  [1,132,55.0][2,161,12.5][3,132,38.0]   [1,2,3][132,161,132][55.0,12.5,38.0]
   ^ one record is contiguous              ^ one COLUMN is contiguous
```

**Row wins** when you want whole records: fetch one customer, write one order, replay a
change log. That is OLTP, and it is why Postgres and MySQL are row stores.

**Column wins** when you want a few fields of very many records. `SUM(total_amount)`
over 3 million trips touches one column out of nineteen, so a columnar engine reads
roughly 5% of the bytes. Values of one type also sit together, which compresses far
better. That is OLAP.

### Where each format sits

| Format | Layout | Typed? | Binary? | Schema lives | Use it for |
|---|---|---|---|---|---|
| **CSV** | Row | ❌ no | ❌ text | Nowhere. The header is a hint | Interchange, nothing else |
| **JSON** | Row | Partly | ❌ text | Repeated in every record | APIs, nested payloads |
| **Avro** | Row | ✅ yes | ✅ yes | File header | Streaming, Kafka messages, **Iceberg's own metadata** |
| **Parquet** | **Column** | ✅ yes | ✅ yes | File footer | Analytics. Our data files |
| **ORC** | **Column** | ✅ yes | ✅ yes | File footer | Same job as Parquet, Hive lineage |

**Avro is not "another CSV".** They are both row-oriented and that is where the
similarity ends. Avro is binary, carries real types, embeds its schema, supports schema
evolution rules, and splits cleanly for parallel reads. CSV is text with no types and no
schema, so every read is a re-parse and a guess.

That is exactly why Iceberg uses **Avro for metadata and Parquet for data**: you read
nearly all fields of a manifest row and you append rows to it, which is row-shaped work.
You read a few columns of millions of data rows, which is column-shaped work.

### DataFrames and SQL tables are not storage formats

Worth stating plainly, because the question comes up.

- A **DataFrame** is an in-memory, lazily-evaluated abstraction. It has no storage
  layout of its own. Spark's internal representation is actually columnar in places
  (Tungsten uses columnar batches), but that is an implementation detail you never see.
- A **SQL table** is a logical object. Its storage could be anything. Postgres stores
  rows. BigQuery, Snowflake, Redshift and ClickHouse store columns. Same SQL, opposite
  physical layout, and the difference shows up entirely in which queries are fast.

---

## 2. Iceberg, mechanically

### Tracing back to a point in time

The timestamp lives in **`metadata.json`**, in the `snapshots` array and in
`snapshot-log`. Every snapshot entry carries `snapshot-id`, `parent-snapshot-id`,
`timestamp-ms`, `operation` and a `manifest-list` path.

`SELECT * FROM bronze.yellow TIMESTAMP AS OF '2026-08-14 10:00:00'` resolves like this:

```
  1. CATALOG              →  current metadata pointer          v7.metadata.json
  2. v7.metadata.json     →  scan the snapshot list, find the latest
                             snapshot whose timestamp-ms <= requested
                                                              snapshot 8472
  3. snapshot 8472 entry  →  its manifest-list path            snap-8472-1-abc.avro
  4. snap-8472-1-abc.avro →  the manifests valid at that instant
                                                              abc-m0.avro, def-m1.avro
  5. those manifests      →  the data files valid at that instant
  6. read those Parquet files
```

**You never consult the current data at all.** You start at a snapshot and follow that
snapshot's own pointers down. Old and new Parquet files sit intermixed in the same
`data/` directory; what separates the versions is only *which manifest lists them*.

That is also why time travel needs no extra storage. Nothing was copied and nothing was
deleted. Reading an old version is just following an older set of pointers.

### Three levels of skipping

Your question was whether it goes one step deeper than the partition. It goes two.

```
  LEVEL 1   manifest list      partition value RANGES per manifest
            ↓ skip whole manifests without opening them

  LEVEL 2   manifest           ONE ROW PER DATA FILE:
            ↓                    · partition values      year=2024, month=3
              skip individual     · record count, size
              part files          · per-column lower_bounds / upper_bounds
                                  · per-column null counts

  LEVEL 3   Parquet footer     per-ROW-GROUP min/max inside one file
            ↓ skip row groups. Iceberg is not involved here, this is Parquet's own
```

So for `year=2024/month=3/` holding `part-00000.parquet` and `part-00001.parquet`, the
manifest has **two rows, one per file, each with its own min/max**. Query
`WHERE total_amount > 500`, and if part-00000's recorded maximum is 300, **Iceberg skips
that file without opening it.** Then inside part-00001, Parquet's footer skips the row
groups that cannot match.

That is the answer to "how do we know which part file has the range". The manifest knows,
because a manifest row *is* a data file.

> ⚠️ Stats are not kept for every column unconditionally. Iceberg's
> `write.metadata.metrics.default` is `truncate(16)`, and it infers metrics for up to
> `write.metadata.metrics.max-inferred-column-defaults` (100) columns. On a very wide
> table you choose which columns get full stats, because stats cost metadata size.

### Catalogs: do you need a service?

**Your premise was backwards.** Iceberg has catalogs natively, several implementations
of them, and the catalog is a required part of the design. Delta is the one that leans on
the filesystem instead.

| | Iceberg | Delta Lake |
|---|---|---|
| Where "current version" lives | A **catalog**, explicitly | `_delta_log/` next to the data |
| Atomicity comes from | Catalog compare-and-swap | Atomic put-if-absent on the next log file |
| Reading by bare path | Needs a catalog (or `hadoop` catalog) | Works directly |

Neither is better. Iceberg made the pointer explicit so it can be handed to something
that does transactions properly. Delta kept it in the filesystem, which is simpler until
the filesystem cannot promise what it needs.

| Iceberg catalog | Service needed? | Notes |
|---|---|---|
| `hadoop` | **None** | Pointer is a file. ⚠️ **Not safe with concurrent writers.** What we use |
| `jdbc` | A database (Postgres, Cloud SQL) | Cheap and genuinely atomic, via a transaction |
| `hive` | Hive Metastore | The traditional answer |
| `glue` | AWS Glue, managed | No server to run yourself |
| `rest` | A REST catalog server | **The modern standard.** Polaris, Lakekeeper, Unity, Gravitino |
| `nessie` | Nessie server | Adds git-like branches and tags on the table |
| BigLake Metastore | GCP, managed | The GCS-native answer |

**For this project:** `hadoop`, no service, single writer, correct choice.

**For an interview:** *"Locally we used a hadoop catalog, where the pointer is a file, so
it can't do a safe compare-and-swap and concurrent writers can corrupt it. In production
you put a REST catalog, Glue or a JDBC catalog behind it, because the atomic swap has to
live somewhere that can actually perform one."*

### Iceberg across the layers

Every Iceberg table is independent. `bronze.yellow`, `silver.trips` and
`gold.daily_zone_stats` each get their own `metadata/` tree. Nothing is shared except the
catalog that lists them.

Whether it earns its place differs by layer:

| Layer | What Iceberg buys | Verdict |
|---|---|---|
| **Bronze** | Atomic partition replacement, an audit trail of every ingest | Yes |
| **Silver** | `MERGE INTO` for upserts and SCD2, atomic multi-partition writes, late-data handling | **Yes, strongest case** |
| **Gold** | Time travel to explain "why did the dashboard change last Tuesday" | Nice, not essential. Gold is usually fully recomputed |

---

## 3. Sizing: files, partitions, spill

### Does bronze care about partition sizing?

Yes, but for a different reason than the other layers, and the distinction matters.

- **Partition granularity** in bronze is chosen to match the **reprocessing unit**, not
  query speed. One source file is one month, so the partition is one month. Bronze is
  rarely queried directly, so pruning is a bonus, not the goal.
- **File size within the partition** matters everywhere, for the same reason everywhere.

### The numbers

| Thing | Target | Why |
|---|---|---|
| **Output file size** | 128 MB to 1 GB | Iceberg's `write.target-file-size-bytes` defaults to 512 MB |
| **Spark partition in memory** | **100 to 200 MB** | Fits a task's memory budget with room for the operation itself |
| **Task duration** | more than ~1 second | Below that, scheduling overhead dominates the actual work |

**Small files become a problem well before you can name a row count**, because it is
about bytes and file count, not rows. Trouble starts under roughly 10 to 20 MB per file
and is severe under 1 MB. Per file you pay: a listing entry, an open, a footer read, and
a task launch. On object storage every one of those is an HTTP round trip.

The diagnostic to remember: **if your average task runs in under a second, you have too
many partitions or too many files.** That is the same signal you already measured on day
2 when 200 shuffle partitions lost to 2.

For this project: 6 files of 30 to 48 MB across three months is fine. Five years would be
120 files, still fine. The small file problem will show up in **gold**, where 200 default
shuffle partitions write 200 tiny files.

### Spill

**Spill is not a bug and it does not happen every time.** It is Spark's safety valve: a
task's working set does not fit in memory, so it writes sorted runs to local disk and
merges them. The alternative is an OOM that kills the executor and loses all its work.

- Some spill during a large sort or aggregation is **normal**.
- **Heavy, repeated spill is the signal** that partitions are too large.

And to answer the "which partition" question directly: **the Spark partition**, the slice
one task holds in memory. Not a year, not a month, not a `part-00000.parquet`.

Rough arithmetic for the budget one task gets:

```
  per-task memory  ≈  executor memory × spark.memory.fraction (0.6)
                      ─────────────────────────────────────────────
                                  cores per executor

  8 GB × 0.6 / 4 cores  ≈  1.2 GB per task
```

Target 100 to 200 MB of *data* per partition against that, because a sort or a join needs
substantial working space beyond the data itself.

> The two levers are different. **Read** partition count comes from file layout and
> `spark.sql.files.maxPartitionBytes`. **Post-shuffle** partition count comes from
> `spark.sql.shuffle.partitions`. Confusing them is why people tune the wrong one.

---

## 4. Change over time

### CDC, change data capture

Capturing inserts, updates and deletes from a source **as they happen**, usually by
reading the database's write-ahead log rather than querying tables. Tools: Debezium,
GCP Datastream, AWS DMS, Fivetran.

Each event carries an operation (`I`/`U`/`D`), a timestamp, and often before and after
images of the row.

**It is not a property of a layer. It describes how data arrives.** But it flows through
all of them, and this is the sentence worth memorising:

> **Bronze stores the changes. Silver applies them. Gold never sees them.**

| Layer | With CDC input |
|---|---|
| Bronze | Land the change events verbatim, append-only. You now own the change log |
| Silver | **Apply** them: `MERGE INTO` to produce current state, or SCD2 to produce history |
| Gold | Aggregates of silver. CDC is invisible by here |

**Not in this project.** TLC publishes whole monthly files with no changelog, so there is
nothing to capture. That is a correct and complete answer if it comes up, and it is
better than pretending.

### SCD, slowly changing dimensions

⚠️ **Type 1 has nothing to do with data types.** The numbering is a taxonomy of history
strategies, not of column types.

| Type | What happens when an attribute changes | History |
|---|---|---|
| **0** | Nothing. The value is fixed forever | n/a |
| **1** | Overwrite the old value | **Lost** |
| **2** | Close the old row (`valid_to`, `is_current = false`), insert a new one | **Kept** |
| **3** | Add a `previous_value` column | One step back only |
| **4** | Current values in the main table, history in a separate one | Kept, separately |
| **6** | 1 + 2 + 3 combined | Kept, plus a convenience column |

**Type 2 is what they mean when they say "SCD".** The answer that closes the question:

> *"Type 2. When the attribute changes I close the current row by setting `valid_to` and
> `is_current = false`, then insert a new row with a new surrogate key. The grain becomes
> one row per entity per version, and facts join on the surrogate key so they keep
> pointing at the version that was current when the event happened."*

**Where it lives: silver, and only in dimensions, never in facts.** Your reasoning was
right. Bronze cannot host SCD because bronze never modifies a row. Bronze happens to hold
every version because it is append or replace by batch; SCD2 is the *deliberate modelling*
of that history into validity ranges. Different activity, different layer.

In this project the candidate is `dim_zone`. If TLC renames zone 132 from "JFK Airport" to
"JFK Terminal 4", do 2023 trips display the old name or the new one? For zone names Type 1
is honestly fine. **Say why you chose it**, because choosing Type 1 knowingly is a better
answer than defaulting to Type 2 because it sounded more sophisticated.

### Backfill and reprocessing

Two distinct operations that get the same name.

| | Bronze backfill | Silver/gold reprocessing |
|---|---|---|
| What it does | Re-ingest historical periods from the source | Re-derive from bronze |
| Triggered by | New months, or the source republishing one | A rule or logic change |
| Touches the source? | Yes | **No** |
| Frequency | Occasional | Constant |

The second is far more common, and it is the entire payoff of having a bronze layer. Both
are only safe because the pipeline is idempotent, and that connection is exactly what an
interviewer is checking for.

### How much do you reprocess?

The real question, and the one with a decision rule:

| What changed | Reprocess |
|---|---|
| A quality rule or business logic | **Every period it affects**, usually all of it |
| A new source column, going forward | Only from the month it first appeared |
| A bug in one month's ingestion | That month |
| Source republished a month | That month, plus everything derived from it |
| Dimension attribute, Type 1 | Rebuild the gold aggregates that used it |
| Dimension attribute, Type 2 | Nothing historical. New rows only |

**The governing principle: reprocess the smallest range that keeps the output internally
consistent.** A table where 2023 was computed under the old rule and 2024 under the new one
is a lie that nobody can see, and it is far more expensive than the compute you saved.

**The mechanism that makes this affordable is partition-level lineage.** If silver is
partitioned the same way as bronze, "reprocess March" is a bounded job that touches one
directory. If it is not, every change is a full rebuild, and you will avoid making changes
because they are expensive, which is the real cost.

### Late-arriving data

⚠️ **The job does not wait.** This is the correction worth making, because the wrong model
leads to the wrong design.

The job runs on schedule, processes whatever exists, succeeds, and finishes. **Late-arriving
data is records that belong to a period, arriving after that period's job already ran and
succeeded.** Nothing is held open.

How it actually happens:

- A trip on 31 March at 23:50 is uploaded on 2 April, so it lands in the **April** file
- TLC republishes the March file in June with corrected rows in it
- A mobile app buffers events offline and flushes them three days later
- An upstream system retries a failed batch the next morning

Why it hurts: your March aggregate is already published, and it is now wrong.

The concept that unlocks all of it:

> **Event time is when the thing happened. Processing time is when we saw it. Late data is
> the gap between the two.**

Three ways to handle it:

| Approach | What it does | Cost |
|---|---|---|
| **Reprocess the affected period** | Re-run March when late March data arrives | Needs idempotency. **What we do** |
| **Watermark** | "We accept up to 3 days late, then we drop it" | Bounded state, but you knowingly lose data |
| **Route by arrival** | Put the late row in the period it arrived in | Trivial, and wrong for almost all analytics |

**This is why bronze partitions by filename, not by pickup timestamp.** The March file
genuinely contains trips dated 2009. Under filename partitioning they land in `month=3`
because that is when we received them, and silver decides separately what to do about the
disagreement between what the file claims and what the row claims.

---

## 5. Things going wrong

### Skew

⚠️ Neither of the two things you guessed.

- **Not** `part-00000.parquet` being bigger than `part-00001.parquet`
- **Not** March having more rows than January

Both of those exist and neither is what "skew" means, because reading is parallel across
files and Spark does not care much.

**Skew is a post-shuffle phenomenon, on a key, at the Spark-partition level.**

```
  groupBy("PULocationID")   →   all rows for zone 132 must land in ONE task

  task 0  ████████████████████████████████████████  zone 132 (JFK)   8% of all trips
  task 1  ██
  task 2  █
  ...
  task 199 ██                                        one task at 30x the median
```

The stage is not finished until its slowest task is finished, so 199 cores sit idle
waiting for one. That is the whole cost.

**Detection:** Spark UI, the stage page, the task duration summary. Compare **max against
median**. A max many times the median is skew. That number is the war story.

**Skewness** with the -ness is the statistics term for an asymmetric distribution. Related
idea, different word, and it is not what an interviewer means.

**Hot key** is the specific value causing it. **Straggler** is the resulting slow task.

### Schema drift vs data drift

| | What changes | Detected by | Layer |
|---|---|---|---|
| **Schema drift** | The **structure**: columns added, dropped, renamed, retyped, reordered | Comparing the file's schema to a declared contract | Bronze |
| **Schema evolution** | The structure, **deliberately, by you**, in a supported way | It is your own change | Any |
| **Data drift** | The **distribution of values**: mean, null rate, cardinality | Comparing this period's statistics to a reference window | Silver, monitoring |

Schema drift is broader than type changes. Any unexpected structural change counts.

**Data drift is a distributional statement, measured over a population**, not per row. You
compare this month's mean, median, percentiles, null rate, distinct count and category
frequencies against a baseline. A per-row check ("is this fare negative") is **data
quality**, which is a different job.

The pairing worth saying out loud:

> **A schema check catches structure. A drift check catches meaning.**

The null rate for `passenger_count` jumping from 0.1% to 40% passes every schema check
ever written, because the schema is still perfectly valid. Only a drift check sees it.

### Quarantine and dead letter queues

**You were right: silver, not bronze.**

| Layer | What happens to a bad row |
|---|---|
| Bronze | Nothing. It is kept. Bronze has no opinions, so there is nothing to reject |
| **Silver** | **Routed to `rejects/` with a reason code**, and the reject rate is monitored |
| Gold | Should not occur. A failure here is a bug in your code, not bad input |

Bronze's only failure mode is refusing an **entire file** on a contract breach. It never
judges individual rows. Silver judges rows, which is exactly why silver can quarantine and
bronze can only crash.

"Dead letter queue" is the streaming word for the same idea: the topic that unprocessable
messages get routed to instead of being dropped or blocking the pipeline.

---

## 6. Physical layout

Four different levers, constantly confused.

| Lever | Operates on | Effect |
|---|---|---|
| **Partitioning** | Which **folder** a row lands in | Skip whole directories at planning time |
| **Bucketing** | Which of N **files** a row lands in, by hash of a key | Two tables bucketed the same way join **without a shuffle** |
| **Clustering / sorting** | The **order of rows within** files | Tightens min/max stats so file skipping works |
| **Compaction** | Rewrites many small files into fewer large ones | Fixes the small files problem |

### Clustering, since you asked specifically

Neither of your two guesses. **It is about the order of rows inside the files.**

```
  UNSORTED                              SORTED BY total_amount
  part-00000  total_amount 0 … 900      part-00000  total_amount 0 … 40
  part-00001  total_amount 0 … 900      part-00001  total_amount 40 … 900

  WHERE total_amount > 500              WHERE total_amount > 500
  → min/max prunes NOTHING              → part-00000 skipped without opening
```

Same data, same partitioning, same file count. The only difference is which rows went into
which file, and that is what makes the recorded min/max useful instead of useless.

- **Iceberg:** `WRITE ORDERED BY`, stored as a sort order on the table
- **Delta:** `ZORDER BY`, a space-filling curve so several columns prune reasonably well
  rather than one column perfectly
- **BigQuery / Snowflake:** they call it "clustering", same idea

So it operates at file level, but the mechanism is row ordering, and it only pays off if
the engine records per-file statistics. Sorting without stats buys nothing.

### Compaction

Yes, primarily the small files fix. It also does two other jobs: applying a new sort order,
and merging delete files in merge-on-read tables.

`CALL system.rewrite_data_files(...)` in Iceberg. `OPTIMIZE` in Delta.

---

## 7. Maintenance and retention

**You were right about the trade-off.** Precisely:

| Procedure | Deletes | Consequence |
|---|---|---|
| `expire_snapshots(older_than => X)` | Snapshot entries, **and data files no remaining snapshot references** | You can no longer time travel before X |
| `remove_orphan_files(older_than => X)` | Files nothing references at all, usually from failed commits | ⚠️ Use a conservative window. It can delete files a concurrent writer is still creating |
| `rewrite_manifests` | Nothing. Reorganises metadata | Faster planning after many small commits |

Iceberg's defaults: `history.expire.max-snapshot-age-ms` is 5 days,
`history.expire.min-snapshots-to-keep` is 1. **But expiry only happens when you run the
procedure.** Nothing is automatic. A table nobody maintains keeps every version forever and
the storage bill grows quietly.

The real decision is a policy: **how far back do you need to travel, and what is that
worth in storage?** 7 to 30 days is common. Regulated environments keep years and pay for
it.

---

## 8. Explaining atomic commits out loud

Three versions, escalating with how hard they push.

**The one-liner:**

> "A write never edits the table. It writes new files off to the side, invisibly, and then
> swaps one pointer to the new version. That swap is atomic, so a reader sees either the
> whole write or none of it."

**If they ask how the swap works:**

> "The catalog holds the current metadata location for the table. Committing is a
> compare-and-swap: I read that it's at version 3, and I ask the catalog to move it to
> version 4 **only if it's still 3**. If another writer got there first my swap fails, so I
> re-read their version and retry on top of it. That's optimistic concurrency. It's also
> why the catalog has to be something that can genuinely do an atomic compare-and-swap. A
> file-based catalog on object storage can't, which is why production uses a REST catalog
> or Glue."

**If they ask what it buys you:**

> "Four things. No reader ever sees a half-written table. A failed job leaves the table
> untouched instead of half-replaced. Concurrent writers are safe rather than a race. And
> every commit is a snapshot, so rollback is changing a pointer rather than restoring a
> backup. On plain Parquet an overwrite is a delete followed by a write, and anything
> reading in between sees a table that is missing data."

**And the summary sentence for the whole format:**

> "Parquet on object storage is a data lake. Adding Iceberg on top gives you atomic
> commits, snapshots and schema evolution, which is what makes it a lakehouse. Same files,
> different guarantees."

---

## 9. Architecture names

| Name | What it is | Worth your time? |
|---|---|---|
| **Kimball** | Dimensional modelling. Star schemas, facts and dimensions, bottom-up, optimised for query simplicity | ⭐ **Essential.** Your gold layer is Kimball |
| **Inmon** | Top-down. Build a normalised (3NF) enterprise warehouse first, derive marts from it | Know the name and the contrast with Kimball |
| **Data Vault** | Hubs, links and satellites. Built for auditability across many changing sources | Name recognition only, unless a job ad mentions it |
| **Medallion** | Databricks' name for bronze / silver / gold | ⭐ You are building it |
| **Lambda** | A batch layer and a speed layer, results merged at query time | Know it, and know it is largely obsolete |
| **Kappa** | Streaming only. Reprocess by replaying the log | Same |
| **Data mesh** | Domain teams own their data as products. Organisational, not technical | Know the idea. It is a people answer |
| **One Big Table** | Denormalised wide table instead of a star. Cheap on columnar engines | One sentence. Increasingly common |
| **Modern data stack** | Fivetran or Airbyte, plus Snowflake or BigQuery, plus dbt, plus a BI tool | Know the shape and where you would fit |

### Kimball vocabulary, since gold is next

| Term | Meaning |
|---|---|
| ⭐ **Grain** | **"What does one row in this table mean?"** The most important question in modelling, and the one people skip |
| **Fact table** | Measurements at a grain. Long and narrow. `fact_trip`, one row per trip |
| **Dimension table** | The things you slice by. Short and wide. `dim_zone`, `dim_date` |
| **Surrogate key** | A meaningless integer key you generate, so the dimension can change without breaking facts |
| **Natural / business key** | The real-world identifier, like `LocationID` |
| **Conformed dimension** | One `dim_zone` shared by every fact table, rather than each team building its own |
| **Degenerate dimension** | A dimension-like attribute with no dimension table, kept on the fact. An invoice number |
| **Junk dimension** | Several low-cardinality flags collapsed into one small dimension |
| **Factless fact table** | A fact table with no measures, recording that an event happened |
| **Additive / semi-additive / non-additive** | Whether a measure can be summed across every dimension. Revenue is additive, a bank balance is semi-additive (not across time), a ratio is non-additive |
| **Snowflaking** | Normalising a dimension into sub-tables. Usually avoided: it saves storage nobody needed and costs joins everyone pays |

**If you learn one thing from this table, learn grain.** "One row per trip" versus "one row
per trip per passenger" is a different table, a different set of legitimate aggregations,
and getting it wrong makes every number silently wrong.
