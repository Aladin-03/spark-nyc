# How to build a layer

> Parent: [`plan.md`](plan.md) · Applied: [`bronze.md`](bronze.md) · Vocabulary: [`concepts.md`](concepts.md)
>
> The reusable order. Bronze is the worked example, but nothing here is specific to
> bronze: silver and gold follow the same ten steps with different content in each.

---

## The shape of it

```
   0  FRAME        what must be true when this is finished
   1  CONTRACT     what will I accept?
   2  ROW SHAPE    what does one row become?
   3  DESTINATION  declare the table
   4  INGEST       read → check → enrich → commit
   5  RUN          twice
   6  VERIFY       does the data say what I think it says?
   7  OBSERVE      what happened, without reading the data
   8  RECOVER      practise going backwards
   9  MAINTAIN     retention and file sizing
  10  ON DISK      look at what you actually made
```

## Why this order and not another

Three principles govern it. If you remember only these, you can rebuild the list.

**1. Decide the expensive things first.**

```
   steps 0-3   DECISIONS    changing them later rewrites data
   steps 4-5   CODE         changing them re-runs a job
   steps 6-10  OBSERVATION  changing them costs nothing
```

Partitioning is step 3 and not step 8 because changing a partition scheme rewrites every
file you have ever written. A logging statement is free to add on a Tuesday. **Order the
work by the cost of being wrong.**

**2. Each step produces the input the next one needs.**

You cannot write a contract until you know what must be true. You cannot shape a row
until you know what shape you accept. You cannot declare a table until you know the row
shape. You cannot verify until something ran. You cannot choose a retention policy until
you have watched history grow. The list is not a preference, it is a dependency chain.

**3. Failure gets cheaper the earlier you catch it.**

Rejecting a file costs zero bytes. Accepting a bad one costs a reprocess of every layer
downstream, plus however long it takes someone to notice. That asymmetry is why the
check sits before the write, inside step 4, and not after it.

---

## 0. Frame

**Name the properties that must be true when this layer is finished, and a test for each.**

### Why it is first

Every later step is a means to one of these ends. If you cannot say which property a
piece of work serves, the work is decoration. This is also the only definition of "done"
you will get; without it you stop when you get bored.

### What it produces

Three to five named properties, each with a test somebody else could run.

| Layer | Properties |
|---|---|
| **Bronze** | Faithful, idempotent, observable, atomic |
| **Silver** | Correct, **complete** (nothing silently dropped), conformed (one meaning per column), reproducible |
| **Gold** | Fast, **stable** (a number does not move unless the data moved), interpretable (the grain is stated) |

### If you skip it

You build features nobody needed and cannot tell when to stop. The classic symptom is a
layer that keeps growing because every new idea seems as justified as the last.

---

## 1. Contract

**Decide what shape of input you accept, in code, before anything is written.**

### Why it is here

It is the only step that can save you from doing all the others. A file that breaks the
contract should cost you nothing at all.

And it must come **before** the first write, because a contract enforced after writing is
not a contract, it is a report. By then the bad data is in your table and the question
has changed from "do I accept this" to "how do I get it out".

### The cost asymmetry, which is the whole argument

```
   REJECT a good file    →  someone is annoyed for an hour
   ACCEPT a bad file     →  wrong numbers, silently, in every layer downstream,
                            until a human happens to notice
```

Those are not comparable, so the policy is not "be careful", it is **fail loudly by
default and accept only what provably cannot corrupt anything.**

### What it produces

An expected shape held in code, plus a check that raises with a message naming the
columns and the types. `ValueError("schema mismatch")` is not a check, it is a shrug.

### The unit changes per layer, and that is the interesting part

| Layer | The contract is about | Rejecting means |
|---|---|---|
| **Bronze** | The **file's** structure | Fail the whole job. There is nothing to salvage from a file with the wrong columns |
| **Silver** | Each **row's** values | Quarantine that row, keep the rest, and alert on the reject rate |
| **Gold** | Silver's **grain and completeness** | Assert, do not filter. If silver is wrong, gold must not paper over it |

Notice the granularity gets finer as you go down. **Bronze can only crash; silver can
choose.** That is not a limitation of bronze, it is the reason silver exists.

### If you skip it

Bad data becomes NULL, NULL looks like a legitimate value, and you find out from a
dashboard three weeks later. This is the single most common way data pipelines fail, and
it never announces itself.

---

## 2. Row shape

**Decide what one row becomes. Write it as a function.**

### Why it is here

After the contract, because you can only safely add to something whose shape you trust.
Before the destination, because the destination's schema **is** the row shape, so
declaring the table first would be guessing.

### What it produces

A pure function: DataFrame in, DataFrame out, no writing, no side effects. Keeping it
separate from the write is what makes it testable and what lets you look at the result
before committing to it.

### The word that matters here is grain

**"What does one row mean?"** Answer it explicitly at every layer, because it changes,
and because almost every modelling mistake is a grain mistake.

| Layer | Grain | Transformation |
|---|---|---|
| **Bronze** | One source row | Add provenance. Change nothing else |
| **Silver** | One trip | Cast, rename, join dimensions, derive, reject |
| **Gold** | One zone per day | **Aggregate.** The grain changes, which is the point of the layer |

### If you skip it

Enrichment gets tangled into the write call, so you cannot test it without writing, and
you cannot see the result without a round trip through storage.

---

## 3. Destination

**Declare the table. Schema and partition spec, once.**

### Why it is here

Before ingest, because ingest writes into it. After row shape, because the schema is the
row shape.

And there is a subtler reason to declare it early rather than let the first write create
it: **create the table empty, and its history covers the table's entire life.** Bolt the
table format on in month four and the snapshot log starts in month four, which is exactly
when you no longer need it.

### What it produces

```sql
CREATE TABLE … USING iceberg PARTITIONED BY (year, month)
```

Two decisions live here, and they are the most expensive decisions in the layer:

| Decision | Chosen by | Changing it later costs |
|---|---|---|
| **Partition column** | Match the **reprocessing unit**, not the query pattern | Rewriting every file |
| **Sort order** | The high-cardinality column you filter on | Rewriting every file |

"Match the reprocessing unit" is worth holding on to. Bronze partitions by month because
one source file is one month, so "reload March" is one directory. If you partition by
something that does not line up with how you reload, every reload is a full rebuild.

### If you skip it

The schema becomes whatever the first write happened to produce, and partitioning turns
from a decision into a migration.

---

## 4. Ingest

**One function. Read, check, enrich, commit, in that order.**

```
   read  ─►  CHECK  ─►  enrich  ─►  commit
             ^^^^^
             the gate goes before the expensive, hard-to-undo act
```

### Why that internal order

Purely cost. Reading a schema touches a footer. The check is arithmetic on two sets.
Enrichment is lazy and therefore free until something forces it. **The commit is the only
expensive and durable step, so everything that can reject the work belongs before it.**

### The granularity rule

**One function per reprocessable unit.** `land_month(year, month)`, not
`land_everything()`.

Your retry granularity is exactly your function granularity. Write one function that does
all twelve months and a failure in month seven means redoing all twelve, forever.

### If you skip it

The logic lives in notebook cells, which means it cannot be scheduled, cannot be retried
and cannot be tested. A layer that only runs when a human clicks Run is not a pipeline.

---

## 5. Run it twice

**Once proves it works. Twice proves it is idempotent.**

### Why it is a step and not an afterthought

There is no unit test for "the second run did not duplicate anything". Mocks will not
catch it, type checkers will not catch it, and code review usually will not either,
because `mode("append")` looks completely reasonable on the page.

The only instrument that detects it is running the thing twice and comparing a number.

### If you skip it

You discover it during your first backfill, which is also the first time anyone is
watching, and the damage is already distributed across every downstream layer.

---

## 6. Verify

**Ask the data whether it says what you think it says.**

### Why here

After running, obviously. But specifically **before** observing, because verify checks the
data and observe checks the metadata. If the data is wrong, the metadata is irrelevant.

### Three standing questions, at every layer

| Question | Bronze form |
|---|---|
| Did the count survive? | Bronze rows equals source rows, exactly |
| Can every row be traced? | Every row has a source file and a batch id |
| Does one run look like one run? | One batch id spans exactly the months that run touched |

### If you skip it

You end up trusting a green job status. A job status tells you the code did not throw. It
tells you nothing whatsoever about whether the numbers are right.

---

## 7. Observe

**Learn what happened without reading the data.**

### Why here

You can only look at history once there is history, and one snapshot is not a history. It
comes after step 5 for a real reason, not just sequencing: **the second run is what makes
the metadata interesting**, because it is the first time you can see a replacement rather
than a creation.

### What it produces

The ability to answer "what did the last run actually do" from metadata alone: rows added,
rows deleted, files written, which batch, when. On plain files that question needs a
directory walk and a scan. This is most of what a table format buys you.

---

## 8. Recover

**Practise going backwards.**

### Why it is a step at all

Because **the first time you use rollback must not be during an incident.** Everything
about recovery is easy to read about and unfamiliar under pressure: which snapshot, what
the syntax is, whether it is reversible, what it does to readers mid-query.

It comes after observe because you need snapshot ids to roll back to, and observing is
where you get them.

### What it replaces

Deleting directories and re-running, which is what you do without it, and which cannot be
undone if you delete the wrong one.

---

## 9. Maintain

**Retention, compaction, and the fact that none of it is automatic.**

### Why it is this late

**You cannot choose a retention policy before you have watched history grow.** Picking
"seven days" before you know whether a day produces three snapshots or three thousand is
guessing dressed as a decision.

### The other half: it is a different schedule

Maintenance does not belong in the ingest job. Expiring snapshots and compacting files
are weekly operations on the table, not per-run operations inside it. Putting them in the
ingest path makes every run slower and couples two things that fail for different reasons.

### If you skip it

Nothing breaks, which is why it gets skipped. The storage bill grows quietly and the
small-file problem arrives six months later looking like a performance mystery.

---

## 10. On disk

**Look at what you actually made.**

### Why it is last, and why it is not optional

It is confirmation rather than construction, so it cannot come earlier. But skipping it
leaves you with a belief instead of knowledge.

Reading a real `metadata.json` with your own data in it, and finding that
`version-hint.text` contains nothing but the number `10`, teaches more about why
production needs a real catalog than any amount of reading. **An abstraction whose
implementation you have never seen is something you are trusting, not something you
understand.**

---

## The same method, three layers

| Step | Bronze | Silver | Gold |
|---|---|---|---|
| **0 Frame** | Faithful, idempotent, observable, atomic | Correct, complete, conformed, reproducible | Fast, stable, interpretable |
| **1 Contract** | File schema. Fail the job | Row values. Quarantine and alert | Silver's grain. Assert |
| **2 Row shape** | Add provenance only | Cast, join, derive, reject | Aggregate. Grain changes |
| **3 Destination** | Partition by ingest month | Partition by **event** date | Partition by the reporting period |
| **4 Ingest** | `land_month` | `conform_month` | `build_<table>` |
| **5 Run twice** | Row count unchanged | Row count and reject count unchanged | Every number unchanged |
| **6 Verify** | Count matches source | Kept plus rejected equals input | Totals reconcile to silver |
| **7 Observe** | Snapshots per batch | Reject rate over time | Which silver snapshot fed this |
| **8 Recover** | Roll back a bad ingest | Roll back a bad rule | Rebuild from silver |
| **9 Maintain** | Expire snapshots | Expire, compact | Usually just rebuild |
| **10 On disk** | Metadata tree | Reject files | File sizes vs query time |

The row that changes most is **3**, and it is the most important one in the whole table.
**Bronze partitions by when we received it. Silver partitions by when it happened.** The
re-mapping between those two is exactly where late-arriving data is handled, and it is the
reason silver's write has to be a merge rather than a blind overwrite.

---

## Doing it out of order: the symptoms

| Symptom | Step you skipped |
|---|---|
| You keep adding things and cannot tell when the layer is done | 0 |
| You discover the schema rules by debugging production | 1 |
| The transformation can only be tested by writing to disk | 2 |
| Repartitioning means rewriting a year of data | 3 |
| A failure in month seven forces you to redo all twelve | 4 |
| A backfill doubles every number | 5 |
| You trust a green job status | 6 |
| Answering "what did last night's run do" needs a directory walk | 7 |
| Your first rollback happens during an incident | 8 |
| The storage bill grows and nobody knows why | 9 |
| You can describe the format but have never opened one | 10 |

---

## The compressed version

Worth memorising, because it is the same ten lines every time:

```
  0   name what must be true, and how you would test it
  1   decide what you accept, in code, before you write
  2   decide what one row becomes, as a function
  3   declare the table: schema and partitioning, once
  4   read → check → enrich → commit, one function per reload unit
  5   run it twice
  6   ask the data whether it says what you think
  7   ask the metadata what happened
  8   practise going backwards
  9   decide retention only after you have seen history grow
  10  open the files and look
```

Steps 0 to 3 are decisions. Steps 4 and 5 are code. Steps 6 to 10 are looking.
**Most people start at 4, and everything that goes wrong later was decided by that.**
