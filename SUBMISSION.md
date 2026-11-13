# Submission

## Candidate
- **Name:** Bharath Sai
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** 5-8hours

## How to run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d                       # Postgres 16 on :5432; wait for it to be running

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python solution/main.py reset              # empty DB (required before switching seeds)
python solution/main.py ingest data/drop_1
python solution/main.py ingest data/drop_2
python solution/main.py report --out reports/

python -m pytest solution -q               # 148 tests
solution/scripts/verify_idempotency.sh data   # incremental == all-at-once, re-run == no-op
```
More detail is in [`solution/README.md`](solution/README.md).

## Design

```
 leads/*.csv ──► parse + validate ──► merge in memory ──► leads            (one row per workflow+phone)
                  (schema order)        (per file)     └► lead_rejections  (no PII)
                                                        └► lead_file_stats  (counters)
 events/*.jsonl ► parse ─ malformed? ─ duplicate? ─────► events            (append-only ledger,
                          (counted)    (counted)                            incl. orphans)
                                                        └► event_file_stats

           ingested_files (file_key, sha256): every file is ingested in ONE transaction
           together with its row here → re-ingest is a no-op, a crash leaves nothing half-done.

 read time (SQL views, recomputed from current state):
   event_links ─► ledger_events (is_orphan, is_unknown_disposition) ─► usable_events
                                                                        │
                                                       call_facts ◄─────┘  (one row per call)
                                                       lead_facts ◄─ call_facts + leads (bucket…)
                                                       report.py  ◄─ lead_facts / call_facts
```

**Database (PostgreSQL).** I chose Postgres over SQLite to use JSONB, `COPY`, `ON CONFLICT`,
`DISTINCT ON`/`FILTER` aggregates and a trigger to enforce the append-only ledger. Tables:

| Table | Key / constraints | Purpose |
|---|---|---|
| `ingested_files` | PK `file_key`, stores `sha256` | Idempotency + detects a changed file with the same name |
| `leads` | `UNIQUE (workflow_id, phone_hash)` | One row per lead identity. `batch_id` is fixed by the first valid row. Non-PII fields go in `attributes` (JSONB); PII goes in `pii_encrypted` |
| `lead_rejections` | PK `(workflow_id, batch_id, row_number)` | Rejected rows: location + reason only, no PII |
| `lead_file_stats`, `event_file_stats` | PK per file | Counters for `data_quality.json`. Summing per-file rows can't double count on re-runs |
| `dispositions` | PK `(workflow_id, code)` | Catalogue copied from CSV so SQL can join it |
| `events` | PK `event_id`; trigger blocks UPDATE/DELETE | **The interaction ledger.** Append-only and enforced by the database. Stores orphans and unknown dispositions too |

**Key idea: link events to leads at read time, not at write time.** The ledger stores every
well-formed, non-duplicate event with an HMAC of its phone. The views `event_links` →
`ledger_events` join events to leads on `(workflow_id, phone_hash)`, and join overrides through
`call_id` to the lead of that call's other events. So:
- an orphan from `drop_1` links automatically when its lead arrives in `drop_2`, with no
  re-processing and no UPDATE on the ledger;
- the orphan and unknown-disposition counts always reflect the final state;
- drop order can't change the results: incremental, all-at-once and even "all events before any
  leads" produce byte-identical reports (tested).

The trade-off is that report queries recompute the link joins each time. That costs about 14 s
for 630k events (see Results). The alternative, storing `lead_id` on each event, would need
UPDATEs to the ledger when late leads arrive, or a side table to keep in sync.

**PII at rest.**
- **Phone:** an HMAC-SHA256 with a secret key is the join key. It's deterministic, so events can
  match leads. A plain SHA-256 isn't enough here: there are only about 4 billion Indian mobile
  numbers, so it could be brute-forced, and the key prevents that.
- **Full phone and name:** Fernet-encrypted (AES-CBC + HMAC), so they're still recoverable with
  the key.
- **Last 4 digits:** stored in clear, which is all `phone_masked` needs.
- **Events:** store only the phone hash, never the raw number.

Hash and encryption keys are derived separately from one secret, `AU_PII_SECRET`. A test
confirms the name and phone never appear in plaintext in `leads`, and no 10-digit number appears
in any report.

**Derived facts in SQL** (`facts.py`). `call_facts` makes one pass over `usable_events` grouped
by call:
- a call exists only if it has a linked `call.initiated`;
- `connected` and `telephony_failed` are `BOOL_OR`s;
- the effective disposition is the first element of an `ARRAY_AGG` ordered with overrides first,
  then `occurred_at DESC`, then `event_id DESC`;
- the call date is `initiated_at AT TIME ZONE 'Asia/Kolkata'`.

`lead_facts` makes one pass over `call_facts` grouped by lead and assigns the bucket with a
`CASE` in spec order. Python only formats the output.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Postgres, raw SQL via psycopg (no ORM) | Logic is set-based aggregation plus constraint-driven idempotency (`UNIQUE` + `ON CONFLICT`), which is clearer and easier to defend in SQL | SQLite (zero setup); SQLAlchemy |
| Link events to leads at read time (views) | Late leads adopt earlier orphans with no reprocessing, and the ledger stays strictly append-only | Store `lead_id` on events and back-fill when leads arrive |
| One transaction per file, plus `ingested_files(file_key, sha256)` | Re-ingest is a no-op; a crash mid-file leaves nothing behind | Per-row idempotency keys |
| Same file name with **different** content → error | Silently mixing two versions of `batch_001` would corrupt merges; `reset` is explicit | Ingest it as a new version |
| Header normalisation trims again **after** collapsing `_`/whitespace | `_phone_` would otherwise become `" phone "` and match nothing | Follow the spec order literally |
| Two columns mapping to the same field → leftmost wins | Deterministic. Doesn't occur in the data | Prefer the first non-empty value |
| Dates: only the 4 spec formats, strict 2-digit day and month, no `dateutil` | `dateutil` guesses month-first and accepts formats the spec doesn't, which would change warning counts | Lenient parsing |
| Decimal: `Decimal` + ROUND_HALF_UP; `1,00,000` accepted (commas just removed) | Floats round `2.675` to `2.67`. Indian grouping is common in this domain | Strict 3-digit grouping |
| Missing vs invalid: empty → `missing_required:<f>`; present but bad phone → `invalid_phone` | Spec | |
| A required **non-phone** field that's present but unparseable → `invalid:<field>` | The spec only defines `invalid_phone`. Can't occur with the given schemas (required non-phone fields are strings) | Treat it as a warning |
| Defaults (`preferred_language → en`) aren't stored; nulls stay null in `attributes` | Merge rules stay simple ("overwrite non-null only"), and defaults apply after merging by definition. No report outputs these fields | Write defaults at the end of each ingest |
| Malformed events also include: not a JSON object, or missing/unparseable `event_type`, `workflow_id`, `occurred_at` | Such a line can't be placed in the ledger. None in the generated data | Store with NULLs |
| `events_total` counts non-empty lines; whitespace-only lines are ignored | Spec says "every non-empty line" | |
| Duplicate `event_id`: the first occurrence wins (earliest file, then earliest line) | Deterministic | Compare content |
| Override → lead: via the lead of the call's directly-linked events. If they disagreed, the lowest `lead_id` wins | Spec. Never happens in the data | Treat as orphan |
| Unknown disposition is counted only for **linked** events (orphan check comes first) | Spec order: orphan (3) before unknown (4) | |
| Disposition ties on equal `occurred_at` → higher `event_id` is later | Deterministic | |
| A `call_id` initiated twice → the earliest `call.initiated` counts | Deterministic. Doesn't occur | |
| `batch_summary` lists every ingested batch, even one with 0 valid leads | "How is Monday's upload doing?" should still show an all-rejected upload | Only batches with leads |
| `connectivity_rate` computed exactly with `Decimal`, half-up, 4 places | No float rounding surprises | `f"{x:.4f}"` |
| `last_call_at` printed to whole seconds, IST offset | Matches the README example format | |
| All 5 report files read from one REPEATABLE READ snapshot | They always agree with each other, even during a concurrent ingest | |
| Report session disables parallel query (`max_parallel_workers_per_gather = 0`) | Docker's default 64 MB `/dev/shm` crashed parallel hash joins on the large dataset. `docker-compose.yml` is a provided file, so I fixed it on our side | Add `shm_size` to compose |
| Sort with `COLLATE "C"` | Byte order, independent of the DB locale | |

## Failure modes considered

- **Re-running a drop:** every file is skipped by `ingested_files`. Verified: all six tables have
  identical row counts and content hashes before and after.
- **Crash mid-file:** the file's rows, stats and `ingested_files` row commit in one transaction,
  so on restart the file is simply ingested again.
- **Changed file with the same name:** detected by sha256; the pipeline refuses with an error.
- **Events before their lead:** stored, then linked automatically at read time (orphans
  1123 → 462 after `drop_2`).
- **Duplicates across drops:** `events.event_id` is the primary key; `ON CONFLICT DO NOTHING`
  drops them and the difference is counted.
- **Accidental ledger edits:** a database trigger rejects UPDATE/DELETE on `events`.
- **Bad input:**
  - truncated JSON, blank rows and BOM-prefixed CSVs are handled;
  - unknown columns are ignored;
  - impossible dates (`31/02`) and junk amounts become field warnings;
  - leads with a bad phone are rejected, and their events become orphans.
- **Two ingests at once:** lead rows are locked (`FOR UPDATE`) and `ingested_files` has a primary
  key, so the second writer of the same file fails and rolls back instead of double counting.
  Not stress-tested.
- **Not handled:**
  - Rotating `AU_PII_SECRET` makes old hashes unmatchable; there's no re-keying tool.
  - A JSON `phone` given as a number instead of a string is treated as missing, so the event
    becomes an orphan.
  - Non-UTF-8 CSVs fail loudly.
  - Report time grows with the size of the ledger, because reports are fully recomputed.

## Testing

148 tests in `solution/tests/` (`python -m pytest solution -q`, ~4 s):

- **Parsers (90):**
  - all six phone formats, and every invalid phone shape in the generator;
  - all four date formats, impossible dates, and day-first ordering;
  - currency prefixes and half-up rounding;
  - enum case handling;
  - all three timestamp styles, including the IST midnight crossing.
- **Lead ingest (16):**
  - alias headers;
  - rejection reason follows schema order;
  - missing vs invalid;
  - warnings only for optional fields;
  - merge overwrites only non-null fields, and `batch_id` stays the first batch;
  - blank rows aren't numbered;
  - re-ingest is a no-op, and a changed file is refused;
  - no plaintext PII.
- **Event ingest (15):**
  - malformed lines;
  - duplicates within one file and across drops;
  - a drop_1 orphan adopted after drop_2;
  - override linked through `call_id`;
  - unknown dispositions counted only for linked events;
  - append-only trigger.
- **Derived facts (16):**
  - a call without `call.initiated` isn't counted;
  - IST date;
  - override beats a later set, and the latest override wins regardless of file order;
  - an unknown override falls back to the set;
  - the override doesn't mutate the ledger;
  - every bucket and its precedence;
  - an override can move a lead out of `completed`;
  - `current_disposition` skips calls with no disposition.
- **Reports (11):**
  - 4-decimal rate, IST ISO timestamps, masking;
  - `success_leads` counted once per lead per date;
  - **end-to-end on an unseen seed (1234):** incremental + re-run vs "all events first, then
    leads" give byte-identical reports.

Checked outside the test suite:
- **Independent re-implementations:** I rebuilt the data-quality counts and every lead's derived
  facts in plain Python straight from the files. They match the SQL with 0 differences across
  1892 leads.
- **`verify_idempotency.sh`** passes on seeds 42 and 7.
- **README examples:** `connectivity_rate` `0.5625` and `last_call_at`
  `2026-10-14T17:55:24+05:30` both appear in the seed-42 output.

Not tested:
- concurrent ingests;
- non-UTF-8 input;
- schemas with types beyond the five in the spec.

## Results

Apple M3 Pro, Postgres 16 in Docker:

| Dataset | ingest drop_1 | ingest drop_2 | report | total |
|---|---|---|---|---|
| small (2k leads, 8.7k events) | 1.1 s | 0.8 s | 0.4 s | ~2.3 s |
| **large** (144k lead rows, 633k events) | 12.0 s | 11.4 s | 14.4 s | **37.9 s** |

Seed 42 small: 1892 leads, 108 rejections (42 `invalid_phone`), 57 merged duplicates,
122 field warnings, 8745 event lines, 8 malformed, 237 duplicates, 462 orphans,
18 unknown dispositions.

What I found while getting the large dataset under 2 minutes:
1. **The report crashed on the large data** with `could not resize shared memory segment`.
   Parallel hash joins exceeded Docker's 64 MB `/dev/shm`. Fixed by disabling parallel workers for
   the report session.
2. **The first version of the views scanned the ledger 3× per call and 2× per lead** (24 s). I
   rewrote them as single-pass `GROUP BY`s with `FILTER` and `ARRAY_AGG ... ORDER BY` (14 s), and
   the output stayed byte-identical.
3. **Row-by-row lead upserts took about 17 s per file.** Validating and merging each file in
   memory, then doing one `COPY` and one `INSERT ... ON CONFLICT DO UPDATE`, brought total ingest
   from 111 s to 23 s, again with byte-identical output.

## Stretch goals done
- **Performance:** the large dataset runs end to end in 38 s.
- **Aggregation in SQL:** all derived facts and report aggregates are SQL views and queries.
- **Incremental processing:** drop 2 doesn't re-read or re-process drop 1's files. The trade-off
  is that reports are always recomputed from the whole ledger, which is what lets late leads
  adopt earlier orphans for free. At larger scale I'd materialise `call_facts` incrementally,
  keyed by call, and refresh only calls touched by new events or new leads.

## Production at 100k leads/day (short note)
- **Scheduling:** run ingest per drop as it lands (object-storage event → queue → worker), and
  run reports on a schedule after the expected drops. Ingest is already safe to retry.
- **Failures and backfills:** each file is atomic and keyed by sha256, so a retry or backfill is
  just "ingest the files again". A corrected re-upload needs a new file name, or an explicit
  supersede flow. A dead-letter table would hold malformed lines with the file and line number
  for operations to inspect.
- **Scale:**
  - partition `events` by month;
  - turn `call_facts`/`lead_facts` into incrementally maintained tables;
  - keep `AU_PII_SECRET` in a KMS or secrets manager, with key versioning so it can be rotated.
- **Observability:** per-file metrics (rows, rejects, orphans, duration) and alerts when the
  orphan rate or rejection rate jumps compared with the usual baseline.

## If I had another day
1. Persist an incremental `call_facts` table so report time doesn't grow with ledger size.
2. A re-keying command for PII secret rotation.
3. Accept numeric JSON phones, and quarantine malformed event lines in a table for inspection.
4. Property-based tests (Hypothesis) for the parsers.
5. A concurrency test with two ingest processes running at once.
