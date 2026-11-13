# Assignment A: Interaction Ledger & Reporting Pipeline

Python 3.11+ · PostgreSQL 16 (docker) · psycopg 3 · cryptography (Fernet)

## Setup (from a clean clone, at the repo root)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d              # Postgres on localhost:5432 (au / au_local_only / au_assignment)
docker compose ps                 # wait until the container is "running"
```

Port 5432 must be free. To use another Postgres, set
`DATABASE_URL=postgresql://user:pass@host:port/db`.

## Run

```bash
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

python solution/main.py ingest data/drop_1
python solution/main.py ingest data/drop_2
python solution/main.py report --out reports/
```

| Command | What it does |
|---|---|
| `ingest <drop_dir> [<drop_dir> ...]` | Ingest one or more drops, in the order given. Re-running is a no-op. |
| `report --out <dir>` | Writes `batch_summary.csv`, `daily_overview.csv`, `lead_report.csv`, `rejections.csv`, `data_quality.json` |
| `reset` | Drops and recreates all pipeline tables. **Run this before ingesting a different seed.** |

Ingesting a different seed into the same database without `reset` stops with
`error: ... was already ingested with different content`. That guard is on
purpose: it keeps two datasets from mixing.

### Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://au:au_local_only@localhost:5432/au_assignment` | Postgres connection |
| `AU_PII_SECRET` | a dev-only constant | Key for phone HMAC + Fernet encryption. Must stay the same across ingests of one database. |

## Test

```bash
python -m pytest solution -q                          # 148 tests; DB tests need docker Postgres
solution/scripts/verify_idempotency.sh data           # step-by-step idempotency/incremental check (resets the DB)
```

Each database test runs in its own throwaway Postgres schema, so tests never
touch your pipeline data. They're skipped if Postgres isn't running.

## Layout

```
solution/
  main.py                  entry point (python solution/main.py ...)
  pipeline/
    cli.py                 argument parsing
    parsers.py             phone / date / decimal / enum / timestamp / header normalisation
    schema.py              loads workflow schemas + disposition catalogue (read-only inputs)
    pii.py                 HMAC phone hash + Fernet encryption
    db.py                  tables, append-only trigger, ledger views
    ingest_leads.py        CSV -> validated, merged leads (+ rejections)
    ingest_events.py       JSONL -> append-only events ledger
    facts.py               call_facts / lead_facts views (effective disposition, buckets)
    report.py              the five output files
  scripts/verify_idempotency.sh
  tests/
```
