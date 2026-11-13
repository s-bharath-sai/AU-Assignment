"""
PostgreSQL connection and table definitions.

Start the database with `docker compose up -d` from the repo root.
Override the connection with DATABASE_URL.
"""
from __future__ import annotations

import os

import psycopg

DEFAULT_DATABASE_URL = "postgresql://au:au_local_only@localhost:5432/au_assignment"

DDL = """
-- Every input file we have fully processed. A file is ingested inside one
-- transaction together with its row here, so it is either fully in or not at
-- all. Re-ingesting the same file is a no-op; same name + different content
-- is an error (someone changed a file we already loaded).
CREATE TABLE IF NOT EXISTS ingested_files (
    file_key     TEXT PRIMARY KEY,          -- e.g. 'leads/motor_renewal/batch_001.csv'
    kind         TEXT NOT NULL CHECK (kind IN ('leads', 'events')),
    sha256       TEXT NOT NULL,
    drop_name    TEXT NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per lead identity (workflow_id, phone). No plaintext PII.
CREATE TABLE IF NOT EXISTS leads (
    lead_id        BIGSERIAL PRIMARY KEY,
    workflow_id    TEXT NOT NULL,
    phone_hash     TEXT NOT NULL,           -- HMAC of canonical phone: join key for events
    phone_last4    CHAR(4) NOT NULL,        -- enough for '+91******1234'
    external_ref   TEXT NOT NULL,           -- schema field marked "reference": true
    batch_id       TEXT NOT NULL,           -- file stem of the FIRST valid row; never changes
    attributes     JSONB NOT NULL DEFAULT '{}'::jsonb,  -- other non-PII fields, normalised; null = missing
    pii_encrypted  JSONB NOT NULL DEFAULT '{}'::jsonb,  -- field name -> Fernet token
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, phone_hash)
);

-- Rejected lead rows. Deliberately no PII: only where it was and why.
CREATE TABLE IF NOT EXISTS lead_rejections (
    workflow_id  TEXT NOT NULL,
    batch_id     TEXT NOT NULL,
    row_number   INTEGER NOT NULL CHECK (row_number >= 1),
    reason       TEXT NOT NULL,
    PRIMARY KEY (workflow_id, batch_id, row_number)
);

-- Per-file counters for data_quality.json. Summing per-file rows keeps the
-- totals correct under re-runs (a skipped file adds nothing).
CREATE TABLE IF NOT EXISTS lead_file_stats (
    workflow_id        TEXT NOT NULL,
    batch_id           TEXT NOT NULL,
    rows_total         INTEGER NOT NULL,
    rows_rejected      INTEGER NOT NULL,
    duplicates_merged  INTEGER NOT NULL,
    field_warnings     INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, batch_id)
);

-- Disposition catalogue, copied from dispositions.csv on every run so SQL
-- can join against it.
CREATE TABLE IF NOT EXISTS dispositions (
    workflow_id  TEXT NOT NULL,
    code         TEXT NOT NULL,
    is_terminal  BOOLEAN NOT NULL,
    is_success   BOOLEAN NOT NULL,
    PRIMARY KEY (workflow_id, code)
);

-- The interaction ledger. Append-only: every well-formed, non-duplicate event
-- is stored exactly once, INCLUDING orphans and unknown dispositions. Whether
-- an event links to a lead is decided at read time (see event_links), so an
-- orphan from drop_1 links automatically when its lead arrives in drop_2.
CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,     -- dedupe key across all drops
    event_type        TEXT NOT NULL,
    workflow_id       TEXT NOT NULL,
    call_id           TEXT,
    occurred_at       TIMESTAMPTZ NOT NULL, -- normalised from Z / +05:30 / naive-IST
    phone_hash        TEXT,                 -- HMAC of canonical phone; NULL if absent/invalid
    disposition_code  TEXT,                 -- for disposition.* events
    payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_file       TEXT NOT NULL,
    line_number       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_phone_idx ON events (workflow_id, phone_hash);
CREATE INDEX IF NOT EXISTS events_call_idx  ON events (workflow_id, call_id);

-- Enforce append-only at the database level, not just by convention.
CREATE OR REPLACE FUNCTION forbid_ledger_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events ledger is append-only (% not allowed)', TG_OP;
END
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();

-- Per-file event counters for data_quality.json.
CREATE TABLE IF NOT EXISTS event_file_stats (
    file_key           TEXT PRIMARY KEY,
    lines_total        INTEGER NOT NULL,   -- every non-empty line
    malformed          INTEGER NOT NULL,
    duplicates_dropped INTEGER NOT NULL
);

-- Which lead (if any) each stored event belongs to, computed from current state.
--   * normal events: (workflow_id, phone_hash) -> leads
--   * disposition.override: no phone; via call_id -> the lead of that call's
--     other (directly linked) events.
CREATE OR REPLACE VIEW event_links AS
WITH direct AS (
    SELECT e.event_id, e.workflow_id, e.call_id, l.lead_id
    FROM events e
    JOIN leads l ON l.workflow_id = e.workflow_id AND l.phone_hash = e.phone_hash
    WHERE e.event_type <> 'disposition.override'
),
call_lead AS (
    SELECT workflow_id, call_id, MIN(lead_id) AS lead_id
    FROM direct
    WHERE call_id IS NOT NULL
    GROUP BY workflow_id, call_id
)
SELECT e.event_id,
       CASE WHEN e.event_type = 'disposition.override' THEN cl.lead_id ELSE d.lead_id END AS lead_id
FROM events e
LEFT JOIN direct d     ON d.event_id = e.event_id
LEFT JOIN call_lead cl ON e.event_type = 'disposition.override'
                      AND cl.workflow_id = e.workflow_id AND cl.call_id = e.call_id;

-- Ledger with classification flags. Reports read ONLY usable events:
-- linked, and (for disposition.*) with a code in the catalogue.
CREATE OR REPLACE VIEW ledger_events AS
SELECT e.*,
       el.lead_id,
       (el.lead_id IS NULL) AS is_orphan,
       (el.lead_id IS NOT NULL
        AND e.event_type IN ('disposition.set', 'disposition.override')
        AND d.code IS NULL) AS is_unknown_disposition
FROM events e
JOIN event_links el USING (event_id)
LEFT JOIN dispositions d ON d.workflow_id = e.workflow_id AND d.code = e.disposition_code;

CREATE OR REPLACE VIEW usable_events AS
SELECT * FROM ledger_events WHERE NOT is_orphan AND NOT is_unknown_disposition;
"""

TABLES = ["ingested_files", "leads", "lead_rejections", "lead_file_stats",
          "dispositions", "events", "event_file_stats"]


def connect(url: str | None = None, search_path: str | None = None) -> psycopg.Connection:
    conn = psycopg.connect(url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    if search_path:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{search_path}"')
        conn.execute(f'SET search_path TO "{search_path}"')
        conn.commit()
    return conn


def init_db(conn: psycopg.Connection) -> None:
    from .facts import create_views  # derived-fact views sit on top of the tables above

    with conn.transaction():
        conn.execute(DDL)
    create_views(conn)


def sync_dispositions(conn: psycopg.Connection, catalogue) -> None:
    """Replace the dispositions table with the catalogue loaded from CSV."""
    with conn.transaction():
        conn.execute("DELETE FROM dispositions")
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO dispositions (workflow_id, code, is_terminal, is_success) VALUES (%s, %s, %s, %s)",
                [(d.workflow_id, d.code, d.is_terminal, d.is_success) for d in catalogue.values()],
            )


def reset_db(conn: psycopg.Connection) -> None:
    """Drop everything this pipeline owns. Dev/testing only."""
    with conn.transaction():
        for t in TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    init_db(conn)
