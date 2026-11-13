"""
Event ingestion: JSON Lines -> append-only ledger (the `events` table).

Per line, in spec order:
  1. malformed  -> counted, not stored
  2. duplicate  -> counted, not stored (event_id already in the ledger or earlier in this file)
  3. orphan / 4. unknown disposition -> STORED, but classified at read time by
     the ledger_events view. That is what lets an orphan from drop_1 link once
     its lead arrives in drop_2, without re-processing anything.

Each file is loaded in one transaction (COPY into a staging table, then one
INSERT ... ON CONFLICT DO NOTHING), together with its ingested_files row.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg

from .ingest_leads import already_ingested, file_sha256
from .parsers import parse_phone, parse_timestamp
from .pii import PiiVault

DISPOSITION_TYPES = {"disposition.set", "disposition.override"}


# ---------------------------------------------------------------- pure logic

@dataclass
class ParsedEvent:
    event_id: str
    event_type: str
    workflow_id: str
    call_id: str | None
    occurred_at: datetime
    phone_hash: str | None
    disposition_code: str | None
    payload: dict
    line_number: int


def _str_or_none(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def parse_event_line(line: str, line_number: int, vault: PiiVault) -> ParsedEvent | None:
    """Returns None if the line is malformed.

    Malformed (spec): not valid JSON, or no event_id. We also treat as
    malformed anything we cannot place in the ledger at all: not a JSON
    object, or missing/unparseable event_type, workflow_id or occurred_at.
    """
    try:
        obj = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    event_id = _str_or_none(obj.get("event_id"))
    event_type = _str_or_none(obj.get("event_type"))
    workflow_id = _str_or_none(obj.get("workflow_id"))
    occurred_at = parse_timestamp(obj.get("occurred_at")) if isinstance(obj.get("occurred_at"), str) else None
    if not (event_id and event_type and workflow_id and occurred_at):
        return None

    payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}

    raw_phone = obj.get("phone")
    phone = parse_phone(raw_phone) if isinstance(raw_phone, str) else None

    code = None
    if event_type in DISPOSITION_TYPES:
        code = _str_or_none(payload.get("disposition_code"))

    return ParsedEvent(
        event_id=event_id,
        event_type=event_type,
        workflow_id=workflow_id,
        call_id=_str_or_none(obj.get("call_id")),
        occurred_at=occurred_at,
        phone_hash=vault.phone_hash(phone) if phone else None,
        disposition_code=code,
        payload=payload,
        line_number=line_number,
    )


@dataclass
class EventFileStats:
    lines_total: int = 0
    malformed: int = 0
    duplicates_dropped: int = 0
    stored: int = 0
    skipped: bool = False


def read_event_file(path: Path, vault: PiiVault) -> tuple[list[ParsedEvent], int, int]:
    """Returns (parsed events, non-empty line count, malformed count)."""
    events: list[ParsedEvent] = []
    total = malformed = 0
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, start=1):
            if not line.strip():
                continue
            total += 1
            ev = parse_event_line(line, n, vault)
            if ev is None:
                malformed += 1
            else:
                events.append(ev)
    return events, total, malformed


# ---------------------------------------------------------------- database

STAGING_DDL = """
CREATE TEMP TABLE staging_events (
    event_id TEXT, event_type TEXT, workflow_id TEXT, call_id TEXT,
    occurred_at TIMESTAMPTZ, phone_hash TEXT, disposition_code TEXT,
    payload JSONB, source_file TEXT, line_number INTEGER
) ON COMMIT DROP
"""

# DISTINCT ON keeps the first occurrence within the file; ON CONFLICT drops
# event_ids already in the ledger from earlier files/drops.
INSERT_FROM_STAGING = """
INSERT INTO events (event_id, event_type, workflow_id, call_id, occurred_at, phone_hash,
                    disposition_code, payload, source_file, line_number)
SELECT DISTINCT ON (event_id)
       event_id, event_type, workflow_id, call_id, occurred_at, phone_hash,
       disposition_code, payload, source_file, line_number
FROM staging_events
ORDER BY event_id, line_number
ON CONFLICT (event_id) DO NOTHING
"""


def ingest_event_file(conn: psycopg.Connection, vault: PiiVault, path: Path, drop_name: str) -> EventFileStats:
    path = Path(path)
    file_key = f"events/{path.name}"
    sha = file_sha256(path)
    stats = EventFileStats()

    with conn.transaction():
        if already_ingested(conn, file_key, sha):
            stats.skipped = True
            return stats

        events, stats.lines_total, stats.malformed = read_event_file(path, vault)

        conn.execute(STAGING_DDL)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY staging_events (event_id, event_type, workflow_id, call_id, occurred_at, "
                "phone_hash, disposition_code, payload, source_file, line_number) FROM STDIN"
            ) as copy:
                for e in events:
                    copy.write_row((
                        e.event_id, e.event_type, e.workflow_id, e.call_id, e.occurred_at,
                        e.phone_hash, e.disposition_code, json.dumps(e.payload, ensure_ascii=False),
                        file_key, e.line_number,
                    ))
            cur.execute(INSERT_FROM_STAGING)
            stats.stored = cur.rowcount

        stats.duplicates_dropped = len(events) - stats.stored

        conn.execute(
            "INSERT INTO event_file_stats (file_key, lines_total, malformed, duplicates_dropped) "
            "VALUES (%s, %s, %s, %s)",
            (file_key, stats.lines_total, stats.malformed, stats.duplicates_dropped),
        )
        conn.execute(
            "INSERT INTO ingested_files (file_key, kind, sha256, drop_name) VALUES (%s, 'events', %s, %s)",
            (file_key, sha, drop_name),
        )
    return stats


def event_files(drop_dir: Path) -> list[Path]:
    events_dir = Path(drop_dir) / "events"
    if not events_dir.is_dir():
        return []
    return sorted(events_dir.glob("*.jsonl"))


def ingest_events(conn: psycopg.Connection, vault: PiiVault, drop_dir: Path, log=print) -> None:
    drop_name = Path(drop_dir).name
    for path in event_files(drop_dir):
        s = ingest_event_file(conn, vault, path, drop_name)
        if s.skipped:
            log(f"  skip  events/{path.name} (already ingested)")
        else:
            log(f"  events {path.name}: lines={s.lines_total} stored={s.stored} "
                f"malformed={s.malformed} duplicates={s.duplicates_dropped}")
