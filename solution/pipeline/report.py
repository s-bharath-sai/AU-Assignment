"""
Report writers: the five files in the output contract.

Aggregation happens in SQL (over the call_facts / lead_facts views); Python
only formats values. Sorting uses COLLATE "C" (byte order) so the output order
doesn't depend on the database locale.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import psycopg

from .parsers import IST

BUCKETS = ["yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"]


# ---------------------------------------------------------------- formatting

def fmt_bool(v: bool) -> str:
    return "true" if v else "false"


def fmt_ts_ist(ts: datetime | None) -> str:
    """ISO-8601 in IST, e.g. 2026-10-14T17:55:24+05:30. Empty if None."""
    return "" if ts is None else ts.astimezone(IST).isoformat(timespec="seconds")


def fmt_rate(numerator: int, denominator: int) -> str:
    """Exactly 4 decimals, half-up, computed exactly (no float)."""
    if denominator == 0:
        return "0.0000"
    q = Decimal(numerator) / Decimal(denominator)
    return str(q.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def mask_last4(last4: str) -> str:
    return "+91******" + last4


def _write_csv(path: Path, header: list[str], rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


# ---------------------------------------------------------------- 1. batch_summary.csv

BATCH_SUMMARY_SQL = """
SELECT s.workflow_id, s.batch_id,
       COUNT(lf.lead_id)                                                AS total_leads,
       COUNT(*) FILTER (WHERE lf.bucket = 'yet_to_reach_out')             AS yet_to_reach_out,
       COUNT(*) FILTER (WHERE lf.bucket = 'in_progress_telephony_failed') AS in_progress_telephony_failed,
       COUNT(*) FILTER (WHERE lf.bucket = 'in_progress_dialed')           AS in_progress_dialed,
       COUNT(*) FILTER (WHERE lf.bucket = 'completed')                    AS completed
FROM lead_file_stats s                       -- every ingested batch, even if all rows were rejected
LEFT JOIN lead_facts lf ON lf.workflow_id = s.workflow_id AND lf.batch_id = s.batch_id
GROUP BY s.workflow_id, s.batch_id
ORDER BY s.workflow_id COLLATE "C", s.batch_id COLLATE "C"
"""


def write_batch_summary(conn: psycopg.Connection, out_dir: Path) -> None:
    rows = conn.execute(BATCH_SUMMARY_SQL).fetchall()
    _write_csv(out_dir / "batch_summary.csv",
               ["workflow_id", "batch_id", "total_leads", *BUCKETS], rows)


# ---------------------------------------------------------------- 2. daily_overview.csv

DAILY_OVERVIEW_SQL = """
SELECT workflow_id,
       call_date,
       COUNT(*)                                        AS total_calls,
       COUNT(*) FILTER (WHERE connected)               AS connected_calls,
       COUNT(DISTINCT lead_id) FILTER (WHERE is_success) AS success_leads
FROM call_facts
GROUP BY workflow_id, call_date
ORDER BY workflow_id COLLATE "C", call_date
"""


def write_daily_overview(conn: psycopg.Connection, out_dir: Path) -> None:
    rows = [
        (wf, d.isoformat(), total, connected, fmt_rate(connected, total), success)
        for wf, d, total, connected, success in conn.execute(DAILY_OVERVIEW_SQL)
    ]
    _write_csv(out_dir / "daily_overview.csv",
               ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
               rows)


# ---------------------------------------------------------------- 3. lead_report.csv

LEAD_REPORT_SQL = """
SELECT workflow_id, external_ref, batch_id, phone_last4, bucket,
       total_calls, connected_calls, last_call_at, current_disposition, ever_success
FROM lead_facts
ORDER BY workflow_id COLLATE "C", external_ref COLLATE "C", lead_id
"""


def write_lead_report(conn: psycopg.Connection, out_dir: Path) -> None:
    rows = [
        (wf, ref, batch, mask_last4(last4), bucket, total, connected,
         fmt_ts_ist(last_call), current or "", fmt_bool(ever))
        for wf, ref, batch, last4, bucket, total, connected, last_call, current, ever
        in conn.execute(LEAD_REPORT_SQL)
    ]
    _write_csv(out_dir / "lead_report.csv",
               ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
                "connected_calls", "last_call_at", "current_disposition", "ever_success"],
               rows)


# ---------------------------------------------------------------- 4. rejections.csv

def write_rejections(conn: psycopg.Connection, out_dir: Path) -> None:
    rows = conn.execute(
        'SELECT workflow_id, batch_id, row_number, reason FROM lead_rejections '
        'ORDER BY workflow_id COLLATE "C", batch_id COLLATE "C", row_number'
    ).fetchall()
    _write_csv(out_dir / "rejections.csv", ["workflow_id", "batch_id", "row_number", "reason"], rows)


# ---------------------------------------------------------------- 5. data_quality.json

DATA_QUALITY_SQL = """
SELECT
    (SELECT COALESCE(SUM(rows_total), 0)         FROM lead_file_stats),
    (SELECT COALESCE(SUM(rows_rejected), 0)      FROM lead_file_stats),
    (SELECT COALESCE(SUM(duplicates_merged), 0)  FROM lead_file_stats),
    (SELECT COALESCE(SUM(field_warnings), 0)     FROM lead_file_stats),
    (SELECT COALESCE(SUM(lines_total), 0)        FROM event_file_stats),
    (SELECT COALESCE(SUM(malformed), 0)          FROM event_file_stats),
    (SELECT COALESCE(SUM(duplicates_dropped), 0) FROM event_file_stats),
    (SELECT COUNT(*) FROM ledger_events WHERE is_orphan),
    (SELECT COUNT(*) FROM ledger_events WHERE is_unknown_disposition)
"""


def data_quality(conn: psycopg.Connection) -> dict:
    (rows_total, rejected, merged, warnings, ev_total, malformed, dups,
     orphans, unknown) = conn.execute(DATA_QUALITY_SQL).fetchone()
    by_reason = dict(conn.execute(
        'SELECT reason, COUNT(*) FROM lead_rejections GROUP BY reason ORDER BY reason COLLATE "C"'
    ).fetchall())
    return {
        "lead_rows_total": int(rows_total),
        "lead_rows_rejected": int(rejected),
        "rejections_by_reason": {k: int(v) for k, v in by_reason.items()},
        "duplicate_lead_rows_merged": int(merged),
        "field_warnings": int(warnings),
        "events_total": int(ev_total),
        "malformed_events": int(malformed),
        "duplicate_events_dropped": int(dups),
        "orphan_events": int(orphans),
        "unknown_disposition_events": int(unknown),
    }


def write_data_quality(conn: psycopg.Connection, out_dir: Path) -> None:
    with open(out_dir / "data_quality.json", "w", encoding="utf-8") as f:
        json.dump(data_quality(conn), f, indent=2)
        f.write("\n")


# ---------------------------------------------------------------- all

def write_reports(conn: psycopg.Connection, out_dir: Path) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # One REPEATABLE READ snapshot so all five files describe the same state,
    # even if an ingest commits while we're writing.
    conn.commit()
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    # Parallel hash joins allocate dynamic shared memory; Docker's default
    # /dev/shm is only 64 MB, which the large dataset exceeds ("could not
    # resize shared memory segment"). Serial plans are plenty fast here.
    conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")
    try:
        write_batch_summary(conn, out_dir)
        write_daily_overview(conn, out_dir)
        write_lead_report(conn, out_dir)
        write_rejections(conn, out_dir)
        write_data_quality(conn, out_dir)
    finally:
        conn.rollback()
