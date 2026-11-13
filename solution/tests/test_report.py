import csv
import filecmp
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline import db
from pipeline.ingest_events import ingest_events
from pipeline.ingest_leads import ingest_leads
from pipeline.pii import PiiVault
from pipeline.report import fmt_bool, fmt_rate, fmt_ts_ist, mask_last4, write_reports
from pipeline.schema import load_dispositions, load_schemas

REPO = Path(__file__).resolve().parents[2]
GENERATOR = REPO / "assignment-a-data" / "tools" / "generate_data.py"
VAULT = PiiVault("test-secret")
REPORT_FILES = ["batch_summary.csv", "daily_overview.csv", "lead_report.csv", "rejections.csv", "data_quality.json"]


# ---------------------------------------------------------------- formatting

@pytest.mark.parametrize("num, den, expected", [
    (45, 80, "0.5625"),     # README example
    (1, 3, "0.3333"),
    (2, 3, "0.6667"),
    (1, 1, "1.0000"),
    (0, 7, "0.0000"),
    (1, 8, "0.1250"),
    (1, 16000, "0.0001"),   # 0.0000625 -> half-up
])
def test_connectivity_rate_four_decimals(num, den, expected):
    assert fmt_rate(num, den) == expected


def test_timestamp_is_iso_ist():
    ts = datetime(2026, 10, 14, 12, 25, 24, tzinfo=timezone.utc)
    assert fmt_ts_ist(ts) == "2026-10-14T17:55:24+05:30"   # README example
    assert fmt_ts_ist(None) == ""


def test_mask_and_bool():
    assert mask_last4("3210") == "+91******3210"
    assert (fmt_bool(True), fmt_bool(False)) == ("true", "false")


# ---------------------------------------------------------------- daily overview

def _ev(eid, etype, call, t, phone="9876543210", **payload):
    e = {"event_id": eid, "event_type": etype, "workflow_id": "motor_renewal",
         "call_id": call, "occurred_at": t, "payload": payload}
    if etype != "disposition.override":
        e["phone"] = phone
    return json.dumps(e)


def test_daily_overview_success_leads_are_distinct_per_date(conn, tmp_path):
    drop = tmp_path / "drop_1"
    (drop / "leads" / "motor_renewal").mkdir(parents=True)
    (drop / "events").mkdir()
    (drop / "leads" / "motor_renewal" / "batch_001.csv").write_text(
        "customer_name,phone,policy_number\nA,9876543210,P1\nB,9876543211,P2\n", encoding="utf-8")
    (drop / "events" / "events_0001.jsonl").write_text("\n".join([
        # lead A: two successful calls on 10 Oct -> counts once
        _ev("e1", "call.initiated", "c1", "2026-10-10T10:00:00+05:30"),
        _ev("e2", "call.connected", "c1", "2026-10-10T10:00:10+05:30"),
        _ev("e3", "disposition.set", "c1", "2026-10-10T10:05:00+05:30", disposition_code="renewed"),
        _ev("e4", "call.initiated", "c2", "2026-10-10T15:00:00+05:30"),
        _ev("e5", "disposition.set", "c2", "2026-10-10T15:05:00+05:30", disposition_code="renewed"),
        # lead B: call at 23:30 IST on 10 Oct = 18:00 UTC, not connected
        _ev("e6", "call.initiated", "c3", "2026-10-10T18:00:00Z", phone="9876543211"),
        # lead B: 01:00 IST on 11 Oct (19:30 UTC on 10 Oct) -> belongs to 11 Oct
        _ev("e7", "call.initiated", "c4", "2026-10-10T19:30:00Z", phone="9876543211"),
        _ev("e8", "call.connected", "c4", "2026-10-10T19:30:10Z", phone="9876543211"),
    ]) + "\n", encoding="utf-8")

    ingest_leads(conn, VAULT, load_schemas(), drop, log=lambda *_: None)
    ingest_events(conn, VAULT, drop, log=lambda *_: None)
    write_reports(conn, tmp_path / "out")

    rows = list(csv.reader(open(tmp_path / "out" / "daily_overview.csv")))
    assert rows == [
        ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
        ["motor_renewal", "2026-10-10", "3", "1", "0.3333", "1"],
        ["motor_renewal", "2026-10-11", "1", "1", "1.0000", "0"],
    ]


# ---------------------------------------------------------------- end to end on a fresh seed

def _fresh_schema_conn():
    name = f"test_{uuid.uuid4().hex[:8]}"
    c = db.connect(search_path=name)
    db.init_db(c)
    db.sync_dispositions(c, load_dispositions())
    return c, name


def _drop(c, name):
    c.rollback()
    c.execute(f'DROP SCHEMA "{name}" CASCADE')
    c.commit()
    c.close()


def test_split_rerun_and_all_at_once_give_identical_reports(conn, tmp_path):
    """Unseen seed: drop_1 then drop_2 (+ re-run) == both at once, byte for byte."""
    data = tmp_path / "data"
    subprocess.run([sys.executable, str(GENERATOR), "--seed", "1234", "--scale", "small", "--out", str(data)],
                   check=True, capture_output=True)
    schemas = load_schemas()
    quiet = lambda *_: None

    def ingest(c, drop):
        ingest_leads(c, VAULT, schemas, data / drop, log=quiet)
        ingest_events(c, VAULT, data / drop, log=quiet)

    # A: drop_1, report, drop_2, re-run both, report  (uses the fixture schema)
    ingest(conn, "drop_1")
    write_reports(conn, tmp_path / "after_drop1")
    ingest(conn, "drop_2")
    ingest(conn, "drop_1")
    ingest(conn, "drop_2")
    write_reports(conn, tmp_path / "split")

    # B: fresh database, events of drop_2 BEFORE leads of drop_1 -> worst-case order
    c2, name = _fresh_schema_conn()
    try:
        ingest_events(c2, VAULT, data / "drop_1", log=quiet)
        ingest_events(c2, VAULT, data / "drop_2", log=quiet)
        ingest_leads(c2, VAULT, schemas, data / "drop_1", log=quiet)
        ingest_leads(c2, VAULT, schemas, data / "drop_2", log=quiet)
        write_reports(c2, tmp_path / "once")
    finally:
        _drop(c2, name)

    match, mismatch, errors = filecmp.cmpfiles(tmp_path / "split", tmp_path / "once", REPORT_FILES, shallow=False)
    assert mismatch == [] and errors == []

    # Orphans after drop_1 must shrink once drop_2's leads arrive.
    before = json.loads((tmp_path / "after_drop1" / "data_quality.json").read_text())
    after = json.loads((tmp_path / "split" / "data_quality.json").read_text())
    assert after["orphan_events"] < before["orphan_events"]

    # No plaintext phone anywhere in the reports.
    for f in REPORT_FILES:
        text = (tmp_path / "split" / f).read_text()
        assert not any(ch.isdigit() and text[i:i + 10].isdigit() for i, ch in enumerate(text))
