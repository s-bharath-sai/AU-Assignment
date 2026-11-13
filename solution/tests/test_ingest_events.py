import json
from datetime import datetime, timezone

import pytest

from pipeline.ingest_events import ingest_event_file, parse_event_line
from pipeline.ingest_leads import ingest_lead_file
from pipeline.pii import PiiVault
from pipeline.report import data_quality
from pipeline.schema import load_schemas

VAULT = PiiVault("test-secret")
MOTOR = load_schemas()["motor_renewal"]


def ev(event_id, event_type="call.initiated", phone="9876543210", call_id="call_1",
       occurred_at="2026-10-05T13:20:21Z", **payload):
    e = {"event_id": event_id, "event_type": event_type, "workflow_id": "motor_renewal",
         "call_id": call_id, "occurred_at": occurred_at, "payload": payload}
    if phone is not None:
        e["phone"] = phone
    return json.dumps(e)


# ---------------------------------------------------------------- pure parsing

@pytest.mark.parametrize("line", [
    '{"event_id": "evt_0',              # truncated JSON
    "not json at all",
    "[1, 2, 3]",                         # JSON but not an object
    '{"event_type": "call.initiated"}',  # no event_id
    json.dumps({"event_id": "", "event_type": "x", "workflow_id": "w", "occurred_at": "2026-10-05T13:20:21Z"}),
])
def test_malformed_lines(line):
    assert parse_event_line(line, 1, VAULT) is None


def test_event_is_normalised_and_phone_hashed():
    e = parse_event_line(ev("e1", phone="91-98765-43210"), 7, VAULT)
    assert e.occurred_at == datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)
    assert e.phone_hash == VAULT.phone_hash("+919876543210")
    assert e.line_number == 7


def test_naive_timestamp_is_ist():
    e = parse_event_line(ev("e1", occurred_at="2026-10-05 18:50:21"), 1, VAULT)
    assert e.occurred_at == datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)


def test_invalid_phone_gives_no_hash():
    assert parse_event_line(ev("e1", phone="N/A"), 1, VAULT).phone_hash is None


def test_disposition_code_extracted():
    e = parse_event_line(ev("e1", "disposition.set", disposition_code="renewed"), 1, VAULT)
    assert e.disposition_code == "renewed"


# ---------------------------------------------------------------- database



def write_events(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_leads(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("customer_name,phone,policy_number\n" + "".join(r + "\n" for r in rows), encoding="utf-8")
    return path


def lead_of(conn, event_id):
    return conn.execute("SELECT lead_id FROM ledger_events WHERE event_id = %s", (event_id,)).fetchone()[0]


def test_counts_malformed_and_duplicates_within_and_across_files(conn, tmp_path):
    f1 = write_events(tmp_path / "events_0001.jsonl", [ev("e1"), ev("e1"), '{"broken', "", ev("e2")])
    f2 = write_events(tmp_path / "events_1001.jsonl", [ev("e2"), ev("e3")])
    s1 = ingest_event_file(conn, VAULT, f1, "drop_1")
    s2 = ingest_event_file(conn, VAULT, f2, "drop_2")
    assert (s1.lines_total, s1.malformed, s1.duplicates_dropped, s1.stored) == (4, 1, 1, 2)
    assert (s2.lines_total, s2.malformed, s2.duplicates_dropped, s2.stored) == (2, 0, 1, 1)


def test_reingest_events_is_noop(conn, tmp_path):
    f1 = write_events(tmp_path / "events_0001.jsonl", [ev("e1"), ev("e2")])
    ingest_event_file(conn, VAULT, f1, "drop_1")
    assert ingest_event_file(conn, VAULT, f1, "drop_1").skipped
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 2
    assert data_quality(conn)["events_total"] == 2


def test_orphan_links_once_lead_arrives_later(conn, tmp_path):
    ingest_event_file(conn, VAULT, write_events(tmp_path / "events_0001.jsonl", [ev("e1")]), "drop_1")
    assert lead_of(conn, "e1") is None
    assert data_quality(conn)["orphan_events"] == 1

    ingest_lead_file(conn, VAULT, MOTOR, write_leads(tmp_path / "batch_003.csv", ["A,+91 98765 43210,P1"]), "drop_2")
    assert lead_of(conn, "e1") is not None
    assert data_quality(conn)["orphan_events"] == 0


def test_override_links_through_call_id(conn, tmp_path):
    ingest_lead_file(conn, VAULT, MOTOR, write_leads(tmp_path / "batch_001.csv", ["A,9876543210,P1"]), "drop_1")
    ingest_event_file(conn, VAULT, write_events(tmp_path / "events_0001.jsonl", [
        ev("e1", call_id="c1"),
        ev("e2", "disposition.override", phone=None, call_id="c1", disposition_code="renewed"),
        ev("e3", "disposition.override", phone=None, call_id="c_unknown", disposition_code="renewed"),
    ]), "drop_1")
    assert lead_of(conn, "e2") == lead_of(conn, "e1") is not None
    assert lead_of(conn, "e3") is None   # no other events for that call -> orphan


def test_unknown_disposition_counted_only_for_linked_events(conn, tmp_path):
    ingest_lead_file(conn, VAULT, MOTOR, write_leads(tmp_path / "batch_001.csv", ["A,9876543210,P1"]), "drop_1")
    ingest_event_file(conn, VAULT, write_events(tmp_path / "events_0001.jsonl", [
        ev("e1", "disposition.set", disposition_code="RENEWD"),                   # linked, unknown
        ev("e2", "disposition.set", disposition_code="renewed"),                  # linked, known
        ev("e3", "disposition.set", phone="9000000000", disposition_code="xx"),   # orphan wins
    ]), "drop_1")
    dq = data_quality(conn)
    assert dq["unknown_disposition_events"] == 1
    assert dq["orphan_events"] == 1
    usable = {r[0] for r in conn.execute("SELECT event_id FROM usable_events")}
    assert usable == {"e2"}


def test_ledger_is_append_only(conn, tmp_path):
    import psycopg
    ingest_event_file(conn, VAULT, write_events(tmp_path / "events_0001.jsonl", [ev("e1")]), "drop_1")
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("UPDATE events SET call_id = 'x'")
    conn.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("DELETE FROM events")
    conn.rollback()
