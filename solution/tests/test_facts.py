"""
Derived facts: effective disposition, connected, IST date, buckets.

Each test builds a tiny ledger for ONE lead (phone 9876543210) and reads the
call_facts / lead_facts views.
"""
import json
from datetime import date

from pipeline.ingest_events import ingest_event_file
from pipeline.ingest_leads import ingest_lead_file
from pipeline.pii import PiiVault
from pipeline.schema import load_schemas

VAULT = PiiVault("test-secret")
MOTOR = load_schemas()["motor_renewal"]
PHONE = "9876543210"

_seq = 0


def ev(event_type, call_id, t, phone=PHONE, **payload):
    global _seq
    _seq += 1
    e = {"event_id": f"evt_{_seq:06d}", "event_type": event_type, "workflow_id": "motor_renewal",
         "call_id": call_id, "occurred_at": t, "payload": payload}
    if phone is not None and event_type != "disposition.override":
        e["phone"] = phone
    return json.dumps(e)


def setup(conn, tmp_path, lines, leads=("A,9876543210,P1",)):
    lp = tmp_path / "batch_001.csv"
    lp.write_text("customer_name,phone,policy_number\n" + "\n".join(leads) + "\n", encoding="utf-8")
    ingest_lead_file(conn, VAULT, MOTOR, lp, "drop_1")
    if lines:
        ep = tmp_path / "events_0001.jsonl"
        ep.write_text("\n".join(lines) + "\n", encoding="utf-8")
        ingest_event_file(conn, VAULT, ep, "drop_1")


def lead(conn, ref="P1"):
    cols = ["total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success", "bucket"]
    row = conn.execute(f"SELECT {', '.join(cols)} FROM lead_facts WHERE external_ref = %s", (ref,)).fetchone()
    return dict(zip(cols, row))


def call(conn, call_id):
    cols = ["call_date", "connected", "telephony_failed", "effective_disposition"]
    row = conn.execute(f"SELECT {', '.join(cols)} FROM call_facts WHERE call_id = %s", (call_id,)).fetchone()
    return dict(zip(cols, row)) if row else None


def connected_call(cid, t0, code=None, h=10):
    """initiated -> connected -> ended -> optional disposition.set, on 2026-10-<h>."""
    lines = [ev("call.initiated", cid, f"2026-10-{h:02d}T10:00:00+05:30"),
             ev("call.connected", cid, f"2026-10-{h:02d}T10:00:10+05:30"),
             ev("call.ended", cid, f"2026-10-{h:02d}T10:02:00+05:30", duration_sec=110, end_reason="completed")]
    if code:
        lines.append(ev("disposition.set", cid, f"2026-10-{h:02d}T10:03:00+05:30", disposition_code=code))
    return lines


# ---------------------------------------------------------------- calls

def test_call_without_initiated_is_not_counted(conn, tmp_path):
    setup(conn, tmp_path, [ev("call.connected", "c1", "2026-10-10T10:00:00Z")])
    assert call(conn, "c1") is None
    assert lead(conn)["bucket"] == "yet_to_reach_out"


def test_call_date_is_ist(conn, tmp_path):
    # 20:00 UTC on 4 Oct = 01:30 IST on 5 Oct
    setup(conn, tmp_path, [ev("call.initiated", "c1", "2026-10-04T20:00:00Z")])
    assert call(conn, "c1")["call_date"] == date(2026, 10, 5)


def test_connected_flag(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None) + [ev("call.initiated", "c2", "2026-10-11T10:00:00Z")])
    assert call(conn, "c1")["connected"] is True
    assert call(conn, "c2")["connected"] is False
    assert lead(conn)["connected_calls"] == 1


# ---------------------------------------------------------------- effective disposition

def test_latest_set_without_override(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "callback_requested") + [
        ev("disposition.set", "c1", "2026-10-10T11:00:00+05:30", disposition_code="payment_link_sent"),
    ])
    assert call(conn, "c1")["effective_disposition"] == "payment_link_sent"


def test_override_beats_later_set(conn, tmp_path):
    # override at 10:30, set at 11:00 -> override still wins
    setup(conn, tmp_path, connected_call("c1", None) + [
        ev("disposition.override", "c1", "2026-10-10T10:30:00+05:30", disposition_code="not_interested"),
        ev("disposition.set", "c1", "2026-10-10T11:00:00+05:30", disposition_code="renewed"),
    ])
    assert call(conn, "c1")["effective_disposition"] == "not_interested"


def test_latest_override_wins_even_if_it_arrives_first_in_file(conn, tmp_path):
    setup(conn, tmp_path, [
        ev("disposition.override", "c1", "2026-10-12T10:00:00+05:30", disposition_code="renewed"),  # later time
        ev("disposition.override", "c1", "2026-10-11T10:00:00+05:30", disposition_code="not_interested"),
    ] + connected_call("c1", None, "callback_requested"))
    assert call(conn, "c1")["effective_disposition"] == "renewed"


def test_unknown_override_is_ignored_falls_back_to_set(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "renewed") + [
        ev("disposition.override", "c1", "2026-10-11T10:00:00+05:30", disposition_code="RENEWD"),
    ])
    assert call(conn, "c1")["effective_disposition"] == "renewed"


def test_override_does_not_modify_ledger(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "renewed") + [
        ev("disposition.override", "c1", "2026-10-11T10:00:00+05:30", disposition_code="not_interested"),
    ])
    codes = [r[0] for r in conn.execute(
        "SELECT disposition_code FROM events WHERE call_id='c1' AND disposition_code IS NOT NULL ORDER BY occurred_at")]
    assert codes == ["renewed", "not_interested"]   # original still there


# ---------------------------------------------------------------- buckets

def test_bucket_yet_to_reach_out(conn, tmp_path):
    setup(conn, tmp_path, [])
    f = lead(conn)
    assert f["bucket"] == "yet_to_reach_out" and f["total_calls"] == 0
    assert f["last_call_at"] is None and f["current_disposition"] is None and f["ever_success"] is False


def test_bucket_completed_if_any_call_terminal(conn, tmp_path):
    # terminal on an EARLIER call, non-terminal later -> still completed
    setup(conn, tmp_path, connected_call("c1", None, "not_interested", h=10)
          + connected_call("c2", None, "callback_requested", h=11))
    assert lead(conn)["bucket"] == "completed"


def test_completed_beats_telephony_failed(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "renewed") + [
        ev("call.telephony_failed", "c1", "2026-10-10T10:05:00+05:30", reason="sip_503"),
    ])
    assert lead(conn)["bucket"] == "completed"


def test_bucket_telephony_failed_only_if_every_call_failed(conn, tmp_path):
    setup(conn, tmp_path, [
        ev("call.initiated", "c1", "2026-10-10T10:00:00Z"),
        ev("call.telephony_failed", "c1", "2026-10-10T10:00:05Z", reason="sip_503"),
        ev("call.initiated", "c2", "2026-10-11T10:00:00Z"),
        ev("call.telephony_failed", "c2", "2026-10-11T10:00:05Z", reason="carrier_rejected"),
    ])
    assert lead(conn)["bucket"] == "in_progress_telephony_failed"


def test_one_non_failed_call_makes_it_dialed(conn, tmp_path):
    setup(conn, tmp_path, [
        ev("call.initiated", "c1", "2026-10-10T10:00:00Z"),
        ev("call.telephony_failed", "c1", "2026-10-10T10:00:05Z", reason="sip_503"),
        ev("call.initiated", "c2", "2026-10-11T10:00:00Z"),
        ev("call.ended", "c2", "2026-10-11T10:00:40Z", duration_sec=0, end_reason="no_answer"),
    ])
    assert lead(conn)["bucket"] == "in_progress_dialed"


def test_override_can_move_lead_out_of_completed(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "renewed") + [
        ev("disposition.override", "c1", "2026-10-11T10:00:00+05:30", disposition_code="callback_requested"),
    ])
    f = lead(conn)
    assert f["bucket"] == "in_progress_dialed" and f["ever_success"] is False


# ---------------------------------------------------------------- lead report fields

def test_current_disposition_is_from_latest_call_that_has_one(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "callback_requested", h=10)
          + connected_call("c2", None, "payment_link_sent", h=11)
          + [ev("call.initiated", "c3", "2026-10-12T10:00:00+05:30")])   # latest, no disposition
    f = lead(conn)
    assert f["current_disposition"] == "payment_link_sent"
    assert f["total_calls"] == 3
    assert f["last_call_at"].isoformat().startswith("2026-10-12T04:30:00")  # 10:00 IST in UTC


def test_ever_success_any_call(conn, tmp_path):
    setup(conn, tmp_path, connected_call("c1", None, "renewed", h=10)
          + connected_call("c2", None, "callback_requested", h=11))
    assert lead(conn)["ever_success"] is True
