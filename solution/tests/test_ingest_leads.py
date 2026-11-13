from datetime import date
from decimal import Decimal

import pytest

from pipeline.ingest_leads import (
    FileConflict,
    ingest_lead_file,
    is_blank,
    map_headers,
    merge,
    read_lead_rows,
    validate_row,
)
from pipeline.pii import PiiVault
from pipeline.schema import load_schemas

SCHEMAS = load_schemas()
MOTOR = SCHEMAS["motor_renewal"]

GOOD = {
    "customer_name": "Aarav Menon",
    "phone": "+91 98765 43210",
    "policy_number": "MOT/2026/1",
    "policy_expiry": "21/11/2026",
    "premium_amount": "₹27,554",
    "preferred_language": "HI",
}


# ---------------------------------------------------------------- headers

def test_alias_headers_map_to_schema_fields():
    header = ["Language", "Insured Name", "Policy No", "Mobile No", "Expiry Date", "Premium", "agent_notes"]
    mapped = {i: f.name for i, f in map_headers(header, MOTOR).items()}
    assert mapped == {
        0: "preferred_language", 1: "customer_name", 2: "policy_number",
        3: "phone", 4: "policy_expiry", 5: "premium_amount",
    }  # agent_notes (6) ignored


def test_first_column_wins_when_two_map_to_same_field():
    mapped = map_headers(["phone", "Mobile No"], MOTOR)
    assert list(mapped) == [0]


def test_is_blank():
    assert is_blank(["", "  ", "\t"])
    assert is_blank([])
    assert not is_blank(["", "x"])


# ---------------------------------------------------------------- validation

def test_valid_row_is_normalised():
    r = validate_row(MOTOR, GOOD)
    assert not r.rejected and r.warnings == 0
    assert r.values == {
        "customer_name": "Aarav Menon",
        "phone": "+919876543210",
        "policy_number": "MOT/2026/1",
        "policy_expiry": date(2026, 11, 21),
        "premium_amount": Decimal("27554.00"),
        "preferred_language": "hi",
    }


def test_rejection_reason_follows_schema_order():
    # name (1st field) and phone (2nd) both bad -> name wins
    row = {**GOOD, "customer_name": "  ", "phone": "N/A"}
    assert validate_row(MOTOR, row).reason == "missing_required:customer_name"


def test_invalid_phone_before_missing_reference():
    row = {**GOOD, "phone": "5876543210", "policy_number": ""}
    assert validate_row(MOTOR, row).reason == "invalid_phone"


def test_missing_phone_is_missing_not_invalid():
    assert validate_row(MOTOR, {**GOOD, "phone": ""}).reason == "missing_required:phone"


def test_absent_column_counts_as_missing():
    row = {k: v for k, v in GOOD.items() if k != "policy_number"}
    assert validate_row(MOTOR, row).reason == "missing_required:policy_number"


def test_invalid_optional_fields_are_warnings_not_rejections():
    row = {**GOOD, "policy_expiry": "31/02/2026", "premium_amount": "N/A", "preferred_language": "Hindi"}
    r = validate_row(MOTOR, row)
    assert not r.rejected
    assert r.warnings == 3
    assert r.values["policy_expiry"] is None
    assert r.values["premium_amount"] is None
    assert r.values["preferred_language"] is None


def test_missing_optional_field_is_not_a_warning():
    r = validate_row(MOTOR, {**GOOD, "preferred_language": ""})
    assert r.warnings == 0 and r.values["preferred_language"] is None


# ---------------------------------------------------------------- merge

def test_merge_overwrites_only_non_null():
    old = {"a": 1, "b": 2, "c": None}
    new = {"a": None, "b": 20, "c": 30}
    assert merge(old, new) == {"a": 1, "b": 20, "c": 30}


# ---------------------------------------------------------------- file reading

def test_blank_rows_are_skipped_and_not_numbered(tmp_path):
    p = tmp_path / "batch_001.csv"
    p.write_text(
        "customer_name,phone,policy_number\r\n"
        "A,9876543210,P1\r\n"
        ",,\r\n"
        "   ,  ,\r\n"
        "B,9876543211,P2\r\n",
        encoding="utf-8",
    )
    rows = read_lead_rows(p, MOTOR)
    assert [r.row_number for r in rows] == [1, 2]
    assert rows[1].raw["customer_name"] == "B"


# ---------------------------------------------------------------- database (needs docker postgres)



VAULT = PiiVault("test-secret")
HEADER = "customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language\n"


def _write(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + body, encoding="utf-8")
    return path


def test_duplicates_merge_and_batch_id_is_first_seen(conn, tmp_path):
    b1 = _write(tmp_path / "batch_001.csv", "A,9876543210,P1,,1000,\n")
    b2 = _write(tmp_path / "batch_002.csv", "A2,+91 98765 43210,P1,01/12/2026,,ta\n")
    ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")
    s = ingest_lead_file(conn, VAULT, MOTOR, b2, "drop_1")
    assert s.duplicates_merged == 1 and s.new_leads == 0

    row = conn.execute("SELECT batch_id, attributes, pii_encrypted FROM leads").fetchall()
    assert len(row) == 1
    batch_id, attrs, pii = row[0]
    assert batch_id == "batch_001"
    assert attrs == {"policy_expiry": "2026-12-01", "premium_amount": "1000.00", "preferred_language": "ta"}
    assert VAULT.decrypt(pii["customer_name"]) == "A2"
    assert VAULT.decrypt(pii["phone"]) == "+919876543210"


def test_reingest_is_noop(conn, tmp_path):
    b1 = _write(tmp_path / "batch_001.csv", "A,9876543210,P1,,,\n,N/A,P2,,,\n")
    first = ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")
    again = ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")
    assert first.rows_rejected == 1 and again.skipped
    counts = conn.execute(
        "SELECT (SELECT count(*) FROM leads), (SELECT count(*) FROM lead_rejections), "
        "(SELECT count(*) FROM lead_file_stats)"
    ).fetchone()
    assert counts == (1, 1, 1)


def test_changed_file_with_same_name_is_refused(conn, tmp_path):
    b1 = _write(tmp_path / "batch_001.csv", "A,9876543210,P1,,,\n")
    ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")
    _write(b1, "B,9876543211,P2,,,\n")
    with pytest.raises(FileConflict):
        ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")


def test_no_plaintext_pii_stored(conn, tmp_path):
    b1 = _write(tmp_path / "batch_001.csv", "Aarav Menon,9876543210,P1,,,\n")
    ingest_lead_file(conn, VAULT, MOTOR, b1, "drop_1")
    dump = str(conn.execute("SELECT row_to_json(l) FROM leads l").fetchall())
    assert "9876543210" not in dump
    assert "Aarav" not in dump
