from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from pipeline.parsers import (
    IST,
    clean,
    ist_date,
    mask_phone,
    normalize_header,
    parse_date,
    parse_decimal,
    parse_enum,
    parse_phone,
    parse_timestamp,
)


# ---------------------------------------------------------------- clean

@pytest.mark.parametrize("raw", [None, "", "   ", "\t"])
def test_clean_missing(raw):
    assert clean(raw) is None


def test_clean_trims():
    assert clean("  Aarav  ") == "Aarav"


# ---------------------------------------------------------------- headers

@pytest.mark.parametrize("raw, expected", [
    ("customer_name", "customer name"),
    ("  Mobile No ", "mobile no"),
    ("Insured   Name", "insured name"),
    ("POLICY__no", "policy no"),
    ("Mobile _ No", "mobile no"),
    ("_phone_", "phone"),
])
def test_normalize_header(raw, expected):
    assert normalize_header(raw) == expected


# ---------------------------------------------------------------- phone

@pytest.mark.parametrize("raw", [
    "9876543210",
    "+91 98765 43210",
    "09876543210",
    "91-98765-43210",
    "(+91) 9876543210",
    "+919876543210",
    " 98765-43210 ",
])
def test_phone_all_generator_formats(raw):
    assert parse_phone(raw) == "+919876543210"


@pytest.mark.parametrize("raw", [
    "987654321",          # 9 digits
    "5876543210",         # bad first digit
    "022-2345-6789",      # landline -> 11 digits starting 0, then 10 starting 2
    "N/A",
    "+1 415 555 0134",
    "98765432101",        # 11 digits not starting with 0
    "919876543",          # starts 91 but only 9 digits
    "",
    None,
])
def test_phone_invalid(raw):
    assert parse_phone(raw) is None


def test_phone_starting_91_but_10_digits_is_kept():
    # 10 digits starting with 9 -> valid as-is, do NOT strip '91'
    assert parse_phone("9198765432") == "+919198765432"


def test_mask_phone():
    assert mask_phone("+919876543210") == "+91******3210"


# ---------------------------------------------------------------- date

@pytest.mark.parametrize("raw, expected", [
    ("2026-11-21", date(2026, 11, 21)),
    ("21/11/2026", date(2026, 11, 21)),
    ("21-11-2026", date(2026, 11, 21)),
    ("01-Dec-2026", date(2026, 12, 1)),
    ("01-dec-2026", date(2026, 12, 1)),
    ("01-DEC-2026", date(2026, 12, 1)),
    ("  05/11/2026 ", date(2026, 11, 5)),
    ("29/02/2028", date(2028, 2, 29)),   # leap year
])
def test_date_valid(raw, expected):
    assert parse_date(raw) == expected


def test_date_is_day_first():
    # 05/11 is 5 November, not May 11
    assert parse_date("05/11/2026") == date(2026, 11, 5)


@pytest.mark.parametrize("raw", [
    "31/02/2026",     # impossible
    "29/02/2026",     # not a leap year
    "2026-13-01",     # month 13
    "soon",
    "TBD",
    "01-Foo-2026",
    "2026/11/21",     # not an accepted format
    "11-21-2026",     # month-first -> month 21 invalid
    "",
    None,
])
def test_date_invalid(raw):
    assert parse_date(raw) is None


# ---------------------------------------------------------------- decimal

@pytest.mark.parametrize("raw, expected", [
    ("6850", Decimal("6850.00")),
    ("12345.67", Decimal("12345.67")),
    ("₹27,554", Decimal("27554.00")),
    ("₹ 27,554", Decimal("27554.00")),
    ("Rs. 24787", Decimal("24787.00")),
    ("Rs.24787", Decimal("24787.00")),
    ("Rs 24787", Decimal("24787.00")),
    ("rs24787", Decimal("24787.00")),
    ("INR 18678.3", Decimal("18678.30")),
    ("inr18678.3", Decimal("18678.30")),
    ("1,00,000", Decimal("100000.00")),   # Indian grouping
    ("2.675", Decimal("2.68")),           # half-up, float would give 2.67
    ("0", Decimal("0.00")),
])
def test_decimal_valid(raw, expected):
    assert parse_decimal(raw) == expected


@pytest.mark.parametrize("raw", [
    "N/A", "-", "twelve thousand", "-500", "Rs. -5", "12.3.4", ",500", "$100",
    "", None,
])
def test_decimal_invalid(raw):
    assert parse_decimal(raw) is None


# ---------------------------------------------------------------- enum

LANGS = ["en", "hi", "ta", "kn", "te", "mr"]


@pytest.mark.parametrize("raw, expected", [
    ("hi", "hi"), ("HI", "hi"), (" ta ", "ta"), ("En", "en"),
])
def test_enum_valid(raw, expected):
    assert parse_enum(raw, LANGS) == expected


@pytest.mark.parametrize("raw", ["Hindi", "english", "tamil", "", "  ", None])
def test_enum_invalid_or_missing(raw):
    assert parse_enum(raw, LANGS) is None


# ---------------------------------------------------------------- timestamps

def test_timestamp_utc_z():
    ts = parse_timestamp("2026-10-05T13:20:21Z")
    assert ts == datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)


def test_timestamp_offset():
    ts = parse_timestamp("2026-10-05T18:50:21+05:30")
    assert ts == datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)


def test_timestamp_naive_is_ist():
    ts = parse_timestamp("2026-10-03 10:01:00")
    assert ts == datetime(2026, 10, 3, 10, 1, 0, tzinfo=IST)


def test_three_forms_of_same_instant_are_equal():
    a = parse_timestamp("2026-10-05T13:20:21Z")
    b = parse_timestamp("2026-10-05T18:50:21+05:30")
    c = parse_timestamp("2026-10-05 18:50:21")
    assert a == b == c


def test_ist_date_crosses_midnight():
    # 20:00 UTC on 4 Oct is 01:30 IST on 5 Oct
    assert ist_date(parse_timestamp("2026-10-04T20:00:00Z")) == date(2026, 10, 5)


@pytest.mark.parametrize("raw", ["", None, "yesterday", "2026-13-01T00:00:00Z"])
def test_timestamp_invalid(raw):
    assert parse_timestamp(raw) is None
