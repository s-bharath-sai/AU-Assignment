"""
Value parsers for lead and event fields.

Every function here is pure: it takes one raw value and returns either the
normalised value or ``None`` when the value is invalid. Deciding what an
invalid value *means* (reject the row vs. field warning) is the caller's job.

"Missing" (empty / whitespace-only) is checked separately with ``clean()`` so
callers can tell ``missing_required:<field>`` apart from ``invalid_phone``.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

IST = timezone(timedelta(hours=5, minutes=30))


# ---------------------------------------------------------------- generic

def clean(raw: str | None) -> str | None:
    """Trim a raw cell. Empty or whitespace-only counts as missing -> None."""
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


# ---------------------------------------------------------------- headers

_HEADER_SEP = re.compile(r"[\s_]+")


def normalize_header(raw: str) -> str:
    """Lower-case, trim, collapse every run of '_' / whitespace to one space.

    We trim again after collapsing so a header like '_phone_' still matches
    'phone' (the spec's order would otherwise leave a leading space).
    """
    return _HEADER_SEP.sub(" ", raw.lower().strip()).strip()


# ---------------------------------------------------------------- phone

_VALID_MOBILE = re.compile(r"[6-9]\d{9}")


def parse_phone(raw: str | None) -> str | None:
    """Indian mobile -> '+91XXXXXXXXXX', or None if invalid/missing."""
    if raw is None:
        return None
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if not _VALID_MOBILE.fullmatch(digits):
        return None
    return "+91" + digits


# ---------------------------------------------------------------- date

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_ISO_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")         # YYYY-MM-DD
_DMY_SLASH = re.compile(r"(\d{2})/(\d{2})/(\d{4})")        # DD/MM/YYYY
_DMY_DASH = re.compile(r"(\d{2})-(\d{2})-(\d{4})")         # DD-MM-YYYY
_DMON_DASH = re.compile(r"(\d{2})-([A-Za-z]{3})-(\d{4})")  # DD-Mon-YYYY


def _safe_date(y: int, m: int, d: int) -> date | None:
    try:
        return date(y, m, d)
    except ValueError:  # 31/02, month 13, ...
        return None


def parse_date(raw: str | None) -> date | None:
    """Accepts only the four formats in the spec (day-first). Else None.

    Deliberately not using dateutil: it would accept formats the spec does not
    (and guess month-first), which changes warning counts.
    """
    value = clean(raw)
    if value is None:
        return None
    if m := _ISO_DATE.fullmatch(value):
        return _safe_date(int(m[1]), int(m[2]), int(m[3]))
    if m := _DMY_SLASH.fullmatch(value) or _DMY_DASH.fullmatch(value):
        return _safe_date(int(m[3]), int(m[2]), int(m[1]))
    if m := _DMON_DASH.fullmatch(value):
        month = _MONTHS.get(m[2].lower())
        if month is None:
            return None
        return _safe_date(int(m[3]), month, int(m[1]))
    return None


# ---------------------------------------------------------------- decimal

# Currency prefix: ₹, Rs, Rs., INR (case-insensitive), then optional space.
_CURRENCY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
# Digits with optional comma thousands separators and optional fraction.
_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")
_TWO_PLACES = Decimal("0.01")


def parse_decimal(raw: str | None) -> Decimal | None:
    """'₹27,554' / 'Rs. 24787' / 'INR 18678.3' -> Decimal with 2 places.

    Negative numbers are invalid (a leading '-' never matches _NUMBER).
    Uses Decimal + ROUND_HALF_UP so 2.675 -> 2.68 (float would give 2.67).
    """
    value = clean(raw)
    if value is None:
        return None
    value = _CURRENCY_PREFIX.sub("", value, count=1)
    if not _NUMBER.fullmatch(value):
        return None
    try:
        number = Decimal(value.replace(",", ""))
    except InvalidOperation:
        return None
    return number.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------- enum

def parse_enum(raw: str | None, allowed: list[str]) -> str | None:
    """Case-insensitive match against allowed values; returns lower-case."""
    value = clean(raw)
    if value is None:
        return None
    lowered = value.lower()
    allowed_lower = {a.lower() for a in allowed}
    return lowered if lowered in allowed_lower else None


# ---------------------------------------------------------------- timestamps

def parse_timestamp(raw: str | None) -> datetime | None:
    """Event timestamp -> timezone-aware datetime.

    '...Z' = UTC, '...+05:30' = explicit offset, no offset = IST (per spec).
    """
    value = clean(raw)
    if value is None:
        return None
    try:
        ts = datetime.fromisoformat(value)  # 3.11+: handles 'Z' and ' ' separator
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts


def to_ist(ts: datetime) -> datetime:
    return ts.astimezone(IST)


def ist_date(ts: datetime) -> date:
    """The IST calendar date of an aware datetime (used for daily reports)."""
    return ts.astimezone(IST).date()


# ---------------------------------------------------------------- masking

def mask_phone(canonical: str) -> str:
    """'+919876543210' -> '+91******3210'."""
    return "+91******" + canonical[-4:]
