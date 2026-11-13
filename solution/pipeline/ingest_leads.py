"""
Lead ingestion: CSV batch -> validated, normalised, merged leads.

Pure logic (header mapping, row validation, merging) is kept separate from
the database code so it can be unit-tested without Postgres.
"""
from __future__ import annotations

import csv
import json
import hashlib
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg

from .parsers import clean, parse_date, parse_decimal, parse_enum, parse_phone
from .pii import PiiVault
from .schema import Field, WorkflowSchema

# ---------------------------------------------------------------- pure logic


def map_headers(header: list[str], schema: WorkflowSchema) -> dict[int, Field]:
    """Column index -> schema field. Unknown columns are ignored.

    If two columns map to the same field, the first (leftmost) one wins.
    """
    mapping: dict[int, Field] = {}
    taken: set[str] = set()
    for idx, h in enumerate(header):
        f = schema.field_for_header(h)
        if f is not None and f.name not in taken:
            mapping[idx] = f
            taken.add(f.name)
    return mapping


def is_blank(cells: list[str]) -> bool:
    return all(clean(c) is None for c in cells)


def parse_value(f: Field, raw: str):
    """Parse a present (non-missing) value. Returns None if invalid."""
    if f.type == "string":
        return clean(raw)
    if f.type == "phone":
        return parse_phone(raw)
    if f.type == "date":
        return parse_date(raw)
    if f.type == "decimal":
        return parse_decimal(raw)
    if f.type == "enum":
        return parse_enum(raw, list(f.values))
    raise ValueError(f"unsupported type {f.type}")


@dataclass
class RowResult:
    values: dict[str, object] = field(default_factory=dict)  # field -> parsed value or None
    reason: str | None = None   # set => row rejected
    warnings: int = 0

    @property
    def rejected(self) -> bool:
        return self.reason is not None


def validate_row(schema: WorkflowSchema, raw: dict[str, str | None]) -> RowResult:
    """Walk fields in schema order; the first failure is the rejection reason.

    raw: field name -> raw cell text (None if the column is absent).
    """
    result = RowResult()
    warnings = 0
    for f in schema.fields:
        value = clean(raw.get(f.name))
        if value is None:
            if f.required:
                return RowResult(reason=f"missing_required:{f.name}")
            result.values[f.name] = None
            continue
        parsed = parse_value(f, value)
        if parsed is None:
            if f.required:
                # Spec only defines invalid_phone; any other required type that
                # fails parsing gets a generic reason (doesn't occur in given schemas).
                return RowResult(reason="invalid_phone" if f.type == "phone" else f"invalid:{f.name}")
            warnings += 1
        result.values[f.name] = parsed
    result.warnings = warnings
    return result


def merge(old: dict[str, object], new: dict[str, object]) -> dict[str, object]:
    """Later row wins, but only where it actually has a value."""
    merged = dict(old)
    for k, v in new.items():
        if v is not None:
            merged[k] = v
    return merged


def to_json_value(v):
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return f"{v:.2f}"
    return v


# ---------------------------------------------------------------- file reading


@dataclass
class ParsedRow:
    row_number: int
    raw: dict[str, str | None]


def read_lead_rows(path: Path, schema: WorkflowSchema) -> list[ParsedRow]:
    """Non-blank data rows, numbered from 1, mapped to schema field names."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return []
        mapping = map_headers(header, schema)
        rows: list[ParsedRow] = []
        n = 0
        for cells in reader:
            if is_blank(cells):
                continue
            n += 1
            raw = {fld.name: (cells[i] if i < len(cells) else None) for i, fld in mapping.items()}
            rows.append(ParsedRow(n, raw))
        return rows


def file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ---------------------------------------------------------------- database


@dataclass
class FileStats:
    rows_total: int = 0
    rows_rejected: int = 0
    duplicates_merged: int = 0
    field_warnings: int = 0
    new_leads: int = 0
    skipped: bool = False


class FileConflict(Exception):
    """A file with this key was already ingested with different content."""


def already_ingested(conn: psycopg.Connection, file_key: str, sha: str) -> bool:
    row = conn.execute("SELECT sha256 FROM ingested_files WHERE file_key = %s", (file_key,)).fetchone()
    if row is None:
        return False
    if row[0] != sha:
        raise FileConflict(f"{file_key} was already ingested with different content")
    return True


@dataclass
class PendingLead:
    """A lead's state after applying this file's rows (merged in memory)."""
    phone_last4: str
    external_ref: str
    batch_id: str
    attributes: dict
    pii_encrypted: dict


STAGING_LEADS_DDL = """
CREATE TEMP TABLE staging_leads (
    seq INTEGER, workflow_id TEXT, phone_hash TEXT, phone_last4 TEXT, external_ref TEXT,
    batch_id TEXT, attributes JSONB, pii_encrypted JSONB
) ON COMMIT DROP
"""

# batch_id is deliberately NOT updated on conflict: the first batch is permanent.
# ORDER BY seq keeps lead_id assignment in file order (deterministic).
UPSERT_FROM_STAGING = """
INSERT INTO leads (workflow_id, phone_hash, phone_last4, external_ref, batch_id, attributes, pii_encrypted)
SELECT workflow_id, phone_hash, phone_last4, external_ref, batch_id, attributes, pii_encrypted
FROM staging_leads
ORDER BY seq
ON CONFLICT (workflow_id, phone_hash) DO UPDATE SET
    external_ref  = EXCLUDED.external_ref,
    attributes    = EXCLUDED.attributes,
    pii_encrypted = EXCLUDED.pii_encrypted,
    updated_at    = now()
"""


def _split_values(schema: WorkflowSchema, vault: PiiVault, values: dict) -> tuple[dict, dict]:
    """-> (non-PII attributes as JSON values, PII fields encrypted). None stays None."""
    attrs = {f.name: to_json_value(values[f.name])
             for f in schema.fields if not f.pii and not f.reference}
    pii = {f.name: (vault.encrypt(str(values[f.name])) if values[f.name] is not None else None)
           for f in schema.fields if f.pii}
    return attrs, pii


def ingest_lead_file(conn: psycopg.Connection, vault: PiiVault, schema: WorkflowSchema,
                     path: Path, drop_name: str) -> FileStats:
    """Validate and merge a whole file in memory, then write it in bulk.

    Same rules as row-by-row: rows are applied in file order, the first valid
    row of an identity fixes batch_id, later rows overwrite only non-null
    fields. Only the round trips change (one COPY + one upsert per file).
    """
    path = Path(path)
    batch_id = path.stem
    file_key = f"leads/{schema.workflow_id}/{path.name}"
    sha = file_sha256(path)
    stats = FileStats()
    phone_name, ref_name = schema.phone_field.name, schema.reference_field.name

    with conn.transaction():
        if already_ingested(conn, file_key, sha):
            stats.skipped = True
            return stats

        # 1. Validate every row (pure Python).
        rejections: list[tuple] = []
        valid: list[tuple[str, dict]] = []          # (phone_hash, values) in file order
        for row in read_lead_rows(path, schema):
            stats.rows_total += 1
            result = validate_row(schema, row.raw)
            if result.rejected:
                stats.rows_rejected += 1
                rejections.append((schema.workflow_id, batch_id, row.row_number, result.reason))
                continue
            stats.field_warnings += result.warnings
            valid.append((vault.phone_hash(result.values[phone_name]), result.values))

        # 2. Fetch the existing leads this file touches (one query).
        hashes = list({h for h, _ in valid})
        existing = {
            h: PendingLead(last4, ref, b, attrs, pii)
            for h, last4, ref, b, attrs, pii in conn.execute(
                "SELECT phone_hash, phone_last4, external_ref, batch_id, attributes, pii_encrypted "
                "FROM leads WHERE workflow_id = %s AND phone_hash = ANY(%s) FOR UPDATE",
                (schema.workflow_id, hashes),
            )
        }

        # 3. Merge in memory, in file order.
        pending: dict[str, PendingLead] = {}
        for h, values in valid:
            attrs, pii = _split_values(schema, vault, values)
            current = pending.get(h) or existing.get(h)
            if current is None:
                stats.new_leads += 1
                pending[h] = PendingLead(values[phone_name][-4:], values[ref_name], batch_id, attrs, pii)
            else:
                stats.duplicates_merged += 1
                pending[h] = PendingLead(
                    current.phone_last4,
                    values[ref_name] if values[ref_name] is not None else current.external_ref,
                    current.batch_id,                       # first batch wins, forever
                    merge(current.attributes, attrs),
                    merge(current.pii_encrypted, pii),
                )

        # 4. Bulk write: rejections + leads.
        with conn.cursor() as cur:
            if rejections:
                with cur.copy("COPY lead_rejections (workflow_id, batch_id, row_number, reason) FROM STDIN") as cp:
                    for r in rejections:
                        cp.write_row(r)
            if pending:
                cur.execute(STAGING_LEADS_DDL)
                with cur.copy("COPY staging_leads FROM STDIN") as cp:
                    for seq, (h, p) in enumerate(pending.items()):
                        cp.write_row((seq, schema.workflow_id, h, p.phone_last4, p.external_ref, p.batch_id,
                                      json.dumps(p.attributes, ensure_ascii=False),
                                      json.dumps(p.pii_encrypted)))
                cur.execute(UPSERT_FROM_STAGING)

        conn.execute(
            "INSERT INTO lead_file_stats (workflow_id, batch_id, rows_total, rows_rejected, "
            "duplicates_merged, field_warnings) VALUES (%s, %s, %s, %s, %s, %s)",
            (schema.workflow_id, batch_id, stats.rows_total, stats.rows_rejected,
             stats.duplicates_merged, stats.field_warnings),
        )
        conn.execute(
            "INSERT INTO ingested_files (file_key, kind, sha256, drop_name) VALUES (%s, 'leads', %s, %s)",
            (file_key, sha, drop_name),
        )
    return stats


def lead_files(drop_dir: Path) -> list[tuple[str, Path]]:
    """(workflow_id, path) in deterministic order: workflow, then filename."""
    leads_dir = Path(drop_dir) / "leads"
    if not leads_dir.is_dir():
        return []
    out = []
    for wf_dir in sorted(p for p in leads_dir.iterdir() if p.is_dir()):
        for f in sorted(wf_dir.glob("*.csv")):
            out.append((wf_dir.name, f))
    return out


def ingest_leads(conn: psycopg.Connection, vault: PiiVault, schemas: dict[str, WorkflowSchema],
                 drop_dir: Path, log=print) -> None:
    drop_name = Path(drop_dir).name
    for wf, path in lead_files(drop_dir):
        schema = schemas.get(wf)
        if schema is None:
            log(f"  WARN  no schema for workflow {wf!r}, skipping {path}")
            continue
        s = ingest_lead_file(conn, vault, schema, path, drop_name)
        if s.skipped:
            log(f"  skip  {wf}/{path.name} (already ingested)")
        else:
            log(f"  leads {wf}/{path.name}: rows={s.rows_total} new={s.new_leads} "
                f"merged={s.duplicates_merged} rejected={s.rows_rejected} warnings={s.field_warnings}")
