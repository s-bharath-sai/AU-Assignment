"""
Loads the *given* workflow lead schemas and the disposition catalogue.

These files live in assignment-a-data/schemas/ and are read-only inputs.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from .parsers import normalize_header

DEFAULT_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "assignment-a-data" / "schemas"

FIELD_TYPES = {"string", "phone", "date", "decimal", "enum"}


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool
    pii: bool
    reference: bool
    names: frozenset[str]          # normalised name + aliases, used for header matching
    values: tuple[str, ...] = ()   # enum values
    default: str | None = None


@dataclass(frozen=True)
class WorkflowSchema:
    workflow_id: str
    fields: tuple[Field, ...]      # schema order matters: first failure = rejection reason

    @property
    def reference_field(self) -> Field:
        return next(f for f in self.fields if f.reference)

    @property
    def phone_field(self) -> Field:
        return next(f for f in self.fields if f.type == "phone")

    def field_for_header(self, header: str) -> Field | None:
        norm = normalize_header(header)
        for f in self.fields:
            if norm in f.names:
                return f
        return None


@dataclass(frozen=True)
class Disposition:
    workflow_id: str
    code: str
    is_terminal: bool
    is_success: bool


def _parse_field(raw: dict) -> Field:
    if raw["type"] not in FIELD_TYPES:
        raise ValueError(f"unknown field type {raw['type']!r} for {raw['name']!r}")
    names = {normalize_header(raw["name"])}
    names.update(normalize_header(a) for a in raw.get("aliases", []))
    return Field(
        name=raw["name"],
        type=raw["type"],
        required=bool(raw.get("required", False)),
        pii=bool(raw.get("pii", False)),
        reference=bool(raw.get("reference", False)),
        names=frozenset(names),
        values=tuple(v.lower() for v in raw.get("values", [])),
        default=raw.get("default"),
    )


def load_schema(path: Path) -> WorkflowSchema:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    schema = WorkflowSchema(
        workflow_id=data["workflow_id"],
        fields=tuple(_parse_field(f) for f in data["fields"]),
    )
    # Fail fast on schemas the pipeline can't work with.
    refs = [f for f in schema.fields if f.reference]
    phones = [f for f in schema.fields if f.type == "phone"]
    if len(refs) != 1:
        raise ValueError(f"{schema.workflow_id}: expected exactly 1 reference field")
    if len(phones) != 1 or not phones[0].required:
        raise ValueError(f"{schema.workflow_id}: expected exactly 1 required phone field")
    return schema


def load_schemas(schema_dir: Path = DEFAULT_SCHEMA_DIR) -> dict[str, WorkflowSchema]:
    schemas = {}
    for path in sorted(Path(schema_dir).glob("*.json")):
        s = load_schema(path)
        schemas[s.workflow_id] = s
    return schemas


def load_dispositions(schema_dir: Path = DEFAULT_SCHEMA_DIR) -> dict[tuple[str, str], Disposition]:
    """Keyed by (workflow_id, code)."""
    catalogue = {}
    with open(Path(schema_dir) / "dispositions.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = Disposition(
                workflow_id=row["workflow_id"].strip(),
                code=row["code"].strip(),
                is_terminal=row["is_terminal"].strip().lower() == "true",
                is_success=row["is_success"].strip().lower() == "true",
            )
            catalogue[(d.workflow_id, d.code)] = d
    return catalogue
