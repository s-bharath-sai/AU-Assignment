"""
Command line:

    python solution/main.py ingest data/drop_1
    python solution/main.py report --out reports/
    python solution/main.py reset            # drop all pipeline tables (dev only)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import db
from .ingest_events import ingest_events
from .ingest_leads import FileConflict, ingest_leads
from .pii import PiiVault
from .report import write_reports
from .schema import DEFAULT_SCHEMA_DIR, load_dispositions, load_schemas


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline")
    ap.add_argument("--db-url", help="Postgres URL (default: $DATABASE_URL or local docker)")
    ap.add_argument("--schemas", type=Path, default=DEFAULT_SCHEMA_DIR)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest", help="ingest one or more drop directories, in the order given")
    p_ingest.add_argument("drop_dirs", type=Path, nargs="+", metavar="drop_dir")

    p_report = sub.add_parser("report", help="write report files")
    p_report.add_argument("--out", type=Path, required=True)

    sub.add_parser("reset", help="drop and recreate all pipeline tables")

    args = ap.parse_args(argv)
    conn = db.connect(args.db_url)
    try:
        if args.cmd == "reset":
            db.reset_db(conn)
            print("database reset")
            return 0

        db.init_db(conn)
        db.sync_dispositions(conn, load_dispositions(args.schemas))

        if args.cmd == "ingest":
            for d in args.drop_dirs:
                if not d.is_dir():
                    print(f"error: {d} is not a directory", file=sys.stderr)
                    return 2
            vault = PiiVault.from_env()
            schemas = load_schemas(args.schemas)
            try:
                for d in args.drop_dirs:
                    print(f"ingesting {d}")
                    # Leads first, then events. Order doesn't affect results
                    # (linking happens at read time) but it reads naturally.
                    ingest_leads(conn, vault, schemas, d)
                    ingest_events(conn, vault, d)
            except FileConflict as e:
                print(f"error: {e}", file=sys.stderr)
                return 1
            return 0

        if args.cmd == "report":
            write_reports(conn, args.out)
            print(f"reports written to {args.out}")
            return 0
    finally:
        conn.close()
    return 0
