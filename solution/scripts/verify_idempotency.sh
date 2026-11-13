#!/usr/bin/env bash
# Step 7 check: incremental == all-at-once, and re-running changes nothing.
#
#   solution/scripts/verify_idempotency.sh [data_dir]      (default: data)
#
# WARNING: resets the pipeline database. Run from the repo root.
set -euo pipefail

DATA="${1:-data}"
PY="${PYTHON:-python}"
RUN="$PY solution/main.py"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

# Fingerprint of everything stored (row counts + content hash per table).
db_fingerprint() {
  $PY - <<'EOF'
import sys
sys.path.insert(0, "solution")
from pipeline import db
c = db.connect()
for t, order in [("leads", "workflow_id, phone_hash"), ("lead_rejections", "workflow_id, batch_id, row_number"),
                 ("lead_file_stats", "workflow_id, batch_id"), ("events", "event_id"),
                 ("event_file_stats", "file_key"), ("ingested_files", "file_key")]:
    # leads.updated_at / ingested_at are timestamps of *when*, so leave them out.
    cols = {"leads": "workflow_id, phone_hash, phone_last4, external_ref, batch_id, attributes",
            "ingested_files": "file_key, kind, sha256, drop_name"}.get(t, "*")
    n, h = c.execute(f"SELECT count(*), md5(coalesce(string_agg(r::text, '|' ORDER BY r::text), '')) "
                     f"FROM (SELECT {cols} FROM {t}) r").fetchone()
    print(f"  {t:<18} rows={n:<6} md5={h}")
EOF
}

step() { printf '\n== %s\n' "$*"; }
same() {  # same <label> <dirA> <dirB>
  if diff -r "$2" "$3" >/dev/null; then echo "  PASS  $1"; else echo "  FAIL  $1"; diff -r "$2" "$3" | head -20; FAILED=1; fi
}
FAILED=0

step "A. incremental: drop_1, then drop_2"
$RUN reset >/dev/null
$RUN ingest "$DATA/drop_1" >/dev/null
$RUN report --out "$OUT/after_drop1" >/dev/null
$RUN ingest "$DATA/drop_2" >/dev/null
$RUN report --out "$OUT/incremental" >/dev/null
db_fingerprint | tee "$OUT/fp_before_rerun"

step "B. re-run: ingest drop_1 and drop_2 again on the same database"
$RUN ingest "$DATA/drop_1" "$DATA/drop_2" | grep -c "skip" | xargs -I{} echo "  {} files skipped as already ingested"
$RUN report --out "$OUT/rerun" >/dev/null
db_fingerprint | tee "$OUT/fp_after_rerun"

step "C. all at once: fresh database, both drops in one command"
$RUN reset >/dev/null
$RUN ingest "$DATA/drop_1" "$DATA/drop_2" >/dev/null
$RUN report --out "$OUT/all_at_once" >/dev/null

step "Results"
if cmp -s "$OUT/fp_before_rerun" "$OUT/fp_after_rerun"; then
  echo "  PASS  re-run leaves the database unchanged"
else
  echo "  FAIL  re-run changed the database"; FAILED=1
fi
same "re-run gives identical reports"        "$OUT/incremental" "$OUT/rerun"
same "incremental == all at once (reports)"    "$OUT/incremental" "$OUT/all_at_once"

before=$(grep orphan_events "$OUT/after_drop1/data_quality.json" | tr -dc 0-9)
after=$(grep orphan_events "$OUT/incremental/data_quality.json" | tr -dc 0-9)
if [ "$after" -lt "$before" ]; then
  echo "  PASS  late leads adopt earlier orphans (orphan_events $before -> $after)"
else
  echo "  FAIL  orphan_events did not drop ($before -> $after)"; FAILED=1
fi

echo
[ "$FAILED" = 0 ] && echo "ALL CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
