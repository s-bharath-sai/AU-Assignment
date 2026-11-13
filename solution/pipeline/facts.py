"""
Derived facts (spec section "Derived facts"), as SQL views over the ledger.

    usable_events  (from db.py: linked, known dispositions only)
        │
        ├── call_facts : one row per counted call
        │      call counted only if its call.initiated is present and linked
        │      connected, telephony_failed, effective disposition, IST date
        │
        └── lead_facts : one row per lead (including leads with no calls)
               totals, last call, current disposition, ever_success, bucket

Everything is computed from current state on every read, so results never
depend on the order drops were ingested in.
"""
from __future__ import annotations

import psycopg

VIEWS_SQL = """
DROP VIEW IF EXISTS lead_facts;
DROP VIEW IF EXISTS call_facts;

-- One pass over usable_events, grouped by call.
CREATE VIEW call_facts AS
WITH per_call AS (
    SELECT workflow_id, call_id,
           -- A call exists only if its call.initiated event is present and
           -- linked (HAVING below). If a call_id were initiated twice, the
           -- earliest one counts.
           MIN(lead_id)     FILTER (WHERE event_type = 'call.initiated') AS lead_id,
           MIN(occurred_at) FILTER (WHERE event_type = 'call.initiated') AS initiated_at,
           BOOL_OR(event_type = 'call.connected')                        AS connected,
           BOOL_OR(event_type = 'call.telephony_failed')                 AS telephony_failed,
           -- Effective disposition: latest override wins; otherwise latest
           -- set. Ties on occurred_at broken by event_id (higher = later).
           (ARRAY_AGG(disposition_code
                      ORDER BY (event_type = 'disposition.override') DESC,
                               occurred_at DESC, event_id DESC)
                FILTER (WHERE event_type IN ('disposition.set', 'disposition.override')))[1]
                                                                         AS effective_disposition
    FROM usable_events
    WHERE call_id IS NOT NULL
    GROUP BY workflow_id, call_id
    HAVING BOOL_OR(event_type = 'call.initiated')
)
SELECT c.workflow_id,
       c.call_id,
       c.lead_id,
       c.initiated_at,
       (c.initiated_at AT TIME ZONE 'Asia/Kolkata')::date AS call_date,   -- IST date
       c.connected,
       c.telephony_failed,
       c.effective_disposition,
       COALESCE(d.is_terminal, FALSE) AS is_terminal,
       COALESCE(d.is_success, FALSE)  AS is_success
FROM per_call c
LEFT JOIN dispositions d ON d.workflow_id = c.workflow_id AND d.code = c.effective_disposition;

-- One pass over call_facts, grouped by lead; every lead appears (LEFT JOIN).
CREATE VIEW lead_facts AS
WITH per_lead AS (
    SELECT lead_id,
           COUNT(*)                          AS total_calls,
           COUNT(*) FILTER (WHERE connected) AS connected_calls,
           MAX(initiated_at)                 AS last_call_at,
           BOOL_OR(is_terminal)              AS any_terminal,
           BOOL_AND(telephony_failed)        AS all_telephony_failed,
           BOOL_OR(is_success)               AS ever_success,
           -- Effective disposition of the most recent call (by initiated time) that has one.
           (ARRAY_AGG(effective_disposition ORDER BY initiated_at DESC, call_id DESC)
                FILTER (WHERE effective_disposition IS NOT NULL))[1] AS current_disposition
    FROM call_facts
    GROUP BY lead_id
)
SELECT l.lead_id,
       l.workflow_id,
       l.external_ref,
       l.batch_id,
       l.phone_last4,
       COALESCE(p.total_calls, 0)      AS total_calls,
       COALESCE(p.connected_calls, 0)  AS connected_calls,
       p.last_call_at,
       p.current_disposition,
       COALESCE(p.ever_success, FALSE) AS ever_success,
       CASE
           WHEN COALESCE(p.total_calls, 0) = 0 THEN 'yet_to_reach_out'
           WHEN p.any_terminal                 THEN 'completed'
           WHEN p.all_telephony_failed         THEN 'in_progress_telephony_failed'
           ELSE 'in_progress_dialed'
       END AS bucket
FROM leads l
LEFT JOIN per_lead p ON p.lead_id = l.lead_id;
"""


def create_views(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(VIEWS_SQL)
