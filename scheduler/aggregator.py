"""
Cloud Scheduler job — fired daily by Cloud Scheduler → Cloud Run.

Runs a BigQuery aggregation over the previous day's events and
writes results to events_summary. Designed to run as a separate
Cloud Run service on a POST /run endpoint, triggered by Cloud Scheduler
via HTTP target.

Also runs a daily PHI review flag — surfaces any events where
phi_present=True that have not yet been reviewed, for compliance.
"""
from __future__ import annotations
import logging
import os
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, Header, HTTPException
import hmac

log = logging.getLogger("tributary.aggregator")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="tributary-aggregator", version="0.1.0")

AGGREGATION_SQL = """\
INSERT INTO `{project}.tributary.events_summary`
  (summary_date, source_topic, event_type, category, urgency,
   event_count, phi_event_count, avg_amount_cents, total_amount_cents,
   parse_error_count, avg_enrichment_confidence, computed_at)
SELECT
  DATE('{target_date}')                       AS summary_date,
  source_topic,
  event_type,
  COALESCE(category, 'unknown')               AS category,
  COALESCE(urgency,  'unknown')               AS urgency,
  COUNT(*)                                    AS event_count,
  COUNTIF(phi_present)                        AS phi_event_count,
  AVG(amount_cents)                           AS avg_amount_cents,
  SUM(amount_cents)                           AS total_amount_cents,
  COUNTIF(ARRAY_LENGTH(parse_errors) > 0)     AS parse_error_count,
  AVG(enrichment_confidence)                  AS avg_enrichment_confidence,
  CURRENT_TIMESTAMP()                         AS computed_at
FROM `{project}.tributary.events`
WHERE event_date = '{target_date}'
GROUP BY source_topic, event_type, category, urgency
"""

PHI_REVIEW_SQL = """\
SELECT
  event_id,
  received_at,
  event_type,
  entity_type,
  pii_fields_detected,
  urgency
FROM `{project}.tributary.events`
WHERE event_date = '{target_date}'
  AND phi_present = TRUE
ORDER BY
  CASE urgency WHEN 'critical' THEN 1 WHEN 'high' THEN 2
               WHEN 'medium' THEN 3 ELSE 4 END,
  received_at
"""


def _verify_scheduler_token(authorization: str | None) -> bool:
    expected = os.environ.get("SCHEDULER_TOKEN", "")
    if not expected:
        return True
    if not authorization:
        return False
    _, _, token = authorization.partition(" ")
    return hmac.compare_digest(token, expected)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "tributary-aggregator"}


@app.post("/run")
async def run_aggregation(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """
    Run daily aggregation for the previous day.
    Called by Cloud Scheduler. Returns summary of rows written.
    """
    if not _verify_scheduler_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    # Allow override of target date (useful for backfill)
    if "target_date" in body:
        target_date = body["target_date"]
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")

    project = os.environ.get("GCP_PROJECT_ID", "")
    if not project:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_ID not set")

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)

        # Run aggregation
        agg_sql = AGGREGATION_SQL.format(project=project, target_date=target_date)
        agg_job = client.query(agg_sql)
        agg_job.result()   # wait for completion

        # PHI review report
        phi_sql = PHI_REVIEW_SQL.format(project=project, target_date=target_date)
        phi_results = list(client.query(phi_sql).result())

        log.info("aggregation complete date=%s phi_events=%d", target_date, len(phi_results))

        return {
            "target_date": target_date,
            "aggregation": "complete",
            "phi_events_flagged": len(phi_results),
            "phi_event_ids": [row["event_id"] for row in phi_results[:20]],  # cap list
        }

    except Exception as e:
        log.exception("aggregation_error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
