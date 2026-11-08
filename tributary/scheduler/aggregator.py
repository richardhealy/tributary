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
import re
from datetime import date, datetime, timezone, timedelta
from fastapi import FastAPI, Request, Header, HTTPException
import hmac

log = logging.getLogger("tributary.aggregator")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="tributary-aggregator", version="0.1.0")

# The day's summary rows are replaced in one MERGE, so a Cloud Scheduler retry
# or a manual backfill of the same day updates its rows instead of adding a
# second copy. Groups that no longer appear for the day are deleted.
#
# target_date is always a DATE query parameter (@target_date), never text
# formatted into the SQL. Only the project ID, which comes from the
# environment, is formatted in, because table names cannot be parameters.
AGGREGATION_SQL = """\
MERGE `{project}.tributary.events_summary` AS target
USING (
  SELECT
    @target_date                                AS summary_date,
    source_topic,
    event_type,
    COALESCE(category, 'unknown')               AS category,
    COALESCE(urgency, 'unknown')                AS urgency,
    COUNT(*)                                    AS event_count,
    COUNTIF(phi_present)                        AS phi_event_count,
    AVG(amount_cents)                           AS avg_amount_cents,
    SUM(amount_cents)                           AS total_amount_cents,
    COUNTIF(ARRAY_LENGTH(parse_errors) > 0)     AS parse_error_count,
    AVG(enrichment_confidence)                  AS avg_enrichment_confidence,
    CURRENT_TIMESTAMP()                         AS computed_at
  FROM `{project}.tributary.events`
  WHERE event_date = @target_date
  GROUP BY source_topic, event_type, COALESCE(category, 'unknown'), COALESCE(urgency, 'unknown')
) AS source
ON  target.summary_date = source.summary_date
AND target.source_topic = source.source_topic
AND target.event_type   = source.event_type
AND target.category     = source.category
AND target.urgency      = source.urgency
WHEN MATCHED THEN UPDATE SET
  event_count               = source.event_count,
  phi_event_count           = source.phi_event_count,
  avg_amount_cents          = source.avg_amount_cents,
  total_amount_cents        = source.total_amount_cents,
  parse_error_count         = source.parse_error_count,
  avg_enrichment_confidence = source.avg_enrichment_confidence,
  computed_at               = source.computed_at
WHEN NOT MATCHED THEN INSERT
  (summary_date, source_topic, event_type, category, urgency,
   event_count, phi_event_count, avg_amount_cents, total_amount_cents,
   parse_error_count, avg_enrichment_confidence, computed_at)
VALUES
  (source.summary_date, source.source_topic, source.event_type, source.category, source.urgency,
   source.event_count, source.phi_event_count, source.avg_amount_cents, source.total_amount_cents,
   source.parse_error_count, source.avg_enrichment_confidence, source.computed_at)
WHEN NOT MATCHED BY SOURCE AND target.summary_date = @target_date THEN DELETE
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
WHERE event_date = @target_date
  AND phi_present = TRUE
ORDER BY
  CASE urgency WHEN 'critical' THEN 1 WHEN 'high' THEN 2
               WHEN 'medium' THEN 3 ELSE 4 END,
  received_at
"""


_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _parse_target_date(value: object) -> date:
    """Accept only a real calendar date written as YYYY-MM-DD.

    date.fromisoformat alone also accepts forms such as "20261101" and
    "2026-W44-7", so the shape is checked first.
    """
    if isinstance(value, str) and _ISO_DATE.fullmatch(value):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise HTTPException(status_code=400, detail="target_date must be a date in YYYY-MM-DD form")


def _bigquery_client(project: str):
    """Build the BigQuery client. Tests replace this with a stub."""
    from google.cloud import bigquery
    return bigquery.Client(project=project)


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
        pass   # no body, or not JSON: use the default date
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    # Allow override of target date (useful for backfill)
    if "target_date" in body:
        target_date = _parse_target_date(body["target_date"])
    else:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    project = os.environ.get("GCP_PROJECT_ID", "")
    if not project:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_ID not set")

    try:
        from google.cloud import bigquery
        client = _bigquery_client(project)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("target_date", "DATE", target_date)]
        )

        # Run aggregation
        agg_sql = AGGREGATION_SQL.format(project=project)
        agg_job = client.query(agg_sql, job_config=job_config)
        agg_job.result()   # wait for completion

        # PHI review report
        phi_sql = PHI_REVIEW_SQL.format(project=project)
        phi_results = list(client.query(phi_sql, job_config=job_config).result())

        log.info("aggregation complete date=%s phi_events=%d", target_date, len(phi_results))

        return {
            "target_date": target_date.isoformat(),
            "aggregation": "complete",
            "phi_events_flagged": len(phi_results),
            "phi_event_ids": [row["event_id"] for row in phi_results[:20]],  # cap list
        }

    except Exception as e:
        log.exception("aggregation_error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
