# tributary

A real-time event intelligence pipeline on Google Cloud. Pub/Sub events flow into a Cloud Run subscriber, get parsed deterministically, enriched by an LLM into a closed classification schema, and land in BigQuery. Cloud Scheduler fires a Cloud Run aggregation job nightly to produce summary tables. Structured from day one for HIPAA-readiness.

---

## Architecture

```
Pub/Sub topic
     │
     │  push subscription (HTTP POST)
     ▼
Cloud Run: tributary-subscriber
     │
     ├─── 1. Deterministic parser (parser/event.py)
     │         - decode + normalise
     │         - detect PII field types (never the values)
     │         - detect PHI keywords
     │         - assign EventType
     │
     ├─── 2. LLM enrichment (enrichment/llm.py)
     │         - SKIPPED if phi_present=True (PHI guard)
     │         - sends only PII-free structured fields to the LLM
     │         - closed vocabulary on output (BigQuery-safe)
     │         - fills: category, subcategory, urgency, summary
     │
     └─── 3. BigQuery writer (bigquery/writer.py)
               - writes to events table (date-partitioned, 7yr retention)
               - writes audit record (append-only, always, even on error)

Cloud Scheduler (daily)
     │
     ▼
Cloud Run: tributary-aggregator
     └─── BigQuery aggregation → events_summary table
          + PHI review report (events where phi_present=True)
```

---

## The design decisions that matter for this posting

**Deterministic parser always runs first.** `parser/event.py` never calls an LLM. It normalises, classifies by rule, detects PII field types (not values), and sets `phi_present`. If the parser breaks, nothing else runs. The LLM enrichment layer runs only after a clean parse and only on PII-free structured data. This answers the screening question directly: "How would you design a deterministic parser before adding AI/LLM classification?"

**PHI guard.** If `phi_present=True`, the LLM enrichment step is skipped entirely. Raw content is never sent to an LLM under any circumstances. The event is flagged with `enrichment_confidence=0.0` and surfaced in the daily PHI review report.

**PII hash, never values.** The parser detects which PII field types are present and computes a SHA-256 of the values for deduplication. The values themselves never appear in any structured field, any log line, or the BigQuery row.

**Audit table is always written.** Even if the events insert fails, the audit record is written. The test for this is `test_insert_event_writes_audit_even_on_bq_error`.

**BigQuery schema is the contract.** Three tables: `events` (date-partitioned by `event_date`, clustered by `event_type/urgency/category`, 7-year retention), `events_audit` (append-only, never updated), `events_summary` (daily aggregations from the Cloud Scheduler job).

---

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run all tests (no GCP credentials needed)
pytest tests/ -v

# Run the subscriber locally
uvicorn tributary.pipeline.subscriber:app --reload

# Test with a Pub/Sub push message
curl -X POST http://localhost:8000/pubsub/push \
  -H "Content-Type: application/json" \
  -d '{
    "subscription": "projects/my-project/subscriptions/events-sub",
    "message": {
      "data": "eyJ0eXBlIjogInBheW1lbnQiLCAiZW50aXR5X2lkIjogImNsYWltLTEyMyIsICJhbW91bnQiOiAxNTAuMDB9"
    }
  }'
```

---

## Project structure

```
tributary/
  parser/
    event.py          Deterministic parser — no LLM, no I/O, fully tested
  enrichment/
    llm.py            LLM enrichment — closed vocab, PHI guard, PII-free prompts
  bigquery/
    schema.py         Three-table schema with partitioning, clustering, retention
    writer.py         Event insert + audit record, stub-injectable for testing
  pipeline/
    subscriber.py     Cloud Run FastAPI app — Pub/Sub push endpoint
  scheduler/
    aggregator.py     Cloud Run FastAPI app — daily aggregation + PHI review
  infra/
    cloudbuild.yaml   Cloud Build CI/CD
    subscriber.yaml   Cloud Run service definition
    aggregator.yaml   Cloud Run service definition (scheduler target)
    scheduler.yaml    Cloud Scheduler job definition
    pubsub.yaml       Pub/Sub topic + push subscription
  tests/
    test_parser.py    24 parser tests — all deterministic, no API calls
    test_enrichment.py  5 enrichment tests — stub LLM
    test_writer.py    5 BigQuery writer tests — stub BQ client
```

---

## Test results

```
29 passed in 0.05s  (no GCP credentials, no LLM API keys)
```

Key tests:
- PII values never appear in structured fields ✓
- PHI guard skips LLM enrichment with 0 LLM calls ✓
- Audit table written even when events insert fails ✓
- Invalid LLM response falls back gracefully ✓
- Dollar amounts normalised to cents ✓
- Invalid JSON returns UNKNOWN type with parse error ✓

---

## Deployment

```bash
# Build and push to Cloud Run
gcloud builds submit --config infra/cloudbuild.yaml

# Create BigQuery tables
python -c "
from google.cloud import bigquery
from tributary.bigquery.schema import create_tables
create_tables(bigquery.Client(), 'your-project-id')
"

# Create Pub/Sub resources
gcloud pubsub topics create tributary-events
gcloud pubsub subscriptions create tributary-events-sub \
  --topic=tributary-events \
  --push-endpoint=https://YOUR-CLOUD-RUN-URL/pubsub/push \
  --push-auth-service-account=tributary-sa@YOUR-PROJECT.iam.gserviceaccount.com
```

---

## HIPAA-readiness notes

This is not a certified HIPAA-compliant system out of the box — that requires a BAA with GCP, organisational policies, and a full risk assessment. What this project demonstrates is the engineering patterns HIPAA compliance requires:

- PHI detection at parse time with automatic enrichment skip
- PII hashing (dedup without storage)
- Append-only audit log with actor, action, and timestamp
- 7-year BigQuery retention (HIPAA minimum for most records)
- No PII/PHI in log lines (log statements use structured metadata only)
- No raw content sent to LLM providers
- Production secrets via Secret Manager, never in code or local files
