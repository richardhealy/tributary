"""
Cloud Run service — Pub/Sub push subscriber.

Google Cloud pushes Pub/Sub messages to POST /pubsub/push as JSON.
This service:
  1. Validates the request (bearer token check)
  2. Decodes the Pub/Sub envelope
  3. Runs the deterministic parser
  4. Runs LLM enrichment (unless PHI present)
  5. Writes to BigQuery
  6. Returns 200 to ack, 4xx/5xx to nack (Pub/Sub will retry)

Cloud Run is stateless; all state goes to BigQuery.
"""
from __future__ import annotations
import asyncio
import base64
import hashlib
import hmac
import logging
import os
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from tributary.parser.event import parse
from tributary.enrichment.llm import enrich
from tributary.bigquery.writer import BigQueryWriter

log = logging.getLogger("tributary.subscriber")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

app = FastAPI(title="tributary-subscriber", version="0.1.0")
_writer: BigQueryWriter | None = None


def _get_writer() -> BigQueryWriter:
    global _writer
    if _writer is None:
        _writer = BigQueryWriter()
    return _writer


def _verify_token(authorization: str | None) -> bool:
    """Verify the Pub/Sub push subscription bearer token."""
    expected = os.environ.get("PUBSUB_VERIFICATION_TOKEN", "")
    if not expected:
        return True   # dev mode — no token configured
    if not authorization:
        return False
    scheme, _, token = authorization.partition(" ")
    return scheme.lower() == "bearer" and hmac.compare_digest(token, expected)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "tributary-subscriber"}


@app.post("/pubsub/push")
async def pubsub_push(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """
    Pub/Sub push endpoint.
    Returns 200 to ack, 500 to nack and trigger retry.
    Never logs PII/PHI.
    """
    if not _verify_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # ── Decode Pub/Sub envelope ───────────────────────────────────────────────
    message = body.get("message", {})
    if not message:
        raise HTTPException(status_code=400, detail="Missing message field")

    raw_data_b64 = message.get("data", "")
    try:
        raw_bytes = base64.b64decode(raw_data_b64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 data")

    source_topic = body.get("subscription", "unknown")

    # ── Parse → Enrich → Write ────────────────────────────────────────────────
    try:
        event = parse(raw_bytes, source_topic)

        # Never log raw content or PII; log only safe metadata
        log.info(
            "parsed event",
            extra={
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "phi_present": event.phi_present,
                "parse_errors": len(event.parse_errors),
            },
        )

        event = await enrich(event)
        errors = _get_writer().insert_event(event)

        if errors:
            log.error("bq_insert_errors event_id=%s errors=%s", event.event_id, errors)
            return JSONResponse(status_code=500, content={"error": "BigQuery insert failed"})

        return JSONResponse(status_code=200, content={
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "category": event.category,
            "urgency": event.urgency,
        })

    except Exception as e:
        log.exception("pipeline_error: %s", str(e))
        return JSONResponse(status_code=500, content={"error": "pipeline error"})
