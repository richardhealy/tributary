"""
LLM enrichment layer.

Runs AFTER the deterministic parser. Fills enrichment slots
(category, subcategory, urgency, summary) that the parser
left as None. Uses a closed vocabulary for structured fields
so the output is always BigQuery-safe.

HIPAA note: the raw message payload is NEVER sent to the LLM.
Only the structured, PII-free ParsedEvent fields are sent.
If phi_present=True, enrichment is skipped and the event is
flagged for human review.
"""
from __future__ import annotations
import asyncio
import json
import os
from typing import Any, Optional
from tributary.parser.event import ParsedEvent


# ── Closed vocabularies (what the LLM is allowed to output) ───────────────────

VALID_CATEGORIES = frozenset([
    "billing", "clinical", "administrative", "operational",
    "compliance", "technical", "communication", "unknown",
])

VALID_SUBCATEGORIES = frozenset([
    "invoice", "payment", "refund", "claim",
    "lab", "prescription", "appointment", "referral",
    "upload", "form", "status_update", "notification",
    "error", "auth", "audit", "other",
])

VALID_URGENCY = frozenset(["critical", "high", "medium", "low", "informational"])

ENRICHMENT_SYSTEM = """\
You are a data enrichment assistant for an operational event pipeline.
You receive structured metadata about an event (NO raw content, NO PII).
Classify the event into the closed vocabularies provided.
Reply ONLY with a JSON object — no other text.
"""


async def enrich(
    event: ParsedEvent,
    llm_client: Any = None,
) -> ParsedEvent:
    """
    Enrich a ParsedEvent with LLM classification.

    HIPAA guard: if phi_present is True, skip LLM enrichment entirely
    and mark the event for human review. Raw content is never sent.
    """
    if event.phi_present:
        event.category = "clinical"
        event.subcategory = "other"
        event.urgency = "high"
        event.summary = "[PHI PRESENT — enrichment skipped, flagged for human review]"
        event.enrichment_confidence = 0.0
        return event

    if llm_client is None:
        llm_client = _build_client()

    prompt = _build_prompt(event)

    try:
        raw = await _call_llm(llm_client, ENRICHMENT_SYSTEM, prompt)
        parsed = json.loads(raw.strip().lstrip("```json").rstrip("```").strip())

        category    = parsed.get("category", "unknown")
        subcategory = parsed.get("subcategory", "other")
        urgency     = parsed.get("urgency", "low")
        summary     = str(parsed.get("summary", ""))[:500]   # cap length
        confidence  = float(parsed.get("confidence", 0.5))

        # Enforce closed vocabulary
        event.category    = category    if category    in VALID_CATEGORIES    else "unknown"
        event.subcategory = subcategory if subcategory in VALID_SUBCATEGORIES else "other"
        event.urgency     = urgency     if urgency     in VALID_URGENCY       else "low"
        event.summary     = summary
        event.enrichment_confidence = max(0.0, min(1.0, confidence))

    except (json.JSONDecodeError, ValueError, KeyError) as e:
        event.category    = "unknown"
        event.subcategory = "other"
        event.urgency     = "low"
        event.summary     = f"[enrichment_parse_error: {e}]"
        event.enrichment_confidence = 0.0

    return event


def _build_prompt(event: ParsedEvent) -> str:
    """Build the enrichment prompt — only PII-free structured fields."""
    return f"""Classify this operational event. Use ONLY the provided vocabularies.

Event metadata (no PII):
- event_type: {event.event_type.value}
- entity_type: {event.entity_type or 'unknown'}
- action: {event.action or 'unknown'}
- status: {event.status or 'unknown'}
- document_type: {event.document_type or 'none'}
- amount_cents: {event.amount_cents or 'none'}
- source_topic: {event.source_topic}

Valid categories: {sorted(VALID_CATEGORIES)}
Valid subcategories: {sorted(VALID_SUBCATEGORIES)}
Valid urgency levels: {sorted(VALID_URGENCY)}

Reply with JSON:
{{
  "category": "<one of the valid categories>",
  "subcategory": "<one of the valid subcategories>",
  "urgency": "<one of the valid urgency levels>",
  "summary": "<one sentence, max 100 words, no PII>",
  "confidence": <0.0-1.0>
}}"""


def _build_client() -> Any:
    provider = os.getenv("LLM_PROVIDER", "anthropic")
    if provider == "anthropic":
        from anthropic import Anthropic
        return {"type": "anthropic", "client": Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])}
    from openai import AsyncOpenAI
    return {"type": "openai", "client": AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])}


async def _call_llm(client: Any, system: str, user: str) -> str:
    if callable(client):
        return client(system, user)

    model = os.getenv("LLM_MODEL", "claude-haiku-4-5")

    if isinstance(client, dict) and client.get("type") == "anthropic":
        resp = await asyncio.to_thread(
            client["client"].messages.create,
            model=model, max_tokens=256,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return resp.content[0].text

    if isinstance(client, dict) and client.get("type") == "openai":
        resp = await client["client"].chat.completions.create(
            model=model, max_tokens=256,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content or ""

    raise ValueError(f"Unknown client: {type(client)}")
