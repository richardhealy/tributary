"""Enrichment tests — stub LLM, no API calls."""
import asyncio
import json
import pytest
from tributary.parser.event import parse, ParsedEvent, EventType
from tributary.enrichment.llm import enrich, VALID_CATEGORIES, VALID_URGENCY


def stub_llm(response: str):
    def client(system, user):
        return response
    return client

def base_event(**kwargs) -> ParsedEvent:
    defaults = dict(
        event_id="test-001", source_topic="t",
        received_at=__import__('datetime').datetime.now(__import__('datetime').timezone.utc),
        event_type=EventType.PAYMENT_EVENT,
        entity_id="e1", entity_type="claim", action="submitted",
        status="pending", amount_cents=5000, document_type=None,
        document_reference=None, pii_fields_detected=[], pii_hash=None,
        phi_present=False, raw_message_size_bytes=100, parse_errors=[],
    )
    defaults.update(kwargs)
    return ParsedEvent(**defaults)


def test_valid_enrichment_applied():
    response = json.dumps({
        "category": "billing", "subcategory": "claim",
        "urgency": "medium", "summary": "Claim submitted.", "confidence": 0.9
    })
    event = asyncio.run(enrich(base_event(), stub_llm(response)))
    assert event.category == "billing"
    assert event.subcategory == "claim"
    assert event.urgency == "medium"
    assert event.enrichment_confidence == 0.9


def test_invalid_category_falls_back_to_unknown():
    response = json.dumps({
        "category": "invented_category", "subcategory": "claim",
        "urgency": "high", "summary": "Test.", "confidence": 0.8
    })
    event = asyncio.run(enrich(base_event(), stub_llm(response)))
    assert event.category == "unknown"


def test_phi_present_skips_llm():
    """If phi_present=True, the LLM must never be called."""
    call_count = [0]
    def counting_client(system, user):
        call_count[0] += 1
        return json.dumps({"category": "clinical", "subcategory": "other",
                           "urgency": "high", "summary": "", "confidence": 0.0})

    event = asyncio.run(enrich(base_event(phi_present=True), counting_client))
    assert call_count[0] == 0
    assert event.enrichment_confidence == 0.0
    assert "PHI PRESENT" in event.summary


def test_malformed_llm_response_falls_back_gracefully():
    event = asyncio.run(enrich(base_event(), stub_llm("not json at all")))
    assert event.category == "unknown"
    assert event.enrichment_confidence == 0.0
    assert "enrichment_parse_error" in (event.summary or "")


def test_summary_capped_at_500_chars():
    long_summary = "A" * 1000
    response = json.dumps({
        "category": "billing", "subcategory": "invoice",
        "urgency": "low", "summary": long_summary, "confidence": 0.7
    })
    event = asyncio.run(enrich(base_event(), stub_llm(response)))
    assert len(event.summary or "") <= 500
