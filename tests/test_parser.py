"""Parser tests — deterministic, no API calls, no GCP."""
import json
import pytest
from tributary.parser.event import parse, EventType


def msg(payload: dict) -> bytes:
    return json.dumps(payload).encode()


# ── Event type detection ──────────────────────────────────────────────────────

def test_document_upload_detected():
    event = parse(msg({"type": "document_upload", "entity_id": "e1"}), "topic/docs")
    assert event.event_type == EventType.DOCUMENT_UPLOAD

def test_form_submission_detected():
    event = parse(msg({"type": "form_submission", "entity_id": "p1"}), "topic/forms")
    assert event.event_type == EventType.FORM_SUBMISSION

def test_payment_event_detected():
    event = parse(msg({"type": "payment", "amount": 150.00}), "topic/billing")
    assert event.event_type == EventType.PAYMENT_EVENT

def test_status_change_detected():
    event = parse(msg({"type": "status_change", "status": "approved"}), "topic/status")
    assert event.event_type == EventType.STATUS_CHANGE

def test_unknown_type_for_garbage():
    event = parse(msg({"type": "banana"}), "topic/misc")
    assert event.event_type == EventType.UNKNOWN

def test_invalid_json_returns_unknown():
    event = parse(b"not json at all", "topic/bad")
    assert event.event_type == EventType.UNKNOWN
    assert len(event.parse_errors) > 0


# ── Amount normalisation ──────────────────────────────────────────────────────

def test_amount_dollar_to_cents():
    event = parse(msg({"type": "payment", "amount": 99.99}), "t")
    assert event.amount_cents == 9999

def test_amount_already_cents():
    event = parse(msg({"type": "payment", "amount_cents": 5000}), "t")
    assert event.amount_cents == 5000

def test_amount_invalid_ignored():
    event = parse(msg({"type": "payment", "amount": "not-a-number"}), "t")
    assert event.amount_cents is None
    assert any("amount_parse_error" in e for e in event.parse_errors)


# ── PII detection ─────────────────────────────────────────────────────────────

def test_email_detected():
    event = parse(msg({"type": "form", "email": "patient@example.com"}), "t")
    assert "email" in event.pii_fields_detected
    assert event.pii_hash is not None

def test_ssn_detected():
    event = parse(msg({"type": "form", "ssn": "123-45-6789"}), "t")
    assert "ssn" in event.pii_fields_detected
    assert event.phi_present is False   # SSN is PII but not PHI unless clinical context

def test_mrn_detected():
    event = parse(msg({"type": "form", "mrn": "MRN: ABC123456"}), "t")
    assert "mrn" in event.pii_fields_detected

def test_phi_keyword_detected():
    event = parse(msg({"type": "status", "note": "patient diagnosis confirmed"}), "t")
    assert event.phi_present is True

def test_no_pii_in_clean_event():
    event = parse(msg({"type": "system", "action": "ping"}), "t")
    assert event.pii_fields_detected == []
    assert event.pii_hash is None
    assert event.phi_present is False

def test_pii_hash_consistent():
    payload = {"type": "form", "email": "test@example.com"}
    e1 = parse(msg(payload), "t")
    e2 = parse(msg(payload), "t")
    assert e1.pii_hash == e2.pii_hash

def test_pii_values_not_in_structured_fields():
    """PII values must never appear in the ParsedEvent structured fields."""
    event = parse(msg({
        "type": "form",
        "email": "secret@patient.com",
        "ssn": "123-45-6789",
        "name": "Jane Doe",
        "entity_id": "safe-opaque-id",
    }), "t")
    # Stringify all structured fields and check no PII leaked
    safe_fields = str([
        event.entity_id, event.entity_type, event.action, event.status,
        event.document_type, event.document_reference, event.summary,
        event.category, event.subcategory,
    ])
    assert "secret@patient.com" not in safe_fields
    assert "123-45-6789" not in safe_fields


# ── Event ID generation ───────────────────────────────────────────────────────

def test_event_id_from_payload():
    event = parse(msg({"event_id": "my-id-123", "type": "system"}), "t")
    assert event.event_id == "my-id-123"

def test_event_id_generated_when_missing():
    event = parse(msg({"type": "system"}), "t")
    assert len(event.event_id) == 16   # SHA-256 truncated to 16 hex chars

def test_source_topic_preserved():
    event = parse(msg({"type": "system"}), "projects/my-project/subscriptions/my-sub")
    assert event.source_topic == "projects/my-project/subscriptions/my-sub"
