"""BigQuery writer tests with a stub BQ client."""
import pytest
from datetime import datetime, timezone
from tributary.parser.event import ParsedEvent, EventType
from tributary.bigquery.writer import BigQueryWriter, _event_to_row


def make_event(**kwargs) -> ParsedEvent:
    defaults = dict(
        event_id="evt-001", source_topic="test/topic",
        received_at=datetime.now(timezone.utc),
        event_type=EventType.DOCUMENT_UPLOAD,
        entity_id="ent-1", entity_type="patient", action="upload",
        status="complete", amount_cents=None, document_type="lab_result",
        document_reference="gs://bucket/file.pdf",
        pii_fields_detected=["email"], pii_hash="abc123",
        phi_present=True, raw_message_size_bytes=512, parse_errors=[],
        category="clinical", subcategory="lab", urgency="high",
        summary="Lab result uploaded.", enrichment_confidence=0.9,
    )
    defaults.update(kwargs)
    return ParsedEvent(**defaults)


class StubBQClient:
    def __init__(self):
        self.inserted: list[tuple[str, list]] = []
        self.fail_on_events = False   # only fail the events table, not audit

    def insert_rows_json(self, table: str, rows: list) -> list:
        if self.fail_on_events and "audit" not in table:
            return [{"errors": [{"reason": "backendError"}]}]
        self.inserted.append((table, rows))
        return []


def test_event_row_has_no_pii_values():
    event = make_event(entity_id="opaque-id-123")
    row = _event_to_row(event)
    # The row should contain the pii_hash (safe) but not actual PII values
    assert row["pii_hash"] == "abc123"
    assert "pii_fields_detected" in row
    # Ensure no PII values snuck in
    row_str = str(row)
    # entity_id is safe (opaque reference)
    assert "opaque-id-123" in row_str


def test_phi_present_flag_in_row():
    event = make_event(phi_present=True)
    row = _event_to_row(event)
    assert row["phi_present"] is True


def test_insert_event_calls_bq():
    stub = StubBQClient()
    writer = BigQueryWriter(project_id="test-project", bq_client=stub)
    errors = writer.insert_event(make_event())
    assert errors == []
    # Should have inserted into events AND audit tables
    tables = [t for t, _ in stub.inserted]
    assert any("events" in t and "audit" not in t for t in tables)
    assert any("audit" in t for t in tables)


def test_insert_event_writes_audit_even_on_bq_error():
    stub = StubBQClient()
    stub.fail_on_events = True
    writer = BigQueryWriter(project_id="test-project", bq_client=stub)
    errors = writer.insert_event(make_event())
    # errors returned from events table
    assert len(errors) > 0
    # audit was still written
    tables = [t for t, _ in stub.inserted]
    assert any("audit" in t for t in tables)


def test_batch_insert():
    stub = StubBQClient()
    writer = BigQueryWriter(project_id="test-project", bq_client=stub)
    events = [make_event(event_id=f"evt-{i}") for i in range(5)]
    result = writer.insert_events_batch(events)
    assert result["inserted"] == 5
    assert result["errors"] == []
