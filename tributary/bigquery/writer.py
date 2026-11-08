"""
BigQuery writer — inserts ParsedEvents into the events table
and writes audit records.
"""
from __future__ import annotations
import os
import uuid
from datetime import datetime, timezone
from typing import Optional
from tributary.parser.event import ParsedEvent

try:
    from google.cloud import bigquery as _bq
    BQ_AVAILABLE = True
except ImportError:
    BQ_AVAILABLE = False


def _event_to_row(event: ParsedEvent) -> dict:
    return {
        "event_id":               event.event_id,
        "source_topic":           event.source_topic,
        "received_at":            event.received_at.isoformat(),
        "event_date":             event.received_at.date().isoformat(),
        "event_type":             event.event_type.value,
        "entity_id":              event.entity_id,
        "entity_type":            event.entity_type,
        "action":                 event.action,
        "status":                 event.status,
        "amount_cents":           event.amount_cents,
        "document_type":          event.document_type,
        "document_reference":     event.document_reference,
        "pii_fields_detected":    event.pii_fields_detected,
        "pii_hash":               event.pii_hash,
        "phi_present":            event.phi_present,
        "category":               event.category,
        "subcategory":            event.subcategory,
        "urgency":                event.urgency,
        "summary":                event.summary,
        "enrichment_confidence":  event.enrichment_confidence,
        "raw_message_size_bytes": event.raw_message_size_bytes,
        "parse_errors":           event.parse_errors,
        "parse_version":          event.parse_version,
        "inserted_at":            datetime.now(timezone.utc).isoformat(),
    }


def _audit_row(event_id: str, action: str, actor: str, details: Optional[dict] = None) -> dict:
    import json
    return {
        "audit_id":    str(uuid.uuid4()),
        "event_id":    event_id,
        "audit_action": action,
        "actor":       actor,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "details":     json.dumps(details or {}),
    }


class BigQueryWriter:
    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: str = "tributary",
        bq_client=None,
    ):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID", "test-project")
        self.dataset_id = dataset_id
        self._client = bq_client   # injected in tests; real client built lazily

    def _get_client(self):
        if self._client is not None:
            return self._client
        if not BQ_AVAILABLE:
            raise RuntimeError("google-cloud-bigquery not installed")
        from google.cloud import bigquery
        self._client = bigquery.Client(project=self.project_id)
        return self._client

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def insert_event(self, event: ParsedEvent, actor: str = "tributary-subscriber") -> list[dict]:
        """
        Insert one event into the events table and write an audit record.
        Returns any BigQuery insertion errors (empty list = success).
        """
        client = self._get_client()
        row = _event_to_row(event)
        errors = client.insert_rows_json(self._table_ref("events"), [row])

        # Write audit record regardless of insert outcome
        audit = _audit_row(
            event_id=event.event_id,
            action="ingested",
            actor=actor,
            details={
                "event_type": event.event_type.value,
                "phi_present": event.phi_present,
                "parse_errors": event.parse_errors,
                "bq_errors": errors,
            },
        )
        client.insert_rows_json(self._table_ref("events_audit"), [audit])
        return errors

    def insert_events_batch(
        self, events: list[ParsedEvent], actor: str = "tributary-subscriber"
    ) -> dict:
        """Batch insert up to 500 events."""
        client = self._get_client()
        rows = [_event_to_row(e) for e in events]
        errors = client.insert_rows_json(self._table_ref("events"), rows)

        audit_rows = [
            _audit_row(e.event_id, "ingested", actor, {"phi_present": e.phi_present})
            for e in events
        ]
        client.insert_rows_json(self._table_ref("events_audit"), audit_rows)
        return {"inserted": len(rows), "errors": errors}
