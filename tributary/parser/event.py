"""
Deterministic event parser.

Rule: deterministic parsing ALWAYS runs first and produces a
structured ParsedEvent. The LLM enrichment layer in enrichment/
runs AFTER this and can only fill fields the parser left as None.
The parser never calls an LLM. This separation is the core design
decision: if the deterministic layer breaks, the LLM never fires.

HIPAA note: the parser is the first point where PII fields are
identified and tagged. PII is never written to logs.
"""
from __future__ import annotations
import re
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional
from enum import Enum


class EventType(str, Enum):
    DOCUMENT_UPLOAD   = "document_upload"
    FORM_SUBMISSION   = "form_submission"
    STATUS_CHANGE     = "status_change"
    PAYMENT_EVENT     = "payment_event"
    SYSTEM_EVENT      = "system_event"
    UNKNOWN           = "unknown"


class PIIField(str, Enum):
    NAME    = "name"
    EMAIL   = "email"
    PHONE   = "phone"
    DOB     = "dob"
    SSN     = "ssn"
    ADDRESS = "address"
    MRN     = "mrn"     # medical record number


@dataclass
class ParsedEvent:
    # Identity
    event_id: str
    source_topic: str
    received_at: datetime
    event_type: EventType

    # Normalised payload (no raw PII in structured fields)
    entity_id: Optional[str]          # opaque reference to the subject
    entity_type: Optional[str]        # "patient", "claim", "user", etc.
    action: Optional[str]
    status: Optional[str]
    amount_cents: Optional[int]       # if payment event
    document_type: Optional[str]
    document_reference: Optional[str] # GCS URI, never content

    # PII handling
    pii_fields_detected: list[str]    # which PII field names were present
    pii_hash: Optional[str]           # SHA-256 of canonical PII for dedup (no PII stored)
    phi_present: bool                 # HIPAA: was PHI detected?

    # Enrichment slots (filled by LLM layer, not the parser)
    category: Optional[str]           = None
    subcategory: Optional[str]        = None
    urgency: Optional[str]            = None
    summary: Optional[str]            = None
    enrichment_confidence: Optional[float] = None

    # Lineage
    raw_message_size_bytes: int       = 0
    parse_errors: list[str]           = field(default_factory=list)
    parse_version: str                = "1.0"


# ── PII detection patterns ────────────────────────────────────────────────────

_PII_PATTERNS: list[tuple[PIIField, re.Pattern]] = [
    (PIIField.EMAIL,  re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')),
    (PIIField.PHONE,  re.compile(r'\b(\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b')),
    (PIIField.SSN,    re.compile(r'\b\d{3}-\d{2}-\d{4}\b')),
    (PIIField.DOB,    re.compile(r'\b(0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])[-/](19|20)\d{2}\b')),
    (PIIField.MRN,    re.compile(r'\bMRN[-:\s]*([A-Z0-9]{6,12})\b', re.IGNORECASE)),
]

_PHI_KEYWORDS = frozenset([
    "diagnosis", "medication", "prescription", "lab result", "icd-10",
    "cpt code", "patient", "medical record", "clinical", "treatment",
    "condition", "procedure", "admission", "discharge", "mrn",
])


def _detect_pii(text: str) -> tuple[list[str], Optional[str], bool]:
    """
    Detect PII field types in text. Returns (field_names, pii_hash, phi_present).
    Does NOT return the PII values themselves.
    """
    detected: list[str] = []
    pii_values: list[str] = []

    for field_type, pattern in _PII_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            detected.append(field_type.value)
            pii_values.extend(str(m) for m in matches)

    text_lower = text.lower()
    phi_present = any(kw in text_lower for kw in _PHI_KEYWORDS)

    pii_hash = None
    if pii_values:
        canonical = "|".join(sorted(pii_values))
        pii_hash = hashlib.sha256(canonical.encode()).hexdigest()

    return detected, pii_hash, phi_present


def parse(raw_message: bytes, source_topic: str) -> ParsedEvent:
    """
    Parse a raw Pub/Sub message into a structured ParsedEvent.
    This is the deterministic layer — no LLM calls, no external I/O.
    """
    received_at = datetime.now(timezone.utc)
    errors: list[str] = []
    payload: dict = {}

    # ── Decode ────────────────────────────────────────────────────────────────
    try:
        payload = json.loads(raw_message.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        errors.append(f"decode_error: {e}")

    # ── Event ID ──────────────────────────────────────────────────────────────
    event_id = (
        payload.get("event_id")
        or payload.get("id")
        or hashlib.sha256(raw_message).hexdigest()[:16]
    )

    # ── Event type classification (deterministic rules only) ──────────────────
    raw_type = str(payload.get("type", "") or payload.get("event_type", "")).lower()
    event_type = EventType.UNKNOWN
    if "upload" in raw_type or "document" in raw_type:
        event_type = EventType.DOCUMENT_UPLOAD
    elif "form" in raw_type or "submission" in raw_type:
        event_type = EventType.FORM_SUBMISSION
    elif "status" in raw_type or "state" in raw_type:
        event_type = EventType.STATUS_CHANGE
    elif "payment" in raw_type or "charge" in raw_type or "invoice" in raw_type:
        event_type = EventType.PAYMENT_EVENT
    elif "system" in raw_type or "health" in raw_type or "ping" in raw_type:
        event_type = EventType.SYSTEM_EVENT

    # ── Extract structured fields ─────────────────────────────────────────────
    entity_id   = payload.get("entity_id") or payload.get("patient_id") or payload.get("user_id")
    entity_type = payload.get("entity_type") or payload.get("record_type")
    action      = payload.get("action") or payload.get("verb")
    status      = payload.get("status") or payload.get("state")
    document_type      = payload.get("document_type") or payload.get("doc_type")
    document_reference = payload.get("document_uri") or payload.get("gcs_uri") or payload.get("file_ref")

    # ── Amount (normalise to cents) ───────────────────────────────────────────
    amount_cents: Optional[int] = None
    raw_amount = payload.get("amount") or payload.get("amount_cents")
    if raw_amount is not None:
        try:
            v = float(raw_amount)
            # Heuristic: if < 1000 assume dollars, convert
            amount_cents = int(v * 100) if v < 1000 else int(v)
        except (ValueError, TypeError):
            errors.append(f"amount_parse_error: {raw_amount!r}")

    # ── PII scan ──────────────────────────────────────────────────────────────
    full_text = json.dumps(payload)
    pii_fields, pii_hash, phi_present = _detect_pii(full_text)

    return ParsedEvent(
        event_id=str(event_id),
        source_topic=source_topic,
        received_at=received_at,
        event_type=event_type,
        entity_id=str(entity_id) if entity_id else None,
        entity_type=str(entity_type) if entity_type else None,
        action=str(action) if action else None,
        status=str(status) if status else None,
        amount_cents=amount_cents,
        document_type=str(document_type) if document_type else None,
        document_reference=str(document_reference) if document_reference else None,
        pii_fields_detected=pii_fields,
        pii_hash=pii_hash,
        phi_present=phi_present,
        raw_message_size_bytes=len(raw_message),
        parse_errors=errors,
    )
