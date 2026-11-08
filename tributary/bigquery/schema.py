"""
BigQuery schema definitions for the tributary pipeline.

Three tables:
  events          — one row per ingested event, partitioned by date
  events_audit    — immutable audit log (append-only, never updated)
  events_summary  — daily aggregations written by the Cloud Scheduler job

HIPAA note:
  - events table stores no raw PII
  - pii_hash is a SHA-256 of PII values (for dedup), never the values
  - phi_present flags rows for restricted-column access policy
  - events_audit is write-once (enforced by IAM, not code)
  - retention: events 7 years (HIPAA minimum), audit 7 years
"""
from __future__ import annotations
from google.cloud import bigquery

DATASET_ID = "tributary"
PROJECT_ID_ENV = "GCP_PROJECT_ID"   # read from env


def events_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("event_id",             "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("source_topic",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("received_at",          "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("event_date",           "DATE",      mode="REQUIRED"),  # partition key
        bigquery.SchemaField("event_type",           "STRING",    mode="REQUIRED"),
        # Normalised entity fields
        bigquery.SchemaField("entity_id",            "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("entity_type",          "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("action",               "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("status",               "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("amount_cents",         "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("document_type",        "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("document_reference",   "STRING",    mode="NULLABLE"),
        # PII / PHI — never the values, only metadata
        bigquery.SchemaField("pii_fields_detected",  "STRING",    mode="REPEATED"),
        bigquery.SchemaField("pii_hash",             "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("phi_present",          "BOOL",      mode="REQUIRED"),
        # LLM enrichment
        bigquery.SchemaField("category",             "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("subcategory",          "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("urgency",              "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("summary",              "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("enrichment_confidence","FLOAT64",   mode="NULLABLE"),
        # Lineage
        bigquery.SchemaField("raw_message_size_bytes","INTEGER",  mode="NULLABLE"),
        bigquery.SchemaField("parse_errors",         "STRING",    mode="REPEATED"),
        bigquery.SchemaField("parse_version",        "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("inserted_at",          "TIMESTAMP", mode="REQUIRED"),
    ]


def audit_schema() -> list[bigquery.SchemaField]:
    """Append-only audit log — no updates, no deletes (enforced by IAM)."""
    return [
        bigquery.SchemaField("audit_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("audit_action",  "STRING",    mode="REQUIRED"),  # ingested/enriched/failed/reviewed
        bigquery.SchemaField("actor",         "STRING",    mode="REQUIRED"),  # service-account or user
        bigquery.SchemaField("occurred_at",   "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("details",       "JSON",      mode="NULLABLE"),  # structured metadata, no PII
    ]


def summary_schema() -> list[bigquery.SchemaField]:
    """Daily aggregation table written by Cloud Scheduler job."""
    return [
        bigquery.SchemaField("summary_date",        "DATE",    mode="REQUIRED"),
        bigquery.SchemaField("source_topic",        "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("event_type",          "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("category",            "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("urgency",             "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("event_count",         "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("phi_event_count",     "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("avg_amount_cents",    "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount_cents",  "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("parse_error_count",   "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("avg_enrichment_confidence", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("computed_at",         "TIMESTAMP", mode="REQUIRED"),
    ]


def create_tables(client: bigquery.Client, project_id: str) -> None:
    """Create the dataset and all three tables if they don't exist."""
    dataset_ref = bigquery.DatasetReference(project_id, DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset.default_table_expiration_ms = None   # no auto-expiry; retention managed by policy
    client.create_dataset(dataset, exists_ok=True)

    # events — date-partitioned, clustered by event_type and urgency
    events_ref = dataset_ref.table("events")
    events_table = bigquery.Table(events_ref, schema=events_schema())
    events_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_date",
        expiration_ms=int(7 * 365.25 * 24 * 3600 * 1000),  # 7 years (HIPAA minimum)
    )
    events_table.clustering_fields = ["event_type", "urgency", "category"]
    client.create_table(events_table, exists_ok=True)

    # audit — date-partitioned, no expiry
    audit_ref = dataset_ref.table("events_audit")
    audit_table = bigquery.Table(audit_ref, schema=audit_schema())
    audit_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="occurred_at",
    )
    client.create_table(audit_table, exists_ok=True)

    # summary — no partitioning needed (small table)
    summary_ref = dataset_ref.table("events_summary")
    summary_table = bigquery.Table(summary_ref, schema=summary_schema())
    client.create_table(summary_table, exists_ok=True)
