"""Aggregator tests with a stub BigQuery client.

The aggregator builds its SQL from a date. These tests check that the date is
validated, that it reaches BigQuery only as a query parameter, and that the
summary write replaces a day's rows instead of appending to them.
"""
from datetime import UTC, date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from google.cloud import bigquery

from tributary.scheduler import aggregator


class StubJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class StubBQClient:
    """Records every query and its job config instead of calling BigQuery."""

    def __init__(self, phi_rows=None):
        self.queries: list[tuple[str, bigquery.QueryJobConfig | None]] = []
        self.phi_rows = phi_rows or []

    def query(self, sql, job_config=None):
        self.queries.append((sql, job_config))
        return StubJob(self.phi_rows if sql.lstrip().startswith("SELECT") else [])


@pytest.fixture
def stub(monkeypatch):
    client = StubBQClient(phi_rows=[{"event_id": "evt-1"}, {"event_id": "evt-2"}])
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.delenv("SCHEDULER_TOKEN", raising=False)
    monkeypatch.setattr(aggregator, "_bigquery_client", lambda project: client)
    return client


def post_run(json=None, content=None):
    headers = {"content-type": "application/json"} if content is not None else None
    return TestClient(aggregator.app).post("/run", json=json, content=content, headers=headers)


def date_param(day: date) -> list[bigquery.ScalarQueryParameter]:
    return [bigquery.ScalarQueryParameter("target_date", "DATE", day)]


def test_target_date_reaches_bigquery_only_as_a_query_parameter(stub):
    resp = post_run(json={"target_date": "2026-11-01"})

    assert resp.status_code == 200
    assert resp.json() == {
        "target_date": "2026-11-01",
        "aggregation": "complete",
        "phi_events_flagged": 2,
        "phi_event_ids": ["evt-1", "evt-2"],
    }
    assert len(stub.queries) == 2
    for sql, job_config in stub.queries:
        assert "2026-11-01" not in sql
        assert "@target_date" in sql
        assert job_config.query_parameters == date_param(date(2026, 11, 1))


@pytest.mark.parametrize(
    "target_date",
    [
        "2026-11-01' OR TRUE OR '",                      # the injection case
        "2026-11-01'; DELETE FROM tributary.events --",
        "2026-13-01",                                    # no such month
        "2026-02-30",                                    # no such day
        "20261101",                                      # fromisoformat accepts this
        "2026-W44-7",                                    # and this
        "",
        20261101,
        None,
    ],
)
def test_bad_target_date_is_rejected_before_any_query(stub, target_date):
    resp = post_run(json={"target_date": target_date})

    assert resp.status_code == 400
    assert "YYYY-MM-DD" in resp.json()["detail"]
    assert stub.queries == []


@pytest.mark.parametrize("body", ["[]", '["target_date"]', '"2026-11-01"', "null"])
def test_body_that_is_not_a_json_object_is_rejected(stub, body):
    resp = post_run(content=body)

    assert resp.status_code == 400
    assert stub.queries == []


def test_no_body_aggregates_yesterday_in_utc(stub):
    before = datetime.now(UTC).date() - timedelta(days=1)
    resp = post_run()
    after = datetime.now(UTC).date() - timedelta(days=1)

    assert resp.status_code == 200
    sent = stub.queries[0][1].query_parameters[0].value
    assert sent in {before, after}
    assert resp.json()["target_date"] == sent.isoformat()


def test_summary_write_replaces_the_day_instead_of_appending(stub):
    """A Cloud Scheduler retry or a backfill must not double a day's rows.

    The write is one MERGE: rows for the day that still exist are updated, new
    ones inserted, and rows for that day with no matching group are deleted.
    A plain INSERT would add a second copy of the day on every re-run.
    """
    post_run(json={"target_date": "2026-11-01"})
    post_run(json={"target_date": "2026-11-01"})

    summary_writes = [sql for sql, _ in stub.queries if "events_summary" in sql]
    assert len(summary_writes) == 2
    for sql in summary_writes:
        assert sql.lstrip().startswith("MERGE `test-project.tributary.events_summary`")
        assert "INSERT INTO" not in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY SOURCE AND target.summary_date = @target_date THEN" in sql


def test_groups_on_the_same_values_it_writes(stub):
    """category and urgency are written as COALESCE(..., 'unknown').

    Grouping on the raw columns would put NULL and 'unknown' in separate groups
    that both come out as 'unknown', which a MERGE rejects as two source rows
    for one target row.
    """
    post_run(json={"target_date": "2026-11-01"})

    merge_sql = stub.queries[0][0]
    group_by = next(line for line in merge_sql.splitlines() if "GROUP BY" in line)
    assert "COALESCE(category, 'unknown')" in group_by
    assert "COALESCE(urgency, 'unknown')" in group_by
