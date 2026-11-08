# Changelog

## 2026-11-09

### Fixed

- The aggregator's `POST /run` put the `target_date` from the request body straight into its SQL, so a value such as `2026-11-01' OR TRUE OR '` changed both queries (the PHI review query would then list PHI event IDs from every day). `target_date` must now be a real date written as `YYYY-MM-DD` and is passed to BigQuery as a DATE query parameter; anything else, or a body that is not a JSON object, gets a 400 before any query runs.
- Re-running the aggregation for a day (a Cloud Scheduler retry or a backfill) inserted a second copy of that day's summary rows. The summary write is now a single `MERGE` that replaces the day's rows.
- The summary query grouped on the raw `category` and `urgency` columns while writing them as `COALESCE(..., 'unknown')`, so an event with no category and one with category `unknown` could be counted in two rows with the same key. It now groups on the written values.

### Added

- `tests/test_aggregator.py`: 17 tests with a stub BigQuery client covering the date check, the query parameter, the default date and the shape of the summary write.

## 2026-10-06

### Fixed

- Neither Cloud Run service could start. The Python code lived at the repo root while every module imported `tributary.*`, so `uvicorn tributary.pipeline.subscriber:app` failed with `ModuleNotFoundError: No module named 'tributary'`, both from the README quickstart and inside the Docker images. The tests only passed because pytest happened to put the parent folder on `sys.path` when the checkout was named `tributary`; in a checkout with any other name (Cloud Build uses `/workspace`) test collection failed too. The code now lives in a `tributary/` package directory.
- The Dockerfiles now copy only the `tributary/` package instead of the whole build context, and a `.dockerignore` keeps `.env`, `.venv` and `.git` out of the context.

### Added

- `tests/test_service_startup.py`: both services import in a fresh interpreter from the repo root and from the layout each Dockerfile builds, and both `/health` endpoints answer.
- `PROGRESS.md` with the known issues found while checking the code.
- README: corrected project structure and test counts, a note on what the local push example needs, and a status and known limits section.

### Removed

- `infra/__init__.py` (`infra/` holds YAML, not Python).
