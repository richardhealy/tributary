# Changelog

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
