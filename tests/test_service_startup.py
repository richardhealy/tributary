"""Service start-up tests: both Cloud Run apps must import the way they are run.

pytest puts the right directory on sys.path for the other tests, so they can
pass while the services themselves fail to start. These tests use a fresh
interpreter instead: once from the repo root (as `uvicorn` in the README does)
and once from a copy of what each Dockerfile puts in /app.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
SERVICES = {
    "Dockerfile.subscriber": "tributary.pipeline.subscriber:app",
    "Dockerfile.aggregator": "tributary.scheduler.aggregator:app",
}
SKIP = shutil.ignore_patterns(".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache")


def _import_app(target: str, cwd: Path) -> subprocess.CompletedProcess:
    """Import a uvicorn "module:attr" target in a new interpreter run from cwd."""
    module, _, attr = target.partition(":")
    code = f"import importlib; importlib.import_module({module!r}).{attr}"
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=cwd, env=env, capture_output=True, text=True, check=False,
    )


def _docker_layout(dockerfile: Path, app_dir: Path) -> str:
    """Copy what the Dockerfile COPYs into app_dir (standing in for /app).

    Returns the uvicorn target from the Dockerfile's CMD.
    """
    target = ""
    for line in dockerfile.read_text().splitlines():
        if line.startswith("COPY "):
            src, dst = line.split()[1:3]
            if (REPO / src).is_dir():
                shutil.copytree(REPO / src, app_dir / dst, ignore=SKIP, dirs_exist_ok=True)
            else:
                shutil.copy(REPO / src, app_dir / dst)
        elif line.startswith("CMD "):
            cmd = json.loads(line[len("CMD "):])
            target = cmd[cmd.index("uvicorn") + 1]
    return target


@pytest.mark.parametrize("target", SERVICES.values())
def test_app_imports_from_repo_root(target):
    result = _import_app(target, REPO)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(("dockerfile", "target"), SERVICES.items())
def test_app_imports_in_docker_image_layout(dockerfile, target, tmp_path):
    assert _docker_layout(REPO / dockerfile, tmp_path) == target
    result = _import_app(target, tmp_path)
    assert result.returncode == 0, result.stderr


def test_subscriber_health():
    from tributary.pipeline.subscriber import app
    resp = TestClient(app).get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "tributary-subscriber"}


def test_aggregator_health():
    from tributary.scheduler.aggregator import app
    resp = TestClient(app).get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "tributary-aggregator"}
