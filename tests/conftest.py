from __future__ import annotations

import os
from pathlib import Path

import pytest
from conduit.config import ConnectorSpec, load_all

ROOT = Path(__file__).resolve().parent.parent
CONNECTORS = ROOT / "connectors"


@pytest.fixture(scope="session")
def specs() -> dict[str, ConnectorSpec]:
    return load_all(CONNECTORS)


@pytest.fixture(autouse=True)
def _secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-test")
    monkeypatch.setenv("CRM_WEBHOOK_SECRET", "whsec-test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
