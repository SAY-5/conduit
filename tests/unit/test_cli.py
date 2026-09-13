"""CLI behaviour that needs no AWS: submit-time rejection, config, and schema output."""

from __future__ import annotations

import json
from pathlib import Path

from conduit.cli import app
from conduit.models import Task
from typer.testing import CliRunner

ROOT = Path(__file__).resolve().parent.parent.parent
CONNECTORS = str(ROOT / "connectors")
SCHEMAS = str(ROOT / "schemas")


class StubQueue:
    def __init__(self) -> None:
        self.sent = []

    def send_batch(self, envelopes) -> int:
        self.sent.extend(envelopes)
        return len(self.sent)


def test_submit_rejects_invalid_tasks_up_front(tmp_path: Path, monkeypatch):
    stub = StubQueue()
    monkeypatch.setattr("conduit.cli.SqsQueue.by_name", lambda name: stub)
    tasks = tmp_path / "tasks.json"
    tasks.write_text(
        json.dumps(
            [
                Task(id="OK-1", title="fine").model_dump(mode="json"),
                Task(id="BAD-1", title="n" * 201).model_dump(mode="json"),
                Task(id="BAD-2", title="t", status="paused").model_dump(mode="json"),
            ]
        )
    )
    result = CliRunner().invoke(
        app,
        [
            "submit",
            str(tasks),
            "-c",
            "webhook-crm",
            "--connectors-dir",
            CONNECTORS,
            "--schemas-dir",
            SCHEMAS,
        ],
    )
    assert result.exit_code == 1, result.output
    assert [e.task.id for e in stub.sent] == ["OK-1"]
    assert "rejected BAD-1 v1: name: length 201 exceeds 200 (max_length)" in result.output
    assert "rejected BAD-2 v1: status:" in result.output and "(enum)" in result.output
    assert "submitted 1 messages (1 tasks x 1) to conduit-webhook-crm, 2 rejected" in result.output


def test_config_validate_reports_mapping_size_and_schema_version():
    result = CliRunner().invoke(
        app, ["config", "validate", "--connectors-dir", CONNECTORS, "--schemas-dir", SCHEMAS]
    )
    assert result.exit_code == 0, result.output
    assert "webhook-crm      webhook" in result.output and "mapping=6" in result.output
    assert "quarantine=conduit-webhook-crm-quarantine" in result.output
    assert "schema=v1" in result.output and "schema=v2" in result.output


def test_schema_check_reports_every_version():
    result = CliRunner().invoke(app, ["schema", "check", "--schemas-dir", SCHEMAS])
    assert result.exit_code == 0, result.output
    assert "ok  jira-support     versions=1..2" in result.output
    assert "ok  slack-ops        versions=1..1" in result.output


def test_schema_check_refuses_a_breaking_change(tmp_path: Path):
    connector = tmp_path / "acme"
    connector.mkdir(parents=True)
    (connector / "v1.yaml").write_text("fields:\n  title:\n    type: string\n")
    (connector / "v2.yaml").write_text("fields:\n  title:\n    type: string\n    required: true\n")
    result = CliRunner().invoke(app, ["schema", "check", "--schemas-dir", str(tmp_path)])
    assert result.exit_code == 2
    assert "acme/v2 is a breaking change from v1" in result.output
    assert "title: became required" in result.output
