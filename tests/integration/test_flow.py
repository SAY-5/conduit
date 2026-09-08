"""End-to-end through real SQS and DynamoDB APIs (LocalStack) and the HTTP fakes."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from conduit.cli import app
from conduit.core.queue import list_dead_letters, redrive_policy, replay_dead_letters
from typer.testing import CliRunner

from tests.integration.conftest import LOCALSTACK, task

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("name", ["slack-ops", "jira-support", "webhook-crm"])
def test_message_flows_through_sqs_to_the_remote(rig_factory, name):
    rig = rig_factory(name)
    rig.submit(task("F-1"), task("F-2"))
    stats = rig.run()
    assert stats.delivered == 2 and stats.failed == 0
    inbox = rig.fake.inbox()
    assert inbox["count"] == 2 and inbox["unique_keys"] == 2
    assert {e["task_id"] for e in inbox["entries"]} == {"F-1", "F-2"}
    assert rig.queue.depth()["ApproximateNumberOfMessages"] == 0


def test_transient_failure_is_retried_then_delivered(rig_factory):
    rig = rig_factory("jira-support")
    rig.fake.faults(rate_limit_tasks=["R-1"], rate_limit_count=2)
    rig.submit(task("R-1"))
    stats = rig.run()
    assert stats.delivered == 1 and stats.retried == 2 and stats.failed == 0
    assert len(stats.retry_delays) == 2 and all(
        d <= rig.spec.retry.max_seconds for d in stats.retry_delays
    )
    assert rig.fake.client.get("/_faults").json()["rate_limit_hits"] == {"R-1": 2}
    assert rig.fake.inbox()["count"] == 1


def test_permanent_failure_exhausts_max_receive_count_into_dlq(rig_factory):
    rig = rig_factory("webhook-crm")
    assert redrive_policy(rig.queue)["maxReceiveCount"] == rig.spec.queue.max_receive_count == 2
    rig.fake.faults(hard_fail_tasks=["P-1"])
    rig.submit(task("P-1"), task("P-2"))
    stats = rig.run(idle_polls=3)
    assert stats.delivered == 1
    assert stats.failed == 2 and stats.retried == 0
    assert stats.dead_lettered == 1
    dead = list_dead_letters(rig.dlq)
    assert [m.envelope.task.id for m in dead] == ["P-1"]
    assert rig.queue.depth()["ApproximateNumberOfMessages"] == 0
    assert rig.fake.inbox()["count"] == 1


def test_duplicate_submit_is_deduplicated(rig_factory):
    rig = rig_factory("slack-ops")
    rig.submit(task("D-1"), task("D-1"), task("D-1"), task("D-2"))
    stats = rig.run()
    assert stats.delivered == 2 and stats.deduplicated == 2
    inbox = rig.fake.inbox()
    assert inbox["count"] == 2 and inbox["unique_keys"] == 2


def test_new_version_updates_the_same_jira_issue(rig_factory):
    rig = rig_factory("jira-support")
    rig.submit(task("V-1", 1))
    rig.run()
    rig.submit(task("V-1", 2, title="renamed"))
    rig.run()
    entries = rig.fake.inbox()["entries"]
    assert [e["op"] for e in entries] == ["create", "update"]
    assert entries[0]["issue"] == entries[1]["issue"]


def test_dlq_replay_delivers_after_the_fault_is_fixed(rig_factory):
    rig = rig_factory("webhook-crm")
    rig.fake.faults(hard_fail_tasks=["X-1", "X-2"])
    rig.submit(task("X-1"), task("X-2"))
    rig.run(idle_polls=3)
    assert len(list_dead_letters(rig.dlq)) == 2
    rig.fake.clear_faults()
    assert replay_dead_letters(rig.dlq, rig.queue) == 2
    stats = rig.run()
    assert stats.delivered == 2
    assert list_dead_letters(rig.dlq) == []
    assert rig.fake.inbox()["count"] == 2


def test_cli_submit_and_dlq_commands(rig_factory, tmp_path: Path, monkeypatch):
    rig = rig_factory("webhook-crm")
    connectors = tmp_path / "connectors"
    connectors.mkdir()
    (connectors / f"{rig.spec.name}.yaml").write_text(
        json.dumps(rig.spec.model_dump(mode="json", exclude={"name"}))
    )
    tasks = tmp_path / "tasks.jsonl"
    tasks.write_text("\n".join(task(f"C-{i}").model_dump_json() for i in range(3)))
    monkeypatch.setenv("CONDUIT_AWS_ENDPOINT_URL", LOCALSTACK)
    runner = CliRunner()
    result = runner.invoke(
        app, ["submit", str(tasks), "-c", rig.spec.name, "--connectors-dir", str(connectors)]
    )
    assert result.exit_code == 0, result.output
    assert "submitted 3 messages" in result.output
    rig.fake.faults(hard_fail_tasks=["C-0", "C-1", "C-2"])
    rig.run(idle_polls=3)
    result = runner.invoke(
        app, ["dlq", "list", "-c", rig.spec.name, "--connectors-dir", str(connectors)]
    )
    assert result.exit_code == 0, result.output
    assert result.output.count('"task_id"') == 3
    rig.fake.clear_faults()
    result = runner.invoke(
        app, ["dlq", "replay", "-c", rig.spec.name, "--connectors-dir", str(connectors)]
    )
    assert "replayed 3 messages" in result.output
    assert rig.run().delivered == 3


def test_csv_submit(rig_factory, tmp_path: Path, monkeypatch):
    rig = rig_factory("slack-ops")
    connectors = tmp_path / "connectors"
    connectors.mkdir()
    (connectors / f"{rig.spec.name}.yaml").write_text(
        json.dumps(rig.spec.model_dump(mode="json", exclude={"name"}))
    )
    csv_path = tmp_path / "tasks.csv"
    run_id = uuid.uuid4().hex[:6]
    csv_path.write_text(
        "id,title,priority,labels\n"
        f"CSV-{run_id}-1,First,high,ops|urgent\n"
        f"CSV-{run_id}-2,Second,low,\n"
    )
    monkeypatch.setenv("CONDUIT_AWS_ENDPOINT_URL", LOCALSTACK)
    result = CliRunner().invoke(
        app, ["submit", str(csv_path), "-c", rig.spec.name, "--connectors-dir", str(connectors)]
    )
    assert result.exit_code == 0, result.output
    assert rig.run().delivered == 2
