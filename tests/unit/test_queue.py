"""Malformed SQS payloads must not prevent delivery of the rest of a batch."""

import json

import boto3
import pytest
import structlog
from botocore.stub import Stubber
from conduit.core.queue import SqsQueue, drain
from structlog.testing import capture_logs

VALID = {
    "task": {"id": "T-1", "title": "Sync task"},
    "connector": "slack-ops",
    "idempotency_key": "test-key",
    "submitted_at": "2026-09-08T12:00:00Z",
}


@pytest.mark.parametrize(
    "invalid_body",
    [
        "not JSON: confidential content",
        json.dumps({"task": {"id": "T-2", "title": "confidential content"}}),
        json.dumps({**VALID, "submitted_at": "2026-09-08T12:00:00"}),
    ],
    ids=["invalid-json", "invalid-schema", "naive-timestamp"],
)
def test_receive_retains_valid_messages_around_a_malformed_body(invalid_body):
    client = boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue = SqsQueue("https://sqs.us-east-1.amazonaws.com/123456789012/test", client)
    raw_messages = [
        {
            "MessageId": message_id,
            "ReceiptHandle": f"receipt-{message_id}",
            "Body": body,
            "Attributes": {"ApproximateReceiveCount": "2"},
        }
        for message_id, body in [
            ("before", json.dumps(VALID)),
            ("invalid", invalid_body),
            ("after", json.dumps({**VALID, "task": {"id": "T-3", "title": "Next task"}})),
        ]
    ]
    with Stubber(client) as stub, capture_logs() as logs:
        stub.add_response("receive_message", {"Messages": raw_messages})
        messages = queue.receive(wait_seconds=0)
        # No deletion is expected: invalid messages must remain available for redrive.
        stub.assert_no_pending_responses()

    assert [message.message_id for message in messages] == ["before", "after"]
    assert [message.envelope.task.id for message in messages] == ["T-1", "T-3"]
    assert all(message.receive_count == 2 for message in messages)
    warnings = [entry for entry in logs if entry["event"] == "queue.invalid_message"]
    assert len(warnings) == 1
    assert warnings[0]["message_id"] == "invalid"
    assert "confidential content" not in json.dumps(logs)


def test_receive_propagates_transport_failure():
    client = boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue = SqsQueue("https://sqs.us-east-1.amazonaws.com/123456789012/test", client)
    with Stubber(client) as stub:
        stub.add_client_error("receive_message", service_error_code="AccessDenied")
        with pytest.raises(client.exceptions.ClientError, match="AccessDenied"):
            queue.receive(wait_seconds=0)


@pytest.mark.parametrize("consumer", ["drain", "worker", "cli"])
def test_consumers_continue_after_an_invalid_only_response(consumer, monkeypatch):
    from conduit.cli import app
    from conduit.config import ConnectorSpec
    from conduit.core.idempotency import MemoryIdempotencyStore
    from conduit.worker import Worker
    from typer.testing import CliRunner

    from tests.unit.test_cli import CONNECTORS
    from tests.unit.test_worker import FakeQueue, ScriptedAdapter

    client = boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue = SqsQueue("https://sqs.us-east-1.amazonaws.com/123456789012/test", client)
    with Stubber(client) as stub:
        for message_id, body in [("invalid", "bad JSON"), ("valid", json.dumps(VALID))]:
            stub.add_response(
                "receive_message",
                {
                    "Messages": [
                        {"MessageId": message_id, "ReceiptHandle": message_id, "Body": body}
                    ]
                },
            )
        if consumer == "worker":
            stub.add_response("delete_message", {})
        stub.add_response("receive_message", {"Messages": []})
        if consumer == "drain":
            assert [m.message_id for m in drain(queue)] == ["valid"]
        elif consumer == "worker":
            spec = ConnectorSpec(
                name="slack-ops", type="slack", target="#ops", secrets={"token": "SLACK_BOT_TOKEN"}
            )
            adapter = ScriptedAdapter(spec, {})
            stats = Worker(
                spec, adapter, MemoryIdempotencyStore(), queue, quarantine=FakeQueue(1)
            ).run(idle_polls=1, wait_seconds=0)
            assert stats.delivered == 1
        else:
            stub.add_response("change_message_visibility", {})
            monkeypatch.setattr("conduit.cli.SqsQueue.by_name", lambda _: queue)
            original_logging = structlog.get_config().copy()
            try:
                result = CliRunner().invoke(
                    app, ["dlq", "list", "-c", "slack-ops", "--connectors-dir", CONNECTORS]
                )
                assert result.exit_code == 0, result.output
                rows = [json.loads(line) for line in result.stdout.splitlines()]
                assert [row["task_id"] for row in rows] == ["T-1"]
                assert "queue.invalid_message" in result.stderr
            finally:
                structlog.configure(**original_logging)
        stub.assert_no_pending_responses()
