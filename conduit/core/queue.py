"""SQS producer/consumer with long polling, visibility extension, batch delete, and DLQ tools."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError

from conduit.config import ConnectorSpec
from conduit.models import Envelope

BATCH = 10


def aws_client(service: str, **kwargs: Any):
    """Client honoring ``CONDUIT_AWS_ENDPOINT_URL`` (LocalStack) when set."""
    endpoint = os.environ.get("CONDUIT_AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client(service, endpoint_url=endpoint or None, region_name=region, **kwargs)


@dataclass
class Message:
    receipt_handle: str
    message_id: str
    envelope: Envelope
    receive_count: int


class SqsQueue:
    def __init__(self, queue_url: str, client: Any | None = None) -> None:
        self.queue_url = queue_url
        self._client = client or aws_client("sqs")

    @classmethod
    def by_name(cls, name: str, client: Any | None = None) -> SqsQueue:
        client = client or aws_client("sqs")
        url = client.get_queue_url(QueueName=name)["QueueUrl"]
        return cls(url, client)

    @property
    def name(self) -> str:
        return self.queue_url.rsplit("/", 1)[-1]

    def send(self, envelope: Envelope, delay_seconds: int = 0) -> str:
        response = self._client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=envelope.model_dump_json(),
            DelaySeconds=delay_seconds,
            MessageAttributes=_attributes(envelope),
        )
        return response["MessageId"]

    def send_batch(self, envelopes: Iterable[Envelope]) -> int:
        sent = 0
        chunk: list[Envelope] = []
        for envelope in envelopes:
            chunk.append(envelope)
            if len(chunk) == BATCH:
                sent += self._send_chunk(chunk)
                chunk = []
        if chunk:
            sent += self._send_chunk(chunk)
        return sent

    def _send_chunk(self, chunk: list[Envelope]) -> int:
        entries = [
            {
                "Id": str(i),
                "MessageBody": env.model_dump_json(),
                "MessageAttributes": _attributes(env),
            }
            for i, env in enumerate(chunk)
        ]
        response = self._client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        failed = response.get("Failed", [])
        if failed:
            raise RuntimeError(f"send_message_batch failed for {len(failed)} entries: {failed[0]}")
        return len(response.get("Successful", []))

    def receive(
        self, *, max_messages: int = BATCH, wait_seconds: int = 20, visibility: int | None = None
    ) -> list[Message]:
        params: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MaxNumberOfMessages": max(1, min(BATCH, max_messages)),
            "WaitTimeSeconds": wait_seconds,
            "AttributeNames": ["ApproximateReceiveCount"],
            "MessageAttributeNames": ["All"],
        }
        if visibility is not None:
            params["VisibilityTimeout"] = visibility
        response = self._client.receive_message(**params)
        messages = []
        for raw in response.get("Messages", []):
            messages.append(
                Message(
                    receipt_handle=raw["ReceiptHandle"],
                    message_id=raw["MessageId"],
                    envelope=Envelope.model_validate_json(raw["Body"]),
                    receive_count=int(raw.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
                )
            )
        return messages

    def extend_visibility(self, receipt_handle: str, seconds: int) -> None:
        self._client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=max(0, min(43200, seconds)),
        )

    def delete(self, receipt_handle: str) -> None:
        self._client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)

    def delete_batch(self, receipt_handles: list[str]) -> None:
        for start in range(0, len(receipt_handles), BATCH):
            chunk = receipt_handles[start : start + BATCH]
            self._client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=[{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(chunk)],
            )

    def depth(self) -> dict[str, int]:
        attrs = self._client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        return {k: int(v) for k, v in attrs.items()}

    def purge(self) -> None:
        self._client.purge_queue(QueueUrl=self.queue_url)


def _attributes(envelope: Envelope) -> dict[str, Any]:
    return {
        "connector": {"DataType": "String", "StringValue": envelope.connector},
        "idempotency_key": {"DataType": "String", "StringValue": envelope.idempotency_key},
        "task_id": {"DataType": "String", "StringValue": envelope.task.id},
    }


def ensure_queues(spec: ConnectorSpec, client: Any | None = None) -> tuple[SqsQueue, SqsQueue]:
    """Create ``conduit-<name>`` and its DLQ with a redrive policy.

    Terraform owns these in a deployment; this exists for tests and ad hoc local runs.
    """
    client = client or aws_client("sqs")
    dlq_url = client.create_queue(
        QueueName=spec.dlq_name,
        Attributes={"MessageRetentionPeriod": str(spec.queue.dlq_retention_seconds)},
    )["QueueUrl"]
    dlq_arn = client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    url = client.create_queue(
        QueueName=spec.queue_name,
        Attributes={
            "VisibilityTimeout": str(spec.queue.visibility_timeout_seconds),
            "MessageRetentionPeriod": str(spec.queue.message_retention_seconds),
            "ReceiveMessageWaitTimeSeconds": "20",
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": spec.queue.max_receive_count}
            ),
        },
    )["QueueUrl"]
    return SqsQueue(url, client), SqsQueue(dlq_url, client)


def redrive_policy(queue: SqsQueue) -> dict[str, Any] | None:
    attrs = queue._client.get_queue_attributes(
        QueueUrl=queue.queue_url, AttributeNames=["RedrivePolicy"]
    )["Attributes"]
    raw = attrs.get("RedrivePolicy")
    return json.loads(raw) if raw else None


def drain(queue: SqsQueue, *, visibility: int = 30, limit: int | None = None) -> Iterator[Message]:
    """Receive every currently visible message once (they stay hidden for ``visibility``)."""
    seen = 0
    while limit is None or seen < limit:
        batch = queue.receive(max_messages=BATCH, wait_seconds=1, visibility=visibility)
        if not batch:
            return
        for message in batch:
            yield message
            seen += 1
            if limit is not None and seen >= limit:
                return


def list_dead_letters(dlq: SqsQueue, limit: int | None = None) -> list[Message]:
    """Peek at dead letters without consuming them; visibility is handed back immediately."""
    messages = list(drain(dlq, visibility=30, limit=limit))
    for message in messages:
        dlq.extend_visibility(message.receipt_handle, 0)
    return messages


def replay_dead_letters(dlq: SqsQueue, target: SqsQueue, limit: int | None = None) -> int:
    """Move dead letters back onto ``target`` with a fresh receive count."""
    moved = 0
    for message in drain(dlq, visibility=60, limit=limit):
        envelope = message.envelope.model_copy(update={"attempt": message.envelope.attempt + 1})
        target.send(envelope)
        dlq.delete(message.receipt_handle)
        moved += 1
    return moved


def queue_exists(name: str, client: Any | None = None) -> bool:
    client = client or aws_client("sqs")
    try:
        client.get_queue_url(QueueName=name)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {
            "AWS.SimpleQueueService.NonExistentQueue",
            "QueueDoesNotExist",
        }:
            return False
        raise
