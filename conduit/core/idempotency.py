"""Idempotency keys and the DynamoDB claim store.

Key derivation is deterministic: ``sha256(connector | task_id | task_version)``.
The store is a single table with a conditional ``PutItem``. A worker *claims* a
key before delivering; if the item already exists and is ``delivered`` the task
is a duplicate and short-circuits. A claim that failed mid-flight is released so
SQS redrive (and eventually DLQ replay) can try again; a claim whose worker died
expires through ``lease_until`` so another worker can take it over.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

import boto3
from botocore.exceptions import ClientError

KEY_VERSION = "v1"


def idempotency_key(connector: str, task_id: str, task_version: int) -> str:
    material = f"{KEY_VERSION}|{connector}|{task_id}|{task_version}".encode()
    return hashlib.sha256(material).hexdigest()


def latest_pk(connector: str, task_id: str) -> str:
    return f"latest|{connector}|{task_id}"


class ClaimState(StrEnum):
    IN_PROGRESS = "in_progress"
    DELIVERED = "delivered"


@dataclass(frozen=True)
class ClaimResult:
    acquired: bool
    state: ClaimState | None = None
    remote_id: str | None = None

    @property
    def duplicate(self) -> bool:
        return not self.acquired and self.state == ClaimState.DELIVERED


class IdempotencyStore(Protocol):
    def claim(self, key: str, *, connector: str, task_id: str) -> ClaimResult: ...
    def mark_delivered(self, key: str, remote_id: str | None) -> None: ...
    def release(self, key: str) -> None: ...
    def latest(self, connector: str, task_id: str) -> str | None: ...


class DynamoIdempotencyStore:
    """Conditional-put store; the table has ``pk`` (S) and a TTL on ``expires_at``."""

    def __init__(
        self,
        table_name: str,
        *,
        ttl_seconds: int = 7 * 24 * 3600,
        lease_seconds: int = 120,
        client: Any | None = None,
        clock=time.time,
    ) -> None:
        self.table_name = table_name
        self.ttl_seconds = ttl_seconds
        self.lease_seconds = lease_seconds
        self._clock = clock
        self._client = client or boto3.client("dynamodb")

    def claim(self, key: str, *, connector: str, task_id: str) -> ClaimResult:
        now = int(self._clock())
        item = {
            "pk": {"S": key},
            "connector": {"S": connector},
            "task_id": {"S": task_id},
            "state": {"S": ClaimState.IN_PROGRESS},
            "lease_until": {"N": str(now + self.lease_seconds)},
            "claimed_at": {"N": str(now)},
            "expires_at": {"N": str(now + self.ttl_seconds)},
        }
        try:
            self._client.put_item(
                TableName=self.table_name,
                Item=item,
                ConditionExpression=(
                    "attribute_not_exists(pk) OR (#s = :inprog AND lease_until < :now)"
                ),
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={
                    ":inprog": {"S": ClaimState.IN_PROGRESS},
                    ":now": {"N": str(now)},
                },
            )
            return ClaimResult(acquired=True, state=ClaimState.IN_PROGRESS)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        existing = self._client.get_item(
            TableName=self.table_name, Key={"pk": {"S": key}}, ConsistentRead=True
        ).get("Item", {})
        state = ClaimState(existing.get("state", {}).get("S", ClaimState.IN_PROGRESS))
        remote_id = existing.get("remote_id", {}).get("S")
        return ClaimResult(acquired=False, state=state, remote_id=remote_id)

    def mark_delivered(self, key: str, remote_id: str | None) -> None:
        now = int(self._clock())
        expr = "SET #s = :done, delivered_at = :now"
        values: dict[str, Any] = {":done": {"S": ClaimState.DELIVERED}, ":now": {"N": str(now)}}
        if remote_id:
            expr += ", remote_id = :rid"
            values[":rid"] = {"S": remote_id}
        self._client.update_item(
            TableName=self.table_name,
            Key={"pk": {"S": key}},
            UpdateExpression=expr,
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues=values,
        )
        if remote_id:
            item = self._client.get_item(
                TableName=self.table_name, Key={"pk": {"S": key}}, ConsistentRead=True
            ).get("Item", {})
            connector = item.get("connector", {}).get("S")
            task_id = item.get("task_id", {}).get("S")
            if connector and task_id:
                self._client.put_item(
                    TableName=self.table_name,
                    Item={
                        "pk": {"S": latest_pk(connector, task_id)},
                        "remote_id": {"S": remote_id},
                        "updated_at": {"N": str(now)},
                        "expires_at": {"N": str(now + self.ttl_seconds)},
                    },
                )

    def latest(self, connector: str, task_id: str) -> str | None:
        item = self._client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": latest_pk(connector, task_id)}},
            ConsistentRead=True,
        ).get("Item")
        return item.get("remote_id", {}).get("S") if item else None

    def release(self, key: str) -> None:
        """Drop an in-progress claim so a redrive or replay can deliver."""
        try:
            self._client.delete_item(
                TableName=self.table_name,
                Key={"pk": {"S": key}},
                ConditionExpression="#s = :inprog",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":inprog": {"S": ClaimState.IN_PROGRESS}},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

    def ensure_table(self) -> None:
        """Create the table locally (tests and the demo); Terraform owns it elsewhere."""
        try:
            self._client.describe_table(TableName=self.table_name)
            return
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        self._client.create_table(
            TableName=self.table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self._client.get_waiter("table_exists").wait(TableName=self.table_name)
        self._client.update_time_to_live(
            TableName=self.table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
        )


class MemoryIdempotencyStore:
    """Same semantics without DynamoDB; used by unit tests of the worker."""

    def __init__(self, clock=time.time, lease_seconds: int = 120) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self._latest: dict[tuple[str, str], str] = {}
        self._clock = clock
        self.lease_seconds = lease_seconds

    def claim(self, key: str, *, connector: str, task_id: str) -> ClaimResult:
        now = self._clock()
        existing = self._items.get(key)
        if existing and not (
            existing["state"] == ClaimState.IN_PROGRESS and existing["lease_until"] < now
        ):
            return ClaimResult(False, existing["state"], existing.get("remote_id"))
        self._items[key] = {
            "state": ClaimState.IN_PROGRESS,
            "lease_until": now + self.lease_seconds,
            "connector": connector,
            "task_id": task_id,
        }
        return ClaimResult(True, ClaimState.IN_PROGRESS)

    def mark_delivered(self, key: str, remote_id: str | None) -> None:
        item = self._items[key]
        item.update(state=ClaimState.DELIVERED, remote_id=remote_id)
        if remote_id:
            self._latest[(item["connector"], item["task_id"])] = remote_id

    def release(self, key: str) -> None:
        if self._items.get(key, {}).get("state") == ClaimState.IN_PROGRESS:
            del self._items[key]

    def latest(self, connector: str, task_id: str) -> str | None:
        return self._latest.get((connector, task_id))
