"""Generic webhook adapter: HMAC-SHA256 signed JSON POST with an Idempotency-Key header."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

from conduit.adapters.base import Adapter
from conduit.models import DeliveryResult, DeliveryStatus, Task

SIGNATURE_HEADER = "X-Conduit-Signature"
TIMESTAMP_HEADER = "X-Conduit-Timestamp"


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def verify(secret: str, timestamp: str, body: bytes, signature: str, max_age: int = 300) -> bool:
    try:
        if abs(time.time() - int(timestamp)) > max_age:
            return False
    except ValueError:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body), signature)


class WebhookAdapter(Adapter):
    type = "webhook"

    def payload(self, task: Task, idempotency_key: str) -> dict[str, Any]:
        data = self.mapped(task) if self.spec.mapping else task.model_dump(mode="json")
        return {
            "event": "task.synced",
            "connector": self.spec.name,
            "idempotency_key": idempotency_key,
            "task": data,
        }

    def deliver(self, task: Task, idempotency_key: str, remote_id: str | None = None):
        body = json.dumps(
            self.payload(task, idempotency_key), separators=(",", ":"), sort_keys=True
        ).encode()
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self.secret("signing_secret"), timestamp, body),
        }
        response = self._client.post(self.spec.target, content=body, headers=headers)
        self.raise_for_status(response)
        remote_id = None
        if response.headers.get("content-type", "").startswith("application/json"):
            remote_id = str(response.json().get("id", "")) or None
        return DeliveryResult(
            status=DeliveryStatus.DELIVERED,
            connector=self.spec.name,
            task_id=task.id,
            idempotency_key=idempotency_key,
            remote_id=remote_id,
        )

    def healthcheck(self) -> bool:
        try:
            response = self._client.head(self.spec.target)
        except Exception:
            return False
        return response.status_code < 500
