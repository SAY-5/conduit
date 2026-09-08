"""Task payloads and delivery outcomes shared by every adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime, BaseModel, Field


class Task(BaseModel):
    """A unit of work to sync to an external system.

    ``id`` plus ``version`` uniquely identify a task revision; the idempotency
    key is derived from them so a resubmit of the same revision is a no-op.
    """

    id: str = Field(min_length=1, max_length=256)
    version: int = Field(default=1, ge=1)
    title: str = Field(min_length=1, max_length=1024)
    body: str = ""
    status: str = "open"
    priority: str = "normal"
    assignee: str | None = None
    labels: list[str] = Field(default_factory=list)
    url: str | None = None
    fields: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def field(self, path: str) -> Any:
        """Resolve a dotted path against the task, used by field mappings."""
        current: Any = self
        for part in path.split("."):
            if isinstance(current, BaseModel):
                current = getattr(current, part, None)
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current


class DeliveryStatus(StrEnum):
    DELIVERED = "delivered"
    DEDUPLICATED = "deduplicated"
    REJECTED = "rejected"
    FAILED = "failed"


class DeliveryResult(BaseModel):
    """What an adapter reports back after one delivery attempt."""

    status: DeliveryStatus
    connector: str
    task_id: str
    idempotency_key: str
    remote_id: str | None = None
    attempts: int = 1
    detail: str = ""


class Envelope(BaseModel):
    """The message body placed on the connector queue."""

    task: Task
    connector: str
    idempotency_key: str
    submitted_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    attempt: int = 0
