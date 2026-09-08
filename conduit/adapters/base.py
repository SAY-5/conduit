"""The one interface every integration implements."""

from __future__ import annotations

import abc
import os
from typing import Any

import httpx

from conduit.config import ConnectorSpec
from conduit.core.mapping import apply_mapping, render
from conduit.core.retry import classify_response
from conduit.models import DeliveryResult, Task


class SecretError(RuntimeError):
    pass


class Adapter(abc.ABC):
    """Deliver one task revision to a remote system, idempotently.

    Implementations raise ``TransientError`` for retryable failures and
    ``PermanentError`` for everything else; the worker handles backoff and the
    dead-letter path. ``deliver`` must be safe to call twice with the same
    ``idempotency_key``: the remote side receives the key so it can dedupe too.
    """

    type: str

    def __init__(self, spec: ConnectorSpec, client: httpx.Client | None = None) -> None:
        self.spec = spec
        self._client = client or httpx.Client(timeout=spec.retry.timeout_seconds)

    @abc.abstractmethod
    def deliver(
        self, task: Task, idempotency_key: str, remote_id: str | None = None
    ) -> DeliveryResult:
        """Deliver ``task``; ``remote_id`` is the id recorded for its previous revision."""

    @abc.abstractmethod
    def healthcheck(self) -> bool: ...

    def close(self) -> None:
        self._client.close()

    def secret(self, logical: str) -> str:
        env = self.spec.secrets.get(logical)
        if env is None:
            raise SecretError(f"connector {self.spec.name!r} declares no secret {logical!r}")
        value = os.environ.get(env)
        if not value:
            raise SecretError(f"environment variable {env} (secret {logical!r}) is not set")
        return value

    def render(self, template: str, task: Task) -> Any:
        """Resolve a mapping source: a bare field path or a ``$``-template string."""
        return render(template, task)

    def mapped(self, task: Task) -> dict[str, Any]:
        """Remote fields after defaults, coercion, validation, and truncation."""
        return apply_mapping(self.spec, task)

    def raise_for_status(self, response: httpx.Response) -> None:
        err = classify_response(response, self.spec.retry)
        if err is not None:
            raise err
