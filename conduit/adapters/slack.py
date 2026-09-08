"""Slack adapter: chat.postMessage with Block Kit, or an incoming webhook URL."""

from __future__ import annotations

from typing import Any

import httpx

from conduit.adapters.base import Adapter
from conduit.core.retry import PermanentError, TransientError
from conduit.models import DeliveryResult, DeliveryStatus, Task

RETRYABLE_SLACK_ERRORS = {"ratelimited", "service_unavailable", "internal_error", "fatal_error"}


class SlackAdapter(Adapter):
    type = "slack"

    @property
    def api_base(self) -> str:
        return (self.spec.base_url or "https://slack.com").rstrip("/")

    @property
    def uses_incoming_webhook(self) -> bool:
        return self.spec.target.startswith("https://")

    def blocks(self, task: Task) -> list[dict[str, Any]]:
        mapped = self.mapped(task)
        title = mapped.get("title") or task.title
        body = mapped.get("body") or task.body
        meta = [f"*Status:* {task.status}", f"*Priority:* {task.priority}"]
        if task.assignee:
            meta.append(f"*Assignee:* {task.assignee}")
        header: dict[str, Any] = {
            "type": "header",
            "text": {"type": "plain_text", "text": str(title)[:150]},
        }
        section: dict[str, Any] = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": (str(body) or " ")[:3000]},
        }
        context: dict[str, Any] = {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "  ".join(meta)}],
        }
        blocks = [header, section, context]
        if task.url:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open task"},
                            "url": task.url,
                        }
                    ],
                }
            )
        return blocks

    def payload(self, task: Task, idempotency_key: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "text": f"{task.title} [{task.id} v{task.version}]",
            "blocks": self.blocks(task),
            "metadata": {
                "event_type": "conduit_task",
                "event_payload": {
                    "task_id": task.id,
                    "version": task.version,
                    "idempotency_key": idempotency_key,
                },
            },
        }
        if not self.uses_incoming_webhook:
            payload["channel"] = self.spec.target
        return payload

    def deliver(self, task: Task, idempotency_key: str, remote_id: str | None = None):
        payload = self.payload(task, idempotency_key)
        headers = {"Idempotency-Key": idempotency_key}
        if self.uses_incoming_webhook:
            url = self.spec.target
        else:
            url = f"{self.api_base}/api/chat.postMessage"
            headers["Authorization"] = f"Bearer {self.secret('token')}"
        response = self._client.post(url, json=payload, headers=headers)
        self.raise_for_status(response)
        ts = self._check_ok(response)
        return DeliveryResult(
            status=DeliveryStatus.DELIVERED,
            connector=self.spec.name,
            task_id=task.id,
            idempotency_key=idempotency_key,
            remote_id=ts,
        )

    def _check_ok(self, response: httpx.Response) -> str | None:
        """Slack returns HTTP 200 with ``ok: false`` for most API errors."""
        if self.uses_incoming_webhook:
            return None
        data = response.json()
        if data.get("ok"):
            return data.get("ts")
        error = data.get("error", "unknown_error")
        if error in RETRYABLE_SLACK_ERRORS:
            raise TransientError(f"slack: {error}", status=response.status_code)
        raise PermanentError(f"slack: {error}", status=response.status_code)

    def healthcheck(self) -> bool:
        if self.uses_incoming_webhook:
            return True
        response = self._client.post(
            f"{self.api_base}/api/auth.test",
            headers={"Authorization": f"Bearer {self.secret('token')}"},
        )
        return response.is_success and bool(response.json().get("ok"))
