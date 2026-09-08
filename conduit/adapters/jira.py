"""Jira Cloud adapter: REST v3 create/update without JQL.

Upsert strategy: the worker hands over the issue key recorded for the previous
revision of this task (``remote_id``). When present, the adapter updates that
issue; when absent, it creates one. Every issue carries a summary tag
``[conduit:<task id>]`` and, when the spec maps one, a custom field holding the
task id, so a human can find it and a replay can never produce an untagged
duplicate. No JQL search is ever issued.
"""

from __future__ import annotations

from typing import Any

from conduit.adapters.base import Adapter
from conduit.core.retry import PermanentError
from conduit.models import DeliveryResult, DeliveryStatus, Task

PRIORITY_MAP = {"low": "Low", "normal": "Medium", "high": "High", "urgent": "Highest"}


def summary_tag(task_id: str) -> str:
    return f"[conduit:{task_id}]"


def adf_paragraph(text: str) -> dict[str, Any]:
    """Atlassian Document Format for a plain paragraph."""
    return {
        "type": "doc",
        "version": 1,
        "content": [{"type": "paragraph", "content": [{"type": "text", "text": text or " "}]}],
    }


class JiraAdapter(Adapter):
    type = "jira"

    @property
    def api_base(self) -> str:
        return f"{(self.spec.base_url or '').rstrip('/')}/rest/api/3"

    def _auth(self) -> tuple[str, str]:
        return (self.secret("email"), self.secret("api_token"))

    def issue_fields(self, task: Task) -> dict[str, Any]:
        mapped = self.mapped(task)
        summary = str(mapped.pop("summary", None) or task.title)
        tag = summary_tag(task.id)
        if tag not in summary:
            summary = f"{summary} {tag}"
        fields: dict[str, Any] = {
            "project": {"key": self.spec.target},
            "summary": summary[:255],
            "issuetype": {"name": str(mapped.pop("issuetype", None) or "Task")},
            "description": adf_paragraph(str(mapped.pop("description", None) or task.body)),
            "labels": [label.replace(" ", "-") for label in task.labels] + ["conduit"],
        }
        priority = mapped.pop("priority", None) or PRIORITY_MAP.get(task.priority)
        if priority:
            fields["priority"] = {"name": str(priority)}
        for remote, value in mapped.items():
            if value is not None:
                fields[remote] = value
        return fields

    def deliver(self, task: Task, idempotency_key: str, remote_id: str | None = None):
        fields = self.issue_fields(task)
        headers = {"Idempotency-Key": idempotency_key, "Accept": "application/json"}
        key = remote_id
        if key:
            response = self._client.put(
                f"{self.api_base}/issue/{key}",
                json={"fields": {k: v for k, v in fields.items() if k != "project"}},
                headers=headers,
                auth=self._auth(),
            )
            if response.status_code == 404:
                key = None
            else:
                self.raise_for_status(response)
        if not key:
            response = self._client.post(
                f"{self.api_base}/issue", json={"fields": fields}, headers=headers, auth=self._auth()
            )
            self.raise_for_status(response)
            key = response.json().get("key")
            if not key:
                raise PermanentError("jira: create response missing issue key")
        return DeliveryResult(
            status=DeliveryStatus.DELIVERED,
            connector=self.spec.name,
            task_id=task.id,
            idempotency_key=idempotency_key,
            remote_id=key,
        )

    def healthcheck(self) -> bool:
        response = self._client.get(f"{self.api_base}/myself", auth=self._auth())
        return response.is_success
