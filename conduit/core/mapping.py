"""Typed field mappings: render, default, coerce, validate, and truncate before delivery.

The worker runs ``validate_task`` before it claims an idempotency key, so a task
that can never be delivered is rejected up front with the field and reason
instead of cycling through retries and the dead-letter queue.
"""

from __future__ import annotations

import string
from typing import Any

from conduit.config import ConnectorSpec, FieldRule, FieldType
from conduit.models import Task

TRUE_WORDS = {"true", "1", "yes", "on"}
FALSE_WORDS = {"false", "0", "no", "off"}


class MappingError(ValueError):
    """A task does not fit the connector's mapping; ``reason`` is a metric label."""

    def __init__(self, field: str, reason: str, detail: str) -> None:
        super().__init__(f"{field}: {detail}")
        self.field = field
        self.reason = reason
        self.detail = detail


def render(source: str, task: Task) -> Any:
    """Resolve a bare field path (type preserved) or a ``$``-template (string)."""
    if "$" not in source:
        return task.field(source)
    values = {
        "id": task.id,
        "version": task.version,
        "title": task.title,
        "body": task.body,
        "status": task.status,
        "priority": task.priority,
        "assignee": task.assignee or "",
        "url": task.url or "",
        "labels": ",".join(task.labels),
        **{k: v for k, v in task.fields.items() if isinstance(v, str | int | float)},
    }
    return string.Template(source).safe_substitute(values)


def coerce(value: Any, type_: FieldType) -> Any:
    """Convert ``value`` to ``type_``; raises ValueError when it cannot be read that way."""
    if type_ == "any":
        return value
    if type_ == "string":
        return value if isinstance(value, str) else str(value)
    if type_ == "integer":
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError("not a whole number")
        return int(value)
    if type_ == "number":
        if isinstance(value, bool):
            raise ValueError("boolean is not a number")
        return float(value)
    if type_ == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, int | float):
            return bool(value)
        word = str(value).strip().lower()
        if word in TRUE_WORDS:
            return True
        if word in FALSE_WORDS:
            return False
        raise ValueError("not a boolean word")
    if isinstance(value, list | tuple | set):
        return list(value)
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    raise ValueError("not a list")


def apply_rule(remote: str, rule: FieldRule, task: Task) -> Any:
    value = render(rule.source, task) if rule.source is not None else None
    if value is None:
        value = rule.default
    if value is None:
        if rule.required:
            raise MappingError(remote, "required", "required field is missing")
        return None
    try:
        value = coerce(value, rule.type)
    except (TypeError, ValueError) as exc:
        raise MappingError(remote, "type", f"expected {rule.type}, got {value!r} ({exc})") from exc
    if rule.enum is not None and value not in rule.enum:
        raise MappingError(remote, "enum", f"{value!r} is not one of {rule.enum}")
    overlong = rule.max_length is not None and isinstance(value, str | list)
    if overlong and len(value) > rule.max_length:
        if not rule.truncate:
            raise MappingError(
                remote, "max_length", f"length {len(value)} exceeds {rule.max_length}"
            )
        value = value[: rule.max_length]
    return value


def apply_mapping(spec: ConnectorSpec, task: Task) -> dict[str, Any]:
    """Every remote field for ``task``; raises ``MappingError`` on the first violation."""
    return {remote: apply_rule(remote, rule, task) for remote, rule in spec.mapping.items()}


def validate_task(spec: ConnectorSpec, task: Task) -> MappingError | None:
    try:
        apply_mapping(spec, task)
    except MappingError as err:
        return err
    return None
