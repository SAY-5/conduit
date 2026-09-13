"""Versioned source schemas: what a task must already look like to be worth delivering.

Mapping rules describe the remote side of a connector. A schema describes the
source side: the fields the producer promised to send. The worker checks the
schema first, so a payload the producer got wrong is moved to the connector's
quarantine queue instead of the dead-letter queue, which stays reserved for
deliveries the handler could not complete.

Schemas live in ``schemas/<connector>/v<N>.yaml`` and are loaded as an ordered
registry. Loading refuses a breaking change between consecutive versions: a new
version may loosen what it accepts but never tighten it, because tightening
would quarantine payloads the previous version let through.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from conduit.config import FieldType
from conduit.models import Task

# ``value`` of the key type is also a valid value of every type it maps to.
WIDER: dict[FieldType, set[FieldType]] = {
    "integer": {"number", "any"},
    "number": {"any"},
    "string": {"any"},
    "boolean": {"any"},
    "list": {"any"},
    "any": set(),
}


class SchemaError(ValueError):
    """A task does not match its source schema; ``reason`` is a metric label."""

    def __init__(self, field: str, reason: str, detail: str) -> None:
        super().__init__(f"{field}: {detail}")
        self.field = field
        self.reason = reason
        self.detail = detail


class RegistryError(ValueError):
    pass


class SchemaField(BaseModel):
    """One source field. Values are checked as they arrive, never coerced."""

    type: FieldType = "any"
    required: bool = False
    enum: list[Any] | None = None
    max_length: int | None = Field(default=None, ge=1)


class SourceSchema(BaseModel):
    """One version of one connector's source contract."""

    connector: str
    version: int = Field(ge=1)
    fields: dict[str, SchemaField]

    @property
    def label(self) -> str:
        return f"{self.connector}/v{self.version}"


def matches(value: Any, type_: FieldType) -> bool:
    if type_ == "any":
        return True
    if type_ == "boolean":
        return isinstance(value, bool)
    if isinstance(value, bool):
        return False
    if type_ == "string":
        return isinstance(value, str)
    if type_ == "integer":
        return isinstance(value, int)
    if type_ == "number":
        return isinstance(value, int | float)
    return isinstance(value, list)


def check_field(path: str, rule: SchemaField, value: Any) -> None:
    if value is None:
        if rule.required:
            raise SchemaError(path, "missing", "required source field is absent")
        return
    if not matches(value, rule.type):
        raise SchemaError(path, "type", f"expected {rule.type}, got {type(value).__name__}")
    if rule.enum is not None and value not in rule.enum:
        raise SchemaError(path, "enum", f"{value!r} is not one of {rule.enum}")
    sized = rule.max_length is not None and isinstance(value, str | list)
    if sized and len(value) > rule.max_length:
        raise SchemaError(path, "too_long", f"length {len(value)} exceeds {rule.max_length}")


def validate_payload(schema: SourceSchema, task: Task) -> SchemaError | None:
    """Check ``task`` against every field in ``schema``; first violation wins."""
    for path, rule in schema.fields.items():
        try:
            check_field(path, rule, task.field(path))
        except SchemaError as err:
            return err
    return None


def breaking_changes(old: SourceSchema, new: SourceSchema) -> list[str]:
    """Fields where ``new`` would reject a payload ``old`` accepted."""
    breaks = []
    for path, after in new.fields.items():
        before = old.fields.get(path)
        if after.required and not (before and before.required):
            breaks.append(f"{path}: became required")
        if before is None:
            continue
        if after.type != before.type and after.type not in WIDER[before.type]:
            breaks.append(f"{path}: type {before.type} narrowed to {after.type}")
        if after.enum is not None and (before.enum is None or set(after.enum) < set(before.enum)):
            breaks.append(f"{path}: enum no longer accepts every previous value")
        if after.max_length is not None and (
            before.max_length is None or after.max_length < before.max_length
        ):
            breaks.append(f"{path}: max_length lowered to {after.max_length}")
    return breaks


class SchemaRegistry:
    """Every version of every connector's schema, newest last."""

    def __init__(self, versions: dict[str, list[SourceSchema]]) -> None:
        self._versions = versions

    def __contains__(self, connector: str) -> bool:
        return connector in self._versions

    def connectors(self) -> list[str]:
        return sorted(self._versions)

    def history(self, connector: str) -> list[SourceSchema]:
        return list(self._versions.get(connector, []))

    def latest(self, connector: str) -> SourceSchema | None:
        history = self._versions.get(connector)
        return history[-1] if history else None

    def at(self, connector: str, version: int) -> SourceSchema:
        for schema in self._versions.get(connector, []):
            if schema.version == version:
                return schema
        raise RegistryError(f"no schema {connector}/v{version}")


def load_schema(path: Path, connector: str) -> SourceSchema:
    if not path.stem.startswith("v") or not path.stem[1:].isdigit():
        raise RegistryError(f"{path}: schema files are named v<N>.yaml")
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise RegistryError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise RegistryError(f"{path}: top level must be a mapping")
    try:
        return SourceSchema.model_validate(
            {"connector": connector, "version": int(path.stem[1:]), **raw}
        )
    except ValueError as exc:
        raise RegistryError(f"{path}: {exc}") from exc


def load_registry(directory: Path) -> SchemaRegistry:
    """Load ``<directory>/<connector>/v<N>.yaml``, refusing any breaking change.

    A missing directory is an empty registry: schemas are optional, and a
    connector without one accepts whatever its mapping rules can render.
    """
    versions: dict[str, list[SourceSchema]] = {}
    if not directory.is_dir():
        return SchemaRegistry(versions)
    for connector_dir in sorted(p for p in directory.iterdir() if p.is_dir()):
        connector = connector_dir.name
        history = sorted(
            (load_schema(path, connector) for path in connector_dir.glob("*.yaml")),
            key=lambda s: s.version,
        )
        if not history:
            continue
        numbers = [s.version for s in history]
        if numbers != list(range(1, len(numbers) + 1)):
            raise RegistryError(f"{connector}: versions must run 1..N, found {numbers}")
        for old, new in zip(history, history[1:], strict=False):
            breaks = breaking_changes(old, new)
            if breaks:
                raise RegistryError(
                    f"{new.label} is a breaking change from v{old.version}: " + "; ".join(breaks)
                )
        versions[connector] = history
    return SchemaRegistry(versions)
