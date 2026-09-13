"""Source schemas: payload checks, the versioned registry, and the compatibility rule."""

from __future__ import annotations

from pathlib import Path

import pytest
from conduit.core.schema import (
    RegistryError,
    SchemaField,
    SourceSchema,
    breaking_changes,
    load_registry,
    validate_payload,
)
from conduit.models import Task

ROOT = Path(__file__).resolve().parent.parent.parent


def schema(fields: dict[str, SchemaField], at: int = 1) -> SourceSchema:
    return SourceSchema(connector="acme", version=at, fields=fields)


def write(directory: Path, connector: str, version: int, body: str) -> None:
    target = directory / connector
    target.mkdir(parents=True, exist_ok=True)
    (target / f"v{version}.yaml").write_text(body)


def test_a_matching_payload_passes():
    s = schema(
        {
            "title": SchemaField(type="string", required=True),
            "version": SchemaField(type="integer", required=True),
            "labels": SchemaField(type="list"),
        }
    )
    assert validate_payload(s, Task(id="T-1", title="ok", labels=["a"])) is None


@pytest.mark.parametrize(
    "rule,task,reason",
    [
        ({"assignee": SchemaField(required=True)}, Task(id="T", title="t"), "missing"),
        (
            {"priority": SchemaField(type="integer")},
            Task(id="T", title="t", priority="high"),
            "type",
        ),
        (
            {"status": SchemaField(enum=["open", "done"])},
            Task(id="T", title="t", status="paused"),
            "enum",
        ),
        ({"title": SchemaField(max_length=3)}, Task(id="T", title="long"), "too_long"),
    ],
)
def test_violations_carry_a_field_and_reason(rule, task, reason):
    problem = validate_payload(schema(rule), task)
    assert problem is not None and problem.reason == reason
    assert problem.field in rule


def test_a_boolean_is_not_an_integer():
    problem = validate_payload(
        schema({"fields.flag": SchemaField(type="integer")}),
        Task(id="T", title="t", fields={"flag": True}),
    )
    assert problem is not None and problem.reason == "type"


@pytest.mark.parametrize(
    "before,after",
    [
        (SchemaField(type="string"), SchemaField(type="string")),
        (SchemaField(type="string", required=True), SchemaField(type="string")),
        (SchemaField(type="integer"), SchemaField(type="number")),
        (SchemaField(type="string"), SchemaField(type="any")),
        (SchemaField(enum=["a"]), SchemaField(enum=["a", "b"])),
        (SchemaField(max_length=10), SchemaField(max_length=20)),
        (SchemaField(max_length=10), SchemaField()),
    ],
)
def test_loosening_is_compatible(before, after):
    assert breaking_changes(schema({"f": before}), schema({"f": after}, at=2)) == []


@pytest.mark.parametrize(
    "before,after,fragment",
    [
        (SchemaField(), SchemaField(required=True), "became required"),
        (SchemaField(type="number"), SchemaField(type="integer"), "narrowed"),
        (SchemaField(enum=["a", "b"]), SchemaField(enum=["a"]), "enum"),
        (SchemaField(), SchemaField(enum=["a"]), "enum"),
        (SchemaField(max_length=20), SchemaField(max_length=10), "max_length"),
        (SchemaField(), SchemaField(max_length=10), "max_length"),
    ],
)
def test_tightening_is_breaking(before, after, fragment):
    breaks = breaking_changes(schema({"f": before}), schema({"f": after}, at=2))
    assert len(breaks) == 1 and fragment in breaks[0]


def test_a_new_optional_field_is_compatible_and_a_required_one_is_not():
    old = schema({"title": SchemaField(type="string")})
    base = {"title": SchemaField(type="string")}
    added_optional = schema({**base, "note": SchemaField()}, at=2)
    added_required = schema({**base, "note": SchemaField(required=True)}, at=2)
    assert breaking_changes(old, added_optional) == []
    assert breaking_changes(old, added_required) == ["note: became required"]


def test_dropping_a_field_is_compatible():
    old = schema({"title": SchemaField(type="string"), "note": SchemaField(required=True)})
    assert breaking_changes(old, schema({"title": SchemaField(type="string")}, at=2)) == []


def test_registry_loads_every_version_in_order(tmp_path: Path):
    write(tmp_path, "acme", 1, "fields:\n  title:\n    type: string\n")
    write(tmp_path, "acme", 2, "fields:\n  title:\n    type: any\n")
    registry = load_registry(tmp_path)
    assert registry.connectors() == ["acme"]
    assert [s.version for s in registry.history("acme")] == [1, 2]
    assert registry.latest("acme").version == 2
    assert registry.at("acme", 1).fields["title"].type == "string"
    assert "acme" in registry and "other" not in registry


def test_registry_refuses_a_breaking_change(tmp_path: Path):
    write(tmp_path, "acme", 1, "fields:\n  title:\n    type: string\n")
    write(tmp_path, "acme", 2, "fields:\n  title:\n    type: string\n    required: true\n")
    with pytest.raises(RegistryError, match="breaking change"):
        load_registry(tmp_path)


def test_registry_refuses_a_gap_in_the_version_run(tmp_path: Path):
    write(tmp_path, "acme", 1, "fields:\n  title:\n    type: string\n")
    write(tmp_path, "acme", 3, "fields:\n  title:\n    type: string\n")
    with pytest.raises(RegistryError, match=r"1..N"):
        load_registry(tmp_path)


def test_a_missing_directory_is_an_empty_registry(tmp_path: Path):
    registry = load_registry(tmp_path / "nope")
    assert registry.connectors() == [] and registry.latest("acme") is None


def test_shipped_schemas_load_and_accept_a_plain_task():
    registry = load_registry(ROOT / "schemas")
    assert registry.connectors() == ["jira-support", "slack-ops", "webhook-crm"]
    task = Task(id="T-1", title="a task", priority="high", labels=["demo"])
    for name in registry.connectors():
        assert validate_payload(registry.latest(name), task) is None
