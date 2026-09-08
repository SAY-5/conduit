"""Mapping rules: shorthand, defaults, coercion, enums, truncation, and up-front rejection."""

from __future__ import annotations

from pathlib import Path

import pytest
from conduit.config import ConfigError, ConnectorSpec, FieldRule, load_spec
from conduit.core.mapping import MappingError, apply_mapping, coerce, validate_task
from conduit.models import Task


def spec_with(mapping: dict) -> ConnectorSpec:
    return ConnectorSpec(
        name="hook",
        type="webhook",
        target="https://x/h",
        secrets={"signing_secret": "S"},
        mapping=mapping,
    )


def test_shorthand_string_is_a_source_only_rule():
    spec = spec_with({"name": "title", "rev": "version"})
    assert spec.mapping["name"] == FieldRule(source="title")
    assert apply_mapping(spec, Task(id="A", title="t", version=3)) == {"name": "t", "rev": 3}


def test_rule_needs_a_source_or_a_default(tmp_path: Path):
    path = tmp_path / "bad.yaml"
    path.write_text(
        "type: webhook\ntarget: https://x/h\nsecrets:\n  signing_secret: S\n"
        "mapping:\n  kind:\n    required: true\n"
    )
    with pytest.raises(ConfigError, match="source or a default"):
        load_spec(path)
    with pytest.raises(ValueError, match="enum"):
        FieldRule(source="status", enum=[])


def test_default_fills_missing_and_constant_needs_no_source():
    spec = spec_with(
        {"owner": {"source": "assignee", "default": "unassigned"}, "kind": {"default": "task"}}
    )
    assert apply_mapping(spec, Task(id="A", title="t")) == {"owner": "unassigned", "kind": "task"}
    assert apply_mapping(spec, Task(id="A", title="t", assignee="ana"))["owner"] == "ana"


def test_required_field_missing_is_rejected():
    spec = spec_with({"owner": {"source": "assignee", "required": True}})
    with pytest.raises(MappingError) as info:
        apply_mapping(spec, Task(id="A", title="t"))
    assert info.value.field == "owner" and info.value.reason == "required"
    assert validate_task(spec, Task(id="A", title="t", assignee="ana")) is None


@pytest.mark.parametrize(
    "value,type_,expected",
    [
        (7, "string", "7"),
        ("42", "integer", 42),
        (3.0, "integer", 3),
        ("2.5", "number", 2.5),
        ("yes", "boolean", True),
        ("off", "boolean", False),
        (1, "boolean", True),
        ("a, b,,c", "list", ["a", "b", "c"]),
        (("x", "y"), "list", ["x", "y"]),
        ({"k": 1}, "any", {"k": 1}),
    ],
)
def test_coercion(value, type_, expected):
    assert coerce(value, type_) == expected


@pytest.mark.parametrize(
    "value,type_",
    [("abc", "integer"), (2.5, "integer"), (True, "integer"), ("maybe", "boolean"), (3, "list")],
)
def test_bad_coercion_is_a_type_rejection(value, type_):
    with pytest.raises(ValueError):
        coerce(value, type_)
    spec = spec_with({"f": {"source": "fields.v", "type": type_}})
    with pytest.raises(MappingError) as info:
        apply_mapping(spec, Task(id="A", title="t", fields={"v": value}))
    assert info.value.reason == "type"


def test_enum_rejects_values_outside_the_list():
    spec = spec_with({"state": {"source": "status", "enum": ["open", "done"]}})
    assert apply_mapping(spec, Task(id="A", title="t", status="done"))["state"] == "done"
    with pytest.raises(MappingError) as info:
        apply_mapping(spec, Task(id="A", title="t", status="weird"))
    assert info.value.reason == "enum" and "weird" in str(info.value)


def test_max_length_truncates_or_rejects():
    task = Task(id="A", title="x" * 300, labels=["a", "b", "c"])
    cut = spec_with({"name": {"source": "title", "max_length": 10}})
    assert apply_mapping(cut, task)["name"] == "x" * 10
    tags = spec_with({"tags": {"source": "labels", "type": "list", "max_length": 2}})
    assert apply_mapping(tags, task)["tags"] == ["a", "b"]
    strict = spec_with({"name": {"source": "title", "max_length": 10, "truncate": False}})
    with pytest.raises(MappingError) as info:
        apply_mapping(strict, task)
    assert info.value.reason == "max_length"


def test_template_with_type_and_default_chain():
    spec = spec_with({"label": {"source": "$id v$version in $region", "default": "n/a"}})
    task = Task(id="T", title="t", fields={"region": "eu"})
    assert apply_mapping(spec, task)["label"] == "T v1 in eu"


def test_shipped_rules_accept_a_normal_task_and_reject_a_bad_one(specs):
    ok = Task(id="T-1", title="Rotate keys", body="soon", assignee="ana")
    for spec in specs.values():
        assert validate_task(spec, ok) is None
    crm = specs["webhook-crm"]
    assert apply_mapping(crm, ok)["owner"] == "ana"
    assert apply_mapping(crm, Task(id="T-1", title="t"))["owner"] == "unassigned"
    bad_state = validate_task(crm, Task(id="T-1", title="t", status="on-hold"))
    assert bad_state is not None and bad_state.reason == "enum"
    long_name = validate_task(crm, Task(id="T-1", title="n" * 201))
    assert long_name is not None and long_name.reason == "max_length"
    jira = specs["jira-support"]
    assert apply_mapping(jira, Task(id="T-1", title="n" * 300))["summary"] == "n" * 255
    assert apply_mapping(jira, ok)["issuetype"] == "Task"
