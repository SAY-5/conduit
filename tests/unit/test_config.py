from pathlib import Path

import pytest
from conduit.config import ConfigError, ConnectorSpec, load_all, load_spec


def test_shipped_connectors_load(specs):
    assert set(specs) == {"slack-ops", "jira-support", "webhook-crm"}
    assert specs["jira-support"].queue_name == "conduit-jira-support"
    assert specs["jira-support"].dlq_name == "conduit-jira-support-dlq"
    assert specs["webhook-crm"].queue.max_receive_count == 2


def test_name_defaults_to_filename(tmp_path: Path):
    path = tmp_path / "pager-oncall.yaml"
    path.write_text("type: slack\ntarget: '#oncall'\nsecrets:\n  token: SLACK_TOKEN\n")
    spec = load_spec(path)
    assert spec.name == "pager-oncall"
    assert spec.retry.max_attempts == 5


@pytest.mark.parametrize(
    "body,fragment",
    [
        ("type: slack\ntarget: '#x'\n", "requires secrets"),
        ("type: jira\ntarget: X\nsecrets:\n  email: A\n  api_token: B\n", "requires base_url"),
        ("type: slack\ntarget: '#x'\nsecrets:\n  token: lower_case\n", "UPPER_CASE"),
        ("type: fax\ntarget: x\n", "type"),
        (
            "type: slack\ntarget: '#x'\nsecrets:\n  token: T\nretry:\n  max_attempts: 0\n",
            "max_attempts",
        ),
        (
            "type: slack\ntarget: '#x'\nsecrets:\n  token: T\n"
            "retry:\n  base_seconds: 5\n  max_seconds: 1\n",
            "max_seconds",
        ),
        ("- not\n- a mapping\n", "top level"),
        ("type: [\n", "invalid YAML"),
    ],
)
def test_invalid_specs_are_rejected(tmp_path: Path, body: str, fragment: str):
    path = tmp_path / "bad.yaml"
    path.write_text(body)
    with pytest.raises(ConfigError, match=fragment):
        load_spec(path)


def test_duplicate_names_rejected(tmp_path: Path):
    for fname in ("a.yaml", "b.yaml"):
        (tmp_path / fname).write_text(
            "name: same\ntype: slack\ntarget: '#x'\nsecrets:\n  token: T\n"
        )
    with pytest.raises(ConfigError, match="duplicate"):
        load_all(tmp_path)


def test_empty_directory_rejected(tmp_path: Path):
    with pytest.raises(ConfigError, match="no connector"):
        load_all(tmp_path)


def test_bad_connector_name():
    with pytest.raises(ValueError):
        ConnectorSpec(name="Has Spaces", type="slack", target="#x", secrets={"token": "T"})
