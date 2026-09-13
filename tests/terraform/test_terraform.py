"""Terraform layout checks: fmt, validate, and the add-one-YAML plan diff.

Plans use a throwaway local state, so they need the terraform binary and the
provider cached by ``terraform init`` but not a running LocalStack.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
TF_DIR = ROOT / "terraform"
CONNECTORS = ROOT / "connectors"

pytestmark = [
    pytest.mark.terraform,
    pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform binary not installed"),
]

NEW_CONNECTOR = """\
type: slack
target: "#oncall"
secrets:
  token: PAGER_SLACK_TOKEN
retry:
  max_attempts: 6
queue:
  max_receive_count: 4
"""

PER_CONNECTOR = {
    "module.queue.aws_sqs_queue.main",
    "module.queue.aws_sqs_queue.dlq",
    "module.queue.aws_sqs_queue.quarantine",
    "module.queue.aws_sqs_queue_redrive_allow_policy.dlq",
    "aws_iam_policy.worker",
    "aws_iam_role.worker",
    "aws_iam_role_policy_attachment.worker",
}


def tf(*args: str, cwd: Path = TF_DIR) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["terraform", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        env={
            "TF_IN_AUTOMATION": "1",
            "PATH": __import__("os").environ["PATH"],
            "HOME": __import__("os").environ["HOME"],
        },
        check=False,
    )


@pytest.fixture(scope="module", autouse=True)
def initialised():
    result = tf("init", "-input=false", "-backend=false")
    assert result.returncode == 0, result.stderr


def test_fmt_and_validate():
    fmt = tf("fmt", "-check", "-recursive")
    assert fmt.returncode == 0, fmt.stdout + fmt.stderr
    validate = tf("validate", "-json")
    assert validate.returncode == 0, validate.stderr
    assert json.loads(validate.stdout)["valid"] is True


def plan_creates(connectors_dir: Path, state: Path) -> set[str]:
    plan_file = state.with_suffix(".tfplan")
    result = tf(
        "plan",
        "-input=false",
        "-var-file=localstack.tfvars",
        f"-var=connectors_dir={connectors_dir}",
        f"-state={state}",
        f"-out={plan_file}",
    )
    assert result.returncode == 0, result.stderr
    shown = tf("show", "-json", str(plan_file))
    assert shown.returncode == 0, shown.stderr
    changes = [rc for rc in json.loads(shown.stdout)["resource_changes"] if rc["mode"] == "managed"]
    assert all(rc["change"]["actions"] == ["create"] for rc in changes)
    return {rc["address"] for rc in changes}


def test_every_shipped_yaml_becomes_a_connector(tmp_path: Path):
    addresses = plan_creates(CONNECTORS, tmp_path / "a.tfstate")
    names = {p.stem for p in CONNECTORS.glob("*.yaml")}
    assert names == {"slack-ops", "jira-support", "webhook-crm"}
    assert "module.idempotency_table.aws_dynamodb_table.this" in addresses
    for name in names:
        for suffix in PER_CONNECTOR:
            assert f'module.connector["{name}"].{suffix}' in addresses
    secrets = 1 + 2 + 1
    assert len(addresses) == 1 + len(names) * len(PER_CONNECTOR) + secrets == 26


def test_adding_one_yaml_adds_exactly_that_connector(tmp_path: Path):
    workdir = tmp_path / "connectors"
    shutil.copytree(CONNECTORS, workdir)
    before = plan_creates(workdir, tmp_path / "before.tfstate")
    (workdir / "pager-oncall.yaml").write_text(NEW_CONNECTOR)
    after = plan_creates(workdir, tmp_path / "after.tfstate")
    added = after - before
    assert before <= after
    expected = {f'module.connector["pager-oncall"].{suffix}' for suffix in PER_CONNECTOR} | {
        'module.connector["pager-oncall"].aws_ssm_parameter.secret["token"]'
    }
    assert added == expected
    assert len(added) == 8
