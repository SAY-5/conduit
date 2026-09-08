import json
import time

import httpx
import pytest
import respx
from conduit.adapters import build_adapter
from conduit.adapters.base import SecretError
from conduit.adapters.jira import summary_tag
from conduit.adapters.webhook import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign, verify
from conduit.core.retry import PermanentError, TransientError
from conduit.models import DeliveryStatus, Task

KEY = "a" * 64


@pytest.fixture
def task() -> Task:
    return Task(
        id="T-100",
        version=2,
        title="Rotate signing keys",
        body="Keys expire Friday",
        priority="high",
        assignee="ana",
        labels=["security", "ops"],
        url="https://tasks.example.com/T-100",
    )


# Slack


@respx.mock
def test_slack_posts_blocks_with_metadata(specs, task):
    route = respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "ts": "1700000000.000100"})
    )
    adapter = build_adapter(specs["slack-ops"])
    result = adapter.deliver(task, KEY)
    assert result.status == DeliveryStatus.DELIVERED and result.remote_id == "1700000000.000100"
    request = route.calls.last.request
    body = json.loads(request.content)
    assert request.headers["Authorization"] == "Bearer xoxb-test"
    assert request.headers["Idempotency-Key"] == KEY
    assert body["channel"] == "#ops"
    assert body["blocks"][0]["text"]["text"] == "[high] Rotate signing keys"
    assert body["metadata"]["event_payload"]["idempotency_key"] == KEY
    assert body["blocks"][-1]["type"] == "actions"


@respx.mock
def test_slack_ok_false_ratelimited_is_transient(specs, task):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(200, json={"ok": False, "error": "ratelimited"})
    )
    with pytest.raises(TransientError):
        build_adapter(specs["slack-ops"]).deliver(task, KEY)


@respx.mock
def test_slack_ok_false_channel_not_found_is_permanent(specs, task):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(200, json={"ok": False, "error": "channel_not_found"})
    )
    with pytest.raises(PermanentError):
        build_adapter(specs["slack-ops"]).deliver(task, KEY)


@respx.mock
def test_slack_http_429_is_transient_with_retry_after(specs, task):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "1"}, json={"ok": False})
    )
    with pytest.raises(TransientError) as info:
        build_adapter(specs["slack-ops"]).deliver(task, KEY)
    assert info.value.retry_after == 1.0


@respx.mock
def test_slack_incoming_webhook_target(specs, task):
    spec = specs["slack-ops"].model_copy(update={"target": "https://hooks.slack.com/services/X"})
    route = respx.post("https://hooks.slack.com/services/X").mock(
        return_value=httpx.Response(200, text="ok")
    )
    result = build_adapter(spec).deliver(task, KEY)
    assert result.remote_id is None
    assert "channel" not in json.loads(route.calls.last.request.content)
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_slack_healthcheck(specs):
    respx.post("https://slack.com/api/auth.test").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    assert build_adapter(specs["slack-ops"]).healthcheck()


def test_missing_secret_is_explicit(specs, task, monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    with pytest.raises(SecretError, match="SLACK_BOT_TOKEN"):
        build_adapter(specs["slack-ops"]).deliver(task, KEY)


# Jira


@respx.mock
def test_jira_creates_issue_with_summary_tag(specs, task):
    route = respx.post("https://example.atlassian.net/rest/api/3/issue").mock(
        return_value=httpx.Response(201, json={"id": "10001", "key": "SUP-42"})
    )
    result = build_adapter(specs["jira-support"]).deliver(task, KEY)
    assert result.remote_id == "SUP-42"
    request = route.calls.last.request
    fields = json.loads(request.content)["fields"]
    assert fields["project"] == {"key": "SUP"}
    assert fields["summary"] == f"Rotate signing keys {summary_tag('T-100')}"
    assert fields["customfield_10042"] == "T-100"
    assert fields["priority"] == {"name": "High"}
    assert fields["description"]["type"] == "doc"
    assert "conduit" in fields["labels"]
    assert request.headers["Idempotency-Key"] == KEY
    assert request.headers["Authorization"].startswith("Basic ")


@respx.mock
def test_jira_updates_known_issue_without_search(specs, task):
    put = respx.put("https://example.atlassian.net/rest/api/3/issue/SUP-42").mock(
        return_value=httpx.Response(204)
    )
    create = respx.post("https://example.atlassian.net/rest/api/3/issue")
    result = build_adapter(specs["jira-support"]).deliver(task, KEY, remote_id="SUP-42")
    assert result.remote_id == "SUP-42"
    assert put.called and not create.called
    assert "project" not in json.loads(put.calls.last.request.content)["fields"]


@respx.mock
def test_jira_recreates_when_known_issue_vanished(specs, task):
    respx.put("https://example.atlassian.net/rest/api/3/issue/SUP-1").mock(
        return_value=httpx.Response(404, json={"errorMessages": ["gone"]})
    )
    respx.post("https://example.atlassian.net/rest/api/3/issue").mock(
        return_value=httpx.Response(201, json={"key": "SUP-2"})
    )
    assert build_adapter(specs["jira-support"]).deliver(task, KEY, remote_id="SUP-1").remote_id == (
        "SUP-2"
    )


@respx.mock
def test_jira_400_is_permanent_and_503_transient(specs, task):
    route = respx.post("https://example.atlassian.net/rest/api/3/issue")
    route.mock(return_value=httpx.Response(400, json={"errors": {"summary": "bad"}}))
    with pytest.raises(PermanentError):
        build_adapter(specs["jira-support"]).deliver(task, KEY)
    route.mock(return_value=httpx.Response(503))
    with pytest.raises(TransientError):
        build_adapter(specs["jira-support"]).deliver(task, KEY)


@respx.mock
def test_jira_timeout_is_transient(specs, task):
    respx.post("https://example.atlassian.net/rest/api/3/issue").mock(
        side_effect=httpx.ReadTimeout("slow")
    )
    with pytest.raises(httpx.ReadTimeout):
        build_adapter(specs["jira-support"]).deliver(task, KEY)


@respx.mock
def test_jira_healthcheck(specs):
    respx.get("https://example.atlassian.net/rest/api/3/myself").mock(
        return_value=httpx.Response(200, json={"accountId": "x"})
    )
    assert build_adapter(specs["jira-support"]).healthcheck()


# Webhook


@respx.mock
def test_webhook_signs_body_and_sets_idempotency_key(specs, task):
    route = respx.post("https://crm.example.com/hooks/conduit").mock(
        return_value=httpx.Response(202, json={"id": "crm-9"})
    )
    result = build_adapter(specs["webhook-crm"]).deliver(task, KEY)
    assert result.remote_id == "crm-9"
    request = route.calls.last.request
    assert request.headers["Idempotency-Key"] == KEY
    ts = request.headers[TIMESTAMP_HEADER]
    assert abs(int(ts) - time.time()) < 5
    assert verify("whsec-test", ts, request.content, request.headers[SIGNATURE_HEADER])
    assert not verify("wrong", ts, request.content, request.headers[SIGNATURE_HEADER])
    payload = json.loads(request.content)
    assert payload["idempotency_key"] == KEY
    assert payload["task"] == {
        "external_id": "T-100",
        "revision": 2,
        "name": "Rotate signing keys",
        "notes": "Keys expire Friday",
        "state": "open",
        "owner": "ana",
    }


def test_signature_rejects_stale_timestamps():
    body = b"{}"
    old = str(int(time.time()) - 600)
    assert not verify("s", old, body, sign("s", old, body))
    assert not verify("s", "not-a-number", body, "sha256=00")


@respx.mock
def test_webhook_5xx_then_4xx_classification(specs, task):
    route = respx.post("https://crm.example.com/hooks/conduit")
    route.mock(return_value=httpx.Response(502))
    with pytest.raises(TransientError):
        build_adapter(specs["webhook-crm"]).deliver(task, KEY)
    route.mock(return_value=httpx.Response(422, json={"error": "schema"}))
    with pytest.raises(PermanentError):
        build_adapter(specs["webhook-crm"]).deliver(task, KEY)


@respx.mock
def test_webhook_healthcheck_uses_head(specs):
    respx.head("https://crm.example.com/hooks/conduit").mock(return_value=httpx.Response(405))
    assert build_adapter(specs["webhook-crm"]).healthcheck()
    respx.head("https://crm.example.com/hooks/conduit").mock(side_effect=httpx.ConnectError("x"))
    assert not build_adapter(specs["webhook-crm"]).healthcheck()


def test_render_templates_and_paths(specs, task):
    adapter = build_adapter(specs["slack-ops"])
    task.fields["region"] = "eu-west-1"
    assert adapter.render("title", task) == "Rotate signing keys"
    assert adapter.render("fields.region", task) == "eu-west-1"
    assert adapter.render("$id v$version in $region", task) == "T-100 v2 in eu-west-1"
    assert adapter.render("missing.path", task) is None
