import boto3
import pytest
from conduit.core.idempotency import (
    ClaimState,
    DynamoIdempotencyStore,
    MemoryIdempotencyStore,
    idempotency_key,
)
from moto import mock_aws


def test_key_is_deterministic_and_version_sensitive():
    a = idempotency_key("slack-ops", "T-1", 1)
    assert a == idempotency_key("slack-ops", "T-1", 1)
    assert a != idempotency_key("slack-ops", "T-1", 2)
    assert a != idempotency_key("jira-support", "T-1", 1)
    assert len(a) == 64


@pytest.fixture
def dynamo_store():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        store = DynamoIdempotencyStore("conduit-idempotency", client=client, lease_seconds=60)
        store.ensure_table()
        ttl = client.describe_time_to_live(TableName="conduit-idempotency")
        assert ttl["TimeToLiveDescription"]["AttributeName"] == "expires_at"
        yield store


@pytest.fixture(params=["dynamo", "memory"])
def store(request, dynamo_store):
    return dynamo_store if request.param == "dynamo" else MemoryIdempotencyStore(lease_seconds=60)


def test_conditional_put_claims_once(store):
    key = idempotency_key("slack-ops", "T-1", 1)
    first = store.claim(key, connector="slack-ops", task_id="T-1")
    assert first.acquired and first.state == ClaimState.IN_PROGRESS
    second = store.claim(key, connector="slack-ops", task_id="T-1")
    assert not second.acquired and second.state == ClaimState.IN_PROGRESS
    assert not second.duplicate


def test_delivered_claim_is_a_duplicate(store):
    key = idempotency_key("slack-ops", "T-2", 1)
    assert store.claim(key, connector="slack-ops", task_id="T-2").acquired
    store.mark_delivered(key, "1700000000.1")
    again = store.claim(key, connector="slack-ops", task_id="T-2")
    assert again.duplicate and again.remote_id == "1700000000.1"
    assert store.latest("slack-ops", "T-2") == "1700000000.1"


def test_release_lets_a_redrive_claim_again(store):
    key = idempotency_key("jira-support", "T-3", 1)
    assert store.claim(key, connector="jira-support", task_id="T-3").acquired
    store.release(key)
    assert store.claim(key, connector="jira-support", task_id="T-3").acquired


def test_release_never_drops_a_delivered_record(store):
    key = idempotency_key("jira-support", "T-4", 1)
    store.claim(key, connector="jira-support", task_id="T-4")
    store.mark_delivered(key, "SUP-1")
    store.release(key)
    assert store.claim(key, connector="jira-support", task_id="T-4").duplicate


def test_expired_lease_can_be_taken_over():
    now = {"t": 1_000.0}
    store = MemoryIdempotencyStore(clock=lambda: now["t"], lease_seconds=30)
    key = idempotency_key("webhook-crm", "T-5", 1)
    assert store.claim(key, connector="webhook-crm", task_id="T-5").acquired
    assert not store.claim(key, connector="webhook-crm", task_id="T-5").acquired
    now["t"] += 31
    assert store.claim(key, connector="webhook-crm", task_id="T-5").acquired


def test_dynamo_expired_lease_can_be_taken_over():
    with mock_aws():
        now = {"t": 1_000}
        client = boto3.client("dynamodb", region_name="us-east-1")
        store = DynamoIdempotencyStore("t", client=client, lease_seconds=30, clock=lambda: now["t"])
        store.ensure_table()
        key = idempotency_key("webhook-crm", "T-6", 1)
        assert store.claim(key, connector="webhook-crm", task_id="T-6").acquired
        assert not store.claim(key, connector="webhook-crm", task_id="T-6").acquired
        now["t"] += 31
        assert store.claim(key, connector="webhook-crm", task_id="T-6").acquired
        item = client.get_item(TableName="t", Key={"pk": {"S": key}})["Item"]
        assert int(item["expires_at"]["N"]) == now["t"] + store.ttl_seconds


def test_latest_pointer_follows_versions(store):
    for version, remote in ((1, "SUP-10"), (2, "SUP-10"), (3, "SUP-10")):
        key = idempotency_key("jira-support", "T-7", version)
        assert store.claim(key, connector="jira-support", task_id="T-7").acquired
        store.mark_delivered(key, remote)
    assert store.latest("jira-support", "T-7") == "SUP-10"
    assert store.latest("jira-support", "nope") is None
