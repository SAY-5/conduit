"""Quarantine against real SQS: bad payloads go aside, not into the dead-letter queue."""

from __future__ import annotations

import pytest
from conduit.core.queue import list_dead_letters, list_quarantined, redrive_quarantined
from conduit.core.schema import SchemaField, SourceSchema
from conduit.models import DeliveryStatus

from tests.integration.conftest import task

pytestmark = pytest.mark.integration


def region_schema(version: int = 1, **overrides: SchemaField) -> SourceSchema:
    fields = {
        "title": SchemaField(type="string", required=True),
        "fields.region": SchemaField(type="string", required=True, enum=["emea", "amer"]),
    }
    fields.update(overrides)
    return SourceSchema(connector="it", version=version, fields=fields)


def test_a_bad_payload_is_quarantined_not_dead_lettered(rig_factory):
    rig = rig_factory("slack-ops", schema=region_schema())
    rig.submit(
        task("Q-1", fields={"region": "emea"}),
        task("Q-2"),
        task("Q-3", fields={"region": "apac"}),
    )
    stats = rig.run()

    assert stats.delivered == 1 and stats.quarantined == 2 and stats.dead_lettered == 0
    assert rig.fake.inbox()["count"] == 1
    assert list_dead_letters(rig.dlq) == []
    held = list_quarantined(rig.quarantine)
    assert sorted(m.envelope.task.id for m in held) == ["Q-2", "Q-3"]
    notes = {m.envelope.task.id: m.envelope.quarantine for m in held}
    assert notes["Q-2"].stage == "schema" and notes["Q-2"].reason == "missing"
    assert notes["Q-3"].reason == "enum" and notes["Q-3"].field == "fields.region"
    assert rig.queue.depth()["ApproximateNumberOfMessages"] == 0


def test_a_mapping_failure_is_quarantined_with_its_own_stage(rig_factory):
    rig = rig_factory("webhook-crm")
    rig.submit(task("QM-1", title="x" * 201))
    result = rig.worker.handle(rig.queue.receive(wait_seconds=3)[0])

    assert result.status == DeliveryStatus.QUARANTINED
    held = list_quarantined(rig.quarantine)
    assert [m.envelope.task.id for m in held] == ["QM-1"]
    note = held[0].envelope.quarantine
    assert note.stage == "mapping" and note.reason == "max_length" and note.field == "name"
    assert list_dead_letters(rig.dlq) == []


def test_a_compatible_schema_change_keeps_accepting_the_same_payloads(rig_factory):
    relaxed = region_schema(
        2, **{"fields.region": SchemaField(type="string", enum=["emea", "amer", "apac"])}
    )
    rig = rig_factory("slack-ops", schema=relaxed)
    rig.submit(task("QC-1", fields={"region": "emea"}), task("QC-2", fields={"region": "apac"}))
    stats = rig.run()

    assert stats.delivered == 2 and stats.quarantined == 0
    assert rig.fake.inbox()["count"] == 2
    assert list_quarantined(rig.quarantine) == []


def test_quarantined_messages_are_redriven_after_the_schema_is_fixed(rig_factory):
    rig = rig_factory("slack-ops", schema=region_schema())
    rig.submit(task("QR-1", fields={"region": "apac"}), task("QR-2", fields={"region": "apac"}))
    assert rig.run().quarantined == 2
    assert rig.fake.inbox()["count"] == 0

    rig.worker.schema = region_schema(
        2, **{"fields.region": SchemaField(type="string", required=True, enum=["emea", "apac"])}
    )
    assert redrive_quarantined(rig.quarantine, rig.queue) == 2
    stats = rig.run()

    assert stats.delivered == 2
    inbox = rig.fake.inbox()
    assert inbox["count"] == 2 and inbox["unique_keys"] == 2
    assert list_quarantined(rig.quarantine) == []
    assert list_dead_letters(rig.dlq) == []


def test_redrive_moves_only_what_quarantine_held_when_it_started(rig_factory):
    rig = rig_factory("slack-ops", schema=region_schema())
    rig.submit(task("QB-1"), task("QB-2"))
    assert rig.run().quarantined == 2

    assert redrive_quarantined(rig.quarantine, rig.queue) == 2
    assert rig.run().quarantined == 4
    assert len(list_quarantined(rig.quarantine)) == 2


def test_a_redriven_message_keeps_its_key_and_loses_its_note(rig_factory):
    rig = rig_factory("slack-ops", schema=region_schema())
    rig.submit(task("QK-1"))
    rig.run()
    original = list_quarantined(rig.quarantine)[0].envelope
    assert original.quarantine is not None and original.attempt == 0

    redrive_quarantined(rig.quarantine, rig.queue)
    back = rig.queue.receive(wait_seconds=3)[0].envelope
    assert back.idempotency_key == original.idempotency_key
    assert back.quarantine is None and back.attempt == 1
