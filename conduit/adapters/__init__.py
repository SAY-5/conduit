"""Adapter registry: connector type name to implementation."""

from __future__ import annotations

from conduit.adapters.base import Adapter
from conduit.config import ConnectorSpec


def build_adapter(spec: ConnectorSpec, **kwargs) -> Adapter:
    from conduit.adapters.jira import JiraAdapter
    from conduit.adapters.slack import SlackAdapter
    from conduit.adapters.webhook import WebhookAdapter

    registry: dict[str, type[Adapter]] = {
        "slack": SlackAdapter,
        "jira": JiraAdapter,
        "webhook": WebhookAdapter,
    }
    return registry[spec.type](spec, **kwargs)
