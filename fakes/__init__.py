"""Small FastAPI stand-ins for Slack, Jira, and a webhook receiver.

Each fake records what it received (with idempotency keys) at ``GET /_inbox`` and
accepts failure injection at ``POST /_faults`` or through environment variables,
so integration tests and the demo can prove retry, dedup, and dead-lettering.
"""
