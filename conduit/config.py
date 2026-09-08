"""Connector specs: one YAML file per integration, validated with pydantic."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

ConnectorType = Literal["slack", "jira", "webhook"]


class RetryPolicy(BaseModel):
    """Exponential backoff with full jitter, bounded by ``max_attempts``."""

    max_attempts: int = Field(default=5, ge=1, le=20)
    base_seconds: float = Field(default=0.5, gt=0)
    max_seconds: float = Field(default=30.0, gt=0)
    multiplier: float = Field(default=2.0, ge=1.0)
    retry_on_status: list[int] = Field(default_factory=lambda: [408, 425, 429, 500, 502, 503, 504])
    timeout_seconds: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _max_at_least_base(self) -> RetryPolicy:
        if self.max_seconds < self.base_seconds:
            raise ValueError("max_seconds must be >= base_seconds")
        return self


class RateLimit(BaseModel):
    requests_per_second: float = Field(default=10.0, gt=0)


class QueueSpec(BaseModel):
    """Per-connector SQS settings; Terraform reads the same keys."""

    max_receive_count: int = Field(default=3, ge=1, le=100)
    visibility_timeout_seconds: int = Field(default=60, ge=1, le=43200)
    message_retention_seconds: int = Field(default=345600, ge=60, le=1209600)
    dlq_retention_seconds: int = Field(default=1209600, ge=60, le=1209600)


class ConnectorSpec(BaseModel):
    """Everything the worker and Terraform need to know about one integration."""

    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    type: ConnectorType
    target: str = Field(min_length=1, description="Channel, project key, or URL")
    base_url: str | None = None
    secrets: dict[str, str] = Field(
        default_factory=dict,
        description="Logical secret name to environment variable name",
    )
    mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Remote field to task field path or template",
    )
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    rate_limit: RateLimit = Field(default_factory=RateLimit)
    queue: QueueSpec = Field(default_factory=QueueSpec)
    idempotency_ttl_seconds: int = Field(default=7 * 24 * 3600, ge=60)

    @field_validator("secrets")
    @classmethod
    def _env_names(cls, value: dict[str, str]) -> dict[str, str]:
        for logical, env in value.items():
            if not env.isidentifier() or env != env.upper():
                raise ValueError(f"secret {logical!r} must map to an UPPER_CASE env var name")
        return value

    @model_validator(mode="after")
    def _required_secrets(self) -> ConnectorSpec:
        required = {
            "slack": {"token"},
            "jira": {"email", "api_token"},
            "webhook": {"signing_secret"},
        }[self.type]
        missing = required - set(self.secrets)
        if missing:
            raise ValueError(f"{self.type} connector requires secrets: {sorted(missing)}")
        if self.type == "jira" and not self.base_url:
            raise ValueError("jira connector requires base_url")
        return self

    @property
    def queue_name(self) -> str:
        return f"conduit-{self.name}"

    @property
    def dlq_name(self) -> str:
        return f"conduit-{self.name}-dlq"


class ConfigError(ValueError):
    pass


def load_spec(path: Path) -> ConnectorSpec:
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    raw.setdefault("name", path.stem)
    try:
        return ConnectorSpec.model_validate(raw)
    except ValueError as exc:
        raise ConfigError(f"{path}: {exc}") from exc


def load_all(directory: Path) -> dict[str, ConnectorSpec]:
    specs: dict[str, ConnectorSpec] = {}
    for path in sorted(directory.glob("*.yaml")):
        spec = load_spec(path)
        if spec.name in specs:
            raise ConfigError(f"duplicate connector name {spec.name!r} in {path}")
        specs[spec.name] = spec
    if not specs:
        raise ConfigError(f"no connector YAML files found in {directory}")
    return specs
