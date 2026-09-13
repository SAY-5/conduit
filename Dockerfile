# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5.4 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --extra fakes
COPY conduit ./conduit
COPY fakes ./fakes
RUN uv sync --frozen --no-dev --extra fakes

FROM python:3.12-slim AS runtime
RUN groupadd --system conduit && useradd --system --gid conduit --uid 10001 conduit \
    && apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=conduit:conduit /app/.venv /app/.venv
COPY --chown=conduit:conduit conduit ./conduit
COPY --chown=conduit:conduit fakes ./fakes
COPY --chown=conduit:conduit connectors ./connectors
COPY --chown=conduit:conduit schemas ./schemas
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 CONDUIT_CONNECTORS_DIR=/app/connectors \
    CONDUIT_SCHEMAS_DIR=/app/schemas
USER conduit
EXPOSE 9100
ENTRYPOINT []
CMD ["conduit", "--help"]
