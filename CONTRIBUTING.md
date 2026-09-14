# Contributing

## Setup

```
make setup          # uv sync --all-extras (Python 3.12)
make lint           # ruff check, ruff format --check, terraform fmt -check
make test-unit      # no Docker needed
make up             # LocalStack, fakes, workers
make tf-apply       # terraform apply against LocalStack
make test           # unit + integration + terraform tests
make demo           # full end-to-end run; make demo-down to tear down
```

Integration tests skip unless `CONDUIT_LOCALSTACK_URL` is set (`make test-integration` sets
it). Terraform tests need the `terraform` binary and network access for the first `init`.

## Adding a connector

1. Add `connectors/<name>.yaml` with `type`, `target`, `secrets`, and any `mapping`, `retry`,
   `rate_limit`, or `queue` overrides. `conduit config validate` must pass.
2. Run `terraform -chdir=terraform plan -var-file=localstack.tfvars`; expect 7 new resources
   plus one SSM parameter per secret.
3. Add the worker service to `deploy/docker-compose.yml` if you want it in the local stack.

## Adding an adapter type

1. Implement `Adapter` in `conduit/adapters/<type>.py`: `deliver`, `healthcheck`, and the
   error classification for that API's failure shapes.
2. Register it in `conduit/adapters/__init__.py` and extend `ConnectorType` and the required
   secrets table in `conduit/config.py`.
3. Add respx unit tests in `tests/unit/test_adapters.py` and, if the demo should cover it, a
   fake under `fakes/` plus an integration test.

## Conventions

* Conventional commit subjects on one line (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
* Ruff is the formatter and linter (line length 100). Terraform is `terraform fmt` clean.
* Tests accompany behaviour changes. Unit tests must not need Docker.
* No credentials in YAML or code; secrets are env var names resolved at runtime and SSM
  placeholders in Terraform.
