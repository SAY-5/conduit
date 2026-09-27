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
make demo-verify    # the demo, then the provenance check on the block in README.md
make readme-check   # that check on its own, no Docker
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
* Tests accompany behaviour changes. Unit tests must not need Docker. `make test-unit` fails
  below 85% line coverage of `conduit/` (87% when the floor was set; the remainder is the
  AWS-facing code the LocalStack integration suite covers).
* No credentials in YAML or code; secrets are env var names resolved at runtime and SSM
  placeholders in Terraform.
* Every figure in a document comes from a run that is named beside it. The demo block in
  README.md carries the commit, version, LocalStack image, and date that produced it;
  `make readme-check` (also a CI step) fails when that commit is not an ancestor of HEAD or
  the version and image no longer match, and lists the commits that have touched the measured
  code since. Re-run `make demo` and repaste before tagging a release.
* The browser demo parses the shipped `connectors/*.yaml` and `schemas/*/v*.yaml` rather than
  its own copies: run `npm run embed` in `web/` after changing one, and `npm run embed:check`
  (also a CI step) fails when the generated module and the files disagree.
