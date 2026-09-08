UV ?= uv
COMPOSE ?= docker compose -f deploy/docker-compose.yml
TF ?= terraform -chdir=terraform
TF_STATE ?= localstack.tfstate
LOCALSTACK_URL ?= http://localhost:4566

export AWS_DEFAULT_REGION ?= us-east-1
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test

.PHONY: setup lint format test test-unit test-integration tf-init tf-validate tf-plan tf-apply tf-destroy \
        up down demo demo-down clean

setup:            ## install the project and dev tools into .venv
	$(UV) sync --all-extras

lint:             ## ruff check + format check + terraform fmt check
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	terraform fmt -check -recursive terraform

format:
	$(UV) run ruff check --fix .
	$(UV) run ruff format .
	terraform fmt -recursive terraform

test-unit:        ## fast tests, no Docker
	$(UV) run pytest tests/unit -q

test-integration: ## needs LocalStack (make up)
	CONDUIT_LOCALSTACK_URL=$(LOCALSTACK_URL) $(UV) run pytest tests/integration tests/terraform -q -m "integration or terraform"

test: test-unit test-integration

tf-init:
	$(TF) init -input=false

tf-validate: tf-init
	terraform fmt -check -recursive terraform
	$(TF) validate

tf-plan: tf-init   ## plan against LocalStack
	$(TF) plan -input=false -var-file=localstack.tfvars -state=$(TF_STATE)

tf-apply: tf-init  ## apply against LocalStack (creates queues, DLQs, table, roles, SSM params)
	$(TF) apply -input=false -auto-approve -var-file=localstack.tfvars -state=$(TF_STATE)

tf-destroy: tf-init
	$(TF) destroy -input=false -auto-approve -var-file=localstack.tfvars -state=$(TF_STATE)

up:               ## LocalStack + fakes + workers
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v --remove-orphans

demo: up tf-apply ## full end-to-end run with the summary block
	$(UV) run python demo/run.py

demo-down: down
	rm -f terraform/$(TF_STATE) terraform/$(TF_STATE).backup

clean: demo-down
	rm -rf .venv .pytest_cache .ruff_cache terraform/.terraform
