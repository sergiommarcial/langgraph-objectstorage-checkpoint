.DEFAULT_GOAL := help
SHELL := /bin/bash

# Every target runs through `uv` -- never assumes an activated venv or a
# particular shell state. `uv run`/`uv sync` manage an isolated .venv
# regardless of what's on the caller's PATH.
UV := uv
REQUIRED_PYTHON := $(shell cat .python-version 2>/dev/null)

BOLD := \033[1m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
CYAN := \033[36m
RESET := \033[0m

.PHONY: help
help: ## Show this help
	@echo -e "$(BOLD)langgraph-checkpoint-objectstorage$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-18s$(RESET) %s\n", $$1, $$2}'

.PHONY: check-uv
check-uv: ## Verify uv is installed (prompts to install if missing)
	@if command -v uv >/dev/null 2>&1; then \
		echo -e "$(GREEN)uv found:$(RESET) $$(uv --version)"; \
	else \
		echo -e "$(YELLOW)uv not found on PATH.$(RESET)"; \
		read -p "Install uv now via the official installer (curl | sh)? [y/N] " ans; \
		if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
			curl -LsSf https://astral.sh/uv/install.sh | sh; \
			echo -e "$(YELLOW)uv installed -- open a new shell (or re-source your profile) so it's on PATH, then re-run make.$(RESET)"; \
			exit 1; \
		else \
			echo -e "$(RED)uv is required. Install manually: https://docs.astral.sh/uv/getting-started/installation/$(RESET)"; \
			exit 1; \
		fi; \
	fi

.PHONY: check-python
check-python: check-uv ## Verify a valid Python toolchain is resolvable for this repo
	@echo "Required Python (.python-version): $(REQUIRED_PYTHON)"
	@if resolved=$$(uv python find $(REQUIRED_PYTHON) 2>/dev/null); then \
		echo -e "$(GREEN)Resolved:$(RESET) $$resolved"; \
	else \
		echo -e "$(YELLOW)Python $(REQUIRED_PYTHON) not installed locally -- uv will download it automatically on the next sync.$(RESET)"; \
	fi

.PHONY: install
install: check-python ## Install package + s3/gcs extras + dev/test toolchain into an isolated .venv
	@$(UV) sync --all-extras --group dev

.PHONY: lint
lint: install ## Static analysis: black --check, pyflakes, bandit, vulture (runs before test/build)
	@status=0; \
	echo "-- black --check --"; \
	$(UV) run black --check src tests vulture_whitelist.py || status=1; \
	echo "-- pyflakes --"; \
	$(UV) run pyflakes src tests || status=1; \
	echo "-- bandit (src only -- tests use intentional fake credentials/asserts) --"; \
	$(UV) run bandit -r -q src || status=1; \
	echo "-- vulture (whitelisted for public API -- see vulture_whitelist.py) --"; \
	$(UV) run vulture src vulture_whitelist.py || status=1; \
	if [ $$status -eq 0 ]; then \
		echo -e "$(GREEN)Lint clean.$(RESET)"; \
	else \
		echo -e "$(RED)Lint failed.$(RESET) See above. \`make format\` fixes black issues automatically."; \
		exit 1; \
	fi

.PHONY: format
format: install ## Apply black formatting in place
	@$(UV) run black src tests vulture_whitelist.py

.PHONY: test
test: lint ## Run the full test suite (integration tests auto-skip without `make compose-up`)
	@$(UV) run pytest

.PHONY: test-unit
test-unit: lint ## Run only unit tests (tests/unit -- no external services)
	@$(UV) run pytest tests/unit

.PHONY: test-integration
test-integration: lint compose-up ## Run integration tests (starts docker-compose emulators first)
	@$(UV) run pytest tests/integration

.PHONY: check-docker
check-docker: ## Verify the Docker daemon is reachable
	@if docker info >/dev/null 2>&1; then \
		echo -e "$(GREEN)Docker daemon reachable.$(RESET)"; \
	else \
		echo -e "$(RED)Docker daemon not reachable.$(RESET) Start Docker (e.g. Docker Desktop) and retry."; \
		exit 1; \
	fi

.PHONY: compose-up
compose-up: check-docker ## Start local S3/GCS emulators (moto-server, fake-gcs-server)
	@docker compose up -d
	@echo -e "$(GREEN)Emulators up.$(RESET) moto-server on :5001, fake-gcs-server on :4443."

.PHONY: compose-down
compose-down: check-docker ## Stop local S3/GCS emulators
	@docker compose down

.PHONY: compose-logs
compose-logs: ## Tail logs from the emulator containers
	@docker compose logs -f

.PHONY: build
build: lint ## Build the wheel/sdist into dist/
	@$(UV) build

.PHONY: clean
clean: ## Remove the venv, caches, and build artifacts
	@rm -rf .venv .pytest_cache dist build *.egg-info
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	@echo -e "$(GREEN)Cleaned.$(RESET)"
