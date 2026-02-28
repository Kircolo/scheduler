SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV ?= .venv

# Prefer venv python if it exists; otherwise use system python3.
PY := $(shell \
  if [ -x "$(VENV)/bin/python" ]; then echo "$(VENV)/bin/python"; \
  elif [ -x "$(VENV)/Scripts/python.exe" ]; then echo "$(VENV)/Scripts/python.exe"; \
  else echo python3; fi)

PIP := $(PY) -m pip

APP  ?= server:app
HOST ?= 0.0.0.0
PORT ?= 8000

.PHONY: help
help:
	@echo ""
	@echo "Targets:"
	@echo "  make venv         Create local virtualenv at $(VENV)/"
	@echo "  make install      Install deps from requirements.txt (into venv)"
	@echo "  make test         Run all tests (unittest)"
	@echo "  make api-test     Run API tests only"
	@echo "  make sched-test   Run scheduler tests only"
	@echo "  make run          Run the API server (uvicorn)"
	@echo "  make clean        Remove Python cache files"
	@echo "  make distclean    clean + remove venv"
	@echo ""
	@echo "Examples:"
	@echo "  make venv install"
	@echo "  make test"
	@echo "  make run PORT=9001"
	@echo ""

.PHONY: venv
venv:
	@python3 -m venv "$(VENV)"
	@echo "Created venv at $(VENV)/"
	@echo "Activate (optional): source $(VENV)/bin/activate"

.PHONY: install
install: venv
	@if [ ! -f requirements.txt ]; then \
		echo "Missing requirements.txt (we can add it next)."; \
		echo "For now you can manually: $(PIP) install fastapi uvicorn python-multipart httpx"; \
		exit 1; \
	fi
	@$(PIP) install --upgrade pip
	@$(PIP) install -r requirements.txt

.PHONY: test
test:
	@$(PY) -m unittest discover -v -s . -p "test_*.py"

.PHONY: api-test
api-test:
	@$(PY) -m unittest -v tests.test_api

.PHONY: sched-test
sched-test:
	@$(PY) -m unittest -v tests.test_scheduler

.PHONY: smoke
smoke:
	@set -euo pipefail; \
	  host=127.0.0.1; \
	  port=$${PORT:-8000}; \
	  echo "Starting server for smoke test on $$host:$$port..."; \
	  $(PY) -m uvicorn $(APP) --host $$host --port $$port --log-level warning & \
	  pid=$$!; \
	  trap "kill $$pid >/dev/null 2>&1 || true" EXIT; \
	  for i in {1..50}; do \
	    if curl -fsS "http://$$host:$$port/healthz" >/dev/null; then \
	      echo "Server is up."; \
	      break; \
	    fi; \
	    sleep 0.2; \
	  done; \
	  echo "GET /healthz"; \
	  curl -fsS "http://$$host:$$port/healthz"; echo; \
	  echo "GET /stats"; \
	  curl -fsS "http://$$host:$$port/stats" >/dev/null; \
	  echo "Smoke test passed."

.PHONY: ci
ci:
	@$(MAKE) test
	@$(MAKE) smoke PORT=$${PORT:-18000}

.PHONY: run
run:
	@$(PY) -m uvicorn $(APP) --host $(HOST) --port $(PORT)

.PHONY: clean
clean:
	@find . -type d -name "__pycache__" -prune -exec rm -rf {} + || true
	@find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete || true

.PHONY: distclean
distclean: clean
	@rm -rf "$(VENV)" || true