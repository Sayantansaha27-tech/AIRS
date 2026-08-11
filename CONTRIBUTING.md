# Contributing to AIRS

Thank you for your interest in contributing. This document explains how the project is structured and how to get a working development environment up.

---

## Table of Contents

- [Getting started](#getting-started)
- [Project structure](#project-structure)
- [Development workflow](#development-workflow)
- [Code style](#code-style)
- [Testing](#testing)
- [Submitting changes](#submitting-changes)
- [Opening issues](#opening-issues)

---

## Getting started

### Prerequisites

| Tool | Minimum version |
|------|-----------------|
| Python | 3.11 |
| Node.js | 20 |
| Docker + Compose | 24 |
| uv (optional) | latest |

### Clone and install

```bash
git clone https://github.com/Sayantansaha27-tech/AIRS.git
cd AIRS

# Python (backend)
pip install -r requirements.txt

# Frontend
cd ui && npm install
```

### Run everything locally

```bash
cp .env.example .env          # add OPENAI_API_KEY if you want OpenAI
docker compose up --build
```

On first run, pull the local LLM models:

```bash
docker exec -it airs-ollama ollama pull qwen2.5:7b-instruct
docker exec -it airs-ollama ollama pull rjmalagon/qwen2:1.5b-instruct
```

---

## Project structure

```
config/               Central YAML configuration (airs.yaml)
infra/                Base Python Dockerfile + monitoring configs
services/
  ingestion-service/  Log intake + external source polling
  log-processor/      Normalization + OpenSearch indexing
  anomaly-service/    z-score + keyword/level anomaly detection
  correlation-service Incident grouping + deduplication
  ai-service/         RCA generation (Ollama / OpenAI)
  api-gateway/        REST API + SSE stream + ChatOps
shared/airs_shared/   Shared models, Kafka helpers, settings
ui/                   Next.js + Tailwind dashboard
tests/                Pytest suite
docs/                 Architecture diagram + Kafka contracts
samples/              Demo log dataset (logs.jsonl)
```

---

## Development workflow

1. **Create a branch** from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Work on one service at a time.** Each service is a self-contained FastAPI app under `services/<name>/app/main.py`. Shared models and helpers live in `shared/airs_shared/`.

3. **Run the affected service in isolation** (Kafka and OpenSearch still need to be running):
   ```bash
   docker compose up kafka opensearch redis ollama -d
   AIRS_CONFIG_FILE=config/airs.yaml PYTHONPATH=shared \
     uvicorn services/ai-service/app/main:app --reload --port 8005
   ```

4. **Check before committing:**
   ```bash
   ruff check .
   ruff format .
   pytest
   ```

---

## Code style

- **Python:** [ruff](https://github.com/astral-sh/ruff) with the settings in `pyproject.toml` (`line-length=100`, Python 3.11 target, `E F I B UP` ruleset).
- **TypeScript/React:** project uses Next.js defaults + Tailwind. No dedicated linter config beyond `tsconfig.json` strict mode.
- **YAML/JSON:** keep configs in `config/airs.yaml`; avoid hard-coding values in service code.

---

## Testing

```bash
# Run all backend tests
pytest

# Run a single test file
pytest tests/test_models.py -v

# Type checking (advisory)
mypy shared
```

There are currently no integration tests that require a live Kafka/OpenSearch cluster; all tests mock external dependencies.

---

## Submitting changes

1. Push your branch and open a **Pull Request** against `main`.
2. The CI pipeline will run lint, tests, and Docker build smoke tests automatically.
3. One reviewer approval is required before merging.
4. Squash-merge is preferred to keep the `main` history clean.

---

## Opening issues

When filing a bug, please include:
- Steps to reproduce
- Expected vs actual behaviour
- Service name and relevant log snippet
- Docker Compose or `docker logs` output if applicable

For feature requests, describe the use case and why it fits the AIRS design principles (event-driven, pluggable AI, observable).
