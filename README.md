# DBT Repository Analyzer (Framework)

A modular, production-ready webhook service for analyzing dbt repository changes and data product promotions in GitLab. This service uses a FastAPI-based webhook, a flexible pipeline architecture, and LLM-driven analysis to provide actionable, context-aware merge request (MR) reviews.

---

## Overview

This service listens for GitLab webhook events (MRs and notes) and automatically:
- Analyzes dbt model and SQL changes for correctness, performance, and best practices (with Snowflake focus)
- Checks for missing tests, documentation, and CI/CD issues
- Posts or updates MR comments with structured, actionable reviews
- Detects and summarizes data product promotions

The system is highly extensible: add new analyzers, notifiers, or context generators with minimal code changes.

---

## Architecture

- **FastAPI Webhook Service**: Receives GitLab webhook events and orchestrates analysis pipelines.
- **Pipeline Pattern**: Each pipeline consists of context generators, analyzers (LLM or rule-based), and notifiers.
- **Prompt Configuration**: Prompts for LLM analysis are centrally managed and can be customized per repository or pattern.
- **Context Handling**: All MR context (diffs, metadata, etc.) is built up front and passed through the pipeline.
- **Cleanup**: Temporary directories (e.g., for git clones) are always cleaned up after use.

---

## Quick Start

### Prerequisites
- Python 3.8+
- dbt-core==1.8.x and dbt-snowflake
- Google Cloud service account credentials (for Gemini/VertexAI)
- GitLab API token

### Installation
```bash
# Clone the repo and enter the framework directory
cd dbt-repo-analyzer/framework

# Install dependencies
pip install -r requirements.txt
```

### Configuration
Set the following environment variables:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json
GITLAB_API_TOKEN=your_gitlab_token
WEBHOOK_SECRET=your_webhook_secret  # Optional, recommended for production
GITLAB_API_BASE_URL=https://gitlab.cee.redhat.com  # Or your GitLab instance
```

### Running the Service
```bash
uvicorn framework.webhook_service:app --host 0.0.0.0 --port 8000 --reload
```

### Webhook Setup
- Configure your GitLab project to POST MR and note events to `/webhook/gitlab` on your running service.

---

## Key Features
- **LLM-Driven Analysis**: Uses Gemini/VertexAI for SQL, dbt, and CI/CD review with repository-specific prompts.
- **Promotion Detection**: Identifies and summarizes data product promotions.
- **Customizable Prompts**: Easily add or override prompts for different repos in `framework/config/prompts.py`.
- **Extensible Pipelines**: Add new analyzers, notifiers, or context generators by subclassing and registering in the pipeline.
- **Robust Error Handling**: All errors are logged; user-facing errors are clean and never leak stack traces.
- **Automatic Cleanup**: All temporary directories (e.g., for git clones) are deleted after use.

---

## Development & Testing

- All main logic is in the `framework/` directory.
- To run tests:
```bash
pytest
```
- To add new analyzers, notifiers, or context generators, see the respective subdirectories and follow the base class interfaces.

---

## Configuration Reference
- **Prompts**: See `framework/config/prompts.py` for all LLM prompt templates and repo-specific overrides.
- **Pipeline**: See `framework/pipeline.py` for the pipeline orchestration logic.
- **Webhook Service**: See `framework/webhook_service.py` for the FastAPI entrypoint and event handlers.

---

## Legacy: dbt_repo_analyzer.py

The legacy monolithic script `dbt_repo_analyzer.py` is still present for reference and backward compatibility. **New deployments should use the FastAPI service in `/framework`.**

---

## License
MIT 