# DBT Repository Analyzer

A GitLab webhook handler that automates the analysis of dbt repository changes and data product promotions. This service integrates with GitLab merge requests to provide intelligent insights about data model changes and their impact.

## Overview

This service monitors GitLab merge requests for data product promotions and automatically analyzes:
- Changes to dbt models and their dependencies
- CI/CD pipeline configurations

## Prerequisites

- Python 3.8+
- Google Cloud service account credentials (JSON file)
- GitLab API access token (optional, defaults to environment variable)

## Configuration

The service is designed to work with minimal configuration. The only required setup is providing your Google Cloud service account credentials:

```bash
# Required
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json

# Optional (with defaults)
GITLAB_API_TOKEN=your_gitlab_token  # Defaults to environment variable
WEBHOOK_SECRET=your_webhook_secret  # Defaults to empty (not recommended for production)
REPO_CLONE_COOLDOWN_MINUTES=1       # Defaults to 1 minute
```

The service will automatically extract the following from your service account credentials:
- Project ID
- Location (defaults to us-central1)
- Authentication details

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd dbt-repo-analyzer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. For development/testing:
```bash
pip install -r requirements_test.txt
```

## Running the Service

### Local Development
```bash
uvicorn dbt_repo_analyzer:app --reload
```

### Docker
```bash
docker build -t dbt-repo-analyzer .
docker run -p 8000:8000 \
  -v /path/to/your/service-account.json:/app/service-account.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account.json \
  dbt-repo-analyzer
```

## API Endpoints

- `POST /webhook/gitlab` - GitLab webhook endpoint
- `GET /health` - Health check endpoint

## Development

### Running Tests
```bash
pytest test_dbt_repo_analyzer.py
```
