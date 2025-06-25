# Dockerfile for DBT Repo Analyzer (Framework)
FROM python:3.11-alpine

# Build argument for commit SHA
ARG COMMIT_SHA=unknown

# Install system dependencies including git and build tools
RUN apk add --no-cache \
    git \
    curl \
    build-base \
    libffi-dev \
    openssl-dev \
    python3-dev \
    musl-dev \
    gcc \
    g++ \
    make \
    && rm -rf /var/cache/apk/*

# Install dbt-core 1.8.x and dbt-snowflake
RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-snowflake

# Configure git to disable SSL verification globally
RUN git config --global http.sslVerify false && \
    git config --global http.sslCAInfo "" && \
    git config --global http.sslCAPath "" && \
    git config --global http.sslCert "" && \
    git config --global http.sslKey "" && \
    git config --global http.sslCertPasswordProtected false

# Set environment variables to disable SSL verification and include commit SHA
ENV GIT_SSL_NO_VERIFY=true \
    PYTHONHTTPSVERIFY=0 \
    CURL_CA_BUNDLE="" \
    REQUESTS_CA_BUNDLE="" \
    SSL_VERIFY=false \
    COMMIT_SHA=${COMMIT_SHA}

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY framework/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy framework application code
COPY framework/ ./framework/

# Make /app and /tmp writable for any user (OpenShift compatibility)
RUN chmod -R g+rwX /app && \
    chmod -R g+rwX /tmp

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the FastAPI application
CMD ["uvicorn", "framework.webhook_service:app", "--host", "0.0.0.0", "--port", "8000"]
