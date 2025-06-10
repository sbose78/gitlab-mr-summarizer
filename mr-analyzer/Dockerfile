FROM python:3.11-alpine

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    git \
    curl \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    && rm -rf /var/cache/apk/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY webhook_service.py .
#COPY prompt_template.txt .

# OpenShift runs containers with arbitrary UIDs in the root group
# Set proper permissions for the root group
RUN chgrp -R 0 /app && \
    chmod -R g+rwX /app && \
    chown -R 1001:0 /app

# Create a non-root user (OpenShift will override the UID but respect the group)
USER 1001

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the webhook service
CMD ["python", "mr_webhook_service.py"]
