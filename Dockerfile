# SPDX-License-Identifier: Apache-2.0

FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./
COPY LICENSE README.md ./

# Create directories for runtime data
RUN mkdir -p /app/dicts /app/out /app/samples

# Builder stage for dictionary training
FROM base as dict-trainer
ENTRYPOINT ["python3", "train_dict.py"]

# Edge agent stage
FROM base as edge-agent
EXPOSE 7000
ENTRYPOINT ["python3", "edge_agent.py"]

# Collector server stage
FROM base as collector
EXPOSE 7000
ENTRYPOINT ["python3", "collector_server.py"]

# Proxy stage
FROM base as proxy
EXPOSE 8080
ENTRYPOINT ["python3", "proxy.py"]

# Default stage runs the collector
FROM collector as default
