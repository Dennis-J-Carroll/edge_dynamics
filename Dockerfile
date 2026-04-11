# SPDX-License-Identifier: Apache-2.0

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Build dep for zstandard C extension
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

# Install Python package (pyproject.toml layout)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY src/ src/
COPY LICENSE README.md ./

# Runtime directories
RUN mkdir -p /app/dicts /app/out

# ---------- Agent stage ----------
FROM base AS edge-agent
EXPOSE 8080
CMD ["python", "-m", "edge_dynamics.edge_agent"]

# ---------- Collector stage ----------
FROM base AS collector
EXPOSE 7000
CMD ["python", "-m", "edge_dynamics.collector_server"]

# ---------- Default: collector ----------
FROM collector AS default
