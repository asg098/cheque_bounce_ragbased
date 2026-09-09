# ==============================================================================
# JudiQ AI — Production Multi-Stage Dockerfile
# ==============================================================================

# Build Stage
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
RUN pip install --no-cache-dir --user gunicorn psycopg2-binary prometheus-client

# Final Production Stage
FROM python:3.12-slim AS runner

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    tesseract-ocr \
    libtesseract-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root system user for enterprise container security compliance
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g appgroup -s /bin/bash -m appuser

# Copy python dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local
ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy Backend Application Code
COPY --chown=appuser:appgroup backend /app/backend

# Copy Frontend SPA for unified serving
COPY --chown=appuser:appgroup frontend /app/frontend

RUN chown -R appuser:appgroup /app

USER appuser

WORKDIR /app/backend

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/ready || exit 1

# Launch with Gunicorn Process Manager with Uvicorn Workers
# Note: Concurrency of 1 worker maintains in-memory Caseroom WebSocket broadcast state consistency
CMD ["gunicorn", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "--timeout", "120", "main:app"]
