# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim as production

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy application code
COPY data_pipeline/ ./data_pipeline/
COPY sql/ ./sql/

# Create data directories
RUN mkdir -p /app/data/bronze /app/data/silver /app/data/gold /app/logs

# Health check - verify the pipeline package is importable.
# The pipeline container runs as a batch job (no HTTP endpoint), so we
# can't curl a /health route. A smoke import gives a meaningful signal
# that the image is wired up correctly.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import data_pipeline; import data_pipeline.pipeline"]

# Default command - run the pipeline
CMD ["python", "-m", "data_pipeline.pipeline"]

# Dashboard stage
FROM production as dashboard

COPY dashboard/ ./dashboard/

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
