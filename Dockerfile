# ─── Imovela — production image ──────────────────────────────────────────────
# Multi-stage build keeps the final image lean: ~700MB with Chromium baked in.

FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# System deps Chromium needs at runtime — install once in builder layer
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt


# ─── Runtime image ───────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/root/.local/bin:$PATH \
    PYTHONPATH=/app

# Playwright Chromium runtime libraries (matches playwright 1.44 deps list)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
        libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
        libgbm1 libxshmfence1 libasound2 libpango-1.0-0 libcairo2 \
        libdrm2 libxext6 libx11-xcb1 libwayland-client0 \
        ca-certificates fonts-liberation \
        && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Pip packages from builder
COPY --from=builder /root/.local /root/.local

# Pre-install the Chromium binary so the container is self-contained
RUN python -m playwright install chromium --with-deps || \
    python -m playwright install chromium

COPY . /app

# Volume mounts at runtime: /app/data (DB) + /app/logs (rotating logs)
VOLUME ["/app/data", "/app/logs"]

EXPOSE 8501

# Default = launch the Streamlit dashboard. Override in docker-compose.yml
# for the scheduler service so a single image serves both roles.
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
