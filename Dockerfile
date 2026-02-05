FROM python:3.12-slim

ENV UV_CACHE_DIR=/tmp/uv-cache
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src
RUN uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "python", "-m", "energy_forecast.pipeline.ingest_tibber"]
