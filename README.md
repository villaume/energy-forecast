# Energy Forecast

## Setup

```bash
uv sync
```

## Junior Dev

```bash
uv run junior-dev
```

## Run tests

```bash
uv run pytest -q
```

## Tibber

```bash
uv run python -m energy_forecast.pipeline.ingest_tibber --start 2025-09-01
```

```bash
uv run python -m energy_forecast.pipeline.ingest_tibber --resume
```
