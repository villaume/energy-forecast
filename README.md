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

## HA OS + GHCR (scheduled)

1) Push to `main` to build/push `ghcr.io/villaume/energy-forecast:latest`.
2) On HA OS, create `/config/energy-forecast.env` with required env vars.
3) Copy `scripts/ha_tibber_ingest.sh` to `/config/bin/` and `chmod +x` it.
4) Run from HA automation every 4 hours:

```yaml
shell_command:
  tibber_ingest: /config/bin/ha_tibber_ingest.sh
```
