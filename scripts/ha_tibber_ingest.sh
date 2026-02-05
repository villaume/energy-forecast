#!/usr/bin/env sh
set -eu

IMAGE="ghcr.io/villaume/energy-forecast:latest"
ENV_FILE="/config/energy-forecast.env"

/usr/bin/docker pull "$IMAGE"
/usr/bin/docker run --rm \
  --env-file "$ENV_FILE" \
  "$IMAGE" \
  --latest-hours 24 \
  --offset-hours 2 \
  --chunk-hours 96 \
  --self-heal
