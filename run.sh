#!/usr/bin/env bash
set -euo pipefail

# Run once or loop depending on environment variable
if [ "${ONE_SHOT:-false}" = "true" ]; then
  python /app/rotatarr.py
else
  python /app/rotatarr.py
fi
