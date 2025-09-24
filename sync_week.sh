#!/usr/bin/env bash
set -euo pipefail

WEEK="${1:-}"
if [[ -z "$WEEK" ]]; then
  echo "Usage: $0 <week_number>" >&2
  exit 1
fi

# Sync logs + the specific week outputs
./sync_to_drive.sh --logs
./sync_to_drive.sh --week "$WEEK"
