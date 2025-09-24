#!/bin/bash
set -euo pipefail

echo "[SYMLINK CHECK]"

for f in out/msf_week.csv out/week_games.csv scripts/msf_week.csv scripts/week_games.csv; do
  if [ -L "$f" ]; then
    target=$(readlink "$f")
    if [ -e "$f" ]; then
      echo "OK: $f -> $target"
    else
      echo "BROKEN: $f -> $target"
      exit 1
    fi
  else
    echo "NOT A SYMLINK: $f"
  fi
done
