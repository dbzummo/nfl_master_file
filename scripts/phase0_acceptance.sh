#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

must_exist() { [[ -e "$1" ]] || { echo "[FATAL] Missing: $1"; exit 90; }; }

echo "[STEP] WEEK=1"
WEEK=1 ./scripts/phase0_run.sh

echo "[STEP] Validate W1"
must_exist "out/2025w01/results/finals.csv"
must_exist "out/2025w01/week_predictions.csv"
must_exist "out/2025w01/model_board.csv"
must_exist "out/2025w01/run_manifest.json"
must_exist "out/2025w01/checksums.txt"
must_exist "reports/2025w01/board_week.html"
must_exist "reports/2025w01/eval_ats.html"

echo "[STEP] WEEK=2"
WEEK=2 ./scripts/phase0_run.sh

echo "[STEP] Validate W2"
must_exist "out/2025w02/results/finals.csv"
must_exist "out/2025w02/week_predictions.csv"
must_exist "out/2025w02/model_board.csv"
must_exist "out/2025w02/run_manifest.json"
must_exist "out/2025w02/checksums.txt"
must_exist "reports/2025w02/board_week.html"
must_exist "reports/2025w02/eval_ats.html"

echo "[STEP] Commit artifacts"
git add config/week_windows_2025.json out/2025w01 out/2025w02 reports/2025w01 reports/2025w02 artifacts/phase0 || true
git commit -m "Phase 0 baseline: freeze 2025w01 + 2025w02 (reproducible, checksums logged)" || true

TAG="baseline/2025w01w02"
if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "[INFO] Tag $TAG already exists."
else
  echo "[STEP] Tag $TAG"
  git tag -a "$TAG" -m "Phase 0 baseline: W1+W2 frozen & reproducible"
fi

echo "[OK] Phase 0 acceptance complete."
