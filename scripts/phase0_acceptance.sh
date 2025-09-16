#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

require_clean(){ git diff --quiet && git diff --cached --quiet || { echo "[FATAL] git not clean"; exit 1; }; }
must(){ [[ -e "$1" ]] || { echo "[FATAL] Missing: $1"; exit 2; }; }

require_clean
echo "[STEP] WEEK=1"
WEEK=1 ./scripts/phase0_run.sh

echo "[STEP] Validate W1"
must "out/2025w01/results/finals.csv"
must "out/2025w01/week_predictions.csv"
must "out/2025w01/model_board.csv"
must "out/2025w01/run_manifest.json"
must "out/2025w01/_phase0_logs/2025w01_pass1.log"
must "out/2025w01/_phase0_logs/2025w01_pass2.log"
must "reports/2025w01/board_week.html"
must "reports/2025w01/eval_ats.html"

require_clean
echo "[STEP] WEEK=2"
WEEK=2 ./scripts/phase0_run.sh

echo "[STEP] Validate W2"
must "out/2025w02/results/finals.csv"
must "out/2025w02/week_predictions.csv"
must "out/2025w02/model_board.csv"
must "out/2025w02/run_manifest.json"
must "out/2025w02/_phase0_logs/2025w02_pass1.log"
must "out/2025w02/_phase0_logs/2025w02_pass2.log"
must "reports/2025w02/board_week.html"
must "reports/2025w02/eval_ats.html"

echo "[STEP] Commit artifacts"
git add config/week_windows_2025.json out/2025w01 out/2025w02 reports/2025w01 reports/2025w02
git commit -m "Phase 0 baseline: freeze 2025w01 + 2025w02 (reproducible, checksums logged)" || true

TAG="baseline/2025w01w02"
git rev-parse "$TAG" >/dev/null 2>&1 || git tag -a "$TAG" -m "Phase 0 baseline: W1+W2 frozen & reproducible"
echo "[OK] Phase 0 acceptance complete."
