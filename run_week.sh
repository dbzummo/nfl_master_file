cat > run_week.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail

# Load env if present (API keys, etc.)
if [[ -f .env ]]; then set -a; . ./.env; set +a; fi

# Prepare output dirs
mkdir -p out out/validation out/msf out/odds reports logs artifacts

echo "[run] fetch schedule + odds (no secrets committed)"
python3 fetch_odds.py || true                    # uses msf/odds providers you wired
python3 fetch_injuries.py || true               # fallback script if provider available

echo "[run] compute / update injuries for current week"
python3 update_injuries_week.py                  # writes out/injuries_week.csv + validation logs
python3 compute_injury_adjustments.py            # computes adjustments used by board

echo "[run] build model lines + board"
python3 run_predictions.py                       # your main model signal(s)
python3 apply_calibration_to_predictions.py      # apply A/B calibrator
python3 make_model_lines_and_board.py            # emits out/model_board.csv (+ dp_injury_raw joined)

echo "[run] render weekly board (optional)"
python3 weekly_report.py || true                 # may produce reports/board_week.html

echo "[run] validate end-state contracts"
python3 scripts/validate_injuries_contract.py
python3 scripts/validate_calibration_contract.py

echo "[done] week pipeline complete"
SH
chmod +x run_week.sh
