#!/usr/bin/env bash
set -euo pipefail

WEEK="${1:-}"
if [[ -z "$WEEK" ]]; then
  echo "Usage: $0 <week_number>" >&2
  exit 1
fi

# --- compute START/END (YYYYMMDD) for odds from anchor (2025-09-04) ---
python3 - "$WEEK" >/tmp/_wk_dates.env <<'PY'
import sys
from datetime import date, timedelta
wk = int(sys.argv[1])
anchor = date(2025,9,4)
start  = anchor + timedelta(days=(wk-1)*7)
end    = start + timedelta(days=6)
print(f"START={start:%Y%m%d}")
print(f"END={end:%Y%m%d}")
PY
# shellcheck disable=SC1091
source /tmp/_wk_dates.env || true
rm -f /tmp/_wk_dates.env
echo "[INFO] Using window START=${START} END=${END} for week ${WEEK}"

printf -v WEEK2 "%02d" "$WEEK"
mkdir -p logs logs/errors "out/week${WEEK2}" "reports/2025w${WEEK2}"
LOG="logs/run_$(date +%Y%m%d-%H%M)_w${WEEK2}.log"
: > "${LOG}"
echo "[RUN] Week ${WEEK2} | logging to ${LOG}"

# --- pipeline ---
if python3 scripts/update_injuries_week.py \
  && python3 scripts/compute_injury_adjustments.py \
  && python3 scripts/fetch_msf_week.py "$WEEK" \
  && {  # make the games baseline visible anywhere downstream might look
       SRC="out/msf/week_games.csv"; \
       test -f "${SRC}" || { echo "[FATAL] ${SRC} missing after fetch"; exit 1; }; \
       ln -sf "${SRC}" week_games.csv; \
       ln -sf "${SRC}" msf_week.csv; \
       ln -sf "${SRC}" out/week_games.csv; \
       ln -sf "${SRC}" out/msf_week.csv; \
       ln -sf "${SRC}" scripts/week_games.csv; \
       ln -sf "${SRC}" scripts/msf_week.csv; \
       echo "[DEBUG] Baselines ready:"; \
       ls -l week_games.csv msf_week.csv out/week_games.csv out/msf_week.csv scripts/week_games.csv scripts/msf_week.csv || true; \
     } \
  && START="$START" END="$END" python3 scripts/fetch_odds.py "$WEEK" \
  && python3 scripts/join_week_with_elo.py \
  && python3 scripts/join_week_with_market.py \
  && python3 scripts/ensure_week_predictions.py \
  && {  # build the board, then verify output exists before emitting
       test -f out/week_with_elo.csv || { echo "[FATAL] missing out/week_with_elo.csv before board"; exit 1; }; \
       test -f out/week_predictions.csv || { echo "[FATAL] missing out/week_predictions.csv before board"; exit 1; }; \
       python3 scripts/make_model_lines_and_board.py; \
       echo "[DEBUG] After board step:"; ls -l out/model_board*.csv || true; \
       test -f out/model_board.csv || { echo "[FATAL] make_model_lines_and_board.py did not produce out/model_board.csv"; exit 1; }; \
     } \
  && python3 scripts/emit_week_predictions_from_board.py \
>> "${LOG}" 2>&1
then
  echo "[OK] Pipeline finished. See ${LOG}"
else
  echo "[ERR] Pipeline failed. Log is preserved at ${LOG}"
  cp "${LOG}" "logs/errors/" || true
  exit 1
fi

# --- organize outputs by week ---
if [[ -f out/week_with_market.csv ]]; then
  mv -v out/week_with_market.csv "out/week${WEEK2}/"
fi
if [[ -f out/week_predictions.csv ]]; then
  mv -v out/week_predictions.csv "out/week${WEEK2}/"
fi
if [[ -f out/model_board.csv ]]; then
  mv -v out/model_board.csv "out/week${WEEK2}/"
fi
if [[ -f reports/board_week.html ]]; then
  mv -v reports/board_week.html "reports/2025w${WEEK2}/"
fi

echo "[DONE] Week ${WEEK2} organization complete."
