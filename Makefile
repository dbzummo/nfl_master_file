export SEASON ?= 2025-regular
export PYTHONPATH := $(CURDIR)

.PHONY: all preflight week elo emit_preds align finals eval_ats eval_su clean

all: week
	@echo "[OK] ALL DONE"

preflight:
	python3 scripts/preflight.py

# Deterministic Elo: prefer pinned snapshot; else compute+convert
elo:
	@set -euo pipefail; \
	if [ -f data/elo/elo_ratings_by_date.csv ]; then \
	  cp -f data/elo/elo_ratings_by_date.csv out/elo_ratings_by_date.csv; \
	  echo "[OK] Elo snapshot → out/elo_ratings_by_date.csv"; \
	else \
	  echo "[STEP] Computing Elo and converting → out/elo_ratings_by_date.csv"; \
	  python3 scripts/compute_elo.py; \
	  python3 scripts/elo_make_by_date.py; \
	fi; \
	test -s out/elo_ratings_by_date.csv || (echo "[FATAL] no Elo by-date CSV"; exit 1)

# One deterministic, fail-closed weekly build that produces board + reports
week: preflight elo
	@test -n "$(START)" -a -n "$(END)" || (echo "[FATAL] set START and END (YYYYMMDD)"; exit 1)
	python3 scripts/fetch_odds.py --start "$(START)" --end "$(END)" --season "$(SEASON)"
	python3 scripts/join_week_with_elo.py
	python3 scripts/calibrate_probs.py
	python3 scripts/update_injuries_week.py
	python3 scripts/compute_injury_adjustments.py
	python3 scripts/make_model_lines_and_board.py
	@test -f out/model_board.csv || (echo "[FATAL] make_model_lines_and_board.py did not produce out/model_board.csv"; exit 1)
	python3 scripts/msc_09_apply_bayes_weights.py
	python3 scripts/lock_board_schema.py
	python3 scripts/ensure_nonempty_csv.py out/model_board.csv
	python3 scripts/ensure_model_line_calibration.py
	python3 scripts/render_board.py
	@test -f reports/board_week.html || (echo "[FATAL] render_board.py did not produce reports/board_week.html"; exit 1)
	python3 scripts/validate_and_manifest.py
	python3 scripts/check_board_finals_ids.py
	# downstream artifacts required by acceptance
	python3 scripts/emit_week_predictions_from_board.py
	python3 scripts/verify_alignment.py
	python3 scripts/finals_for_window.py
	python3 scripts/msc_07_eval_ats.py
	@echo "[OK] Week artifacts → reports/board_week.html and reports/eval_ats.html"

emit_preds:
	python3 scripts/emit_week_predictions_from_board.py

align:
	python3 scripts/verify_alignment.py

finals:
	python3 scripts/finals_for_window.py

eval_ats:
	python3 scripts/msc_07_eval_ats.py
	@echo "[OK] ATS eval → reports/eval_ats.html"

eval_su:
	python3 scripts/generate_scorecards.py
	@echo "[OK] SU eval → reports/*_eval.html"

clean:
	rm -f out/elo_ratings_by_date.csv out/elo_ratings.csv out/elo_games_enriched.csv out/elo_season_start.csv
	rm -f out/week_with_market.csv out/week_with_elo.csv out/model_board.csv
	rm -f out/week_predictions.csv out/week_predictions_norm_* out/predictions_week.csv
	rm -f out/results/finals.csv out/results/week_results.csv
	test -f reports/eval_ats.html || (echo "[FATAL] ATS eval did not produce reports/eval_ats.html"; exit 1)
.PHONY: calibrate
calibrate:
	@set -euo pipefail
	set -a; source .env; set +a
	export CAL_TRAIN_HISTORY_GLOB='history/enriched_202[2-4]*.csv'
	export CAL_TRAIN_START_SEASON=2022 CAL_TRAIN_END_SEASON=2024 MIN_CAL_ROWS=200
	python3 scripts/run_week.py
	jq . out/calibration/model_line_calibration.json
	jq . out/calibration/meta.json

.PHONY: check-cal
check-cal:
	jq -e '.a!=null and .b!=null and .n==240' out/calibration/model_line_calibration.json >/dev/null
	jq -e '.n_rows==240 and .hist_glob=="history/enriched_202[2-4]*.csv"' out/calibration/meta.json >/dev/null
	csvcut -c p out/calibration/train_sample.csv | tail -n +2 | awk '{if($$1==0.5) f=1} END{exit(f)}'
	@echo "[OK] calibration healthy"

.PHONY: contract-check
contract-check:
	python3 scripts/validate_calibration_contract.py
