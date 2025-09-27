# --- Makefile for NFL_MASTER_FILE ---
# Force bash everywhere with strict flags
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: all calibrate contract-check validate clean

# Default target
all: calibrate contract-check validate

# Calibration target: load .env if present, then run calibration
calibrate:
	@[ -f .env ] && ( set -a; . ./.env; set +a ) || echo "[WARN] .env missing; using environment vars only"
	@echo "[calibrate] starting..."
	@python3 scripts/refit_model_line_calibration.py
	@echo "[calibrate] done"

# Post-calibration contract validation
contract-check:
	@python3 scripts/validate_calibration_contract.py
	@jq -e '.a!=null and .b!=null and .n==240' out/calibration/model_line_calibration.json
	@jq -e '.n_rows==240 and .hist_glob=="history/enriched_202[2-4]*.csv"' out/calibration/meta.json
	@csvcut -c p out/calibration/train_sample.csv | tail -n +2 | awk '{if($$1==0.5) f=1} END{exit(f)}'
	@echo "[OK] calibration contract honored"

# Validation step (calls Python validator)
validate:
	@echo "[validate] running..."
	@python3 scripts/validate_calibration_contract.py
	@echo "[validate] done"

# Cleanup helper
clean:
	@echo "[clean] removing outputs..."
	rm -rf out/calibration/*
	rm -f out/injuries_week.csv
	rm -f out/validation/weekly_status.csv
	rm -f out/validation/validation_log.jsonl
	@echo "[clean] done"

# ------------------------------
# Contract: board -> predictions_week.csv (game_id,p_home)
# Enforces Prime Directive: no drift between board and evaluator.
# ------------------------------

.PHONY: contract
contract: out/model_board.csv week_games.csv
	@echo "[contract] emitting predictions_week.csv from board"
	@python3 scripts/emit_week_predictions_from_board.py \
		--board out/model_board.csv \
		--games week_games.csv \
		--out predictions_week.csv \
		--prob-col p_home_model
	@echo "[contract] verifying board ↔ eval alignment"
	@python3 scripts/verify_alignment.py
	@echo "[contract] OK"

# ci: trigger cal on PR
