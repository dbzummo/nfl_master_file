export SEASON ?= 2025-regular
export WEEK_TAG ?= auto

.PHONY: all preflight week emit_preds align finals eval_ats eval_su eval_week clean

# -------- One command to do everything accurately --------
all: preflight week emit_preds align finals eval_ats
	@echo "[OK] ALL DONE → Board: reports/board_week.html | ATS eval: reports/eval_ats.html"

# -------- Safety rails --------
preflight:
	python3 scripts/preflight.py

align:
	python3 scripts/verify_alignment.py

# -------- Weekly build (data → board) --------
week:
	@test -n "$(START)" -a -n "$(END)" || (echo "[FATAL] set START and END (YYYYMMDD)"; exit 1)
	python3 scripts/fetch_odds.py --start "$(START)" --end "$(END)" --season "$(SEASON)"
	python3 scripts/join_week_with_elo.py
	python3 scripts/calibrate_probs.py
	-bash scripts/pull_injuries_and_adjust.sh || true
	python3 scripts/update_injuries_week.py
	python3 scripts/compute_injury_adjustments.py
	python3 scripts/make_model_lines_and_board.py
	python3 scripts/msc_09_apply_bayes_weights.py
	python3 scripts/lock_board_schema.py
	python3 scripts/validate_and_manifest.py
	python3 scripts/render_board.py
	@echo "[OK] Week artifacts → reports/board_week.html"

# Emit week_predictions.csv **from the board** (post-blend) so eval matches exactly
emit_preds:
	python3 scripts/emit_week_predictions_from_board.py

# Build finals.csv for START..END (used by SU & ATS eval)
finals:
	python3 scripts/finals_for_window.py

# -------- Evaluations --------
# ATS = source of truth (spread applied)
eval_ats:
	python3 scripts/msc_07_eval_ats.py
	@echo "[OK] ATS eval → reports/eval_ats.html"

# SU = diagnostic only (no spread)
eval_su:
	python3 scripts/generate_scorecards.py
	@echo "[OK] SU eval → reports/*_eval.html"

# Convenience: do both SU+ATS after emit_preds+finals (kept for compatibility)
eval_week: emit_preds align finals eval_su eval_ats
	@echo "[OK] Eval artifacts written to reports/"

clean:
	rm -f out/week_with_market.csv out/week_with_elo.csv out/model_board.csv
	rm -f out/week_predictions.csv out/week_predictions_norm_* out/predictions_week.csv
	rm -f out/results/finals.csv out/results/week_results.csv
