#!/usr/bin/env python3
import subprocess

def run(cmd):
    print("+", " ".join(cmd))
    import subprocess as _sp
    try:
        _sp.check_call(cmd)
    except Exception as _e:
        print("[WARN] step failed:", cmd, _e)

def main():
    # 0) Ensure Elo snapshot exists (your helper handles backfill)
    try:
        run(["python3","scripts/elo_snapshot_from_ratings.py"])
    except Exception as e:
        print("[WARN] Elo snapshot pre-step failed:", e)

    # 1) Upstream build (joins, odds prep, market)
    run(["python3","scripts/join_week_with_elo.py"])

    # 2) NEW: fit safe calibration from labeled history (writes out/calibration/*.json)
    run(["python3","scripts/refit_model_line_calibration.py"])

    # 3) Injuries → adjustments
    run(["python3","scripts/update_injuries_week.py"])

    # 4) Build board (your board already sanity-gates bad calibrations)
    run(["python3","scripts/make_model_lines_and_board.py"])

    # 5) Validate
    run(["python3","scripts/validate_week.py"])
    print("[OK] Weekly run complete.")

if __name__ == "__main__":
    main()
