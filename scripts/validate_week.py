INFO = "[INFO]"
#!/usr/bin/env python3
import sys, json, glob
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

OK   = "\x1b[32m[OK]\x1b[0m"
WARN = "\x1b[33m[WARN]\x1b[0m"
FATAL= "\x1b[31m[FATAL]\x1b[0m"

P_WEEK_GAMES   = Path("out/msf/week_games.csv")
P_WITH_ELO     = Path("out/week_with_elo.csv")
P_WITH_MARKET  = Path("out/week_with_market.csv")
P_BOARD        = Path("out/model_board.csv")
P_ADJ          = Path("out/injury_adjustments.csv")
RAW_SNAP_DIR   = Path("raw/msf/injuries")
VAL_DIR        = Path("out/validation")
VAL_DIR.mkdir(parents=True, exist_ok=True)
P_LOG_JSONL    = VAL_DIR / "validation_log.jsonl"
P_LOG_CSV      = VAL_DIR / "weekly_status.csv"

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def read_csv_safe(p, needed=None):
    if not p.exists():
        return None, f"{p} missing"
    try:
        df = pd.read_csv(p)
    except Exception as e:
        return None, f"{p} unreadable: {e}"
    if needed:
        missing = set(needed) - set(df.columns)
        if missing:
            return None, f"{p} missing columns {sorted(missing)}"
    return df, None

def latest_snapshot_info():
    snaps = sorted(glob.glob(str(RAW_SNAP_DIR / "*.json")))
    if not snaps:
        return None, None
    latest = snaps[-1]
    # derive ts from filename if present
    ts = Path(latest).stem  # e.g., 2025-09-24T20-20-11Z
    return latest, ts

def main():
    ts = now_iso()
    status = {"ts": ts, "level": "OK", "messages": []}

    # 1) Core artifacts
    wk, err = read_csv_safe(P_WEEK_GAMES, needed=["home_abbr","away_abbr","game_date"])
    if err:
        print(FATAL, err); status["level"]="FATAL"; status["messages"].append(err); rc=1
        # still log and exit
        with P_LOG_JSONL.open("a", encoding="utf-8") as f: f.write(json.dumps(status)+"\n")
        sys.exit(1)
    print(OK, f"{P_WEEK_GAMES} rows={len(wk)}")

    elo, err = read_csv_safe(P_WITH_ELO)
    print((WARN if err else OK), err or f"{P_WITH_ELO} rows={len(elo)}")
    if err: status["messages"].append(err); status["level"] = max(status["level"], "OK")  # non-fatal

    wm, err = read_csv_safe(P_WITH_MARKET, needed=["home_abbr","away_abbr","market_p_home"])
    print((WARN if err else OK), err or f"{P_WITH_MARKET} rows={len(wm)}")
    if err: status["messages"].append(err)

    board, err = read_csv_safe(P_BOARD, needed=["home_abbr","away_abbr","p_home_model","market_p_home"])
    print((WARN if err else OK), err or f"{P_BOARD} rows={len(board)}")
    if err: status["messages"].append(err)

    # 2) Injuries status
    latest_path, latest_ts = latest_snapshot_info()
    if latest_path:
        print(OK, f"injuries snapshot latest -> {latest_path} (ts={latest_ts})")
    else:
        msg = "no injuries snapshots in raw/msf/injuries/"
        print(WARN, msg); status["messages"].append(msg)

    adj, err = read_csv_safe(P_ADJ, needed=["home_abbr","away_abbr","elo_delta_home","elo_delta_away"])
    if err:
        print(WARN, err); status["messages"].append(err)
        inj_rows = 0
        nonzero = False
    else:
        inj_rows = len(adj)
        nonzero = not (adj[["elo_delta_home","elo_delta_away"]].fillna(0).eq(0).all().all())
        if inj_rows == 0:
            print(WARN, "injury_adjustments.csv has 0 rows (pipeline wrote empty file).")
            status["messages"].append("injury_adjustments:0rows")
        elif not nonzero:
            print(WARN, "injury_adjustments present but all team deltas are zero.")
            status["messages"].append("injury_adjustments:all_zero")
        else:
            print(OK, "injury adjustments applied (non-zero deltas present).")



    # 4) Model vs Market sanity (flag extreme divergences)
    try:
        mm = pd.read_csv("out/week_with_market.csv")
        # Prefer p_home_model from week_with_elo if present there
        elo_df, _ = read_csv_safe(Path("out/week_with_elo.csv"))
        if elo_df is not None and "p_home_model" in elo_df.columns:
            mm = mm.merge(elo_df[["home_abbr","away_abbr","p_home_model"]], on=["home_abbr","away_abbr"], how="left")
        if "p_home_model" in mm.columns:
            mm["gap"] = (mm["p_home_model"] - mm["market_p_home"]).abs()
            bad = mm[mm["gap"] > 0.35][["home_abbr","away_abbr","p_home_model","market_p_home","gap"]]
            if len(bad):
                print(WARN, f"Model vs Market large gap (>0.35) rows={len(bad)}")
                print(bad.to_string(index=False))
                status["messages"].append(f"model_market_gap:{len(bad)}")
    except Exception as _e:
        pass


    # --- Injury swing QA ---
    try:
        import pandas as pd, numpy as np, math
        wk  = pd.read_csv("out/week_with_elo.csv")
        adj = pd.read_csv("out/injury_adjustments.csv")
        m = wk.merge(adj, on=["home_abbr","away_abbr"], how="left").fillna({"elo_delta_home":0.0,"elo_delta_away":0.0})
        k = math.log(10)/400
        z0 = np.log(np.clip(m["p_home_model"],1e-6,1-1e-6)/np.clip(1-m["p_home_model"],1e-6,1))
        z1 = z0 + k*(m["elo_delta_home"]-m["elo_delta_away"])
        p1 = 1/(1+np.exp(-z1))
        m["dp_injury"] = p1 - m["p_home_model"]
        m["delo_net"]  = m["elo_delta_home"]-m["elo_delta_away"]
        big = m[(m["dp_injury"].abs()>0.05) | (m["delo_net"].abs()>60)]
        if len(big):
            print(WARN, f"Injury swing large in {len(big)} game(s):")
            print(big[["home_abbr","away_abbr","dp_injury","delo_net"]].to_string(index=False))
            status["messages"].append(f"injury_swing:{len(big)}")
    except Exception:
        pass
    # --- end Injury swing QA ---


    # --- Guardrails / Provenance ---
    import os, time, pandas as pd, numpy as np, json
    from pathlib import Path

    # 0) Elo snapshot must have 32 teams
    try:
        elo = pd.read_csv("data/elo/current_ratings.csv")
        nteams = elo["team_abbr"].nunique() if "team_abbr" in elo.columns else len(elo)
        if nteams != 32:
            print(ERR, f"Elo snapshot has {nteams} teams (expect 32).")
            status["messages"].append(f"elo_teams:{nteams}")
            if os.environ.get("VALIDATOR_STRICT","0") == "1":
                raise SystemExit(2)
    except Exception as _e:
        print(WARN, f"Elo snapshot check error: {_e}")

    # 1) Market freshness (mtime)
    try:
        odds_path = Path("out/odds_week_norm.csv")
        if odds_path.exists():
            age_hours = (time.time() - odds_path.stat().st_mtime) / 3600.0
            max_age = float(os.environ.get("MARKET_MAX_AGE_HOURS","6"))
            if age_hours > max_age:
                print(WARN, f"Market file stale: {age_hours:.1f}h > {max_age}h")
                status["messages"].append(f"market_stale:{age_hours:.1f}")
    except Exception as _e:
        print(WARN, f"Market freshness check error: {_e}")

    # 2) Calibration-after-injuries integrity
    try:
        board = pd.read_csv("out/model_board.csv")
        calib_present = Path("out/calibration/model_line_calibration.json").exists()
        if calib_present:
            # when calibrator exists, p_home_model should equal p_home_post_injury_cal (or be very close)
            if {"p_home_post_injury_cal","p_home_model"}.issubset(board.columns):
                ok = np.allclose(board["p_home_model"], board["p_home_post_injury_cal"], atol=1e-12, rtol=0)
                print(INFO if ok else WARN, f"p_home_model uses calibrated post-injury: {ok}")
                if not ok and os.environ.get("VALIDATOR_STRICT","0") == "1":
                    raise SystemExit(3)
        else:
            # no calibrator: if raw column exists, p_home_model should match it
            if {"p_home_post_injury_raw","p_home_model"}.issubset(board.columns):
                ok = np.allclose(board["p_home_model"], board["p_home_post_injury_raw"], atol=1e-12, rtol=0)
                print(INFO if ok else WARN, f"p_home_model uses raw post-injury: {ok}")
    except Exception as _e:
        print(WARN, f"Calibration integrity check error: {_e}")

    # 3) Extreme injury swing tripwire (|dp| > 0.15)
    try:
        board = pd.read_csv("out/model_board.csv")
        cand = "dp_injury_raw" if "dp_injury_raw" in board.columns else ("dp_injury" if "dp_injury" in board.columns else None)
        if cand:
            extreme = board[board[cand].abs() > float(os.environ.get("INJURY_MAX_DP","0.15"))]
            if len(extreme):
                print(WARN, f"Extreme injury swing in {len(extreme)} game(s) (>{os.environ.get('INJURY_MAX_DP','0.15')}):")
                print(extreme[["home_abbr","away_abbr",cand]].to_string(index=False))
                status["messages"].append(f"injury_extreme:{len(extreme)}")
                if os.environ.get("VALIDATOR_STRICT","0") == "1":
                    raise SystemExit(4)
    except Exception as _e:
        print(WARN, f"Extreme swing check error: {_e}")
    # --- end Guardrails / Provenance ---

    # Calibration provenance check
    try:
        import pandas as _pd, numpy as _np
        _mb = _pd.read_csv('out/model_board.csv')
        _has_cols = set(("p_home_post_injury_raw","p_home_post_injury_cal")).issubset(_mb.columns)
        _uses_cal = _has_cols and _np.allclose(_mb.get("p_home_model"), _mb.get("p_home_post_injury_cal"))
        if "calibration_used" in _mb.columns:
            _flag_consistent = (int(_uses_cal) == int(_mb["calibration_used"].max()))
            print(INFO, f"calibration_used flag present: True, consistent: {_flag_consistent}")
        else:
            print(INFO, f"calibration_used flag present: False, uses_cal: {_uses_cal}")
    except Exception as _e:
        print(WARN, f"Calibration provenance check error: {_e}")


    # --- Calibration provenance snapshot (non-fatal) ---
    try:
        import json as _json, pandas as _pd
        from pathlib import Path as _Path
        _cal_dir = _Path("out/calibration")
        _meta_p  = _cal_dir / "meta.json"
        _samp_p  = _cal_dir / "train_sample.csv"
        if _meta_p.exists():
            _meta = _json.load(open(_meta_p))
            print(INFO, "calibration_meta:", {k:_meta.get(k) for k in ("n_rows","a","b","base_logloss","cal_logloss","date_min","date_max","season_start","season_end")})
        else:
            print(WARN, "calibration_meta missing.")
        if _samp_p.exists():
            try:
                _samp = _pd.read_csv(_samp_p, nrows=1)
                print(INFO, f"calibration_sample present: True; columns={list(_samp.columns)}")
            except Exception as _e:
                print(WARN, f"calibration_sample unreadable: {_e}")
        else:
            print(WARN, "calibration_sample missing.")
    except Exception as _e:
        print(WARN, f"Calibration provenance read error: {_e}")
    # --- end calibration provenance ---

# 3) Summarize and write logs (never fail build due to injuries)
    row = {
        "ts": ts,
        "week_games": len(wk),
        "week_with_market": (len(wm) if wm is not None else 0),
        "board_rows": (len(board) if board is not None else 0),
        "inj_adjust_rows": inj_rows,
        "inj_has_nonzero": int(bool(nonzero)),
        "latest_inj_snapshot": (latest_ts or ""),
    }

    # JSONL append
    with P_LOG_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": ts, "row": row, "messages": status["messages"]})+"\n")

    # CSV append-or-create
    write_header = not P_LOG_CSV.exists()
    df = pd.DataFrame([row])
    df.to_csv(P_LOG_CSV, mode="a", index=False, header=write_header)

    print(OK, f"validation logged -> {P_LOG_CSV}")
    # Always green exit (per your request)
    sys.exit(0)

if __name__ == "__main__":
    main()
