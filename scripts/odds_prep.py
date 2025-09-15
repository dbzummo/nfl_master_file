#!/usr/bin/env python3
import argparse, os, sys, pandas as pd

INGEST_DIR = "out/ingest"
IN_FILE    = f"{INGEST_DIR}/week_games.csv"
OUT_FILE   = f"{INGEST_DIR}/week_games.odds.csv"

def log(m):  print(f"[odds_prep] {m}")
def err(m):  print(f"[odds_prep][ERR] {m}", file=sys.stderr)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYYMMDD inclusive")
    p.add_argument("--end",   required=True, help="YYYYMMDD inclusive")
    return p.parse_args()

def to_dash(d):  # 20250911 -> 2025-09-11
    d = str(d).strip()
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

def coerce_cols(df):
    cols = set(df.columns)
    # accept either away/home or away_team/home_team
    if {"away","home"}.issubset(cols):
        df = df.rename(columns={"away":"away_team","home":"home_team"})
    need = {"date","msf_game_id","away_team","home_team"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"{IN_FILE} missing required columns: {sorted(miss)}")
    # normalize types
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["away_team"] = df["away_team"].astype(str).str.upper()
    df["home_team"] = df["home_team"].astype(str).str.upper()
    return df[["date","msf_game_id","away_team","home_team"]]

def main():
    args = parse_args()
    win_lo = to_dash(args.start)
    win_hi = to_dash(args.end)

    try:
        raw = pd.read_csv(IN_FILE)
    except Exception as e:
        err(f"cannot read {IN_FILE}: {e}")
        sys.exit(1)

    log(f"week_games.csv columns: {list(raw.columns)}")
    t = coerce_cols(raw)

    # strict window filter (inclusive)
    w = t[(t["date"] >= win_lo) & (t["date"] <= win_hi)].copy()
    log(f"rows in window {win_lo}..{win_hi}: {len(w)}")

    if w.empty:
        err(f"no rows in date window {win_lo}..{win_hi}. "
            f"Fix upstream (ingest should output this window) and rerun.")
        sys.exit(1)

    os.makedirs(INGEST_DIR, exist_ok=True)
    w.to_csv(OUT_FILE, index=False)
    log(f"ok wrote {OUT_FILE} rows={len(w)}")

if __name__ == "__main__":
    main()
