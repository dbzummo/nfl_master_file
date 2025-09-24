#!/usr/bin/env python3
import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

# Inputs
BASELINE_CANDIDATES = [
    ROOT / "msf_week.csv",              # symlink commonly present
    ROOT / "out" / "msf" / "week_games.csv",
    ROOT / "out" / "week_games.csv",
]
RATINGS_CSV = ROOT / "data" / "elo" / "current_ratings.csv"

# Output
OUT_CSV = ROOT / "out" / "week_with_elo.csv"

def fatal(msg: str):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)

def pick_baseline() -> Path:
    for p in BASELINE_CANDIDATES:
        if p.exists():
            return p
    fatal(f"No baseline week file found. Looked for: {BASELINE_CANDIDATES!r}")

def read_ratings(p: Path) -> pd.DataFrame:
    if not p.exists():
        fatal(f"Elo ratings file missing: {p}\n"
              "Create it with columns: team_abbr,elo (pre-week ratings).")
    try:
        df = pd.read_csv(p, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(p, encoding='latin-1')
    # allow flexible header case
    cols = {c.lower(): c for c in df.columns}
    ab = cols.get("team_abbr")
    el = cols.get("elo")
    if not ab or not el:
        fatal(f"Ratings must have headers team_abbr,elo. Found: {list(df.columns)}")
    df = df.rename(columns={ab: "team_abbr", el: "elo"}).copy()
    df["team_abbr"] = df["team_abbr"].astype(str).str.strip().str.upper()
    if df["team_abbr"].duplicated().any():
        dups = df[df["team_abbr"].duplicated()]["team_abbr"].unique().tolist()
        fatal(f"Duplicate team_abbr in ratings: {dups}")
    return df[["team_abbr", "elo"]]

def logistic_from_elo(diff):
    # P(home) = 1 / (1 + 10^(-diff/400))
    return 1.0 / (1.0 + (10.0 ** (-(diff / 400.0))))

def main():
    base_path = pick_baseline()
    print(f"[INFO] Using games baseline: {base_path.relative_to(ROOT)}")

    base = pd.read_csv(base_path)
    # Normalize baseline headers we rely on
    cols = {c.lower(): c for c in base.columns}
    need = ["msf_game_id","game_start","home_abbr","away_abbr","venue","status"]
    miss = [c for c in need if c not in cols]
    if miss:
        fatal(f"Baseline missing required columns: {miss}. Found: {list(base.columns)}")
    base = base.rename(columns={cols["home_abbr"]: "home_abbr",
                                cols["away_abbr"]: "away_abbr"}).copy()
    base["home_abbr"] = base["home_abbr"].astype(str).str.strip().str.upper()
    base["away_abbr"] = base["away_abbr"].astype(str).str.strip().str.upper()

    ratings = read_ratings(RATINGS_CSV)


    # Guard: ratings must not be flat
    if ratings['elo'].nunique() <= 1:
        fatal(f"Ratings are flat (nunique={ratings['elo'].nunique()}). "
              f"Update {RATINGS_CSV} with differentiated pre-week Elo.")

    # Join Elo to home/away
    merged = (base
              .merge(ratings.rename(columns={"team_abbr": "home_abbr", "elo": "elo_home"}),
                     on="home_abbr", how="left")
              .merge(ratings.rename(columns={"team_abbr": "away_abbr", "elo": "elo_away"}),
                     on="away_abbr", how="left"))

    # Guardrails: ensure we have Elo for all teams present this week
    missing_home = merged[merged["elo_home"].isna()]["home_abbr"].unique().tolist()
    missing_away = merged[merged["elo_away"].isna()]["away_abbr"].unique().tolist()
    missing = sorted(set(missing_home + missing_away))
    if missing:
        fatal(f"Elo missing for {len(missing)} team(s): {missing}. "
              f"Update {RATINGS_CSV} (team_abbr,elo).")

    # Compute model probability
    merged["elo_diff"] = merged["elo_home"] - merged["elo_away"]
    merged["p_home_model"] = logistic_from_elo(merged["elo_diff"])

    # Order & write
    preferred = [c for c in [
        "msf_game_id","game_start","home_abbr","away_abbr","venue","status",
        "elo_home","elo_away","elo_diff","p_home_model"
    ] if c in merged.columns]
    merged = merged[preferred + [c for c in merged.columns if c not in preferred]]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)
    print(f"[OK] wrote {OUT_CSV} (rows={len(merged)}) with real p_home_model")

if __name__ == "__main__":
    main()
