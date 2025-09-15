#!/usr/bin/env python3
"""
Compute Elo ratings from local history CSVs (no APIs).

Inputs (must contain at least: home_team, away_team, date, home_score, away_score):
  --history_glob  e.g. "history/season_*_from_site.csv" (default)
  --start_season  ignore earlier seasons (default 2019)

Params:
  --k             base K-factor (default 20.0)
  --hfa_elo       home-field advantage in Elo points (default 55)
  --season_regress regression to 1500 at season start (default 0.25)

Outputs:
  out/elo_ratings.csv         team,date,elo_post (snapshot after each game)
  out/elo_season_start.csv    season,team,elo_start
  out/elo_games_enriched.csv  per-game with pre Elo & expected prob (exp_home)
"""
import argparse, glob, os
import pandas as pd
import numpy as np

OUT_DIR = "out"
BASE_ELO = 1500.0

def nfl_season_year(dser):
    dt = pd.to_datetime(dser, errors="coerce")
    m = dt.dt.month; y = dt.dt.year
    return (y.where(m >= 8, y - 1)).astype("Int64")

def read_hist(glob_pat):
    frames = []
    for p in sorted(glob.glob(glob_pat)):
        try:
            df = pd.read_csv(p)
            need = ["home_team","away_team","date","home_score","away_score"]
            if not all(c in df.columns for c in need): continue
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
            df = df.dropna(subset=["date"])
            df["season"] = nfl_season_year(df["date"])
            df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce")
            df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce")
            df = df.dropna(subset=["home_score","away_score"])
            frames.append(df[["home_team","away_team","date","home_score","away_score","season"]])
        except Exception:
            pass
    if not frames:
        raise SystemExit(f"No usable history files matched: {glob_pat}")
    out = pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)
    return out

def expected_prob(elo_diff):
    return 1.0 / (1.0 + 10.0 ** (-(elo_diff) / 400.0))

def mov_multiplier(margin, elo_diff):
    return np.log(max(margin,1)+1.0) * (2.2 / ( (abs(elo_diff)*0.001) + 2.2 ))

def compute_elo(hist, k=20.0, hfa_elo=55.0, season_regress=0.25):
    ratings = {}
    season_start = []
    games_rows = []
    rating_snapshots = []

    for season in sorted([s for s in hist["season"].dropna().unique() if s > 0]):
        season_df = hist[hist["season"] == season].sort_values("date").copy()

        # season regression
        teams = pd.unique(pd.concat([season_df["home_team"], season_df["away_team"]], ignore_index=True))
        for t in teams:
            prev = ratings.get(t, BASE_ELO)
            ratings[t] = (1 - season_regress) * prev + season_regress * BASE_ELO
            season_start.append({"season": int(season), "team": t, "elo_start": ratings[t]})

        # iterate games
        for _, row in season_df.iterrows():
            h, a = row["home_team"], row["away_team"]
            hs, as_ = float(row["home_score"]), float(row["away_score"])
            date = row["date"]

            elo_h = ratings.get(h, BASE_ELO)
            elo_a = ratings.get(a, BASE_ELO)

            diff = (elo_h + hfa_elo) - elo_a
            eh = expected_prob(diff)
            rh = 1.0 if hs > as_ else (0.5 if hs == as_ else 0.0)
            margin = abs(hs - as_)
            g = mov_multiplier(margin, diff)
            delta = k * g * (rh - eh)

            elo_h_post = elo_h + delta
            elo_a_post = elo_a - delta
            ratings[h] = elo_h_post; ratings[a] = elo_a_post

            games_rows.append({
                "date": date.date(), "season": int(season),
                "home_team": h, "away_team": a,
                "elo_home_pre": elo_h, "elo_away_pre": elo_a,
                "elo_diff_pre": diff, "exp_home": eh,
                "home_score": hs, "away_score": as_, "mov": margin, "delta": delta
            })
            rating_snapshots.append({"date": date.date(), "team": h, "elo_post": elo_h_post})
            rating_snapshots.append({"date": date.date(), "team": a, "elo_post": elo_a_post})

    os.makedirs(OUT_DIR, exist_ok=True)
    pd.DataFrame(rating_snapshots).drop_duplicates(["date","team"], keep="last") \
        .to_csv(os.path.join(OUT_DIR, "elo_ratings.csv"), index=False)
    pd.DataFrame(games_rows).to_csv(os.path.join(OUT_DIR, "elo_games_enriched.csv"), index=False)
    pd.DataFrame(season_start).drop_duplicates(["season","team"], keep="last") \
        .to_csv(os.path.join(OUT_DIR, "elo_season_start.csv"), index=False)
    print("Wrote out/elo_ratings.csv, out/elo_games_enriched.csv, out/elo_season_start.csv")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history_glob", default="history/season_*_from_site.csv")
    ap.add_argument("--start_season", type=int, default=2019)
    ap.add_argument("--k", type=float, default=20.0)
    ap.add_argument("--hfa_elo", type=float, default=55.0)
    ap.add_argument("--season_regress", type=float, default=0.25)
    args = ap.parse_args()

    hist = read_hist(args.history_glob)
    hist = hist[hist["season"] >= args.start_season].copy()
    if hist.empty:
        raise SystemExit("No history rows at/after start_season.")
    compute_elo(hist, k=args.k, hfa_elo=args.hfa_elo, season_regress=args.season_regress)

if __name__ == "__main__":
    main()
