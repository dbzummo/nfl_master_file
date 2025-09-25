#!/usr/bin/env python3
import pandas as pd, pathlib, sys

NFL32 = ["ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
         "HOU","IND","JAX","KC","LV","LAC","LA","MIA","MIN","NE","NO","NYG","NYJ",
         "PHI","PIT","SEA","SF","TB","TEN","WAS"]

def main():
    try:
        elo = pd.read_csv("out/elo_ratings.csv")  # expects: date, team, elo_post
    except Exception as e:
        print("[FATAL] missing out/elo_ratings.csv — run compute_elo.py first. Reason:", e)
        sys.exit(1)

    elo["date"] = pd.to_datetime(elo["date"], errors="coerce")
    elo = elo.dropna(subset=["date","team","elo_post"])
    cur = (elo.sort_values(["team","date"])
              .groupby("team").tail(1)[["team","elo_post"]]
              .rename(columns={"team":"team_abbr","elo_post":"elo"}))

    have = set(cur["team_abbr"])
    missing = [t for t in NFL32 if t not in have]

    # Prefer backfill from season-start snapshot if available
    seed = pd.DataFrame(columns=["team_abbr","elo"])
    try:
        s = pd.read_csv("out/elo_season_start.csv")
        # normalize likely column names
        cols = {c.lower(): c for c in s.columns}
        tcol = cols.get("team") or next(iter(s.columns))
        ecol = cols.get("elo_start") or cols.get("elo") or list(s.columns)[1]
        seed = s[[tcol, ecol]].rename(columns={tcol:"team_abbr", ecol:"elo"})
        seed["team_abbr"] = seed["team_abbr"].astype(str).str.strip().str.upper()
    except Exception:
        pass

    if missing:
        rows = []
        for t in missing:
            r = seed.loc[seed["team_abbr"].eq(t)]
            if len(r):
                rows.append({"team_abbr": t, "elo": float(r["elo"].iloc[0])})
            else:
                rows.append({"team_abbr": t, "elo": 1500.0})
        cur = pd.concat([cur, pd.DataFrame(rows)], ignore_index=True)

    cur = cur.sort_values("team_abbr").reset_index(drop=True)
    pathlib.Path("data/elo").mkdir(parents=True, exist_ok=True)
    cur.to_csv("data/elo/current_ratings.csv", index=False)
    print(f"[OK] wrote data/elo/current_ratings.csv rows= {len(cur)}")
    if missing:
        print("[INFO] backfilled:", missing)

if __name__ == "__main__":
    main()
