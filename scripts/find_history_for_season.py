#!/usr/bin/env python3
import sys, glob, csv, os, pandas as pd

def nfl_season_year(dt):
    m = dt.dt.month
    y = dt.dt.year
    return (y.where(m >= 8, y - 1)).astype("Int64")

def header(p):
    try:
        with open(p, newline="", encoding="utf-8") as f:
            return next(csv.reader(f))
    except Exception:
        return []

DATE_CAND = ["date","Date","game_date","GameDate","kickoff","start_time","kickoff_utc","GameDateUTC"]

if len(sys.argv) != 2:
    print("usage: find_history_for_season.py <season_year>", file=sys.stderr); sys.exit(2)

target = int(sys.argv[1])
cands = []
for pat in ["history/*.csv", "*.csv", "artifacts/*.csv", "sources/*.csv"]:
    for f in glob.glob(pat):
        if not os.path.isfile(f): continue
        hdr = set(header(f))
        if not hdr: continue
        # must have teams and some date
        has_home = len({"home_team","home","Home","team_home"} & hdr) > 0
        has_away = len({"away_team","away","Away","team_away"} & hdr) > 0
        date_col = next((c for c in DATE_CAND if c in hdr), None)
        if not (has_home and has_away and date_col): continue
        try:
            df = pd.read_csv(f, usecols=[date_col], nrows=5000)
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.tz_localize(None).dt.date
            seas = nfl_season_year(pd.to_datetime(df[date_col]))
            if (seas == target).any():
                rows = len(df)
                cands.append((f.startswith("history/"), rows, f))
        except Exception:
            pass

if not cands:
    print(f"ERROR: no history CSV found for season {target}", file=sys.stderr)
    sys.exit(1)

best = sorted(cands, reverse=True)[0][2]
print(best)
