#!/usr/bin/env python3
import re, pathlib, csv

src = pathlib.Path("sources/2023_results_by_week.txt")
out = pathlib.Path("history/season_2023_from_site.csv")
out.parent.mkdir(exist_ok=True)

games = []
cur_date = None

# Simple regexes
date_re   = re.compile(r"(\d{4}-\d{2}-\d{2})")
score_re  = re.compile(r"(\d+)-(\d+)")
spread_re = re.compile(r"([-+]?\d+\.?\d*)")
total_re  = re.compile(r"O (\d+\.?\d*)|U (\d+\.?\d*)")

lines = src.read_text().splitlines()
for line in lines:
    line = line.strip()
    if not line:
        continue

    # Match date lines like "Sep 7, 2023"
    if re.match(r"[A-Z][a-z]{2} \d{1,2}, 2023", line):
        # normalize into yyyy-mm-dd
        try:
            import datetime
            cur_date = str(datetime.datetime.strptime(line, "%b %d, %Y").date())
        except Exception:
            cur_date = None
        continue

    # Match game lines with structure "... Favorite ... Score ... Spread ... Underdog ..."
    if "@" in line or line.startswith(tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")):
        # This is too blocky to parse in one go, so skip — actual game info is multi-line.
        continue

# For now we’ll just signal no games
if not games:
    print("⛔ No games parsed yet. Need to refine regex for 2023 format.")

else:
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"])
        w.writeheader()
        for g in games:
            w.writerow(g)
    print(f"✅ Wrote {out} with {len(games)} games.")
