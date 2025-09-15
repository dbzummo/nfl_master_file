import argparse, csv, hashlib, os, sys
from pathlib import Path
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--season", required=True)
parser.add_argument("--week", type=int, required=True)
parser.add_argument("--out", default="out/ingest/week_games.csv")
# CSV rows: date(YYYY-MM-DD),away_abbr,home_abbr
parser.add_argument("--games-csv", required=True)
args = parser.parse_args()

Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)

rows = []
with open(args.games_csv, newline="") as f:
    r = csv.DictReader(f)
    for row in r:
        date = row["date"].strip()
        away = row["away"].strip().upper()
        home = row["home"].strip().upper()
        # Synthesize a stable msf_game_id-like integer from date/teams
        key = f"{date}-{away}-{home}".encode()
        msf_game_id = int(hashlib.md5(key).hexdigest()[:8], 16)
        # Normalize date to YYYY-MM-DD
        try:
            d = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[seed] bad date format: {date}; expected YYYY-MM-DD", file=sys.stderr)
            sys.exit(2)
        rows.append({
            "date": str(d),
            "away_team": away,
            "home_team": home,
            "week": args.week,
            "msf_game_id": msf_game_id
        })

with open(args.out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["date","away_team","home_team","week","msf_game_id"])
    w.writeheader()
    w.writerows(rows)

print(f"[seed] wrote {args.out} rows={len(rows)} (season={args.season} week={args.week})")
