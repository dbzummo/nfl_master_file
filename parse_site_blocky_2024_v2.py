# parse_site_blocky_2024_v2.py
# Reads sources/2024_results_by_week.txt (your blocky paste), parses all games
# Outputs: history/season_2024_from_site.csv with
#   date,home_team,away_team,home_score,away_score,neutral_site,spread_home,total

import re, pathlib, sys
from datetime import datetime

SRC = pathlib.Path("sources/2024_results_by_week.txt")
OUT = pathlib.Path("history/season_2024_from_site.csv")
OUT.parent.mkdir(exist_ok=True)

MONTHS = {
    "Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
    "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12
}
DAY_TOKENS = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"}
SYMBOLS = {"@", "N"}  # @ = home marker for the very next team; N = neutral site
SKIP_START_PHRASES = {
    "2024 results by week",
    "click on a week",
    "2024 results by team",
    "click on a team",
    "back to top",
    "bold =",
    "summary statistics",
}

# ---------- helpers ----------
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def is_day(s: str) -> bool:
    return s in DAY_TOKENS

def parse_date(text_date: str) -> str:
    # Examples: "Sep 5, 2024"  or "Jan 4, 2025"
    m = re.match(r"([A-Za-z]{3})\s+(\d{1,2}),\s*(\d{4})", text_date)
    if not m: raise ValueError(f"Bad date token: {text_date!r}")
    mon = MONTHS[m.group(1)]
    day = int(m.group(2))
    year = int(m.group(3))
    return datetime(year, mon, day).strftime("%Y-%m-%d")

def parse_score(line: str):
    # "W 27-20" or "L 10-18" or "W 26-20 (OT)" -> take the first two integers as FAV-UND
    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", line)
    if not m: raise ValueError(f"Score not found in: {line!r}")
    fav, und = int(m.group(1)), int(m.group(2))
    return fav, und

def parse_spread(line: str):
    # "W -3" / "L -6.5" / "P -7"  -> numeric second token (can be + or -)
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", line)
    if not m: raise ValueError(f"Spread not found in: {line!r}")
    return float(m.group(1))

def parse_total(line: str):
    # "O 46" / "U 40.5" / "P 49"
    m = re.search(r"(\d+(?:\.\d+)?)", line)
    if not m: raise ValueError(f"Total not found in: {line!r}")
    return float(m.group(1))

def looks_like_team(s: str) -> bool:
    # heuristics: uppercase words + spaces, not a symbol, not a header
    if s in SYMBOLS: return False
    if s.lower().startswith("at "): return False
    if s.lower() in {"notes"}: return False
    if re.fullmatch(r"[A-Z0-9 .&'()-]+", s) and len(s) >= 3:
        return True
    return False

def next_nonempty(lines, i):
    n = len(lines)
    while i < n and not lines[i]:
        i += 1
    return i

# ---------- read & normalize ----------
if not SRC.exists():
    print(f"⛔ {SRC} not found. Put your blocky text there.")
    sys.exit(1)

raw = SRC.read_text(encoding="utf-8", errors="ignore")
# normalize to one token per line, strip cruft
lines = [norm(x) for x in raw.replace("\u00A0", " ").splitlines()]
lines = [x for x in lines if x]  # drop empty

# drop obvious headers and page chrome
clean = []
for x in lines:
    lx = x.lower()
    if any(lx.startswith(p) for p in SKIP_START_PHRASES):
        continue
    # skip lone multi-column headers broken onto separate lines
    if lx in {"day","date","time (et)","favorite","score","spread","underdog","over/","under","notes","teams","home","favorites","underdogs","straight","up","ats","over/ unders"}:
        continue
    clean.append(x)

lines = clean
n = len(lines)
i = 0
games = []
errors = 0

# ---------- state machine ----------
while i < n:
    # seek a game: Day, Date, Time
    if not is_day(lines[i]):
        i += 1
        continue

    try:
        dayTok = lines[i]; i += 1
        dateTok = lines[i]; i += 1
        timeTok = lines[i]; i += 1  # we don't need time for history, but this anchors the pattern
        game_date = parse_date(dateTok)

        # FAVORITE side: optional '@' or 'N' tokens may appear, each on its own line.
        neutral = 0
        fav_home = None
        und_home = None

        i = next_nonempty(lines, i)
        # optionally several symbols in any order (@, N)
        seen_symbol = True
        fav_symbols = set()
        while i < n and lines[i] in SYMBOLS:
            fav_symbols.add(lines[i])
            i += 1
        if "N" in fav_symbols: neutral = 1
        if "@" in fav_symbols: fav_home = True  # favorite marked as home

        # favorite team
        if i >= n or not looks_like_team(lines[i]):
            raise ValueError("Expected Favorite team")
        fav_team = lines[i]; i += 1

        # favorite score
        fav_score_line = lines[i]; i += 1
        fav_pts, und_pts = parse_score(fav_score_line)

        # spread (relative to FAVORITE)
        spread_line = lines[i]; i += 1
        spread_fav = parse_spread(spread_line)  # e.g., -3.0 for fav -3

        # UNDERDOG side: optional '@' before team marks underdog as home
        i = next_nonempty(lines, i)
        und_symbols = set()
        while i < n and lines[i] in SYMBOLS:
            und_symbols.add(lines[i])
            i += 1
        if "@" in und_symbols: und_home = True
        if "N" in und_symbols: neutral = 1  # very rare, but handle it

        if i >= n or not looks_like_team(lines[i]):
            raise ValueError("Expected Underdog team")
        und_team = lines[i]; i += 1

        # O/U line
        ou_line = lines[i]; i += 1
        total = parse_total(ou_line)

        # optional trailing note like "at London"
        if i < n and lines[i].lower().startswith("at "):
            neutral = 1
            i += 1

        # Determine home/away according to markers:
        # - If '@' was on favorite block -> favorite is home
        # - Else if '@' was on underdog block -> underdog is home
        # - Else (no @ anywhere): choose favorite as "home" (harmless with neutral flag; consistent otherwise)
        if fav_home and und_home:
            # shouldn’t happen; prefer the last marker encountered (underdog) or default to favorite
            und_home = True
            fav_home = False
        if fav_home is True:
            home_team, away_team = fav_team, und_team
            home_score, away_score = fav_pts, und_pts
            # spread_fav is relative to favorite; since favorite is home, spread_home = spread_fav
            spread_home = float(spread_fav)
        elif und_home is True:
            home_team, away_team = und_team, fav_team
            home_score, away_score = und_pts, fav_pts
            # favorite is away; home is underdog. If favorite is -3, home (dog) is +3 => invert sign.
            spread_home = float(-spread_fav)
        else:
            # no explicit home marker; assume favorite is home
            home_team, away_team = fav_team, und_team
            home_score, away_score = fav_pts, und_pts
            spread_home = float(spread_fav)

        rec = {
            "date": game_date,
            "home_team": home_team.strip().upper(),
            "away_team": away_team.strip().upper(),
            "home_score": int(home_score),
            "away_score": int(away_score),
            "neutral_site": 1 if neutral else 0,
            "spread_home": float(spread_home),
            "total": float(total),
        }
        games.append(rec)

    except Exception as e:
        errors += 1
        # advance cautiously to avoid infinite loops
        i += 1

# ---------- write ----------
import csv
if not games:
    print("⛔ No games parsed. Double-check the input text formatting.")
    # small debug aid: show first few content lines
    for j, ln in enumerate(lines[:12], 1):
        print(f"  · {ln}")
    sys.exit(1)

# sort by date (stable within date)
games.sort(key=lambda r: r["date"])
with OUT.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"])
    w.writeheader()
    w.writerows(games)

print(f"✅ Wrote {OUT} with {len(games)} games. (Skipped {errors} stray rows.)")
# show a couple examples
for r in games[:4]:
    print("   ", r)
