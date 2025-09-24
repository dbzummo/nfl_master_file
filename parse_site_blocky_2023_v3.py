#!/usr/bin/env python3
import re, pathlib, csv
from datetime import datetime

# ---- paths ----
SRC = pathlib.Path("sources/2023_results_by_week.txt")
OUT = pathlib.Path("history/season_2023_from_site.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

# ---- helpers ----
MONTHS = {m:i for i,m in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"], start=1
)}

def norm(s: str) -> str:
    # collapse spaces, strip NBSP and zero-width junk
    s = s.replace("\u00a0"," ").replace("\u200b"," ").replace("\ufeff"," ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

DATE_RE = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+20\d{2}$")
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")  # e.g., 8:20, 9:30, 3:00
SCORE_RE = re.compile(r"^[WLP]\s+(\d+)-(\d+)(?:\s*\(OT\))?$", re.I)
SPREAD_RE = re.compile(r"^[WLP]\s+([+-]?\d+(?:\.\d+)?)$", re.I)
TOTAL_RE  = re.compile(r"^[OUP]\s+([+-]?\d+(?:\.\d+)?)$", re.I)

IGNORABLE = set([
    "2024 Results by Week","2023 Results by Week",
    "Click on a week (or scroll below) to view all games for a week.",
    "Back to top",
    "BOLD = team that covered the spread",
    "Day","Date","Time (ET)","Favorite","Underdog","Score","Spread","Over/","Under","Notes",
    "Week","Favorites","Home","Teams","Home","Favorites","Home","Underdogs","Over/","Unders",
    "Straight","Up","ATS","Playoffs","Round","(Seed)","Home","Favorite","Home","Underdogs",
])

def is_date_line(s: str) -> bool:
    return bool(DATE_RE.match(s))

def to_iso_date(s: str) -> str:
    # "Sep 5, 2023" -> "2023-09-05"
    mon_str, day, year = re.match(r"^([A-Za-z]{3})\s+(\d{1,2}),\s+(20\d{2})$", s).groups()
    month = MONTHS[mon_str]
    return f"{year}-{month:02d}-{int(day):02d}"

def next_nonempty(lines, i):
    n = len(lines)
    j = i
    while j < n and norm(lines[j]) == "":
        j += 1
    if j >= n:
        return None, None
    return j, norm(lines[j])

def looks_like_team(s: str) -> bool:
    # very permissive (we'll upper() later)
    if not s: return False
    if s in {"@","N"}: return False
    if s in IGNORABLE: return False
    if re.match(r"^[A-Za-z .()'/-]+$", s):  # names with spaces & punctuation
        # avoid stray "U 46.5" etc
        if not re.match(r"^[OUPWL]\b", s):
            return True
    return False

# ---- parse ----
if not SRC.exists():
    raise SystemExit(f"⛔ Source not found: {SRC}")

raw_lines = SRC.read_text(encoding="utf-8", errors="ignore").splitlines()
lines = [norm(x) for x in raw_lines]

rows = []
i = 0
n = len(lines)

while i < n:
    s = lines[i]

    # Skip obvious non-game noise
    if not s or s in IGNORABLE:
        i += 1
        continue

    # 1) Find a date line
    if not is_date_line(s):
        i += 1
        continue

    date_iso = to_iso_date(s)
    # 2) Try to read time line (optional)
    j, tline = next_nonempty(lines, i+1)
    if j is None:
        break
    if TIME_RE.match(tline):
        # consume time line
        i = j + 1
    else:
        # keep i at line after date (we'll continue parsing anyway)
        i = j

    # 3) Favorite side: optional location marker then team
    fav_marker = None
    j, maybe = next_nonempty(lines, i)
    if j is None:
        break
    if maybe in {"@", "N"}:
        fav_marker = maybe
        i = j + 1
        j, maybe = next_nonempty(lines, i)
        if j is None: break
    fav_team = maybe

    if not looks_like_team(fav_team):
        # Not a valid start; advance and try again
        i = j + 1
        continue
    i = j + 1

    # 4) Favorite score line: "W 27-20" or "L 10-18" (OT optional)
    j, sline = next_nonempty(lines, i)
    if j is None: break
    m = SCORE_RE.match(sline or "")
    if not m:
        # Sometimes an extra blank/label sneaks in; skip one and retry once
        j2, sline2 = next_nonempty(lines, j+1)
        if j2 is None: break
        m = SCORE_RE.match(sline2 or "")
        if m:
            j = j2
            sline = sline2
        else:
            i = j + 1
            continue
    fav_score = int(m.group(1))
    dog_score = int(m.group(2))
    i = j + 1

    # 5) Spread line: "W -3", "L -6.5", "P -7"
    j, spr = next_nonempty(lines, i)
    if j is None: break
    m = SPREAD_RE.match(spr or "")
    if not m:
        # The site sometimes inserts a blank; try skipping one
        j2, spr2 = next_nonempty(lines, j+1)
        if j2 is None: break
        m = SPREAD_RE.match(spr2 or "")
        if m:
            j = j2
        else:
            i = j + 1
            continue
    fav_spread = float(m.group(1))
    i = j + 1

    # 6) Underdog side: optional "@", then team
    dog_marker = None
    j, maybe = next_nonempty(lines, i)
    if j is None: break
    if maybe == "@":
        dog_marker = "@"
        i = j + 1
        j, maybe = next_nonempty(lines, i)
        if j is None: break
    dog_team = maybe
    if not looks_like_team(dog_team):
        # Rarely the team name is two lines (bad copy/paste). Try concatenating one more line.
        j2, maybe2 = next_nonempty(lines, j+1)
        combined = (dog_team + " " + (maybe2 or "")).strip()
        if looks_like_team(combined):
            dog_team = combined
            j = j2
        else:
            i = j + 1
            continue
    i = j + 1

    # 7) Total line: "O 46", "U 49.5", "P 41"
    j, t = next_nonempty(lines, i)
    if j is None: break
    m = TOTAL_RE.match(t or "")
    if not m:
        # Try skipping one stray token
        j2, t2 = next_nonempty(lines, j+1)
        if j2 is None: break
        m = TOTAL_RE.match(t2 or "")
        if m:
            j = j2
        else:
            i = j + 1
            continue
    total = float(m.group(1))
    i = j + 1

    # 8) Resolve home/away from markers
    #    '@' attached to a side means that side is HOME.
    #    'N' attached to favorite means neutral site, favorite designated as home in the listing.
    neutral_site = 1 if fav_marker == "N" else 0

    if fav_marker == "@":
        home_team = fav_team
        away_team = dog_team
        home_score = fav_score
        away_score = dog_score
        spread_home = fav_spread
    elif dog_marker == "@":
        home_team = dog_team
        away_team = fav_team
        home_score = dog_score
        away_score = fav_score
        spread_home = -fav_spread  # flip to home perspective
    elif fav_marker == "N":  # neutral; favorite listed as (designated) home
        home_team = fav_team
        away_team = dog_team
        home_score = fav_score
        away_score = dog_score
        spread_home = fav_spread
    else:
        # Fallback: if no markers appeared, assume favorite was HOME
        home_team = fav_team
        away_team = dog_team
        home_score = fav_score
        away_score = dog_score
        spread_home = fav_spread

    rows.append({
        "date": date_iso,
        "home_team": home_team.upper(),
        "away_team": away_team.upper(),
        "home_score": home_score,
        "away_score": away_score,
        "neutral_site": neutral_site,
        "spread_home": float(spread_home),
        "total": float(total),
    })

# ---- write ----
if not rows:
    # help user debug by showing the first few meaningful lines
    preview = []
    for s in lines:
        s2 = norm(s)
        if s2 and s2 not in IGNORABLE:
            preview.append("  · " + s2)
        if len(preview) >= 12:
            break
    print("⛔ No games parsed. First non-empty lines after normalization:")
    print("\n".join(preview))
    raise SystemExit(1)

with OUT.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=[
        "date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"
    ])
    w.writeheader()
    w.writerows(rows)

print(f"✅ Wrote {OUT} with {len(rows)} games.")
for r in rows[:5]:
    print("   ", r)
