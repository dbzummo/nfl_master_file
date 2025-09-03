#!/usr/bin/env python3
import re, csv, pathlib
from datetime import datetime

SRC = pathlib.Path("sources/2024_results_by_week.txt")
OUT = pathlib.Path("history/season_2024_from_site.csv")
OUT.parent.mkdir(exist_ok=True)

DAYS = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"}
LOCFLAGS = {"@", "N"}  # @=favorite home, N=neutral (favorite listed first)

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def parse_date(s: str) -> str:
    # e.g. "Sep 5, 2024" -> "2024-09-05"
    return datetime.strptime(s.strip(), "%b %d, %Y").strftime("%Y-%m-%d")

def parse_score(s: str):
    # "W 26-20 (OT)" or "L 10-31" -> (fav_pts, dog_pts)
    s = s.split("(")[0]
    m = re.search(r"(\d+)\D+(\d+)", s)
    if not m:
        raise ValueError(f"Bad score: {s!r}")
    return int(m.group(1)), int(m.group(2))

def parse_num_after_letter(s: str) -> float:
    # "W -3.5" / "L -7" / "P -7" / "O 49.5" / "U 43"
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        raise ValueError(f"No number in: {s!r}")
    return float(m.group(0))

def is_team_line(s: str) -> bool:
    # crude but effective for this blocky paste
    if not s: return False
    if s in DAYS or s in LOCFLAGS: return False
    if re.fullmatch(r"[OUWP]\b.*", s): return False   # lines starting with O/U/W/L/P (not teams)
    if re.fullmatch(r"\d{1,2}:\d{2}", s): return False
    if re.search(r"\d", s): return False
    if s.lower().startswith(("back to top","bold =")): return False
    if "Regular Season" in s or "Playoffs" in s: return False
    return True

def load_lines():
    if not SRC.exists():
        raise SystemExit(f"⛔ Source not found: {SRC}")
    raw = SRC.read_text(encoding="utf-8", errors="ignore").splitlines()
    # normalize and drop empty, but keep “@/N” as separate tokens
    lines = [norm(x) for x in raw]
    lines = [x for x in lines if x]  # drop blanks
    return lines

def main():
    lines = load_lines()
    i, n = 0, len(lines)
    games = []

    def next_line():
        nonlocal i
        if i >= n: return None
        val = lines[i]; i += 1
        return val

    # skip everything until we see the first Day line
    while i < n and lines[i] not in DAYS:
        i += 1

    while i < n:
        # Expect a game block starting with Day / Date / Time (maybe) / LocFlag (maybe)
        day = next_line()
        if day not in DAYS:
            # Not a game start; skip to next potential day
            while i < n and lines[i] not in DAYS:
                i += 1
            continue

        # Date
        if i >= n: break
        date_line = next_line()
        # Sometimes there’s noise; skip until we hit a month-like token (e.g., "Sep 5, 2024")
        while i < n and not re.match(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$", date_line):
            # If we accidentally hit another day, rewind one and treat as next block
            if date_line in DAYS:
                i -= 1
                break
            date_line = next_line()
            if date_line is None: break
        if date_line is None or date_line in DAYS:
            # malformed; try next block
            continue
        date_iso = parse_date(date_line)

        # Time (can be anything like "8:20", "9:30", etc.) — skip it if present
        time_line = lines[i] if i < n else ""
        if re.fullmatch(r"\d{1,2}:\d{2}", time_line):
            i += 1  # consume time

        # Optional location flag line: "@" or "N"
        locflag = ""
        if i < n and lines[i] in LOCFLAGS:
            locflag = lines[i]
            i += 1

        # Favorite team
        if i >= n: break
        fav = next_line()
        while fav in DAYS or fav in LOCFLAGS or not is_team_line(fav):
            if i >= n: break
            fav = next_line()
        if not is_team_line(fav):
            # malformed block, skip ahead
            continue
        fav = fav.upper()

        # Score (fav first)
        if i >= n: break
        score_line = next_line()
        while re.search(r"\d", score_line) is None:
            if i >= n: break
            score_line = next_line()
        try:
            fav_pts, dog_pts = parse_score(score_line)
        except Exception:
            # skip this block if score missing
            continue

        # Spread (for favorite)
        if i >= n: break
        spread_line = next_line()
        # Sometimes there’s an extra token between score and spread; advance until we find a number
        tries = 0
        while re.search(r"[-+]?\d", spread_line) is None and tries < 3 and i < n:
            spread_line = next_line(); tries += 1
        try:
            fav_spread = parse_num_after_letter(spread_line)
        except Exception:
            continue

        # Optional empty spacer lines might appear here — now Underdog team
        if i >= n: break
        under = next_line()
        while not is_team_line(under):
            if i >= n: break
            under = next_line()
        if not is_team_line(under):
            continue
        under = under.upper()

        # Total (O/U line)
        if i >= n: break
        total_line = next_line()
        # Advance until we see a number
        tries = 0
        while re.search(r"\d", total_line) is None and tries < 3 and i < n:
            total_line = next_line(); tries += 1
        try:
            total = parse_num_after_letter(total_line)
        except Exception:
            # If missing, skip this game
            continue

        # Determine home/away from locflag
        neutral = 1 if locflag == "N" else 0
        if locflag == "@":
            # favorite is HOME
            home_team, away_team = fav, under
            home_score, away_score = fav_pts, dog_pts
            spread_home = fav_spread
        else:
            # neutral or favorite listed at underdog’s venue => treat favorite as AWAY (unless neutral)
            # for neutral we’ll still label favorite as HOME for consistency with spread direction
            if neutral:
                home_team, away_team = fav, under
                home_score, away_score = fav_pts, dog_pts
                spread_home = fav_spread
            else:
                home_team, away_team = under, fav
                home_score, away_score = dog_pts, fav_pts
                spread_home = -fav_spread

        games.append({
            "date": date_iso,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "neutral_site": neutral,
            "spread_home": float(spread_home),
            "total": float(total),
        })

    if not games:
        # show first few non-empty lines to debug
        preview = "\n  · " + "\n  · ".join(lines[:12])
        print("⛔ No games parsed. First non-empty lines:" + preview)
        raise SystemExit(1)

    # write csv
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"
        ])
        w.writeheader()
        w.writerows(games)

    print(f"✅ Wrote {OUT} with {len(games)} games.")
    for d in games[:5]:
        print("   ", d)

if __name__ == "__main__":
    main()