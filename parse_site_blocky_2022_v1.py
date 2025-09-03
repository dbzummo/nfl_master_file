import csv, sys
from pathlib import Path

SRC = Path("sources/2022_results_by_week.txt")
OUT = Path("history/season_2022_from_site.csv")

WEEKDAYS = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"}
MONTHS = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
          "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}

def read_lines(p: Path):
    if not p.exists():
        sys.exit(f"⛔ Source not found: {p}")
    # normalize: strip, drop blank-only lines at parse-time
    return [ln.rstrip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()]

def is_weekday(s: str) -> bool:
    s = s.strip().split()[0] if s else ""
    return s in WEEKDAYS

def parse_date(s: str) -> str:
    # Example: "Sep 8, 2024" -> "2024-09-08"
    s = s.strip().replace(",", "")
    parts = s.split()
    if len(parts) != 3: return ""
    mon, day, year = parts
    if mon not in MONTHS: return ""
    try:
        d = int(day)
        y = int(year)
    except:
        return ""
    return f"{y:04d}-{MONTHS[mon]}-{d:02d}"

def parse_score_cell(s: str):
    # "W 27-20" or "L 17-20 (OT)" -> (27,20) or (17,20)
    import re
    m = re.search(r"(\d+)\s*-\s*(\d+)", s)
    if not m: return None, None
    return int(m.group(1)), int(m.group(2))

def parse_spread_cell(s: str):
    # "W -3" / "L -6.5" / "P -7" -> numeric -3 / -6.5 / -7
    import re
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def parse_total_cell(s: str):
    # "O 46" / "U 49.5" / "P 49" -> 46 / 49.5 / 49
    import re
    m = re.search(r"(\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def parse_games(lines):
    # collapse multiple blanks in-place during scan
    lines = [ln.strip() for ln in lines]
    n = len(lines)
    i = 0
    games = []
    while i < n:
        if not lines[i]:
            i += 1
            continue
        # detect start of a game row by a weekday cell
        if not is_weekday(lines[i]):
            i += 1
            continue

        # layout is columnar per row:
        # [weekday] [date] [time] [@|N|blank] [Favorite] [Score] [Spread] [blank?] [Underdog] [Over/Under] [Notes?]
        if i+9 >= n:
            break

        weekday = lines[i].split()[0]; i += 1
        date_cell = lines[i]; i += 1
        time_cell = lines[i]; i += 1

        site_cell = lines[i]; i += 1   # '@' or 'N' or '' (sometimes genuinely blank)
        site_cell = site_cell.strip()

        favorite = lines[i].strip().upper(); i += 1
        score_fav = lines[i]; i += 1
        spread_cell = lines[i]; i += 1

        # some dumps include an empty spacer column here
        if i < n and lines[i] == "":
            i += 1

        underdog = lines[i].strip().upper(); i += 1
        ou_cell = lines[i]; i += 1

        # optional note like "at London", "at Munich", etc.
        if i < n and lines[i].lower().startswith("at "):
            i += 1

        iso_date = parse_date(date_cell)
        fav_pts, dog_pts = parse_score_cell(score_fav)
        fav_spread = parse_spread_cell(spread_cell)
        total = parse_total_cell(ou_cell)

        # skip if parsing failed
        if not iso_date or fav_pts is None or dog_pts is None or fav_spread is None or total is None:
            # ignore malformed row and move on
            continue

        neutral = 1 if site_cell == "N" else 0
        # '@' means the FAVORITE is AWAY (game at underdog)
        favorite_is_away = (site_cell == "@")

        if favorite_is_away:
            home_team, away_team = underdog, favorite
            home_score, away_score = dog_pts, fav_pts
            spread_home = -fav_spread
        else:
            home_team, away_team = favorite, underdog
            home_score, away_score = fav_pts, dog_pts
            spread_home = fav_spread

        games.append({
            "date": iso_date,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "neutral_site": neutral,
            "spread_home": float(spread_home),
            "total": float(total),
        })
    return games

def main():
    lines = read_lines(SRC)
    games = parse_games(lines)
    if not games:
        sample = [ln for ln in lines if ln.strip()][:20]
        print("⛔ No games parsed. First non-empty lines:")
        for s in sample:
            print("  ·", s)
        sys.exit(2)

    OUT.parent.mkdir(exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"
        ])
        w.writeheader()
        w.writerows(games)

    print(f"✅ Wrote {OUT} with {len(games)} games.")
    for g in games[:5]:
        print("   ", g)

if __name__ == "__main__":
    main()
