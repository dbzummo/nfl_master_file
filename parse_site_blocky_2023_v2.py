#!/usr/bin/env python3
import re, csv, pathlib, datetime, sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
CWD = pathlib.Path().resolve()
CANDIDATES = [CWD/"sources"/"2023_results_by_week.txt", SCRIPT_DIR/"sources"/"2023_results_by_week.txt"]

SRC = next((c for c in CANDIDATES if c.exists()), None)
if SRC is None:
    print("⛔ Source not found. Tried:")
    for c in CANDIDATES: print("   -", c)
    sys.exit(1)

OUT = CWD/"history"/"season_2023_from_site.csv"
OUT.parent.mkdir(exist_ok=True, parents=True)

TEAMS = {"ARIZONA CARDINALS","ATLANTA FALCONS","BALTIMORE RAVENS","BUFFALO BILLS","CAROLINA PANTHERS",
"CHICAGO BEARS","CINCINNATI BENGALS","CLEVELAND BROWNS","DALLAS COWBOYS","DENVER BRONCOS",
"DETROIT LIONS","GREEN BAY PACKERS","HOUSTON TEXANS","INDIANAPOLIS COLTS","JACKSONVILLE JAGUARS",
"KANSAS CITY CHIEFS","LAS VEGAS RAIDERS","LOS ANGELES CHARGERS","LOS ANGELES RAMS","MIAMI DOLPHINS",
"MINNESOTA VIKINGS","NEW ENGLAND PATRIOTS","NEW ORLEANS SAINTS","NEW YORK GIANTS","NEW YORK JETS",
"PHILADELPHIA EAGLES","PITTSBURGH STEELERS","SAN FRANCISCO 49ERS","SEATTLE SEAHAWKS",
"TAMPA BAY BUCCANEERS","TENNESSEE TITANS","WASHINGTON COMMANDERS"}

def norm(s): return re.sub(r"\s+"," ", s.strip()).upper()
def is_team_line(s): return norm(s) in TEAMS

def parse_date_line(s, year):
    s=s.strip()
    for fmt in ("%b %d, %Y","%B %d, %Y"):
        try:
            d=datetime.datetime.strptime(s,fmt).date()
            if d.year==year: return d.isoformat()
        except: pass
    return None

score_re  = re.compile(r"^[WLP]\s+(\d+)-(\d+)")
spread_re = re.compile(r"^[WLP]\s+([+-]?\d+(?:\.\d+)?)")
ou_re     = re.compile(r"^[OU]\s+(\d+(?:\.\d+)?)")

def read_lines(path):
    txt = path.read_text(encoding="utf-8", errors="ignore").replace("\u00a0"," ")
    return [ln.rstrip() for ln in txt.splitlines()]

def parse_games(lines):
    games=[]; i=0; n=len(lines); cur_date=None
    def peek(j): return lines[j].strip() if 0<=j<n else ""
    while i<n:
        line=peek(i)
        d=parse_date_line(line,2023)
        if d: cur_date=d; i+=1; continue

        start=i
        fav_marker=None
        if line in {"@","N"}:
            fav_marker=line; i+=1; line=peek(i)

        if not is_team_line(line):
            i+=1; continue

        favorite=norm(line); i+=1

        m=score_re.match(peek(i))
        if not m: i=start+1; continue
        fav_pts,dog_pts=int(m.group(1)),int(m.group(2)); i+=1

        m=spread_re.match(peek(i))
        if not m: i=start+1; continue
        fav_spread=float(m.group(1)); i+=1

        if peek(i)=="":
            i+=1

        dog_marker=None
        if peek(i) in {"@","N"}:
            dog_marker=peek(i); i+=1

        if not is_team_line(peek(i)):
            i=start+1; continue
        underdog=norm(peek(i)); i+=1

        m=ou_re.match(peek(i))
        if not m: i=start+1; continue
        total=float(m.group(1)); i+=1

        neutral = 1 if (fav_marker=="N") else 0

        if fav_marker=="@":
            home,away=favorite,underdog
        elif dog_marker=="@":
            home,away=underdog,favorite
        else:
            home,away=favorite,underdog

        if home==favorite:
            hs,as_=fav_pts,dog_pts
            spread_home=fav_spread
        else:
            hs,as_=dog_pts,fav_pts
            spread_home=-fav_spread

        games.append({
            "date":cur_date or "",
            "home_team":home,
            "away_team":away,
            "home_score":hs,
            "away_score":as_,
            "neutral_site":neutral,
            "spread_home":spread_home,
            "total":total
        })

        if peek(i).lower().startswith("at "): i+=1
    return games

def main():
    lines=read_lines(SRC)
    nonempty=[ln for ln in lines if ln.strip()]
    if len(nonempty)<5:
        print("⛔ Source file looks empty or placeholder. First lines:")
        for s in nonempty[:10]: print("  ·", s)
        sys.exit(2)

    games=parse_games(lines)
    if not games:
        sample=[ln for ln in lines if ln.strip()][:20]
        print("⛔ No games parsed. First non-empty lines:")
        for s in sample: print("  ·", s)
        sys.exit(2)

    with OUT.open("w", newline="") as f:
        w=csv.DictWriter(f, fieldnames=[
            "date","home_team","away_team","home_score","away_score","neutral_site","spread_home","total"
        ])
        w.writeheader(); w.writerows(games)

    print(f"✅ Wrote {OUT} with {len(games)} games.")
    for g in games[:5]: print("   ", g)

if __name__=="__main__":
    main()
