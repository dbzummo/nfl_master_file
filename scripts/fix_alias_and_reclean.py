#!/usr/bin/env python3
import glob, re, pandas as pd, numpy as np
from pathlib import Path

# Canonical 3-letter team abbr map from common long names and stale codes
ALIAS = {
    # legacy relocations / abbrev drift
    "SD":"LAC","OAK":"LV","WSH":"WAS","LAR":"LA","ST LOUIS RAMS":"LA","JAC":"JAX",
    # common long names (add as needed)
    "BUFFALO BILLS":"BUF","NEW ORLEANS SAINTS":"NO","LOS ANGELES CHARGERS":"LAC","LOS ANGELES RAMS":"LA",
    "NEW YORK GIANTS":"NYG","NEW YORK JETS":"NYJ","DALLAS COWBOYS":"DAL","PHILADELPHIA EAGLES":"PHI",
    "MIAMI DOLPHINS":"MIA","NEW ENGLAND PATRIOTS":"NE","BALTIMORE RAVENS":"BAL","PITTSBURGH STEELERS":"PIT",
    "CLEVELAND BROWNS":"CLE","CINCINNATI BENGALS":"CIN","HOUSTON TEXANS":"HOU","TENNESSEE TITANS":"TEN",
    "INDIANAPOLIS COLTS":"IND","JACKSONVILLE JAGUARS":"JAX","KANSAS CITY CHIEFS":"KC","LAS VEGAS RAIDERS":"LV",
    "DENVER BRONCOS":"DEN","LOS ANGELES RAMS (LA)":"LA","SEATTLE SEAHAWKS":"SEA","SAN FRANCISCO 49ERS":"SF",
    "ARIZONA CARDINALS":"ARI","DETROIT LIONS":"DET","GREEN BAY PACKERS":"GB","MINNESOTA VIKINGS":"MIN",
    "ATLANTA FALCONS":"ATL","CAROLINA PANTHERS":"CAR","TAMPA BAY BUCCANEERS":"TB","WASHINGTON COMMANDERS":"WAS",
    "CHICAGO BEARS":"CHI","NEW ORLEANS":"NO","BUFFALO":"BUF" # sometimes city only
}
def norm_team(x:str) -> str:
    if x is None: return ""
    t = str(x).strip().upper()
    # strip stray punctuation variants like "BUF.", "BUF," etc.
    t = t.rstrip(".,;:)")
    return ALIAS.get(t, t)

def coalesce(df, cols):
    for c in cols:
        if c in df.columns: return c
    return None

def load_all():
    files = sorted(glob.glob("history/season_*_from_site.csv"))
    if not files:
        raise SystemExit("[FATAL] No history files at history/season_*_from_site.csv")
    frames=[]
    for f in files:
        df = pd.read_csv(f)
        c_date = coalesce(df, ["date","game_date","start_time","startTime"])
        c_season = coalesce(df, ["season","season_year","seasonYear"])
        c_type = coalesce(df, ["season_type","seasonType"])
        c_home = coalesce(df, ["home_abbr","home_team","homeTeam","home"])
        c_away = coalesce(df, ["away_abbr","away_team","awayTeam","away"])
        c_hs   = coalesce(df, ["home_score","homeScore","homePts","home_points"])
        c_as   = coalesce(df, ["away_score","awayScore","awayPts","away_points"])
        if not all([c_date,c_home,c_away,c_hs,c_as]):
            print(f"[WARN] Skipping {f} (missing core cols)")
            continue
        g = df[[c_date,c_home,c_away,c_hs,c_as] + ([c_season] if c_season else []) + ([c_type] if c_type else [])].copy()
        g.rename(columns={c_date:"date", c_home:"home_abbr", c_away:"away_abbr", c_hs:"home_score", c_as:"away_score"}, inplace=True)
        g["date"] = pd.to_datetime(g["date"], errors="coerce")
        if c_season:
            g.rename(columns={c_season:"season"}, inplace=True)
        else:
            m = re.search(r"season_(\d{4})", Path(f).name)
            g["season"] = int(m.group(1)) if m else g["date"].dt.year
        if c_type:
            g.rename(columns={c_type:"season_type"}, inplace=True)
        else:
            g["season_type"] = None
        # normalize teams
        g["home_abbr"] = g["home_abbr"].map(norm_team)
        g["away_abbr"] = g["away_abbr"].map(norm_team)
        # numeric
        g["home_score"] = pd.to_numeric(g["home_score"], errors="coerce")
        g["away_score"] = pd.to_numeric(g["away_score"], errors="coerce")
        g = g.dropna(subset=["date","home_abbr","away_abbr","home_score","away_score"])
        frames.append(g)
    if not frames:
        raise SystemExit("[FATAL] No usable rows in history files.")
    hist = pd.concat(frames, ignore_index=True)

    # preseason filter: if labeled, keep 'regular'; else drop Aug
    mask_reg = hist["season_type"].fillna("").str.lower().eq("regular")
    if mask_reg.any():
        hist = hist[mask_reg]
    else:
        hist = hist[hist["date"].dt.month >= 9]
    return hist

def main():
    hist = load_all()

    # keep 2024 and 2025 only
    hist = hist[hist["season"].isin([2024, 2025])].copy()

    # cut at Monday after Week 3 of 2025
    cut = pd.Timestamp("2025-09-22")
    hist = hist[(hist["season"] < 2025) | (hist["date"] < cut)].copy()

    outp = Path("out/history_clean_2024_2025_thru_w3.csv")
    outp.parent.mkdir(parents=True, exist_ok=True)
    hist[["date","season","home_abbr","away_abbr","home_score","away_score"]].to_csv(outp, index=False)
    print(f"[OK] wrote {outp} rows={len(hist)}")

    # quick BUF sanity
    buf = hist[(hist["home_abbr"].eq("BUF")) | (hist["away_abbr"].eq("BUF"))].sort_values("date").head(3)
    print("[INFO] BUF rows thru W3:", len(buf))
    if len(buf):
        print(buf[["date","home_abbr","away_abbr","home_score","away_score"]].to_string(index=False))

if __name__ == "__main__":
    main()
