#!/usr/bin/env python3
import argparse, sys, pathlib
import pandas as pd

# --- Canonicalization / aliasing -------------------------------------------------
ALIAS = {
    # Current abbreviations (pass-through)
    "ARI":"ARI","ATL":"ATL","BAL":"BAL","BUF":"BUF","CAR":"CAR","CHI":"CHI","CIN":"CIN","CLE":"CLE",
    "DAL":"DAL","DEN":"DEN","DET":"DET","GB":"GB","HOU":"HOU","IND":"IND","JAX":"JAX","KC":"KC",
    "LAC":"LAC","LA":"LA","LV":"LV","MIA":"MIA","MIN":"MIN","NE":"NE","NO":"NO","NYG":"NYG","NYJ":"NYJ",
    "PHI":"PHI","PIT":"PIT","SEA":"SEA","SF":"SF","TB":"TB","TEN":"TEN","WAS":"WAS","WSH":"WAS",

    # Full team names -> abbrev
    "ARIZONA CARDINALS":"ARI","ATLANTA FALCONS":"ATL","BALTIMORE RAVENS":"BAL","BUFFALO BILLS":"BUF",
    "CAROLINA PANTHERS":"CAR","CHICAGO BEARS":"CHI","CINCINNATI BENGALS":"CIN","CLEVELAND BROWNS":"CLE",
    "DALLAS COWBOYS":"DAL","DENVER BRONCOS":"DEN","DETROIT LIONS":"DET","GREEN BAY PACKERS":"GB",
    "HOUSTON TEXANS":"HOU","INDIANAPOLIS COLTS":"IND","JACKSONVILLE JAGUARS":"JAX","KANSAS CITY CHIEFS":"KC",
    "LOS ANGELES CHARGERS":"LAC","SAN DIEGO CHARGERS":"LAC",
    "LOS ANGELES RAMS":"LA","ST. LOUIS RAMS":"LA",
    "LAS VEGAS RAIDERS":"LV","OAKLAND RAIDERS":"LV",
    "MIAMI DOLPHINS":"MIA","MINNESOTA VIKINGS":"MIN","NEW ENGLAND PATRIOTS":"NE","NEW ORLEANS SAINTS":"NO",
    "NEW YORK GIANTS":"NYG","NEW YORK JETS":"NYJ","PHILADELPHIA EAGLES":"PHI","PITTSBURGH STEELERS":"PIT",
    "SEATTLE SEAHAWKS":"SEA","SAN FRANCISCO 49ERS":"SF","TAMPA BAY BUCCANEERS":"TB","TENNESSEE TITANS":"TEN",
    # Washington timeline
    "WASHINGTON REDSKINS":"WAS","WASHINGTON FOOTBALL TEAM":"WAS","WASHINGTON COMMANDERS":"WAS",
}

def canon(team: str) -> str:
    t = str(team).upper().strip()
    return ALIAS.get(t, t)

# --- IO helpers ------------------------------------------------------------------
def read_week_games():
    candidates = [
        ("out/msf_details/msf_week.csv", ["date","away_team","home_team","week","msf_game_id"]),
        ("out/ingest/week_games.csv",    ["date","away_team","home_team","week","msf_game_id"]),
    ]
    for path, need_cols in candidates:
        p = pathlib.Path(path)
        if p.exists():
            df = pd.read_csv(p)
            missing = [c for c in need_cols if c not in df.columns]
            if missing:
                if missing == ["msf_game_id"]:
                    df["msf_game_id"] = pd.NA
                else:
                    print(f"[WARN] {path} missing {missing}, skipping …", file=sys.stderr); continue
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["away_team"] = df["away_team"].map(canon)
            df["home_team"] = df["home_team"].map(canon)
            df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
            return df, path
    sys.exit("[FATAL] No weekly baseline found (msf_week.csv or week_games.csv)")

def read_elo_ratings(path="out/elo_ratings_by_date.csv"):
    p = pathlib.Path(path)
    if not p.exists():
        sys.exit(f"[FATAL] Elo ratings not found at {path}. Run the Elo step first.")
    df = pd.read_csv(p)
    need = {"date","team","elo"}
    if not need.issubset(df.columns):
        sys.exit(f"[FATAL] {path} missing {sorted(need - set(df.columns))}")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    # normalize team names from history (full names) to abbrevs
    df["team"] = df["team"].map(canon)
    df = df.sort_values(["team","date"])
    return df

# --- Elo lookup / transform ------------------------------------------------------
def latest_elo_before(ratings, team, game_date):
    r = ratings.loc[ratings["team"].eq(team)]
    if r.empty: return 1500.0
    rb = r[r["date"] < game_date]
    if not rb.empty: return float(rb.iloc[-1]["elo"])
    ro = r[r["date"] <= game_date]
    if not ro.empty: return float(ro.iloc[-1]["elo"])
    return 1500.0

def expected_home_from_diff(elo_diff):
    return 1.0 / (1.0 + 10.0 ** (-elo_diff / 400.0))

# --- Main ------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Join current week's games with latest Elo ratings.")
    ap.add_argument("--hfa", type=float, default=55.0,
                    help="Home-field Elo advantage added to (E_home - E_away). Default: 55")
    ap.add_argument("--elo", default="out/elo_ratings_by_date.csv",
                    help="Path to Elo ratings-by-date CSV")
    ap.add_argument("--out", default="out/week_with_elo.csv",
                    help="Output CSV path")
    args = ap.parse_args()

    games, games_src = read_week_games()
    ratings = read_elo_ratings(args.elo)

    rows = []
    for _, g in games.iterrows():
        gdate = g["date"]; home = g["home_team"]; away = g["away_team"]
        elo_home_pre = latest_elo_before(ratings, home, gdate)
        elo_away_pre = latest_elo_before(ratings, away, gdate)
        elo_diff_pre = (elo_home_pre - elo_away_pre) + args.hfa
        exp_home = expected_home_from_diff(elo_diff_pre)

        rows.append({
            "date": gdate.isoformat(),
            "week": g.get("week", pd.NA),
            "away_team": away,
            "home_team": home,
            "msf_game_id": g.get("msf_game_id", pd.NA),
            "elo_home_pre": round(elo_home_pre, 6),
            "elo_away_pre": round(elo_away_pre, 6),
            "elo_diff_pre": round(elo_diff_pre, 6),
            "elo_exp_home": round(exp_home, 6),
        })

    out = pd.DataFrame(rows).sort_values(["date","home_team","away_team"])
    outp = pathlib.Path(args.out); outp.parent.mkdir(parents=True, exist_ok=True); out.to_csv(outp, index=False)

    dmin, dmax = out["date"].min(), out["date"].max()
    print(f"[JOIN] games source: {games_src}")
    print(f"[JOIN] wrote {outp} rows={len(out)} | date range {dmin} → {dmax}")
    print(out.head(8).to_string(index=False))

    mask = (out["home_team"].eq("GB")) & (out["away_team"].eq("WAS"))
    if mask.any():
        s = out[mask].iloc[0]
        print(f"\n[CHECK] {s['away_team']} @ {s['home_team']} on {s['date']}: "
              f"elo_home_pre={s['elo_home_pre']:.1f}, elo_away_pre={s['elo_away_pre']:.1f}, "
              f"elo_diff_pre={s['elo_diff_pre']:.1f}, elo_exp_home={s['elo_exp_home']:.3f}")

if __name__ == "__main__":
    main()