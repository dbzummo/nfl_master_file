#!/usr/bin/env python3
import argparse, os, sys
import pandas as pd

MSF_BOX = "out/msf_details/boxscores_week.csv"
OUT_DIR = "out/ingest"
WEEK_GAMES = os.path.join(OUT_DIR, "week_games.csv")
WEEK_FEATS = os.path.join(OUT_DIR, "week_features.csv")

def die(msg, code=1):
    print(f"[ingest][ERR] {msg}", file=sys.stderr)
    sys.exit(code)

def parse_args():
    p = argparse.ArgumentParser(description="Ingest week results → canonical game list + minimal features")
    p.add_argument("--season", required=True, help="e.g. 2025-regular")
    p.add_argument("--week", type=int, required=True, help="NFL week number (int)")
    return p.parse_args()

def load_msf():
    if not os.path.exists(MSF_BOX):
        die(f"missing {MSF_BOX}. Run fetch_week_details.py first.")
    df = pd.read_csv(MSF_BOX)
    need = {"date","msf_game_id","week","status","team","opponent","home_away"}
    missing = need - set(df.columns)
    if missing:
        die(f"{MSF_BOX} missing required columns: {sorted(missing)}")
    # Normalize date to YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"].astype(str).str[:10], errors="coerce")
    return df

def build_week_games(df_week):
    """
    One row per game using the HOME perspective.
    Columns expected downstream:
      date, msf_game_id, away_team, home_team, week, status,
      away_full, home_full, final_away, final_home
    """
    # keep only HOME rows -> each game appears once
    home = df_week[df_week["home_away"].astype(str).str.upper() == "HOME"].copy()
    if home.empty:
        return home.assign(
            away_team=pd.Series(dtype=str),
            home_team=pd.Series(dtype=str),
            away_full=pd.Series(dtype=str),
            home_full=pd.Series(dtype=str),
            final_away=pd.Series(dtype=float),
            final_home=pd.Series(dtype=float),
        )

    # team/opponent may be abbreviations already; standardize to uppercase strings
    for c in ["team","opponent","status"]:
        home[c] = home[c].astype(str)

    out = pd.DataFrame({
        "date": home["date"].dt.strftime("%Y-%m-%d"),
        "msf_game_id": home["msf_game_id"],
        "away_team": home["opponent"].str.upper(),
        "home_team": home["team"].str.upper(),
        "week": home["week"].astype(int),
        "status": home["status"],
        # keep full-name echoes (if upstream provided long names later we can fill them in)
        "away_full": home["opponent"],
        "home_full": home["team"],
        # not available at this stage → placeholders
        "final_away": pd.NA,
        "final_home": pd.NA,
    })
    # deterministic order
    return out.sort_values(["date","home_team","away_team"]).reset_index(drop=True)

def build_min_features(games):
    # Minimal placeholder features so downstream joins don’t break
    if games.empty:
        return pd.DataFrame(columns=["date","team","feature","value","source"])
    rows = []
    for _, r in games.iterrows():
        rows.append({"date": r["date"], "team": r["home_team"], "feature": "placeholder", "value": 1, "source": "ingest"})
        rows.append({"date": r["date"], "team": r["away_team"], "feature": "placeholder", "value": 1, "source": "ingest"})
    return pd.DataFrame(rows, columns=["date","team","feature","value","source"])

def main():
    args = parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)

    msf = load_msf()
    # Filter to requested WEEK
    dfw = msf[msf["week"].astype(int) == int(args.week)].copy()
    if dfw.empty:
        die(f"no rows in {MSF_BOX} for week={args.week}. (season={args.season})")

    games = build_week_games(dfw)
    if games.empty:
        die(f"no HOME rows present for week={args.week} in {MSF_BOX}")

    games.to_csv(WEEK_GAMES, index=False)
    print(f"[ingest][ok] wrote {WEEK_GAMES} rows={len(games)}")

    feats = build_min_features(games)
    feats.to_csv(WEEK_FEATS, index=False)
    print(f"[ingest][ok] wrote {WEEK_FEATS} rows={len(feats)}")

if __name__ == "__main__":
    main()
