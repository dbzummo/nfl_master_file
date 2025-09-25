#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

WEEK = Path("out/msf/week_games.csv")
INJ  = Path("out/injuries_week.csv")
ADJ  = Path("out/injury_adjustments.csv")

def load_week_games() -> pd.DataFrame:
    df = pd.read_csv(WEEK)
    # expected: home_abbr, away_abbr, game_date
    need = {"home_abbr","away_abbr","game_date"}
    miss = need - set(df.columns)
    if miss:
        raise SystemExit(f"[FATAL] week_games missing columns: {sorted(miss)}")
    return df[["home_abbr","away_abbr","game_date"]].copy()

def load_inj() -> pd.DataFrame:
    if not INJ.exists():
        print("[WARN] injuries_week.csv missing; proceeding with empty injuries.")
        return pd.DataFrame(columns=["team_abbr","position","status_norm"])
    df = pd.read_csv(INJ)
    for col in ("team_abbr","position","status_norm"):
        if col not in df.columns:
            df[col] = ""
    # keep only rows with a team tag
    df["team_abbr"] = df["team_abbr"].astype(str).str.strip().str.upper()
    df = df[df["team_abbr"].ne("")]
    return df

# ---- weights (can be tuned later) ----
def _pos_family(p):
    p = str(p or "").upper()
    if p in ("QB",): return "QB"
    if p in ("LT","RT","LG","RG","C","OL","T","G"): return "OL"
    if p in ("WR","TE","FB","RB","HB"): return "SKILL"
    if p in ("EDGE","DE","DT","DL","LB"): return "FRONT7"
    if p in ("CB","S","FS","SS","DB"): return "COVER"
    if p in ("K","P","LS"): return "ST"
    return "OTHER"

STATUS_W = {
    "OUT": 1.00,
    "DOUBTFUL": 0.65,
    "QUESTIONABLE": 0.35,
    "PROBABLE": 0.15,
    "ACTIVE": 0.00,
}

POS_W = {
    "QB": 38.0,
    "OL": 6.0,
    "SKILL": 4.0,
    "FRONT7": 5.0,
    "COVER": 6.0,
    "ST": 1.0,
    "OTHER": 2.0,
}

def compute_deltas(week: pd.DataFrame, inj: pd.DataFrame) -> pd.DataFrame:
    team_imp = inj.copy()
    # derive player-level impact
    fam = team_imp["position"].map(_pos_family)
    sw  = team_imp["status_norm"].astype(str).str.upper().map(STATUS_W).fillna(0.0)
    pw  = fam.map(POS_W).fillna(0.0)
    team_imp["impact"] = (sw * pw).astype(float)

    # aggregate to team totals (positive number = total penalty to team strength)
    team_tot = team_imp.groupby("team_abbr", as_index=False)["impact"].sum()

    # join to week and convert to Elo deltas (negative = team dinged)
    out = week.merge(
        team_tot.rename(columns={"team_abbr":"home_abbr","impact":"elo_delta_home"}),
        on="home_abbr", how="left"
    ).merge(
        team_tot.rename(columns={"team_abbr":"away_abbr","impact":"elo_delta_away"}),
        on="away_abbr", how="left"
    )

    out["elo_delta_home"] = -out["elo_delta_home"].fillna(0.0)
    out["elo_delta_away"] = -out["elo_delta_away"].fillna(0.0)

    # canonical slug
    out["slug"] = (
        out["game_date"].astype(str).str.replace('"','',regex=False)
        + "-" + out["away_abbr"] + "-" + out["home_abbr"]
    )

    return out[["home_abbr","away_abbr","elo_delta_home","elo_delta_away","game_date","slug"]]

def main():
    week = load_week_games()
    inj  = load_inj()
    merged = compute_deltas(week, inj)
    ADJ.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(ADJ, index=False)

    nonzero = int(((merged["elo_delta_home"]!=0) | (merged["elo_delta_away"]!=0)).sum())
    print(f"[OK] wrote {ADJ} rows={len(merged)}; games with nonzero deltas={nonzero}")

if __name__ == "__main__":
    main()
