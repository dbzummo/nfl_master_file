#!/usr/bin/env python3
# scripts/compute_injury_adjustments.py
#
# Read bulk MSF injuries CSV → compute team-level impacts (with OL emphasis) → write canonical 32-team output.
# - Accepts team/team_abbr and various probability/status fields.
# - Canonicalizes team codes and drops invalids (fixes 'LA' and 'NAN').
# - Ensures exactly the 32 current NFL teams in output (fills missing with zeros).
#
# Inputs:
#   out/injuries/injuries_feed.csv
#   out/week_predictions.csv  (used to learn which team codes appear; also for safety fill)
#
# Output:
#   out/injuries/injury_impacts.csv  (columns: team,total_players_flagged,total_impact,ol_players_flagged,ol_impact)

from pathlib import Path
from typing import List, Dict
import pandas as pd

INJURIES_CSV = Path("out/injuries/injuries_feed.csv")
IMPACTS_CSV  = Path("out/injuries/injury_impacts.csv")
PRED_PATH    = Path("out/week_predictions.csv")

# Canonical 32 NFL team codes we expect
NFL32 = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB","HOU","IND",
    "JAX","KC","LAC","LAR","LV","MIA","MIN","NE","NO","NYG","NYJ","PHI","PIT","SEA",
    "SF","TB","TEN","WAS"
]

# Map legacy/odd codes to canonical
TEAM_CANON = {
    "LA": "LAR",   # old/ambiguous LA -> Rams (Chargers are LAC)
    "WSH": "WAS",
    "OAK": "LV",
    "STL": "LAR",
    "SD": "LAC",
    "": None,
    "NAN": None,
    "NONE": None
}

# Positions considered OL
OL_POS = {"C","G","T","LT","LG","RG","RT"}

# Generic weights by playing probability/status
GENERIC_WEIGHT = {
    "OUT": 1.0, "DOUBTFUL": 0.6, "QUESTIONABLE": 0.2, "PROBABLE": 0.1,
    "ACTIVE": 0.0, "DAY-TO-DAY": 0.1,
    "IR": 1.0, "INJURY_RESERVE": 1.0, "PUP": 0.8, "NFI": 0.8, "SUSPENDED": 0.8,
    None: 0.0, "": 0.0
}
# Heavier weights for OL (with team cap)
OL_WEIGHT = {
    "OUT": 0.8, "DOUBTFUL": 0.4, "QUESTIONABLE": 0.1, "PROBABLE": 0.05,
    "ACTIVE": 0.0, "IR": 0.8, "PUP": 0.6, "NFI": 0.6, "SUSPENDED": 0.6,
    None: 0.0, "": 0.0
}
OL_TEAM_CAP = 2.0

def _canonicalize_probability(val) -> str:
    if pd.isna(val): return ""
    s = str(val).strip().upper()
    if s in {"Q","QUEST"}: return "QUESTIONABLE"
    if s in {"D","DOUBT"}: return "DOUBTFUL"
    if s in {"P","PROB"}:  return "PROBABLE"
    if s in {"OUT (IR)","ON IR","INJURED_RESERVE"}: return "IR"
    return s

def _canonicalize_team(code: str) -> str | None:
    if pd.isna(code): return None
    s = str(code).strip().upper()
    s = TEAM_CANON.get(s, s)
    if s is None: return None
    # drop anything not in NFL32 after mapping
    return s if s in NFL32 else None

def _load_injuries_df() -> pd.DataFrame:
    if not INJURIES_CSV.exists():
        return pd.DataFrame(columns=["team","position","prob_norm"])
    df = pd.read_csv(INJURIES_CSV)

    # team / team_abbr handling
    team_col = "team" if "team" in df.columns else ("team_abbr" if "team_abbr" in df.columns else None)
    if team_col is None:
        df["team"] = None
    else:
        df["team"] = df[team_col]

    # position handling
    if "position" not in df.columns:
        df["position"] = ""

    # probability/status handling
    prob_col = next((c for c in ["playing_probability","probability","status","injury_status"] if c in df.columns), None)
    if prob_col is None:
        df["prob_norm"] = ""
    else:
        df["prob_norm"] = df[prob_col].apply(_canonicalize_probability)

    # normalize strings
    df["position"] = df["position"].astype(str).str.upper().str.strip()

    # canonicalize teams and drop invalids
    df["team"] = df["team"].apply(_canonicalize_team)
    df = df[~df["team"].isna()].copy()

    return df[["team","position","prob_norm"]]

def _teams_from_predictions() -> List[str]:
    if not PRED_PATH.exists():
        return NFL32[:]  # fallback
    p = pd.read_csv(PRED_PATH)
    teams = set()
    for col in ("home_team","away_team"):
        if col in p.columns:
            teams |= set(p[col].astype(str).str.upper().str.strip().tolist())
    # keep only valid NFL32
    teams = {t for t in teams if t in NFL32}
    # if somehow empty, return full NFL32
    return sorted(teams) if teams else NFL32[:]

def _compute_impacts(df: pd.DataFrame) -> pd.DataFrame:
    # if empty, return zero rows for known teams
    teams = _teams_from_predictions()
    if df.empty:
        return pd.DataFrame({
            "team": teams,
            "total_players_flagged": [0]*len(teams),
            "total_impact": [0.0]*len(teams),
            "ol_players_flagged": [0]*len(teams),
            "ol_impact": [0.0]*len(teams),
        })

    # weights
    df["generic_weight"] = df["prob_norm"].map(GENERIC_WEIGHT).fillna(0.0)
    df["is_ol"] = df["position"].isin(OL_POS)
    df["ol_weight"] = df["prob_norm"].map(OL_WEIGHT).fillna(0.0)
    df["ol_weight_eff"] = df["ol_weight"] * df["is_ol"].astype(int)

    # aggregate
    g = df.groupby("team", dropna=False)
    out = g.agg(
        total_players_flagged=("generic_weight", lambda s: int((s > 0).sum())),
        total_impact=("generic_weight", "sum"),
        ol_players_flagged=("ol_weight_eff", lambda s: int((s > 0).sum())),
        ol_impact=("ol_weight_eff", "sum"),
    ).reset_index()

    # cap OL impact
    out["ol_impact"] = out["ol_impact"].clip(0, OL_TEAM_CAP)

    # ensure exactly the NFL32 set (fill missing with zeros, drop extras just in case)
    out = out[out["team"].isin(NFL32)].copy()
    present = set(out["team"])
    missing = [t for t in teams if t not in present]
    if missing:
        fill = pd.DataFrame({
            "team": missing,
            "total_players_flagged": [0]*len(missing),
            "total_impact": [0.0]*len(missing),
            "ol_players_flagged": [0]*len(missing),
            "ol_impact": [0.0]*len(missing),
        })
        out = pd.concat([out, fill], ignore_index=True)

    # stable sort
    return out.sort_values("team").reset_index(drop=True)

def main() -> None:
    df = _load_injuries_df()

    # Optional quick QA (GB OL)
    try:
        gb_ol = df[(df["team"]=="GB") & (df["position"].isin(OL_POS))]
        print("[QA] GB OL injuries:")
        if gb_ol.empty:
            print("  (none)")
        else:
            for _, r in gb_ol.iterrows():
                print(f"  {r['position']}: {r['prob_norm']}")
    except Exception:
        print("[QA] GB OL injuries:\n  (none)")

    impacts = _compute_impacts(df)
    IMPACTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    impacts.to_csv(IMPACTS_CSV, index=False)
    print(f"[OK] wrote injury impacts for {impacts.shape[0]} teams → {IMPACTS_CSV}")

if __name__ == "__main__":
    main()
