#!/usr/bin/env python3
import warnings
from pathlib import Path
import pandas as pd
import numpy as np
import glob

ROOT = Path(__file__).resolve().parents[1]

HIST_GLOB = str(ROOT / "history" / "season_*_from_site.csv")
RATINGS_BY_DATE_CSV = ROOT / "out" / "elo_ratings_by_date.csv"

# --------- Elo utilities (self-contained) ----------
def elo_prob(r_home: float, r_away: float) -> float:
    """Standard Elo to win prob (home vs away)."""
    return 1.0 / (1.0 + 10.0 ** ((r_away - r_home) / 400.0))

# --------- Ratings index helpers ----------
def load_ratings_by_date() -> pd.DataFrame:
    """
    Expect a CSV with columns: date, team, elo
    Ensure date is datetime and table is stably sorted by ['team','date'].
    """
    df = pd.read_csv(RATINGS_BY_DATE_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "team", "elo"]).copy()
    df["team"] = df["team"].astype(str).str.upper().str.strip()
    # stable sort for merge_asof(by=...)
    df = df.sort_values(["team", "date"], kind="mergesort").reset_index(drop=True)
    # rename to internal keys
    return df.rename(columns={"team": "_team", "elo": "_elo"})

def safe_asof_lookup(left_df: pd.DataFrame, ratings_idx: pd.DataFrame, team_col: str) -> pd.Series:
    """
    Robust per-team asof lookup that guarantees sort order for both sides.

    left_df: columns ['date', team_col]
    ratings_idx: columns ['_team','date','_elo'] pre-sorted by ['_team','date']
    """
    import pandas as _pd

    if "date" not in left_df.columns or team_col not in left_df.columns:
        raise ValueError("safe_asof_lookup requires ['date', team_col] in left_df")

    # Normalize left
    left = left_df.copy()
    left["date"] = _pd.to_datetime(left["date"], errors="coerce")
    left["_team"] = left[team_col].astype(str).str.upper().str.strip()
    left["_ix_orig"] = range(len(left))

    # Container for pieces
    out_parts = []

    # Iterate teams to avoid grouped-asof sorting traps
    for t, sub in left.groupby("_team", sort=False):
        sub = sub.sort_values("date", kind="mergesort").copy()

        rsub = ratings_idx[ratings_idx["_team"] == t]
        if not len(rsub):
            # No ratings for team — emit NaNs to be filled by caller
            tmp = sub[["_ix_orig"]].copy()
            tmp["_elo"] = _pd.NA
            out_parts.append(tmp)
            continue

        rsub = rsub.sort_values("date", kind="mergesort")

        merged = _pd.merge_asof(
            sub[["date","_ix_orig"]],
            rsub[["date","_elo"]],
            on="date",
            direction="backward",
            allow_exact_matches=True,
        )
        out_parts.append(merged[["_ix_orig","_elo"]])

    out = _pd.concat(out_parts, ignore_index=True)
    out = out.sort_values("_ix_orig").reset_index(drop=True)
    return out["_elo"]

def enrich_file(path_csv: str, ratings_idx: pd.DataFrame) -> pd.DataFrame:
    """
    Read site season file with columns:
      date, home_team, away_team, home_score, away_score, neutral_site, spread_home, total
    Output: DataFrame with columns: date, home_team, away_team, p, y
    """
    df = pd.read_csv(path_csv)
    if df.empty:
        return df

    # normalize date & teams
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    for col in ["home_team", "away_team"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
        else:
            raise ValueError(f"Missing required column '{col}' in {path_csv}")

    # Stable as-of Elo lookup for both sides
    try:
        elo_home = safe_asof_lookup(df[["date", "home_team"]], ratings_idx, "home_team")
        elo_away = safe_asof_lookup(df[["date", "away_team"]], ratings_idx, "away_team")
    except Exception as e:
        raise RuntimeError(f"{path_csv}: Elo lookup failed: {e}")

    # Compute probabilities
    p = []
    # fallback Elo if missing
    elo_home = elo_home.fillna(1500.0)
    elo_away = elo_away.fillna(1500.0)
    for rh, ra in zip(elo_home.values, elo_away.values):
        p.append(elo_prob(rh, ra))
    df["p"] = p

    # Compute labels
    y = None
    if {"home_score", "away_score"}.issubset(df.columns):
        hs = pd.to_numeric(df["home_score"], errors="coerce")
        aw = pd.to_numeric(df["away_score"], errors="coerce")
        mask = hs.notna() & aw.notna()
        if mask.any():
            y = (hs[mask] > aw[mask]).astype(int).reindex(df.index, fill_value=np.nan)
    if y is None or y.isna().all():
        # try common outcome field fallbacks
        for col in ["home_win", "home_result", "result", "y", "home_won"]:
            if col in df.columns:
                ser = pd.to_numeric(df[col], errors="coerce")
                if ser.notna().any():
                    y = ser.astype(int)
                    break
    if y is None:
        raise RuntimeError(f"{path_csv}: No usable label columns (scores or binary outcome).")

    # sanity and trim
    y = y.fillna(0).astype(int)
    out = df.assign(y=y)[["date", "home_team", "away_team", "p", "y"]]
    return out

def main():
    # Ratings by date must exist & be sorted
    if not RATINGS_BY_DATE_CSV.exists():
        raise SystemExit(f"[ERR] Missing {RATINGS_BY_DATE_CSV}; create it first (your Elo-by-date builder).")
    ratings_idx = load_ratings_by_date()

    files = sorted(glob.glob(HIST_GLOB))
    if not files:
        print(f"[WARN] No history files matched {HIST_GLOB}")
        return

    out_all = []
    for f in files:
        try:
            df_en = enrich_file(f, ratings_idx)
        except pd.errors.EmptyDataError:
            print(f"[WARN] {f}: empty file; skipping.")
            continue
        except Exception as e:
            print(f"[WARN] {f}: could not enrich: {e}; skipping.")
            continue

        out_path = f.replace("season_", "enriched_")
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df_en.to_csv(out_path, index=False)
        print(f"[OK] wrote {out_path} rows= {len(df_en)}")
        out_all.append(df_en)

    if out_all:
        df_all = pd.concat(out_all, ignore_index=True)
        (ROOT / "history").mkdir(parents=True, exist_ok=True)
        df_all.to_csv(ROOT / "history" / "enriched_all.csv", index=False)
        print(f"[OK] wrote combined history -> history/enriched_all.csv rows= {len(df_all)}")

if __name__ == "__main__":
    main()
