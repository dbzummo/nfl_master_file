#!/usr/bin/env python3
import sys, json, pathlib
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Inputs
BASELINE_CSV = ROOT / "out" / "week_with_elo.csv"

# Candidate odds files (priority order)
def odds_candidates():
    week = pathlib.os.getenv("WEEK")
    season = pathlib.os.getenv("SEASON")
    cands = [
        ROOT / "out" / "odds_week_norm.csv",  # preferred if user created/normalized
    ]
    if season and week:
        cands.append(ROOT / "out" / "odds" / f"{season}_w{week}" / "odds_combined.csv")
    cands.append(ROOT / "out" / "odds_week.csv")  # legacy
    return cands

EXPECTED_COLS = [
    "home_abbr","away_abbr","commence_time","book_count","ml_home","ml_away","market_p_home"
]

def load_lookup() -> dict:
    """Optional team name -> abbr lookup."""
    lk_path = ROOT / "teams_lookup.json"
    if lk_path.exists():
        try:
            return json.loads(lk_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def normalize_odds(df: pd.DataFrame, lk: dict) -> pd.DataFrame:
    """Ensure odds has expected columns and normalized team abbreviations, plus pair_key."""
    if df is None or df.empty:
        return pd.DataFrame(columns=EXPECTED_COLS + ["pair_key"])

    # keep only expected cols if present
    keep = [c for c in EXPECTED_COLS if c in df.columns]
    df = df[keep].copy()

    # fill missing expected cols
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # team normalization
    def to_abbr(x):
        if pd.isna(x):
            return x
        s = str(x).strip()
        return lk.get(s, s)

    df["home_abbr"] = df["home_abbr"].map(to_abbr).astype(str).str.strip()
    df["away_abbr"] = df["away_abbr"].map(to_abbr).astype(str).str.strip()

    # pair key (order-insensitive)
    df["pair_key"] = df[["home_abbr","away_abbr"]].apply(
        lambda r: "|".join(sorted([str(r[0]), str(r[1])])), axis=1
    )

    # clean numeric-ish fields
    for c in ["book_count","ml_home","ml_away","market_p_home"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def read_csv_if_usable(path: pathlib.Path) -> pd.DataFrame | None:
    """Return DataFrame if CSV exists and has at least one data row (not just header)."""
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return None
    if df.shape[0] == 0:
        return None
    return df

def main():
    # --- load baseline (week_with_elo) ---
    if not BASELINE_CSV.exists():
        print(f"[FATAL] Baseline not found: {BASELINE_CSV}", file=sys.stderr)
        sys.exit(1)
    base = pd.read_csv(BASELINE_CSV, dtype=str)
    # columns we expect to retain from baseline
    base_keep = [c for c in ["msf_game_id","game_start","home_abbr","away_abbr","venue","status"] if c in base.columns]
    base = base[base_keep].copy()

    # baseline pair_key
    if "home_abbr" not in base.columns or "away_abbr" not in base.columns:
        print("[FATAL] Baseline missing team columns.", file=sys.stderr)
        sys.exit(1)
    base["pair_key"] = base[["home_abbr","away_abbr"]].apply(
        lambda r: "|".join(sorted([str(r[0]), str(r[1])])), axis=1
    )

    # --- choose odds file ---
    cands = [str(p) for p in odds_candidates()]
    print(f"[INFO] Odds candidates: {cands}")

    chosen_path = None
    chosen_df = None
    for p in odds_candidates():
        df = read_csv_if_usable(p)
        if df is not None:
            chosen_path = p
            chosen_df = df
            break

    if chosen_path is None:
        # Write pass-through baseline with expected market columns empty
        print("[WARN] No usable odds file found; passing through baseline.")
        out = base.copy()
        for c in [
            "home_abbr_mkt","away_abbr_mkt","commence_time_mkt","book_count_mkt",
            "ml_home_mkt","ml_away_mkt","market_p_home_raw","market_p_home"
        ]:
            out[c] = pd.NA
        OUT = ROOT / "out" / "week_with_market.csv"
        out.to_csv(OUT, index=False)
        print(f"[OK] Wrote {OUT} (rows={len(out)}) | market_p_home present: False")
        return

    print(f"[INFO] Using odds file: {chosen_path}")

    # --- normalize odds ---
    lk = load_lookup()
    odds_raw = chosen_df.copy()
    odds = normalize_odds(odds_raw, lk)

    # Inherit "raw" market prob before any further manipulation (if needed later)
    if "market_p_home" in odds.columns:
        odds["market_p_home_raw"] = odds["market_p_home"]

    # Market columns with _mkt suffix
    map_cols = {
        "home_abbr": "home_abbr_mkt",
        "away_abbr": "away_abbr_mkt",
        "commence_time": "commence_time_mkt",
        "book_count": "book_count_mkt",
        "ml_home": "ml_home_mkt",
        "ml_away": "ml_away_mkt",
        "market_p_home_raw": "market_p_home_raw",
        "market_p_home": "market_p_home",
        "pair_key": "pair_key",
    }
    odds_renamed = odds[[c for c in map_cols if c in odds.columns]].rename(columns=map_cols)

    # --- join ---
    merged = base.merge(odds_renamed, on="pair_key", how="left")

    # Diagnostics
    matched = merged["market_p_home"].notna().sum() if "market_p_home" in merged.columns else 0
    total = len(merged)
    print(f"[DEBUG] Join candidates: base={len(base)} odds={len(odds)} matched={matched}")

    # --- write ---
    OUT = ROOT / "out" / "week_with_market.csv"
    merged.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} (rows={len(merged)}) | market_p_home present: {matched>0}")

if __name__ == "__main__":
    main()
