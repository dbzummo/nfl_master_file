#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

OUT_DIR = Path("out")
REPORTS_DIR = Path("reports")
BOARD_CSV = OUT_DIR / "model_board.csv"
BOARD_HTML = REPORTS_DIR / "board_week.html"

def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        raise SystemExit(f"[FATAL] Missing required file: {path}")

def build_pair_key(df, home_col="home_abbr", away_col="away_abbr"):
    hc = home_col if home_col in df.columns else None
    ac = away_col if away_col in df.columns else None
    if hc and ac:
        df[home_col] = df[home_col].astype(str).str.strip()
        df[away_col] = df[away_col].astype(str).str.strip()
        return df.apply(lambda r: "|".join(sorted([r[home_col], r[away_col]])), axis=1)
    return pd.Series([None]*len(df))

def canonical_market_prob(df: pd.DataFrame) -> pd.Series:
    """Return best-available market probability, regardless of suffix/raw."""
    for c in [
        "market_p_home",
        "market_p_home_y",
        "market_p_home_x",
        "market_p_home_raw_y",
        "market_p_home_raw_x",
        "market_p_home_raw",
    ]:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any():
                return s
    return pd.Series([pd.NA]*len(df))

def main():
    market = read_csv(OUT_DIR / "week_with_market.csv")
    preds  = read_csv(OUT_DIR / "week_predictions.csv")

    # Ensure both have a pair_key
    if "pair_key" not in market.columns or market["pair_key"].isna().all():
        market["pair_key"] = build_pair_key(market)

    if "pair_key" not in preds.columns or preds["pair_key"].isna().all():
        preds["pair_key"] = build_pair_key(preds)

    # Take ONLY the columns we need from each side
    market_keep = [c for c in [
        "msf_game_id","game_start","home_abbr","away_abbr",
        "venue","status","pair_key",
        "book_count_mkt","ml_home_mkt","ml_away_mkt",
        "market_p_home","market_p_home_raw","commence_time_mkt"
    ] if c in market.columns]
    market_view = market[market_keep].copy()

    # Minimal preds to avoid column collisions
    preds_view = preds[[c for c in ["pair_key","p_home"] if c in preds.columns]].copy()
    if "p_home" not in preds_view.columns:
        preds_view["p_home"] = pd.NA

    # Merge on pair_key only -> no overlapping columns
    board = pd.merge(market_view, preds_view, on="pair_key", how="left")

    # Canonicalize market prob after merge (handles suffix/raw)
    board["market_p_home"] = canonical_market_prob(board)

    # Compute edge where both sides exist
    board["edge"] = (
        pd.to_numeric(board.get("p_home"), errors="coerce")
        - pd.to_numeric(board.get("market_p_home"), errors="coerce")
    )

    # Final column order (only those that exist)
    final_cols = [c for c in [
        "msf_game_id","game_start","home_abbr","away_abbr","venue","status",
        "book_count_mkt","ml_home_mkt","ml_away_mkt",
        "market_p_home","p_home","edge",
        "commence_time_mkt","pair_key"
    ] if c in board.columns]
    board = board[final_cols].copy()

    # Output
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    board.to_csv(BOARD_CSV, index=False)
    print(f"[OK] Wrote {BOARD_CSV} (rows={len(board)})")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    BOARD_HTML.write_text(board.to_html(index=False, border=0), encoding="utf-8")
    print(f"[OK] Rendered HTML board -> {BOARD_HTML}")

if __name__ == "__main__":
    main()
