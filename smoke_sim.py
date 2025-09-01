#!/usr/bin/env python3
"""
smoke_sim.py — offline smoke test for run_monte_carlo.py
Creates tiny synthetic odds/ratings/depth/injuries and verifies:
  - run_simulation returns (preds_df, cards_df)
  - required columns are present
  - artifacts get written to ./out_smoke_*.csv and ./smoke_manifest.json

Run:
  python smoke_sim.py
"""

from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

from run_monte_carlo import run_simulation
from manifest_writer import write_manifest

ROOT = Path(__file__).resolve().parent
OUT_PREDS = ROOT / "out_smoke_predictions.csv"
OUT_CARDS = ROOT / "out_smoke_gamecards.csv"
OUT_MANIFEST = ROOT / "smoke_manifest.json"

REQ_PREDS = ["home_team","away_team","vegas_line","vegas_total","sigma",
             "win_prob_home","cover_prob_home","ou_prob_over","kickoff_utc","neutral_site"]

REQ_CARDS = ["game_id","home_team","away_team","kickoff_utc","neutral_site",
             "rating_home","rating_away","hfa_home","inj_adj_home","inj_adj_away",
             "vegas_line","vegas_total","modeled_spread_home","modeled_total",
             "win_prob_home","cover_prob_home","ou_prob_over","notes"]

def _assert_cols(df: pd.DataFrame, need: list[str], name: str) -> None:
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise AssertionError(f"{name} missing columns: {missing}")

def main() -> None:
    # --- 1) Synthesize tiny slate (two games) ---
    odds_df = pd.DataFrame([
        {
            "home_team": "DAL", "away_team": "PHI",
            "spread_home": -2.5, "spread_away": 2.5,
            "total": 47.5,
            "kickoff_utc": "2025-09-07T17:00:00Z",
            "neutral_site": False
        },
        {
            "home_team": "KC", "away_team": "BUF",
            "spread_home": -3.0, "spread_away": 3.0,
            "total": 51.0,
            "kickoff_utc": "2025-09-07T20:25:00Z",
            "neutral_site": False
        },
    ])

    ratings_df = pd.DataFrame([
        {"team_code":"DAL","rating": 4.2,"uncertainty":0.7,"hfa": 1.2,"last_updated_utc":"2025-08-31T00:00:00Z","week_ended":"2025-08-31"},
        {"team_code":"PHI","rating": 3.8,"uncertainty":0.8,"hfa": 1.1,"last_updated_utc":"2025-08-31T00:00:00Z","week_ended":"2025-08-31"},
        {"team_code":"KC", "rating": 6.1,"uncertainty":0.6,"hfa": 1.0,"last_updated_utc":"2025-08-31T00:00:00Z","week_ended":"2025-08-31"},
        {"team_code":"BUF","rating": 5.5,"uncertainty":0.6,"hfa": 1.0,"last_updated_utc":"2025-08-31T00:00:00Z","week_ended":"2025-08-31"},
    ])

    depth_df = pd.DataFrame([
        # DAL
        {"team_code":"DAL","position":"QB","player":"Dak Prescott","value": 8.0},
        {"team_code":"DAL","position":"WR","player":"CeeDee Lamb","value": 7.2},
        {"team_code":"DAL","position":"LT","player":"Tyron Smith","value": 5.0},
        # PHI
        {"team_code":"PHI","position":"QB","player":"Jalen Hurts","value": 8.2},
        {"team_code":"PHI","position":"WR","player":"A.J. Brown","value": 7.5},
        {"team_code":"PHI","position":"CB","player":"Darius Slay","value": 5.1},
        # KC
        {"team_code":"KC","position":"QB","player":"Patrick Mahomes","value": 9.5},
        {"team_code":"KC","position":"TE","player":"Travis Kelce","value": 8.4},
        {"team_code":"KC","position":"EDGE","player":"George Karlaftis","value": 4.0},
        # BUF
        {"team_code":"BUF","position":"QB","player":"Josh Allen","value": 9.0},
        {"team_code":"BUF","position":"WR","player":"Stefon Diggs","value": 7.8},
        {"team_code":"BUF","position":"LT","player":"Dion Dawkins","value": 4.6},
    ])

    injuries = [
        {"team_code":"DAL","player":"Tyron Smith","status":"Questionable","position":"LT"},
        {"team_code":"PHI","player":"A.J. Brown","status":"Out","position":"WR"},
        {"team_code":"KC", "player":"Travis Kelce","status":"Probable","position":"TE"},
        {"team_code":"BUF","player":"Stefon Diggs","status":"Doubtful","position":"WR"},
    ]

    # --- 2) Run simulation ---
    preds_df, cards_df = run_simulation(odds_df, ratings_df, depth_df, injuries)

    # --- 3) Verify schemas & write outputs ---
    _assert_cols(preds_df, REQ_PREDS, "preds_df")
    _assert_cols(cards_df, REQ_CARDS, "cards_df")

    preds_df.to_csv(OUT_PREDS, index=False)
    cards_df.to_csv(OUT_CARDS, index=False)

    # --- 4) Minimal manifest for the smoke run ---
    run_meta = {
        "kind": "smoke",
        "timestamp_utc": pd.Timestamp.utcnow().isoformat(),
        "python": "n/a",
        "host": "local",
        "config_used": "synthetic"
    }
    inputs = {
        "ratings_source": "embedded synthetic DataFrame",
        "depth_charts_source": "embedded synthetic DataFrame",
        "odds_source": "embedded synthetic DataFrame",
        "injury_source": "embedded synthetic list"
    }
    outputs = {
        "predictions_csv": str(OUT_PREDS.name),
        "gamecards_csv": str(OUT_CARDS.name)
    }
    audits = {
        "note": "Offline smoke test; no roster audit performed."
    }

    write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits)

    print("\n✅ Smoke test complete.")
    print(f"Saved: {OUT_PREDS.name}, {OUT_CARDS.name}")
    print(f"Saved: {OUT_MANIFEST.name}")
    print("\n(preds_df.head())")
    print(preds_df.head().to_string(index=False))
    print("\n(cards_df.head())")
    print(cards_df.head().to_string(index=False))

if __name__ == "__main__":
    main()