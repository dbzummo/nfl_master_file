#!/usr/bin/env python3
"""
run_predictions.py — strict, fail-fast weekly run orchestrator (v5)
- Loads config (master_model_file_v5.0.json)
- Loads env (.env) and asserts API keys
- Runs roster audit (must pass without BLOCK conflicts)
- Loads ratings + merges stadium HFA (never zeroed)
- Pulls odds + injuries (must be non-empty)
- Calls Monte Carlo and writes artifacts (CSV + provenance JSON) via manifest_writer
"""

from __future__ import annotations
import os, sys, json, time, socket, platform, hashlib
from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd

# Local modules
from gates import die, warn, require_env, require_columns, enforce_roster_audit
from hfa_loader import merge_hfa
from manifest_writer import write_manifest
from run_audit import run_roster_audit
from fetch_odds import get_consensus_nfl_odds
from fetch_injuries import fetch_injured_players
from run_monte_carlo import run_simulation
from validators import validate_odds, validate_ratings, validate_depth, validate_injuries
from aliases import apply_aliases
from post_run_calibration import write_calibration
from injuries_fallbacks import derive_injuries_from_rosters

ROOT = Path(__file__).resolve().parent

CONFIG_PATH = ROOT / "master_model_file_v5.0.json"
RATINGS_PATH = ROOT / "kalman_state_preseason.csv"
HFA_PATH     = ROOT / "stadium_hfa_advanced.csv"
DEPTH_PATH   = ROOT / "team_depth_charts_with_values.csv"

OUT_PREDS = ROOT / "out_week1_predictions.csv"
OUT_CARDS = ROOT / "out_week1_gamecards.csv"
OUT_MANIFEST = ROOT / "manifest.json"

REQUIRED_ENV = ["THE_ODDS_API_KEY", "SPORTSDATAIO_API_KEY"]

def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        die(f"Config file missing: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"Failed to parse JSON {path}: {e}")

def _load_depth_charts(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"Depth chart file missing: {path}")
    df = pd.read_csv(path)
    # Minimal schema; your file can have more
    need = ["team_code", "position", "player", "value"]
    require_columns(df, "team_depth_charts_with_values.csv", need)
    df["team_code"] = df["team_code"].astype(str).str.upper().str.strip()
    return df

def _pick_teams_from_odds(odds_df: pd.DataFrame) -> list:
    t = set()
    for col in ("home_team", "away_team"):
        if col not in odds_df.columns:
            die("Odds frame missing home_team/away_team columns.")
        t.update(odds_df[col].astype(str).str.upper().str.strip().tolist())
    return sorted(t)

def run_weekly_predictions() -> Tuple[pd.DataFrame, pd.DataFrame]:

        print("STEP 0: Preparing environment & config...")
        cfg = _load_json(CONFIG_PATH)
        require_env(os.environ, REQUIRED_ENV)
        # provenance: config hash
        try:
            config_hash = hashlib.sha256(Path(CONFIG_PATH).read_bytes()).hexdigest()
        except Exception:
            config_hash = None

        print("STEP 1: Fetching live odds (defines the week & teams in play)...")
        odds_df = get_consensus_nfl_odds()
        if isinstance(odds_df, list):
            odds_df = pd.DataFrame(odds_df)
        need_odds = ["home_team", "away_team", "spread_home", "spread_away", "total", "kickoff_utc", "neutral_site"]
        require_columns(odds_df, "weekly_odds", need_odds)
        if odds_df.empty:
            die("No odds returned for the current week window. Check API key/plan or date window.")
        # alias & validate odds after ratings are loaded (we need team map), so we defer strict validation

        teams_in_play = _pick_teams_from_odds(odds_df)

        print("\nSTEP 2: Running live roster audit (BLOCK/HOLD)…")
        audit_log = run_roster_audit(teams_to_check=teams_in_play)
        enforce_roster_audit(audit_log)

        print("\nSTEP 3: Loading ratings + merging stadium HFA (no zeroing)…")
        ratings_df = merge_hfa(str(RATINGS_PATH), str(HFA_PATH))
        require_columns(ratings_df, "ratings+HFA", ["team_code", "rating", "uncertainty", "hfa"])
        # normalize/validate ratings and odds coherency
        ratings_df = apply_aliases(ratings_df, cols=["team_code"])
        odds_df = apply_aliases(odds_df, cols=["home_team", "away_team"])
        validate_ratings(ratings_df, strict=True)
        validate_odds(odds_df, ratings_df, strict=True)

        print("\nSTEP 4: Loading depth charts…")
        depth_df = _load_depth_charts(DEPTH_PATH)
        depth_df = apply_aliases(depth_df, cols=["team_code"])
        validate_depth(depth_df, strict=True)

        print("\nSTEP 5: Fetching latest injury data (strict)...")
        injury_source = "live"
        injuries = fetch_injured_players()
        inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0
        injuries_live_count = inj_ct
        injuries_fallback_count = 0

        if inj_ct == 0:
            # Conservative fallback from roster statuses (IR/PUP/NFI/Suspended)
            injuries = derive_injuries_from_rosters(teams_in_play)
            inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0
            injuries_fallback_count = inj_ct
            injury_source = "fallback"
            teams_ct = injuries["team_code"].nunique() if inj_ct else 0
            print(f"Roster-derived injuries: {inj_ct} rows across {teams_ct} teams.")

        # Normalize injuries team codes and validate leniently (ok for empty)
        if isinstance(injuries, pd.DataFrame) and not injuries.empty:
            injuries = apply_aliases(injuries, cols=["team_code"])
            validate_injuries(injuries, strict=False)

        print(f"Found {inj_ct} records from injuries (live or fallback).")

        sigma_policy_name = "constant"
        print("\nSTEP 6: Monte Carlo simulations...")
        result = run_simulation(odds_df, ratings_df, depth_df, injuries)
        if isinstance(result, tuple) and len(result) == 2:
            df_pred, df_cards = result
        else:
            df_pred, df_cards = result, pd.DataFrame()

        require_columns(df_pred, "simulation output (preds)", [
            "home_team","away_team","vegas_line","vegas_total","sigma",
            "win_prob_home","cover_prob_home","ou_prob_over","kickoff_utc","neutral_site"
        ])

        print("\n--- WEEKLY PREDICTIONS ---")
        print(df_pred.to_string(index=False))

        print("\nSTEP 7: Writing artifacts...")
        df_pred.to_csv(OUT_PREDS, index=False)
        if not df_cards.empty:
            df_cards.to_csv(OUT_CARDS, index=False)

        run_meta = {
            "runner": platform.node(),
            "timestamp_utc": pd.Timestamp.utcnow().isoformat(),
            "python": platform.python_version(),
            "host": socket.gethostname(),
            "config_used": str(CONFIG_PATH.name)
        }
        inputs = {
            "ratings_csv": str(RATINGS_PATH.name),
            "stadium_hfa_csv": str(HFA_PATH.name),
            "depth_charts_csv": str(DEPTH_PATH.name),
            "odds_provider": "TheOddsAPI",
            "injury_provider": "SportsDataIO (or configured provider)"
        }
        outputs = {
            "predictions_csv": str(OUT_PREDS.name),
            "gamecards_csv": str(OUT_CARDS.name) if OUT_CARDS.exists() else None
        }
        audits = {
            "roster_audit": audit_log
        }

        extras = {
            "injury_source": injury_source if "injury_source" in locals() else "live",
            "injuries_live_count": injuries_live_count if "injuries_live_count" in locals() else 0,
            "injuries_fallback_count": injuries_fallback_count if "injuries_fallback_count" in locals() else 0,
            "sigma_policy": sigma_policy_name if "sigma_policy_name" in locals() else "constant",
            "config_hash": config_hash if "config_hash" in locals() else None
        }

        write_manifest(OUT_MANIFEST, run_meta, inputs, outputs, audits, extras)

        print(f"\nSaved: {OUT_PREDS.name}" + (f", {OUT_CARDS.name}" if OUT_CARDS.exists() else ""))
        print(f"Saved: {OUT_MANIFEST.name}")

        return df_pred, df_cards

if __name__ == "__main__":
    run_weekly_predictions()
