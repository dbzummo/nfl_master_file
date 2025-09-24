#!/usr/bin/env python3
"""
run_audit.py

Compares rosters from two providers for each team and returns a structured log.

Public:
  run_roster_audit(teams_to_check: list[str]) -> dict
Return shape:
{
  "conflicts": {
     "PHI": [
        {"status": "BLOCK"|"HOLD", "details": "..."},
        ...
     ],
     ...
  },
  "meta": {"provider_primary": "...", "provider_secondary": "..."}
}

Rules (strict by default):
- If primary (SportsDataIO) fetch fails → BLOCK
- If both sources return empty → BLOCK
- If mismatch rate > threshold (default 12%) → HOLD (you can tune)
- If key positions missing from primary (QB1, LT1, CB1, K, P) → BLOCK
"""

from __future__ import annotations
from typing import List, Dict, Any
import pandas as pd

# Import the two functions that run_predictions expects to exist
from fetch_rosters import get_roster_sportsdataio, get_roster_nflverse


def _key_positions_missing(primary: pd.DataFrame) -> List[str]:
    """
    Simple sanity: must have some presence at critical positions.
    """
    must_have_any = ["QB", "LT", "C", "CB", "S", "K", "P"]
    missing = []
    for pos in must_have_any:
        # allow matching like 'LT' within OL data
        if pos in ("LT", "C"):
            has = (
                (primary["position"] == pos).any() or
                (primary["position"].isin(["OL", "T", "G"]).any())
            )
        else:
            has = (primary["position"] == pos).any()
        if not has:
            missing.append(pos)
    return missing


def _mismatch_rate(p: pd.DataFrame, s: pd.DataFrame) -> float:
    """
    Jaccard-like distance between player sets (by names).
    """
    pset = set(p["player"].str.lower())
    sset = set(s["player"].str.lower())
    if not pset and not sset:
        return 1.0
    inter = len(pset & sset)
    union = len(pset | sset)
    if union == 0:
        return 1.0
    return 1.0 - (inter / union)


def run_roster_audit(teams_to_check: List[str]) -> Dict[str, Any]:
    conflicts: Dict[str, List[Dict[str, str]]] = {}
    mismatch_hold_threshold = 0.12  # 12% difference => HOLD

    for team in teams_to_check:
        team_conflicts: List[Dict[str, str]] = []
        try:
            primary = get_roster_sportsdataio(team)
        except Exception as e:
            team_conflicts.append({"status": "BLOCK", "details": f"SportsDataIO fetch failed: {e}"})
            conflicts[team] = team_conflicts
            continue

        secondary = get_roster_nflverse(team)

        # If both empty → BLOCK
        if primary.empty and secondary.empty:
            team_conflicts.append({"status": "BLOCK", "details": "Both providers returned empty roster."})
            conflicts[team] = team_conflicts
            continue

        # Key positions present?
        missing_keys = _key_positions_missing(primary)
        if missing_keys:
            team_conflicts.append({"status": "BLOCK", "details": f"Primary roster missing key positions: {missing_keys}"})

        # Compare mismatch rate only if secondary present
        if not secondary.empty and not primary.empty:
            mr = _mismatch_rate(primary, secondary)
            if mr > mismatch_hold_threshold:
                team_conflicts.append({"status": "HOLD", "details": f"Mismatch rate {mr:.1%} exceeds {mismatch_hold_threshold:.0%} threshold."})

        if team_conflicts:
            conflicts[team] = team_conflicts

    return {
        "conflicts": conflicts,
        "meta": {
            "provider_primary": "sportsdataio",
            "provider_secondary": "nflverse"
        }
    }
