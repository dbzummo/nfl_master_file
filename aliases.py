from __future__ import annotations

from typing import Iterable, Dict
import pandas as pd

# Minimal, editable alias map. Left side = raw values you may see in feeds,
# right side = your standardized team_code used everywhere else.
TEAM_ALIASES: Dict[str, str] = {
    # examples:
    "WSH": "WAS",
    "LVR": "LV",
    "JAC": "JAX",
    "LA":  "LAR",
    # add more as needed
}

def _normalize_code(x: str) -> str:
    if x is None:
        return ""
    x = str(x).strip().upper()
    return TEAM_ALIASES.get(x, x)

def apply_aliases(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """
    Return a COPY of df with all given columns normalized via TEAM_ALIASES.
    If a column is missing, it's ignored (no crash).
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype(str).map(_normalize_code)
    return out

__all__ = ["apply_aliases"]
