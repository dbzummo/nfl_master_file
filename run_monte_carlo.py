#!/usr/bin/env python3
"""
run_monte_carlo.py — hardened simulation core that always returns (preds_df, cards_df)

Inputs:
  - odds_df (DataFrame): requires columns
        ['home_team','away_team','spread_home','spread_away','total','kickoff_utc','neutral_site']
  - ratings_df (DataFrame): requires columns
        ['team_code','rating','uncertainty','hfa']
  - depth_df (DataFrame): requires columns
        ['team_code','position','player','value']
  - injuries (DataFrame | list[dict] | None): best-effort parser; ignored if empty

Outputs:
  Tuple[pd.DataFrame, pd.DataFrame]:
    preds_df columns (strict):
        ['home_team','away_team','vegas_line','vegas_total','sigma',
         'win_prob_home','cover_prob_home','ou_prob_over','kickoff_utc','neutral_site']
    cards_df columns (best-effort but stable):
        ['game_id','home_team','away_team','kickoff_utc','neutral_site',
         'rating_home','rating_away','hfa_home','inj_adj_home','inj_adj_away',
         'vegas_line','vegas_total','modeled_spread_home','modeled_total',
         'win_prob_home','cover_prob_home','ou_prob_over','notes']

Design notes:
  - No HFA zeroing; we assume hfa_loader has already merged/stabilized HFA.
  - We blend model and market softly to avoid brittle outputs:
      base_mu = model_spread_home
      vegas_mu = vegas_line (home side)
      mu = w_model * base_mu + (1 - w_model) * vegas_mu
    where w_model defaults to 0.60 (env: BLEND_W_MODEL).
  - Sigma (margin std) defaults to NFL-typical ~13.5 (env: SIGMA_MARGIN).
  - Total variance is handled separately; OU probability uses Normal(total_mu, sigma_total)
      where sigma_total defaults to ~9.5 (env: SIGMA_TOTAL).
  - Injury adjustment is conservative: maps probable/questionable/out/doubtful to
      a small points-shift derived from missing 'value' in depth chart.
  - Reproducible RNG per week: seed via min kickoff date (UTC).
"""

from __future__ import annotations
from typing import Tuple, Dict, Any, Iterable, Optional
import os
import math
import numpy as np
import pandas as pd

REQUIRED_ODDS = ["home_team","away_team","spread_home","spread_away","total","kickoff_utc","neutral_site"]
REQUIRED_RATINGS = ["team_code","rating","uncertainty","hfa"]
REQUIRED_DEPTH = ["team_code","position","player","value"]

def _require_cols(df: pd.DataFrame, need: Iterable[str], name: str) -> None:
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"{name} missing columns: {missing}")

def _norm_team(x: Any) -> str:
    return str(x).upper().strip()

def _to_float_safe(s: Any, default: float = np.nan) -> float:
    try:
        return float(s)
    except Exception:
        return default

def _coerce_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _rng_seed_from_kickoffs(odds_df: pd.DataFrame) -> int:
    # Seed from earliest kickoff date (yyyyMMdd as int) for reproducibility per slate
    try:
        ks = pd.to_datetime(odds_df["kickoff_utc"], utc=True, errors="coerce")
        d = ks.min()
        if pd.isna(d):
            return 20240901  # stable fallback
        return int(d.strftime("%Y%m%d"))
    except Exception:
        return 20240901

def _injury_df_from_any(injuries_any) -> pd.DataFrame:
    if injuries_any is None:
        return pd.DataFrame(columns=["team_code","player","status","position"])
    if isinstance(injuries_any, pd.DataFrame):
        return injuries_any.copy()
    # assume iterable of dicts
    try:
        return pd.DataFrame(list(injuries_any))
    except Exception:
        return pd.DataFrame(columns=["team_code","player","status","position"])

def _build_rating_map(ratings_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    r = {}
    for _, row in ratings_df.iterrows():
        team = _norm_team(row["team_code"])
        r[team] = {
            "rating": float(row["rating"]),
            "uncertainty": float(row.get("uncertainty", 0.0)),
            "hfa": float(row.get("hfa", 0.0)),
        }
    return r

def _team_value_from_depth(depth_df: pd.DataFrame, team: str) -> float:
    # Sum of 'value' as a rough proxy for roster-strength baseline (already your file)
    s = depth_df.loc[depth_df["team_code"] == team, "value"]
    return float(s.sum()) if len(s) else 0.0

def _injury_adjust_points(inj: pd.DataFrame, team: str, depth_df: pd.DataFrame) -> float:
    """
    Conservative points-shift for missing diminished contributors.
    We:
      - join injuries to depth by (team_code, player) in a fuzzy way (casefold match)
      - weight: OUT=-0.6, DOUBTFUL=-0.45, QUESTIONABLE=-0.25, SUSP=-0.6, IR=-0.7, PROBABLE=-0.1
      - cap magnitude at ~2.5 pts
    """
    if inj.empty:
        return 0.0

    t_inj = inj.copy()
    t_inj["team_code"] = t_inj["team_code"].astype(str).str.upper().str.strip()
    t_inj = t_inj[t_inj["team_code"] == team]

    if t_inj.empty:
        return 0.0

    # Normalize player names for a basic join
    def keyify(x): return str(x).lower().strip()

    depth = depth_df[depth_df["team_code"] == team].copy()
    depth["__k"] = depth["player"].map(keyify)
    t_inj["__k"] = t_inj["player"].map(keyify) if "player" in t_inj.columns else ""

    merged = t_inj.merge(depth[["__k","value"]], on="__k", how="left")
    merged["value"] = pd.to_numeric(merged["value"], errors="coerce").fillna(0.0)

    # Status → weight
    def status_weight(s: Any) -> float:
        if s is None:
            return 0.0
        s = str(s).lower()
        if "out" in s or "susp" in s or "injured reserve" in s or s == "ir":
            return -0.6
        if "doubt" in s:
            return -0.45
        if "question" in s or s == "q":
            return -0.25
        if "prob" in s:
            return -0.10
        return 0.0

    merged["w"] = merged["status"].map(status_weight)
    # Map value (0..something) to rough points via soft scaling
    # Using diminishing returns: points = w * log1p(value/avgpos)
    avg_val = max(1.0, depth["value"].mean()) if len(depth) else 5.0
    merged["pts"] = merged["w"] * np.log1p(merged["value"] / avg_val) * 2.2  # soft scaling

    pts = float(merged["pts"].sum())
    return float(np.clip(pts, -2.5, 0.75))  # conservative cap

def _make_game_id(home: str, away: str, kickoff_utc: str) -> str:
    ts = ""
    try:
        ts = pd.to_datetime(kickoff_utc, utc=True, errors="coerce").strftime("%Y%m%dT%H%MZ")
    except Exception:
        ts = "NA"
    return f"{away}@{home}_{ts}"

def _norm_odds(odds_df: pd.DataFrame) -> pd.DataFrame:
    df = odds_df.copy()
    for c in ["home_team","away_team"]:
        df[c] = df[c].map(_norm_team)
    df = _coerce_numeric(df, ["spread_home","spread_away","total"])
    # Ensure 'spread_home' is home spread (negative favorite typical). If absent, derive from away:
    if "spread_home" not in df.columns and "spread_away" in df.columns:
        df["spread_home"] = -df["spread_away"]
    # If both provided and inconsistent, prefer home as single source of truth
    if "spread_home" in df.columns and "spread_away" in df.columns:
        # sanity: enforce spread_away = -spread_home
        df["spread_away"] = -df["spread_home"]
    if "neutral_site" in df.columns:
        # normalize to bool
        df["neutral_site"] = df["neutral_site"].astype(str).str.lower().isin(["1","true","yes","y","t"])
    else:
        df["neutral_site"] = False
    return df

def _model_spread_for_game(
    home: str, away: str,
    ratings_map: Dict[str, Dict[str, float]],
    injuries_df: pd.DataFrame,
    depth_df: pd.DataFrame,
    neutral_site: bool
) -> Dict[str, float]:
    # Base ratings
    rh = ratings_map.get(home, {"rating":0.0,"uncertainty":1.0,"hfa":0.0})
    ra = ratings_map.get(away, {"rating":0.0,"uncertainty":1.0,"hfa":0.0})
    hfa_home = 0.0 if neutral_site else float(rh.get("hfa", 0.0))

    # Injury points (home is positive to margin if away is hurt more, etc.)
    inj_home = _injury_adjust_points(injuries_df, home, depth_df)
    inj_away = _injury_adjust_points(injuries_df, away, depth_df)

    # Model margin (home - away). Higher means home stronger.
    base_margin = (rh["rating"] - ra["rating"]) + hfa_home + (inj_home - inj_away)

    return {
        "rating_home": float(rh["rating"]),
        "rating_away": float(ra["rating"]),
        "hfa_home": float(hfa_home),
        "inj_adj_home": float(inj_home),
        "inj_adj_away": float(inj_away),
        "model_spread_home": float(base_margin)
    }

def _cdf_normal(x: np.ndarray, mu: float, sigma: float) -> np.ndarray:
    z = (x - mu) / max(sigma, 1e-6)
    return 0.5 * (1.0 + math.erf(z / np.sqrt(2.0)))

def run_simulation(
    odds_df: pd.DataFrame,
    ratings_df: pd.DataFrame,
    depth_df: pd.DataFrame,
    injuries
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    # 1) Validate inputs
    _require_cols(odds_df, REQUIRED_ODDS, "odds_df")
    _require_cols(ratings_df, REQUIRED_RATINGS, "ratings_df")
    _require_cols(depth_df, REQUIRED_DEPTH, "depth_df")

    odds_df = _norm_odds(odds_df.copy())
    ratings_df = ratings_df.copy()
    ratings_df["team_code"] = ratings_df["team_code"].map(_norm_team)
    depth_df = depth_df.copy()
    depth_df["team_code"] = depth_df["team_code"].map(_norm_team)
    injuries_df = _injury_df_from_any(injuries)

    # 2) Config / hyperparams
    BLEND_W_MODEL = float(os.getenv("BLEND_W_MODEL", "0.60"))  # 60% model / 40% market by default
    SIGMA_MARGIN = float(os.getenv("SIGMA_MARGIN", "13.5"))     # stdev of point margin
    SIGMA_TOTAL  = float(os.getenv("SIGMA_TOTAL", "9.5"))       # stdev of game total
    SIM_N = int(os.getenv("SIM_N", "50000"))

    # Seed for reproducibility
    np.random.seed(_rng_seed_from_kickoffs(odds_df))

    ratings_map = _build_rating_map(ratings_df)

    preds_rows = []
    cards_rows = []

    for _, g in odds_df.iterrows():
        home = g["home_team"]
        away = g["away_team"]
        neutral = bool(g.get("neutral_site", False))
        vegas_line = _to_float_safe(g.get("spread_home"), 0.0)  # home spread (negative means favorite)
        vegas_total = _to_float_safe(g.get("total"), np.nan)
        kickoff_utc = str(g.get("kickoff_utc",""))

        # 3) Model spread components
        comps = _model_spread_for_game(home, away, ratings_map, injuries_df, depth_df, neutral_site=neutral)
        model_spread = comps["model_spread_home"]

        # 4) Blend model vs. market (mu for margin distribution)
        mu_margin = BLEND_W_MODEL * model_spread + (1.0 - BLEND_W_MODEL) * vegas_line

        # 5) Total mean: small nudge toward market if available; else derive from ratings delta as neutral
        if not np.isnan(vegas_total):
            mu_total = 0.7 * vegas_total + 0.3 * max(35.0, 44.0 - 0.2 * abs(model_spread))
        else:
            mu_total = 44.0 - 0.2 * abs(model_spread)  # harmless fallback

        # 6) Monte Carlo (Normal margin + Normal total)
        #    Note: we could correlate margin & total; for now assume independence (simple & robust).
        margins = np.random.normal(loc=mu_margin, scale=SIGMA_MARGIN, size=SIM_N)
        totals  = np.random.normal(loc=mu_total,  scale=SIGMA_TOTAL,  size=SIM_N)

        # Probabilities
        win_prob_home   = float((margins > 0.0).mean())
        cover_prob_home = float((margins + vegas_line > 0.0).mean())  # home covers when margin > -spread_home
        ou_prob_over    = float((totals > vegas_total).mean()) if not np.isnan(vegas_total) else np.nan

        # Sigma we report = margin sigma (so downstream can audit variability assumptions)
        sigma_report = float(SIGMA_MARGIN)

        preds_rows.append({
            "home_team": home,
            "away_team": away,
            "vegas_line": vegas_line,
            "vegas_total": vegas_total,
            "sigma": sigma_report,
            "win_prob_home": round(win_prob_home, 4),
            "cover_prob_home": round(cover_prob_home, 4),
            "ou_prob_over": round(ou_prob_over, 4) if not np.isnan(ou_prob_over) else np.nan,
            "kickoff_utc": kickoff_utc,
            "neutral_site": bool(neutral),
        })

        cards_rows.append({
            "game_id": _make_game_id(home, away, kickoff_utc),
            "home_team": home,
            "away_team": away,
            "kickoff_utc": kickoff_utc,
            "neutral_site": bool(neutral),
            "rating_home": comps["rating_home"],
            "rating_away": comps["rating_away"],
            "hfa_home": comps["hfa_home"],
            "inj_adj_home": comps["inj_adj_home"],
            "inj_adj_away": comps["inj_adj_away"],
            "vegas_line": vegas_line,
            "vegas_total": vegas_total,
            "modeled_spread_home": float(model_spread),
            "modeled_total": float(mu_total),
            "win_prob_home": round(win_prob_home, 4),
            "cover_prob_home": round(cover_prob_home, 4),
            "ou_prob_over": round(ou_prob_over, 4) if not np.isnan(ou_prob_over) else np.nan,
            "notes": "Blend {:.0%} model / {:.0%} market; sigma_margin={:.1f}, sigma_total={:.1f}".format(
                BLEND_W_MODEL, 1-BLEND_W_MODEL, SIGMA_MARGIN, SIGMA_TOTAL
            )
        })

    preds_df = pd.DataFrame(preds_rows, columns=[
        "home_team","away_team","vegas_line","vegas_total","sigma",
        "win_prob_home","cover_prob_home","ou_prob_over","kickoff_utc","neutral_site"
    ])

    cards_df = pd.DataFrame(cards_rows, columns=[
        "game_id","home_team","away_team","kickoff_utc","neutral_site",
        "rating_home","rating_away","hfa_home","inj_adj_home","inj_adj_away",
        "vegas_line","vegas_total","modeled_spread_home","modeled_total",
        "win_prob_home","cover_prob_home","ou_prob_over","notes"
    ])

    # Final schema safety: ensure dtypes are serializable
    for c in ["vegas_line","vegas_total","sigma","win_prob_home","cover_prob_home","ou_prob_over",
              "rating_home","rating_away","hfa_home","inj_adj_home","inj_adj_away",
              "modeled_spread_home","modeled_total"]:
        if c in preds_df.columns:
            preds_df[c] = pd.to_numeric(preds_df[c], errors="coerce")
        if c in cards_df.columns:
            cards_df[c] = pd.to_numeric(cards_df[c], errors="coerce")

    return preds_df, cards_df
