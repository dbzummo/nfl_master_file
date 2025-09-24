#!/usr/bin/env python3
# scripts/fetch_results_week.py
#
# Robust results pull from MSF (v2.1):
# - Uses season path: /pull/nfl/{season}/games.json
# - Queries dmin→dmax with ±1 day padding
# - Canonicalizes team codes (LA→LAR, WSH→WAS, OAK→LV, SD→LAC, STL→LAR)
# - Backfills any predicted matchups still missing via team-filter sweep over the padded window
# - Writes: out/results/week_results.csv  (date, away_team, home_team, away_score, home_score, home_win)

from __future__ import annotations

import os, csv, time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import requests

# --- Config & paths ---
PRED_PATH = Path("out/week_predictions.csv")
OUT_DIR   = Path("out/results")
OUT_CSV   = OUT_DIR / "week_results.csv"

MSF_KEY = os.getenv("MSF_KEY", "").strip()
MSF_PASS = os.getenv("MSF_PASS", "").strip()

RESULTS_SEASON = os.getenv("RESULTS_SEASON", "").strip()  # e.g. "2025-regular"
RESULTS_FORCE = os.getenv("RESULTS_FORCE", "false").lower() == "true"
RESULTS_TIMEOUT = float(os.getenv("RESULTS_TIMEOUT", "8.0"))
RESULTS_RETRIES = int(os.getenv("RESULTS_RETRIES", "3"))
RESULTS_BACKOFF_BASE = float(os.getenv("RESULTS_BACKOFF_BASE", "0.6"))

TRANSIENT_HTTP = {429, 500, 502, 503, 504}

NFL_CANON = {
    "LA": "LAR", "WSH": "WAS", "OAK": "LV", "SD": "LAC", "STL": "LAR"
}

def _canon_team(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    x = str(s).upper().strip()
    return NFL_CANON.get(x, x) or None

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def _datestr(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _load_week_span_and_pairs() -> Tuple[datetime, datetime, int, Set[Tuple[str,str]]]:
    if not PRED_PATH.exists():
        raise FileNotFoundError(f"Missing {PRED_PATH}")
    df = pd.read_csv(PRED_PATH)
    if "date" not in df.columns or "away_team" not in df.columns or "home_team" not in df.columns:
        raise ValueError("week_predictions.csv must have date, away_team, home_team")
    # date span
    dts = pd.to_datetime(df["date"], errors="coerce")
    dmin = pd.Timestamp(dts.min()).to_pydatetime()
    dmax = pd.Timestamp(dts.max()).to_pydatetime()
    # week (for log only)
    if "week" in df.columns and pd.notna(df["week"]).any():
        wk = int(pd.Series(df["week"].dropna()).mode().iloc[0])
    else:
        wk = int(pd.Series(pd.to_datetime(df["date"]).dt.isocalendar().week).mode().iloc[0])
    # predicted pairs (canon)
    pairs = {( _canon_team(a), _canon_team(h) ) for a,h in zip(df["away_team"], df["home_team"])}
    return dmin, dmax, wk, pairs

def _infer_season(dmin: datetime) -> str:
    return f"{dmin.year}-regular"

def _base_url(season: str) -> str:
    return f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/games.json"

def _http_get(session: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(RESULTS_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=RESULTS_TIMEOUT, auth=(MSF_KEY, MSF_PASS))
            if resp.status_code != 200:
                if resp.status_code in TRANSIENT_HTTP and attempt < RESULTS_RETRIES - 1:
                    time.sleep(RESULTS_BACKOFF_BASE * (2 ** attempt)); continue
                snippet = (resp.text or "")[:400]
                raise RuntimeError(f"HTTP {resp.status_code} url={url} params={params} body={snippet!r}")
            if "json" not in (resp.headers.get("Content-Type","").lower()):
                raise RuntimeError(f"Unexpected content-type: {resp.headers.get('Content-Type')}")
            data = resp.json()
            if not isinstance(data, dict): raise RuntimeError("Invalid JSON")
            return data
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt < RESULTS_RETRIES - 1:
                time.sleep(RESULTS_BACKOFF_BASE * (2 ** attempt)); continue
            raise
        except Exception as e:
            last_err = e; break
    if last_err: raise last_err
    return {}

def _normalize_games(payload: Dict[str, Any], iso_date: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for g in payload.get("games") or []:
        sched = g.get("schedule") or {}
        score = g.get("score") or {}
        home_abbr = _canon_team((sched.get("homeTeam") or {}).get("abbreviation"))
        away_abbr = _canon_team((sched.get("awayTeam") or {}).get("abbreviation"))
        hs = score.get("homeScoreTotal"); as_ = score.get("awayScoreTotal")
        hs_val = int(hs) if str(hs).strip().isdigit() else (None if hs in (None,"","null") else None)
        as_val = int(as_) if str(as_).strip().isdigit() else (None if as_ in (None,"","null") else None)
        home_win = None
        if hs_val is not None and as_val is not None:
            home_win = 1 if hs_val > as_val else (0 if hs_val < as_val else None)
        # Prefer provided iso_date; if absent, try to parse startTime → date
        d = iso_date
        if d is None:
            st = sched.get("startTime")
            if isinstance(st, str) and len(st) >= 10:
                try: d = st[:10]
                except Exception: d = None
        if home_abbr and away_abbr and d:
            rows.append({
                "date": d, "away_team": away_abbr, "home_team": home_abbr,
                "away_score": as_val, "home_score": hs_val, "home_win": home_win
            })
    return rows

def _collect_by_day(session: requests.Session, url: str, d0: datetime, d1: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cur = d0
    while cur <= d1:
        params = {"date": _yyyymmdd(cur), "force": str(RESULTS_FORCE).lower()}
        data = _http_get(session, url, params)
        out.extend(_normalize_games(data, _datestr(cur)))
        time.sleep(0.15)
        cur += timedelta(days=1)
    return out

def _collect_by_team_backfill(session: requests.Session, url: str, d0: datetime, d1: datetime,
                              missing_pairs: Set[Tuple[str,str]]) -> List[Dict[str, Any]]:
    """
    For any (away,home) still missing, ask MSF for those teams for each day in the padded window.
    We use ?team=AWY,HOME plus date to limit payload; if team filter is ignored, we locally filter.
    """
    out: List[Dict[str, Any]] = []
    days = [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]
    for (awy, hom) in sorted(missing_pairs):
        for d in days:
            params = {
                "date": _yyyymmdd(d),
                "team": f"{awy},{hom}",
                "force": str(RESULTS_FORCE).lower()
            }
            try:
                data = _http_get(session, url, params)
                rows = _normalize_games(data, _datestr(d))
                # filter strictly to our pair
                rows = [r for r in rows if r["away_team"] == awy and r["home_team"] == hom]
                out.extend(rows)
                time.sleep(0.1)
            except Exception:
                # non-fatal; continue trying others
                continue
    return out

def main() -> None:
    if not MSF_KEY or not MSF_PASS:
        raise RuntimeError("Missing MSF_KEY/MSF_PASS")

    dmin, dmax, wk, predicted_pairs = _load_week_span_and_pairs()
    season = RESULTS_SEASON or f"{dmin.year}-regular"
    url = _base_url(season)

    # padded window
    d0 = datetime(dmin.year, dmin.month, dmin.day) - timedelta(days=1)
    d1 = datetime(dmax.year, dmax.month, dmax.day) + timedelta(days=1)

    session = requests.Session()

    # pass 1: by-day pull over padded window
    rows = _collect_by_day(session, url, d0, d1)

    # determine which predicted pairs we have
    have_pairs = {(r["away_team"], r["home_team"]) for r in rows}
    missing = {p for p in predicted_pairs if p not in have_pairs}

    # pass 2: targeted team backfill for missing pairs
    if missing:
        rows.extend(_collect_by_team_backfill(session, url, d0, d1, missing))
        have_pairs = {(r["away_team"], r["home_team"]) for r in rows}
        missing = {p for p in predicted_pairs if p not in have_pairs}

    # write file (sorted, dedup by (date,away,home))
    _ensure_parent(OUT_CSV)
    key = lambda r: (r["date"], r["away_team"], r["home_team"])
    rows_sorted = sorted({key(r): r for r in rows}.values(), key=key)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","away_team","home_team","away_score","home_score","home_win"])
        w.writeheader()
        for r in rows_sorted:
            w.writerow(r)

    final_scored = sum(1 for r in rows_sorted if r["home_score"] is not None and r["away_score"] is not None)
    print(f"[OK] Week {wk}: season={season} → wrote {OUT_CSV} with {len(rows_sorted)} games ({final_scored} with final scores).")
    if missing:
        print("[NOTE] Still missing matchups:", ", ".join([f"{a}@{h}" for a,h in sorted(missing)]))

if __name__ == "__main__":
    main()
