#!/usr/bin/env python3
import os, sys, time, json, math, pathlib, argparse
from statistics import median
from typing import List, Dict, Any, Optional
import requests
import pandas as pd

OUT_WEEK_FILE = pathlib.Path("out/week_with_market.csv")
RAW_DIR = pathlib.Path("out/msf/odds_raw")

MSF_API_KEY = os.environ.get("MSF_API_KEY")
MSF_SEASON  = os.environ.get("MSF_SEASON", "current").strip()

def fatal(msg: str, code: int = 2):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)

def american_to_prob(american: Optional[float]) -> Optional[float]:
    """Convert American odds to implied probability (including vig)."""
    if american is None or (isinstance(american, float) and math.isnan(american)):
        return None
    try:
        a = float(american)
    except Exception:
        return None
    if a == 0:
        return None
    if a < 0:
        # favorite
        return (-a) / ((-a) + 100.0)
    else:
        # underdog
        return 100.0 / (a + 100.0)

def devig_two_side(p_home_raw: Optional[float], p_away_raw: Optional[float]) -> Optional[float]:
    """Given raw implied probs (with vig) for both teams, return de-vigged home prob."""
    if p_home_raw is None and p_away_raw is None:
        return None
    if p_home_raw is None:
        return None
    if p_away_raw is None:
        return None
    s = p_home_raw + p_away_raw
    if s <= 0:
        return None
    return p_home_raw / s

def pick_dates(args_dates: Optional[str]) -> List[str]:
    # 1) CLI --dates
    if args_dates:
        xs = [x.strip() for x in args_dates.split(",") if x.strip()]
        if xs:
            print(f"[INFO] dates source: --dates n={len(xs)}")
            return xs

    # 2) out/week_predictions.csv
    wp = pathlib.Path("out/week_predictions.csv")
    if wp.exists():
        try:
            df = pd.read_csv(wp)
            if "date" in df.columns:
                ds = sorted(set(str(int(d)) for d in df["date"].dropna().astype(int)))
                if ds:
                    print(f"[INFO] dates source: out/week_predictions.csv col=date n={len(ds)}")
                    return ds
        except Exception as e:
            print(f"[WARN] could not read {wp}: {e}")

    # 3) out/week_with_elo.csv (game_date)
    wwe = pathlib.Path("out/week_with_elo.csv")
    if wwe.exists():
        try:
            df = pd.read_csv(wwe)
            for cand in ("game_date","date"):
                if cand in df.columns:
                    ds = []
                    for x in df[cand].dropna():
                        s = str(x)
                        # accept YYYYMMDD or ISO date
                        digits = "".join(ch for ch in s if ch.isdigit())
                        if len(digits) >= 8:
                            ds.append(digits[:8])
                    ds = sorted(set(ds))
                    if ds:
                        print(f"[INFO] dates source: {wwe} col={cand} n={len(ds)}")
                        return ds
        except Exception as e:
            print(f"[WARN] could not read {wwe}: {e}")

    fatal("No dates found in any candidate file. Use --dates or ensure a file has a date/game_date column.")

def fetch_day(session: requests.Session, day: str) -> Dict[str, Any]:
    """Fetch one day odds JSON. Retries on 429 with backoff. Adds ?force=false."""
    url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{MSF_SEASON}/date/{day}/odds_gamelines.json"
    params = {"force":"false"}  # crucial to avoid throttling-empty 304s
    backoff = 2.0
    for attempt in range(6):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                # sometimes CDN returns HTML; treat as empty for this day
                print(f"[WARN] {day} odds non-JSON payload; skipping")
                return {}
        if resp.status_code == 304:
            # not modified; treat as cache hit but we don't have a body, skip
            print(f"[INFO] {day} odds 304 Not Modified; skipping (no body).")
            return {}
        if resp.status_code == 403:
            print(f"[WARN] {day} odds HTTP 403: Access Restricted. Check plan add-ons and MSF_SEASON={MSF_SEASON}.")
            return {}
        if resp.status_code == 404:
            print(f"[WARN] {day} odds HTTP 404: No odds for this date (yet).")
            return {}
        if resp.status_code == 429:
            print(f"[WARN] {day} odds HTTP 429: throttled. sleeping {backoff:.1f}s then retry...")
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 10.0)
            continue
        # other codes
        snippet = (resp.text or "")[:220].replace("\n"," ")
        print(f"[WARN] {day} odds HTTP {resp.status_code}: {snippet!r}")
        return {}
    print(f"[WARN] {day} odds gave repeated 429s; giving up.")
    return {}

def extract_rows(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    if not doc or "gameLines" not in doc or not isinstance(doc["gameLines"], list):
        return rows
    for gl in doc["gameLines"]:
        try:
            g = gl.get("game", {})
            gid = g.get("id")
            start = g.get("startTime")
            wk = g.get("week")
            away = g.get("awayTeamAbbreviation")
            home = g.get("homeTeamAbbreviation")
            # lines per source
            for ln in (gl.get("lines") or []):
                src = (ln.get("source") or {}).get("name")
                # Each moneyLines entry has an asOfTime and moneyLine with awayLine/homeLine
                for ml in (ln.get("moneyLines") or []):
                    mlobj = ml.get("moneyLine") or {}
                    away_line = (mlobj.get("awayLine") or {}).get("american")
                    home_line = (mlobj.get("homeLine") or {}).get("american")
                    p_home_raw = american_to_prob(home_line)
                    p_away_raw = american_to_prob(away_line)
                    p_home = devig_two_side(p_home_raw, p_away_raw)
                    # Record even if p_home is None; we’ll drop later
                    rows.append({
                        "msf_game_id": gid,
                        "startTime": start,
                        "week": wk,
                        "away_abbr": away,
                        "home_abbr": home,
                        "source": src,
                        "p_home_book": p_home,
                    })
        except Exception:
            # be resilient to odd shapes in a single source
            continue
    return rows

def main():
    if not MSF_API_KEY:
        fatal("Set MSF_API_KEY in your environment (value: your MSF API key).")
    if not MSF_SEASON:
        fatal("Set MSF_SEASON in your environment (e.g. 2025-regular).")

    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", help="Comma-separated YYYYMMDD list (e.g. 20250925,20250928,20250929)")
    args = ap.parse_args()

    dates = pick_dates(args.dates)
    if not dates:
        fatal("No dates to query.")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    s = requests.Session()
    s.auth = (MSF_API_KEY, "MYSPORTSFEEDS")

    all_rows: List[Dict[str, Any]] = []
    for d in dates:
        doc = fetch_day(s, d)
        # Save raw per-day (debug)
        (RAW_DIR / f"odds_{d}.json").write_text(json.dumps(doc, indent=2), encoding="utf-8")
        rows = extract_rows(doc)
        # add day stamp for group sanity
        for r in rows:
            r["game_date"] = d
        print(f"[INFO] {d} extracted rows: {len(rows)}")
        all_rows.extend(rows)
        time.sleep(1.5)  # gentle pacing between days to avoid 429 bursts

    if not all_rows:
        fatal("No odds rows fetched; check API key, season, *ODDS add-on*, or date coverage.")

    df = pd.DataFrame(all_rows)
    # Ensure numeric
    if "p_home_book" in df.columns:
        df["p_home_book"] = pd.to_numeric(df["p_home_book"], errors="coerce")

    # Median market per game (drop NaNs first)
    def safe_med(s: pd.Series) -> float:
        vals = [float(x) for x in s if pd.notna(x)]
        return float(median(vals)) if vals else float("nan")

    red = (
        df.groupby("msf_game_id", as_index=False)
          .agg({
              "game_date": "first",
              "home_abbr": "first",
              "away_abbr": "first",
              "p_home_book": safe_med
          })
          .rename(columns={"p_home_book": "market_p_home"})
    )

    OUT_WEEK_FILE.parent.mkdir(parents=True, exist_ok=True)
    red.to_csv(OUT_WEEK_FILE, index=False)
    ok_all = red["market_p_home"].notna().sum()
    print(f"[OK] wrote {OUT_WEEK_FILE} (rows={len(red)}) | with market_p_home for {ok_all}/{len(red)} games")

if __name__ == "__main__":
    main()
