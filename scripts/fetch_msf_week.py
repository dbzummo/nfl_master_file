mkdir -p scripts overrides artifacts reports out

cat > scripts/fetch_week_msf.py <<'PY'
#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd

MSF = "https://api.mysportsfeeds.com/v2.1/pull/nfl/2025-regular/games.json"
COLS = ["date","week","away","home","status","away_score","home_score",
        "market_spread","market_total","home_win_prob"]

def get_json(start, end):
    key = os.environ.get("MSF_KEY"); pwd = os.environ.get("MSF_PASS","MYSPORTSFEEDS")
    if not key: sys.exit("[error] MSF_KEY missing")
    r = requests.get(MSF, params={"date": f"{start}-{end}"}, auth=(key, pwd), timeout=30)
    if r.status_code != 200: sys.exit(f"[error] MSF HTTP {r.status_code} — {r.text[:200]}")
    return r.json()

def parse_games(js):
    rows = []
    for g in js.get("games", []):
        s = g["schedule"]; sc = g.get("score")
        date = pd.to_datetime(s["startTime"], utc=True).date().strftime("%Y-%m-%d")
        status = "FINAL" if (sc or {}).get("isCompleted") else ((sc or {}).get("currentQuarter") or "PRE")
        away = s["awayTeam"]["abbreviation"]; home = s["homeTeam"]["abbreviation"]
        rows.append(dict(
            date=date, week=s.get("week"), away=away, home=home, status=status,
            away_score=(sc or {}).get("awayScoreTotal"), home_score=(sc or {}).get("homeScoreTotal"),
            market_spread=None, market_total=None, home_win_prob=None
        ))
    return pd.DataFrame(rows)

def ingest_model_prob(df):
    pth = "out/predictions_week_calibrated_blend.csv"
    if not os.path.exists(pth): return df
    p = pd.read_csv(pth)
    p["date"] = pd.to_datetime(p["date"]).dt.strftime("%Y-%m-%d")
    p = p.rename(columns={"away_team":"away","home_team":"home"})
    return df.merge(p[["date","away","home","home_win_prob"]], on=["date","away","home"], how="left")

def ingest_market_lines(df):
    pth = "overrides/market_lines.csv"
    if not os.path.exists(pth): return df
    m = pd.read_csv(pth, dtype={"market_spread":"float64","market_total":"float64"})
    m["date"] = pd.to_datetime(m["date"]).dt.strftime("%Y-%m-%d")
    return df.merge(m, on=["date","away","home"], how="left", suffixes=("","_m")).assign(
        market_spread=lambda d: d["market_spread"].combine_first(d["market_spread_m"]),
        market_total=lambda d: d["market_total"].combine_first(d["market_total_m"])
    ).drop(columns=["market_spread_m","market_total_m"], errors="ignore")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    a = ap.parse_args()
    js = get_json(a.start, a.end)
    df = parse_games(js)
    if df.empty: sys.exit("[error] no MSF games in that window")
    df = ingest_model_prob(df)
    df = ingest_market_lines(df)
    df = df[COLS]
    os.makedirs("out", exist_ok=True)
    df.to_csv("out/week_canonical.csv", index=False)
    print(f"[ok] wrote out/week_canonical.csv rows={len(df)} window {df.date.min()}→{df.date.max()}")
if __name__ == "__main__":
    main()
PY
chmod +x scripts/fetch_week_msf.py