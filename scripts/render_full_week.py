#!/usr/bin/env python3
"""
render_full_week.py

Renders a single self-contained weekly_report.html including:
- Top table (date, away, home, status/final, market lines if present, model probs)
- Game cards (one per matchup)
- Summary (counts, Brier, and scheme confidence summary)

Now merges scheme adjustments from out/scheme_features_week.csv.
If scheme file missing, it renders without adjustments.

Inputs:
- out/msf_week.csv
- out/odds_week.csv (optional)
- out/predictions_week_calibrated_blend.csv (optional)
- out/scheme_features_week.csv (optional)

Output:
- reports/<window>/weekly_report.html
"""

import os
import sys
import pandas as pd
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "out")
REPORTS = os.path.join(ROOT, "reports")

MSF = os.path.join(OUT, "msf_week.csv")
ODDS = os.path.join(OUT, "odds_week.csv")
PRED = os.path.join(OUT, "predictions_week_calibrated_blend.csv")
SCHEME = os.path.join(OUT, "scheme_features_week.csv")

HTML_TMPL = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>NFL Weekly Report</title>
<style>
 body {{ font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin:24px; background:#0b1020; color:#e7eaf6; }}
 a {{ color:#9ecbff; text-decoration:none; }}
 .pill {{ display:inline-block; padding:2px 8px; border-radius:12px; font-size:12px; background:#1a2340; border:1px solid #2a3357; color:#e7eaf6; }}
 .ok {{ color:#7ee787; }} .warn {{ color:#f2cc60; }} .fail {{ color:#ff7b72; }}
 table {{ border-collapse:collapse; width:100%; margin-bottom:20px; }}
 th, td {{ border:1px solid #2a3357; padding:8px; text-align:left; }}
 th {{ background:#111735; }}
 .cards {{ display:grid; grid-template-columns: repeat(auto-fill,minmax(320px,1fr)); gap:16px; }}
 .card {{ background:#0f1733; border:1px solid #243056; border-radius:12px; padding:16px; }}
 .muted {{ color:#aab3d7; }}
 .small {{ font-size:12px; }}
 .k {{ color:#9ecbff; }} .v {{ color:#e7eaf6; }}
 .summary {{ margin-top:20px; padding:12px; background:#0f1733; border:1px solid #243056; border-radius:12px; }}
</style>
</head>
<body>

<h1>NFL Weekly Report <span class="pill">{window}</span></h1>

<h2>Games</h2>
<table>
  <thead>
    <tr>
      <th>Date</th><th>Away</th><th>Home</th><th>Status</th><th>Final</th>
      <th>Book</th><th>Spread</th><th>Total</th>
      <th>Model HWP</th><th>Brier</th>
      <th>Scheme Δ Total (pts)</th><th>Scheme Δ WinProb (pp)</th><th>Scheme Conf</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>

<h2>Game Cards</h2>
<div class="cards">
  {cards}
</div>

<div class="summary">
  <h3>Summary</h3>
  <div>Completed: <span class="ok">{completed}</span> &nbsp;&nbsp; Pending: <span class="warn">{pending}</span></div>
  <div class="small muted">Avg Brier (completed with model): {avg_brier}</div>
  <div class="small muted">Scheme confidence breakdown — HIGH: {scheme_high} &nbsp; MEDIUM: {scheme_med} &nbsp; LOW: {scheme_low}</div>
</div>

</body></html>
"""

def safe_read_csv(path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            return pd.read_csv(path)
        except Exception:
            return None
    return None

def load_core():
    msf = pd.read_csv(MSF)
    ren = {}
    if "away" in msf.columns and "away_team" not in msf.columns: ren["away"] = "away_team"
    if "home" in msf.columns and "home_team" not in msf.columns: ren["home"] = "home_team"
    if "played_status" in msf.columns and "status" not in msf.columns: ren["played_status"] = "status"
    if ren: msf = msf.rename(columns=ren)
    for c in ["date","away_team","home_team","status"]:
        if c not in msf.columns:
            raise SystemExit(f"[render][FAIL] msf_week.csv missing '{c}'")
    # Normalize types
    try:
        msf["date"] = pd.to_datetime(msf["date"]).dt.date.astype(str)
    except Exception:
        pass
    return msf

def merge_all(msf):
    # Odds (book line)
    odds = safe_read_csv(ODDS)
    if odds is not None:
        # normalize columns
        o = odds.copy()
        low = {c.lower():c for c in o.columns}
        o.columns = [c.lower() for c in o.columns]
        # expected minimal set: date, away_team, home_team, book, market_spread, market_total
        for want in ["date","away_team","home_team"]:
            if want not in o.columns:
                # try alt names
                if want=="date" and "date_utc" in o.columns:
                    o["date"] = o["date_utc"]
                else:
                    o[want] = pd.NA
        merged = msf.merge(o[["date","away_team","home_team","book","market_spread","market_total"]], 
                           on=["date","away_team","home_team"], how="left")
    else:
        merged = msf.copy()
        merged["book"] = pd.NA
        merged["market_spread"] = pd.NA
        merged["market_total"] = pd.NA

    # Predictions (home_win_prob, brier if result exists)
    pred = safe_read_csv(PRED)
    if pred is not None:
        p = pred.copy()
        # normalize
        ren = {}
        if "away" in p.columns and "away_team" not in p.columns: ren["away"]="away_team"
        if "home" in p.columns and "home_team" not in p.columns: ren["home"]="home_team"
        if ren: p = p.rename(columns=ren)
        for c in ["date","away_team","home_team","home_win_prob"]:
            if c not in p.columns:
                # create empty cols if missing
                if c == "home_win_prob":
                    p[c] = pd.NA
                else:
                    raise SystemExit(f"[render][FAIL] predictions missing '{c}'")
        # Optional brier column already in predictions? else compute if results present
        m = merged.merge(p[["date","away_team","home_team","home_win_prob"]], on=["date","away_team","home_team"], how="left")
    else:
        m = merged.copy()
        m["home_win_prob"] = pd.NA

    # Compute brier if final present & prob present
    def brier(row):
        try:
            if pd.isna(row.get("home_win_prob")): return None
            if "final_home" in row and "final_away" in row and not pd.isna(row["final_home"]) and not pd.isna(row["final_away"]):
                home_win = 1.0 if float(row["final_home"]) > float(row["final_away"]) else 0.0
                p = float(row["home_win_prob"])
                return (p - home_win) ** 2
            return None
        except Exception:
            return None
    m["brier"] = m.apply(brier, axis=1)

    # Scheme adjustments
    scheme = safe_read_csv(SCHEME)
    if scheme is not None:
        s = scheme.copy()
        s_cols = ["date","away_team","home_team","scheme_total_adj_pts","scheme_winprob_adj_pp","oc_proe_pp","oc_pace_pp","dc_aggr_pp","scheme_confidence","scheme_notes"]
        for c in s_cols:
            if c not in s.columns:
                s[c] = pd.NA
        m = m.merge(s[s_cols], on=["date","away_team","home_team"], how="left")
    else:
        for c in ["scheme_total_adj_pts","scheme_winprob_adj_pp","oc_proe_pp","oc_pace_pp","dc_aggr_pp","scheme_confidence","scheme_notes"]:
            m[c] = pd.NA

    return m

def fmt(x, nd=1):
    if pd.isna(x): return ""
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return ""

def status_cell(row):
    s = str(row.get("status","")).upper()
    if s == "COMPLETED":
        return f'<span class="pill ok">COMPLETED</span>'
    elif s in ("SCHEDULED","PREGAME","INPROGRESS","IN_PROGRESS"):
        return f'<span class="pill warn">{s}</span>'
    else:
        return f'<span class="pill">{s or "—"}</span>'

def main():
    msf = load_core()
    df = merge_all(msf)

    # window tag
    try:
        dmin = min(pd.to_datetime(df["date"])).date().strftime("%Y-%m-%d")
        dmax = max(pd.to_datetime(df["date"])).date().strftime("%Y-%m-%d")
        window = f"{dmin} → {dmax}"
    except Exception:
        window = "Week"

    # rows for table
    rows = []
    for _, r in df.iterrows():
        final = ""
        if "final_away" in r and "final_home" in r and not pd.isna(r["final_away"]) and not pd.isna(r["final_home"]):
            final = f'{int(r["final_away"])}–{int(r["final_home"])}'
        rows.append(f"""
<tr>
  <td>{r['date']}</td>
  <td>{r['away_team']}</td>
  <td>{r['home_team']}</td>
  <td>{status_cell(r)}</td>
  <td>{final}</td>
  <td>{r.get('book','') if not pd.isna(r.get('book')) else ''}</td>
  <td>{fmt(r.get('market_spread'),1)}</td>
  <td>{fmt(r.get('market_total'),1)}</td>
  <td>{fmt(r.get('home_win_prob'),3)}</td>
  <td>{fmt(r.get('brier'),3)}</td>
  <td>{fmt(r.get('scheme_total_adj_pts'),1)}</td>
  <td>{fmt(r.get('scheme_winprob_adj_pp'),1)}</td>
  <td>{r.get('scheme_confidence','') if not pd.isna(r.get('scheme_confidence')) else ''}</td>
</tr>""")

    # cards
    cards = []
    for _, r in df.iterrows():
        notes = r.get("scheme_notes","")
        if pd.isna(notes): notes = ""
        card = f"""
<div class="card">
  <div><span class="pill">{r['date']}</span></div>
  <h3>{r['away_team']} @ {r['home_team']}</h3>
  <div class="muted small">Status: {str(r.get('status',''))}</div>
  <div class="small">Book: <span class="k">spread</span> <span class="v">{fmt(r.get('market_spread'),1)}</span>,
                     <span class="k">total</span> <span class="v">{fmt(r.get('market_total'),1)}</span></div>
  <div class="small">Model HWP: <span class="v">{fmt(r.get('home_win_prob'),3)}</span> &nbsp; Brier: <span class="v">{fmt(r.get('brier'),3)}</span></div>
  <div class="small">Scheme: ΔTotal <span class="v">{fmt(r.get('scheme_total_adj_pts'),1)} pts</span>,
                     ΔWinProb <span class="v">{fmt(r.get('scheme_winprob_adj_pp'),1)} pp</span> <span class="muted">({r.get('scheme_confidence','')})</span></div>
  <div class="small muted">{notes}</div>
</div>"""
        cards.append(card)

    # summary
    status_series = df.get("status", pd.Series([])).astype(str).str.upper()
    completed = sum(1 for x in status_series.tolist() if x == "COMPLETED")
    pending = len(df) - completed

    bvals = [x for x in df.get("brier", pd.Series([])).tolist() if x is not None and not pd.isna(x)]
    avg_brier = f"{(sum(bvals)/len(bvals)):.3f}" if bvals else "—"

    sc = df.get("scheme_confidence", pd.Series([])).fillna("")
    scheme_high = sum(1 for x in sc if str(x).upper()=="HIGH")
    scheme_med  = sum(1 for x in sc if str(x).upper()=="MEDIUM")
    scheme_low  = sum(1 for x in sc if str(x).upper()=="LOW")

    html = HTML_TMPL.format(
        window=window,
        rows="\n".join(rows),
        cards="\n".join(cards),
        completed=completed,
        pending=pending,
        avg_brier=avg_brier,
        scheme_high=scheme_high,
        scheme_med=scheme_med,
        scheme_low=scheme_low
    )

    # write to reports
    os.makedirs(REPORTS, exist_ok=True)
    # produce a dated folder using min date
    folder = f"{df['date'].min()}_{df['date'].max()}_{datetime.now().strftime('%Y%m%d-%H%M')}"
    outdir = os.path.join(REPORTS, folder)
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "weekly_report.html")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[done] {outpath}")

if __name__ == "__main__":
    main()