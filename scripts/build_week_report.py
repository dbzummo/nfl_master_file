#!/usr/bin/env python3
import os, json, base64, io, datetime as dt
import pandas as pd

# Inputs (non-destructive to modeling):
#  - PRED_WEEK env var preferred (e.g., out/predictions_week_calibrated_with_market.csv)
#  - else fall back to out/predictions_week_calibrated_blend.csv
PREFS = [
    os.environ.get("PRED_WEEK"),
    "out/predictions_week_calibrated_with_market.csv",
    "out/predictions_week_calibrated_blend.csv",
]

ART_DIR   = "artifacts"
CARDS_DIR = os.path.join(ART_DIR, "game_cards")
TABLE_OUT = os.path.join(ART_DIR, "week_table.csv")
HTML_OUT  = os.path.join(ART_DIR, "weekly_report.html")

REQ_COLS  = ["home_team","away_team","date","home_win_prob"]
OPT_COLS  = ["line","total"]

def pick_source():
    for p in PREFS:
        if p and os.path.isfile(p):
            return p
    raise SystemExit("No predictions CSV found. Set PRED_WEEK or create out/predictions_week_calibrated*_*.csv.")

def load_alpha_note():
    try:
        j = json.load(open("out/best_alpha.json"))
        return j.get("alpha")
    except Exception:
        return None

def fmt_prob(p):
    try:
        return f"{float(p)*100:0.1f}%"
    except Exception:
        return "—"

def fmt_line(v):
    try:
        f = float(v)
        # show +/− with one decimal
        return f"{f:+0.1f}"
    except Exception:
        return "—"

def fmt_total(v):
    try:
        f = float(v)
        return f"{f:0.1f}"
    except Exception:
        return "—"

def read_predictions():
    src = pick_source()
    df = pd.read_csv(src)
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Predictions file missing required columns: {missing}")
    # optional columns
    for c in OPT_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # normalize types
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    # sort by date then home team for stability
    df = df.sort_values(["date","home_team","away_team"]).reset_index(drop=True)

    # write a clean CSV snapshot used by HTML & (optionally) others
    snap = df.copy()
    snap["home_win_prob_pct"] = (pd.to_numeric(snap["home_win_prob"], errors="coerce")*100).round(1)
    snap.rename(columns={"home_win_prob_pct":"home_win_prob_%"}).to_csv(TABLE_OUT, index=False)
    return df, src

def find_card_for_row(r):
    """Try to find a pre-rendered card PNG that matches the row."""
    if not os.path.isdir(CARDS_DIR):
        return None
    date_str = r["date"].date().isoformat() if pd.notna(r["date"]) else "unknown"
    home = str(r["home_team"]).upper().replace(" ","_")
    away = str(r["away_team"]).upper().replace(" ","_")

    # common filename pattern from your pipeline:
    # YYYY-MM-DD_AWAY_at_HOME.png
    cand = f"{date_str}_{away}_at_{home}.png"
    p = os.path.join(CARDS_DIR, cand)
    return p if os.path.isfile(p) else None

def b64_img(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")

def build_cards_section(df):
    cards_html = []
    for _, r in df.iterrows():
        p = find_card_for_row(r)
        if p:
            cards_html.append(f'<div class="card"><img src="data:image/png;base64,{b64_img(p)}" alt="game card" /></div>')
        else:
            # graceful fallback mini-card
            line = fmt_line(r.get("line"))
            total = fmt_total(r.get("total"))
            cards_html.append(
                f'''<div class="card fallback">
                       <div class="title">{r["away_team"]} @ {r["home_team"]}</div>
                       <div class="meta">{r["date"].date() if pd.notna(r["date"]) else "—"}</div>
                       <div class="row"><span>P(Home)</span><span>{fmt_prob(r["home_win_prob"])}</span></div>
                       <div class="row"><span>Line</span><span>{line}</span></div>
                       <div class="row"><span>Total</span><span>{total}</span></div>
                    </div>'''
            )
    return "\n".join(cards_html)

def build_table_section(df):
    # build HTML table with alternating row shading, graceful “—” for missing
    head = "<tr><th>Date</th><th>Away</th><th>Home</th><th>P(Home)</th><th>Line</th><th>Total</th></tr>"
    rows = []
    for i, r in df.iterrows():
        cls = "odd" if (i % 2) else "even"
        date_str = r["date"].strftime("%Y-%m-%d") if pd.notna(r["date"]) else "—"
        rows.append(
            f"<tr class='{cls}'>"
            f"<td>{date_str}</td>"
            f"<td>{r['away_team']}</td>"
            f"<td>{r['home_team']}</td>"
            f"<td>{fmt_prob(r['home_win_prob'])}</td>"
            f"<td>{fmt_line(r['line'])}</td>"
            f"<td>{fmt_total(r['total'])}</td>"
            f"</tr>"
        )
    return f"<table><thead>{head}</thead><tbody>\n" + "\n".join(rows) + "\n</tbody></table>"

def date_range_label(df):
    ds = df["date"].dropna()
    if ds.empty: return "Week: (dates unavailable)"
    lo, hi = ds.min().date(), ds.max().date()
    if lo == hi: return f"Week of {lo}"
    return f"{lo} — {hi}"

def build_html(df, src_path):
    os.makedirs(ART_DIR, exist_ok=True)
    alpha = load_alpha_note()
    alpha_str = f" • blend α={alpha}" if alpha is not None else ""
    when = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    date_lbl = date_range_label(df)

    table_html = build_table_section(df)
    cards_html = build_cards_section(df)

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>NFL Weekly Report</title>
<style>
 body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 24px; background:#fff; color:#222; }}
 h1 {{ margin: 0 0 8px 0; font-size: 28px; }}
 .meta {{ color:#555; margin-bottom: 18px; font-size: 13px; }}
 table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
 th, td {{ padding: 10px 8px; text-align: left; border-bottom: 1px solid #eee; }}
 th {{ background: #fafafa; position: sticky; top: 0; z-index: 1; }}
 tr.even {{ background: #fff; }}
 tr.odd  {{ background: #fcfcfc; }}
 .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 14px; margin-top: 18px; }}
 .card {{ border: 1px solid #eee; border-radius: 12px; padding: 10px; background: #fff; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }}
 .card img {{ width: 100%; height: auto; display:block; border-radius: 8px; }}
 .card.fallback .title {{ font-weight: 600; margin-bottom: 6px; }}
 .card.fallback .meta  {{ color:#666; font-size: 12px; margin-bottom: 8px; }}
 .card.fallback .row   {{ display:flex; justify-content: space-between; font-size: 13px; padding: 4px 0; border-top: 1px dashed #eee; }}
 .footer {{ color:#666; font-size: 12px; margin-top: 24px; }}
 .small  {{ color:#666; font-size: 12px; }}
</style>
</head>
<body>
  <h1>NFL Weekly Report</h1>
  <div class="meta small">
    {date_lbl} • source: {os.path.basename(src_path)}{alpha_str} • calibrated
  </div>

  <h2>Games Table</h2>
  {table_html}

  <h2>Game Cards</h2>
  <div class="grid">
    {cards_html}
  </div>

  <div class="footer">Generated {when}</div>
</body>
</html>
"""
    with open(HTML_OUT, "w", encoding="utf-8") as f:
        f.write(html)

def main():
    df, src = read_predictions()
    build_html(df, src)
    print(f"Wrote {TABLE_OUT}")
    print(f"Wrote {HTML_OUT}")
    print(f"Cards dir (reused if present): {CARDS_DIR}")

if __name__ == "__main__":
    main()
