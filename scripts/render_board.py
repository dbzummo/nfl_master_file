#!/usr/bin/env python3
import sys, os, html
from pathlib import Path
import pandas as pd

BOARD = Path("out/model_board.csv")
OUTDIR = Path("reports")
OUTHTML = OUTDIR / "board_week.html"

def fatal(msg, code=2):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(code)

def main():
    if not BOARD.exists():
        fatal("render_board.py: missing out/model_board.csv (run board step first)")
    try:
        df = pd.read_csv(BOARD)
    except Exception as e:
        fatal(f"render_board.py: failed reading board: {e}")

    if df.empty:
        fatal("render_board.py: board is empty; refusing to write report")

    # Minimal, deterministic HTML render (no external assets, no randomness)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    cols = [c for c in [
        "date","week","away_team","home_team",
        "p_home_model","vegas_line_home",
        "elo_exp_home","elo_diff_pre",
        "inj_home_pts","inj_away_pts",
        "model_line_home","edge_adj","confidence","confidence_adj",
        "msf_game_id"
    ] if c in df.columns]

    # Round a few numeric columns for display consistency
    for c, nd in [("p_home_model",6),("elo_exp_home",6),("vegas_line_home",2),
                  ("elo_diff_pre",1),("inj_home_pts",2),("inj_away_pts",2),
                  ("confidence",4),("confidence_adj",4),("model_line_home",2),("edge_adj",2)]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(nd)

    df_display = df[cols] if cols else df
    table_html = df_display.to_html(index=False, escape=False)

    meta = f"""
    <ul>
      <li>rows: {len(df)}</li>
      <li>min date: {html.escape(str(df['date'].min())) if 'date' in df else '-'}</li>
      <li>max date: {html.escape(str(df['date'].max())) if 'date' in df else '-'}</li>
    </ul>
    """

    html_doc = f"""<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="utf-8"/>
  <title>Weekly Board</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px; font-size: 14px; }}
    th {{ position: sticky; top: 0; background: #f4f4f4; }}
    .wrap {{ max-width: 1200px; margin: 24px auto; }}
    h1 {{ margin: 0 0 8px; }}
    .meta {{ color: #555; margin: 8px 0 16px; }}
  </style>
</head><body>
  <div class="wrap">
    <h1>Weekly Board</h1>
    <div class="meta">{meta}</div>
    {table_html}
  </div>
</body></html>
"""
    OUTHTML.write_text(html_doc, encoding="utf-8")
    print(f"[OK] rendered → {OUTHTML}")

if __name__ == "__main__":
    main()
