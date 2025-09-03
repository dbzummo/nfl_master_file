#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import numpy as np
import datetime as dt

PRED = Path("predictions_week.csv")
OUT_HTML = Path("weekly_report.html")

def _fmt_pct(x):
    return f"{100*x:.1f}%"

def load_predictions():
    if not PRED.exists():
        raise SystemExit(f"⛔ Missing {PRED}")
    df = pd.read_csv(PRED)

    # Prefer calibrated probs; otherwise fall back to raw
    prob_cols_pref = ["home_win_prob_cal", "home_winprob", "home_win_prob", "prob", "p", "pred"]
    prob_col = next((c for c in prob_cols_pref if c in df.columns), None)
    if not prob_col:
        raise SystemExit("⛔ No probability-like column found in predictions_week.csv.")

    df["prob"] = df[prob_col].astype(float).clip(1e-6, 1-1e-6)

    # Common convenience columns
    if "spread_home" in df.columns:
        df["spread_home"] = pd.to_numeric(df["spread_home"], errors="coerce")
    if "total" in df.columns:
        df["total"] = pd.to_numeric(df["total"], errors="coerce")

    # Implied edge vs 50-50
    df["edge_vs_coin"] = (df["prob"] - 0.5)

    # Sort strongest edges first
    df = df.sort_values("edge_vs_coin", ascending=False).reset_index(drop=True)
    return df, prob_col

def build_html(df: pd.DataFrame, prob_src: str) -> str:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    n = len(df)

    # Basic summary
    calib_flag = ("home_win_prob_cal" == prob_src)
    title = "Weekly Predictions (Calibrated)" if calib_flag else "Weekly Predictions"
    subtitle = f"Probability source: <code>{prob_src}</code> • Generated {now}"

    # Select display columns if present
    cols = [c for c in [
        "kickoff_utc", "home_team", "away_team",
        "spread_home", "total",
        "prob", "edge_vs_coin",
        "vegas_line", "vegas_total"
    ] if c in df.columns]

    df_disp = df.copy()
    if "kickoff_utc" in df_disp.columns:
        # nicer time
        df_disp["kickoff_utc"] = df_disp["kickoff_utc"].astype(str)
    if "prob" in df_disp.columns:
        df_disp["prob"] = df_disp["prob"].map(_fmt_pct)
    if "edge_vs_coin" in df_disp.columns:
        df_disp["edge_vs_coin"] = (100*df_disp["edge_vs_coin"]).map(lambda x: f"{x:+.1f} pp")

    # HTML table
    table_html = df_disp[cols].to_html(index=False, escape=False)

    # Put it together
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
h1 {{ margin-bottom: 4px; }}
h2 {{ margin-top: 0; color: #555; font-weight: 500; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; }}
th {{ background: #f7f7f7; text-align: left; }}
tr:nth-child(even) {{ background: #fafafa; }}
code {{ background: #f0f0f0; padding: 2px 4px; border-radius: 4px; }}
.badge {{ display:inline-block; padding: 2px 6px; border-radius: 8px; font-size:12px; border:1px solid #ccc; }}
</style>
</head>
<body>
  <h1>{title}</h1>
  <h2>{subtitle}</h2>
  <p><span class="badge">games: {n}</span>
     <span class="badge">calibrated: {"yes" if calib_flag else "no"}</span></p>
  {table_html}
</body>
</html>"""

def main():
    df, prob_src = load_predictions()
    html = build_html(df, prob_src)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"✅ Wrote {OUT_HTML} • using prob source: {prob_src}")

if __name__ == "__main__":
    main()