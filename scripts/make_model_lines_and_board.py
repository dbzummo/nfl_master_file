#!/usr/bin/env python3
# scripts/make_model_lines_and_board.py
#
# Purpose
# - Build the weekly model board using the **MSF week window** (dmin→dmax), not “future-only”.
# - Always produce:
#     - out/model_board.csv
#     - reports/board_week.html
# - Be column-flexible for probabilities (prefer calibrated).
#
# Inputs
#   out/week_predictions.csv      (required)
#   out/msf_details/msf_week.csv  (preferred for week window; falls back to preds date span)
#
# Output schema (CSV)
#   date, away_team, home_team, p_home, [msf_game_id if available]
#
# Notes
# - If zero rows after filtering, writes an empty board HTML and a 0-row CSV (no crash).
# - Canonical p_home preference: p_home → p_home_cal_platt → p_home_cal_iso → p_home_raw → elo_exp_home

from pathlib import Path
import pandas as pd
import html as _html
import webbrowser

PRED_PATH = Path("out/week_predictions.csv")
MSF_WEEK  = Path("out/msf_details/msf_week.csv")
OUT_CSV   = Path("out/model_board.csv")
REPORTS   = Path("reports")
HTML_PATH = REPORTS / "board_week.html"


def _load_predictions() -> pd.DataFrame:
    if not PRED_PATH.exists():
        raise FileNotFoundError(f"Missing {PRED_PATH}")

    df = pd.read_csv(PRED_PATH)

    # Normalize key fields to string for safety
    for c in ("date", "away_team", "home_team"):
        if c in df.columns:
            df[c] = df[c].astype(str)

    # Ensure we have a canonical p_home
    pref = [
        "p_home",
        "p_home_cal_platt",
        "p_home_cal_iso",
        "p_home_raw",
        "elo_exp_home",
    ]
    src = next((c for c in pref if c in df.columns), None)
    if src is None:
        raise ValueError(f"Could not find any probability column among: {pref}")

    if "p_home" not in df.columns:
        df["p_home"] = df[src].astype(float).clip(0.0, 1.0)
    else:
        # Make sure it's numeric 0..1
        df["p_home"] = pd.to_numeric(df["p_home"], errors="coerce").fillna(0.0).clip(0.0, 1.0)

    # Convert date to datetime for window filtering
    try:
        df["_dt"] = pd.to_datetime(df["date"])
    except Exception as e:
        raise ValueError(f"Failed to parse dates in {PRED_PATH}: {e}")

    return df


def _week_window(pred_df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Return (dmin, dmax) for the MSF week window if available;
    otherwise fall back to span of predictions.
    """
    if MSF_WEEK.exists():
        try:
            w = pd.read_csv(MSF_WEEK)
            if "date" in w.columns and len(w) > 0:
                dmin = pd.to_datetime(w["date"]).min()
                dmax = pd.to_datetime(w["date"]).max()
                if pd.notna(dmin) and pd.notna(dmax):
                    return dmin.normalize(), dmax.normalize()
        except Exception:
            pass

    # Fallback: use predictions span
    dmin = pred_df["_dt"].min().normalize()
    dmax = pred_df["_dt"].max().normalize()
    return dmin, dmax


def _filter_to_week(df: pd.DataFrame, dmin: pd.Timestamp, dmax: pd.Timestamp) -> pd.DataFrame:
    mask = (df["_dt"] >= dmin) & (df["_dt"] <= dmax)
    out = df.loc[mask].copy()
    return out


def _write_csv(df: pd.DataFrame) -> int:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    keep = [c for c in ["date", "away_team", "home_team", "p_home", "msf_game_id"] if c in df.columns]
    out = df[keep].sort_values("date").reset_index(drop=True)
    out.to_csv(OUT_CSV, index=False)
    return out.shape[0]


def _write_html(df: pd.DataFrame, dmin: pd.Timestamp, dmax: pd.Timestamp) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)

    def tr(r) -> str:
        d = _html.escape(str(r["date"]))
        matchup = f"{_html.escape(str(r['away_team']))}@{_html.escape(str(r['home_team']))}"
        ph = f"{float(r['p_home']):.3f}" if pd.notna(r["p_home"]) else "—"
        return f"<tr><td>{d}</td><td>{matchup}</td><td>{ph}</td></tr>"

    rows = "\n".join(tr(r) for _, r in df.iterrows())

    empty_msg = ""
    if df.empty:
        empty_msg = "<p><em>No games after applying the MSF week window.</em></p>"

    html_doc = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Model Board</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 24px; }}
    h2 {{ margin: 0 0 8px; }}
    .meta {{ color: #555; margin-bottom: 16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background: #f6f6f6; }}
    tbody tr:nth-child(even) {{ background: #fafafa; }}
    .muted {{ color: #777; font-size: 0.9em; }}
  </style>
</head>
<body>
  <h2>Week Board</h2>
  <div class="meta">Date window: {dmin.date()} → {dmax.date()}</div>
  {empty_msg}
  <table>
    <thead>
      <tr><th>Date</th><th>Matchup</th><th>P(Home)</th></tr>
    </thead>
    <tbody>
      {rows if rows else '<tr><td colspan="3" class="muted">No rows</td></tr>'}
    </tbody>
  </table>
</body>
</html>
"""
    HTML_PATH.write_text(html_doc, encoding="utf-8")

    # Try to open the HTML (matches previous behavior)
    try:
        webbrowser.open(HTML_PATH.as_uri())
    except Exception:
        pass


def main() -> None:
    # Load predictions and determine MSF week window
    pred = _load_predictions()
    dmin, dmax = _week_window(pred)

    # Filter to week window
    week = _filter_to_week(pred, dmin, dmax)

    # Write outputs (never crash on empty)
    nrows = _write_csv(week)
    print(f"[OK] wrote {OUT_CSV}  rows={nrows}")

    _write_html(week, dmin, dmax)
    print(f"[OK] wrote {HTML_PATH}")

if __name__ == "__main__":
    main()