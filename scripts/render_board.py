#!/usr/bin/env python3
import os, sys, html, json
from pathlib import Path
import pandas as pd

IN_CSV = Path("out/model_board.csv")
OUT_DIR = Path("reports")
OUT_HTML = OUT_DIR / "board_week.html"

def fatal(msg: str, code: int = 2):
    print(f"[FATAL] render_board.py: {msg}", file=sys.stderr)
    sys.exit(code)

def main():
    # IO preconditions
    if not IN_CSV.exists():
        fatal("missing out/model_board.csv (upstream build step did not emit board)")

    try:
        df = pd.read_csv(IN_CSV)
    except Exception as e:
        fatal(f"unable to read {IN_CSV}: {e}")

    # Always create reports directory
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Deterministic ordering for auditability
    order_cols = [c for c in ["date","home_team","away_team"] if c in df.columns]
    if order_cols:
        df = df.sort_values(order_cols, kind="mergesort").reset_index(drop=True)

    # Minimal, schema-flex HTML table — renders whatever columns exist
    cols = list(df.columns)
    row_count = int(len(df))
    # Soft sanity: board can be empty, but we still render a page
    note = ""
    if row_count == 0:
        note = "<p style='color:#c00'><strong>NOTE:</strong> Board has 0 rows. Upstream produced an empty board; rendering anyway for traceability.</p>"

    # Metadata for reproducibility
    env_meta = {
        "START": os.environ.get("START",""),
        "END": os.environ.get("END",""),
        "SEASON": os.environ.get("SEASON",""),
    }

    def ths(cols):
        return "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)

    def tds(row):
        return "".join(f"<td>{html.escape(str(v))}</td>" for v in row)

    # Build table body (streamed to avoid huge strings if needed)
    rows_html = []
    if row_count:
        for _, r in df.iterrows():
            rows_html.append(f"<tr>{tds([r.get(c,'') for c in cols])}</tr>")

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>NFL Week Board</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
 body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
 h1, h2 {{ margin: 0 0 12px; }}
 table {{ border-collapse: collapse; width: 100%; }}
 th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 14px; }}
 th {{ background: #f6f6f6; text-align: left; position: sticky; top: 0; }}
 .meta {{ font-size: 12px; color: #555; margin-bottom: 12px; }}
 .count {{ margin: 12px 0; }}
</style>
</head>
<body>
  <h1>Weekly Model Board</h1>
  <div class="meta">
    <div>Window: <code>{html.escape(env_meta["START"])}</code> … <code>{html.escape(env_meta["END"])}</code></div>
    <div>Season: <code>{html.escape(env_meta["SEASON"])}</code></div>
    <div>Source CSV: <code>{html.escape(str(IN_CSV))}</code></div>
  </div>
  <div class="count">Rows: <strong>{row_count}</strong></div>
  {note}
  <div style="overflow:auto; max-height: 80vh; border: 1px solid #eee;">
    <table>
      <thead><tr>{ths(cols)}</tr></thead>
      <tbody>
        {"".join(rows_html)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    OUT_HTML.write_text(html_doc, encoding="utf-8")
    print(f"[OK] wrote {OUT_HTML} rows={row_count}")

if __name__ == "__main__":
    main()
