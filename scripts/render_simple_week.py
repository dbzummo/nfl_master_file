#!/usr/bin/env python3
import os, pandas as pd
from string import Template

SRC = "out/predictions_week_calibrated_blend.csv"

HTML = Template("""<!doctype html>
<html><head>
<meta charset="utf-8" />
<title>NFL Weekly Report</title>
<style>
  body { font-family: -apple-system, system-ui, Arial, sans-serif; padding: 24px; }
  h1 { margin: 0 0 8px; }
  .meta { color:#555; margin:6px 0 16px; font-size:14px; }
  table { border-collapse: collapse; width:100%; }
  th, td { border-bottom:1px solid #eee; padding:10px; text-align:left; }
  th { background:#fafafa; }
  td.r { text-align:right; }
  tr.win td { background: #f0fff4; }    /* greenish */
  tr.loss td { background: #fff5f5; }   /* reddish */
  .pill { font-size:12px; padding:2px 6px; border-radius:999px; border:1px solid #ddd; }
</style>
</head><body>
  <h1>NFL Weekly Report</h1>
  <div class="meta">$meta</div>
  <table>
    <thead>
      <tr>
        <th>Date</th><th>Away</th><th>Home</th><th class="r">P(Home)</th>
        <th class="r">Final</th><th>Status</th><th class="r">Brier</th>
      </tr>
    </thead>
    <tbody>
      $rows
    </tbody>
  </table>
</body></html>
""")

def brier(p, y):
    try:
        return (float(p) - float(y))**2
    except Exception:
        return None

def main():
    if not os.path.exists(SRC):
        raise SystemExit(f"[error] missing {SRC} (run fetch first)")

    w = pd.read_csv(SRC)
    if w.empty:
        raise SystemExit("[error] empty week csv")

    # Prepare fields
    w["P"] = w["home_win_prob"].astype(float)
    # textual final if present
    w["FinalTxt"] = w.apply(
        lambda r: (f"{int(r.final_away)}–{int(r.final_home)}"
                   if pd.notna(r.final_home) and pd.notna(r.final_away) else "—"),
        axis=1
    )
    # outcome + grading where available
    w["Y"] = w.apply(lambda r: (1.0 if str(r.get("home_win_actual")).lower()=="true" else
                                0.0 if str(r.get("home_win_actual")).lower()=="false" else None), axis=1)
    w["Brier"] = w.apply(lambda r: brier(r.P, r.Y) if r.Y is not None else None, axis=1)

    rows = []
    for _, r in w.iterrows():
        pct = f"{r.P*100:.1f}%"
        btxt = "—" if pd.isna(r.Brier) else f"{r.Brier:.3f}"
        klass = ""
        if r.Y is not None:
            klass = "win" if ((r.P>=0.5 and r.Y==1.0) or (r.P<0.5 and r.Y==0.0)) else "loss"
        rows.append(
            f"<tr class='{klass}'><td>{r.date}</td>"
            f"<td>{r.away_team}</td><td>{r.home_team}</td>"
            f"<td class='r'>{pct}</td><td class='r'>{r.FinalTxt}</td>"
            f"<td><span class='pill'>{r.status}</span></td>"
            f"<td class='r'>{btxt}</td></tr>"
        )

    meta = f"source: {SRC} — games={len(w)} — window {w['date'].min()} → {w['date'].max()}"
    html = HTML.substitute(meta=meta, rows="\n".join(rows))

    outdir = f"reports/{w['date'].min()}_{w['date'].max()}_{pd.Timestamp.utcnow().strftime('%Y%m%d-%H%M')}"
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "weekly_report.html"), "w") as f:
        f.write(html)
    w.to_csv(os.path.join(outdir, "week_table_v3.csv"), index=False)
    print(f"[done] {outdir}/weekly_report.html")

if __name__ == "__main__":
    main()