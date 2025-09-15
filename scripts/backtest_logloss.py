#!/usr/bin/env python3
import argparse, pathlib
import pandas as pd
import numpy as np

def safe_clip(p):
    return np.clip(p.astype(float), 1e-12, 1 - 1e-12)

def logloss_vec(p, y):
    p = safe_clip(p)
    return -(y*np.log(p) + (1-y)*np.log(1-p))

def derive_season_from_date(df, date_col="date"):
    d = pd.to_datetime(df[date_col], errors="coerce")
    # NFL season “year” starts in August: Aug–Dec -> same year; Jan–Jul -> previous year
    yr = d.dt.year
    mon = d.dt.month
    season = np.where(mon >= 8, yr, yr - 1)
    return pd.Series(season, index=df.index)

def main():
    ap = argparse.ArgumentParser(description="Backtest Elo (exp_home) probabilities on history.")
    ap.add_argument("--infile", default="out/elo_games_enriched.csv",
                    help="CSV with columns: date, home_team, away_team, exp_home, home_score, away_score (season optional)")
    ap.add_argument("--outdir", default="out/backtest", help="Output directory")
    args = ap.parse_args()

    src = pathlib.Path(args.infile)
    if not src.exists():
        raise SystemExit(f"[FATAL] Missing {src}. Run the Elo step first.")

    df = pd.read_csv(src)

    need_base = {"date","home_team","away_team","exp_home","home_score","away_score"}
    missing = [c for c in need_base if c not in df.columns]
    if missing:
        raise SystemExit(f"[FATAL] {src} missing columns: {missing}")

    # Ensure season exists (derive if absent)
    if "season" not in df.columns:
        df["season"] = derive_season_from_date(df, "date")
        print("[INFO] Derived 'season' from 'date' (NFL Aug→Dec = same year; Jan→Jul = previous year).")

    # outcome + metrics
    y = (df["home_score"].astype(float) > df["away_score"].astype(float)).astype(int)
    p = df["exp_home"].astype(float)

    df_out = df.copy()
    df_out["y_homewin"] = y
    df_out["prob_home"] = p
    df_out["brier"] = (p - y)**2
    df_out["logloss"] = logloss_vec(p, y)

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    details_csv = outdir / "backtest_details.csv"
    cols = ["date","season","away_team","home_team","prob_home","y_homewin","brier","logloss"]
    df_out[cols].to_csv(details_csv, index=False)

    overall = pd.DataFrame({
        "games":[len(df_out)],
        "brier":[df_out["brier"].mean()],
        "logloss":[df_out["logloss"].mean()]
    })
    by_season = (df_out
                 .groupby("season", as_index=False)
                 .agg(games=("logloss","count"),
                      brier=("brier","mean"),
                      logloss=("logloss","mean"))
                 .sort_values("season"))

    by_season_csv = outdir / "summary_by_season.csv"
    overall_csv = outdir / "summary_overall.csv"
    by_season.to_csv(by_season_csv, index=False)
    overall.to_csv(overall_csv, index=False)

    print(f"[BACKTEST] wrote {details_csv} rows={len(df_out)}")
    print("[BACKTEST] overall:")
    print(overall.to_string(index=False, float_format=lambda x: f"{x:.6f}"))
    print("\n[BACKTEST] by season:")
    print(by_season.to_string(index=False, float_format=lambda x: f"{x:.6f}"))

    # Minimal HTML
    html = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Backtest Report</title>",
        "<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#0f1115;color:#e6e6e6;padding:24px}",
        "h1,h2{margin:0 0 12px} table{border-collapse:collapse;width:100%;margin:12px 0}",
        "th,td{padding:8px 10px;border-bottom:1px solid #2a2f3a;text-align:left} th{color:#9fb4ff}",
        ".kpi{display:flex;gap:24px;margin:12px 0 20px}.card{background:#151924;border:1px solid #22293a;border-radius:10px;padding:12px 14px}",
        ".mono{font-variant-numeric:tabular-nums}.muted{color:#9aa3b2}</style></head><body>",
        "<h1>Backtest Report</h1>",
        "<div class='kpi'>",
        f"<div class='card'><div class='muted'>Games</div><div class='mono'>{len(df_out)}</div></div>",
        f"<div class='card'><div class='muted'>Brier (overall)</div><div class='mono'>{overall['brier'].iloc[0]:.4f}</div></div>",
        f"<div class='card'><div class='muted'>Log Loss (overall)</div><div class='mono'>{overall['logloss'].iloc[0]:.4f}</div></div>",
        "</div>",
        "<h2>By Season</h2>",
        by_season.rename(columns={"season":"Season","games":"Games","brier":"Brier","logloss":"Log Loss"}).to_html(index=False),
        "<h2>Sample (last 20)</h2>",
        df_out[["date","away_team","home_team","prob_home","y_homewin","brier","logloss"]]
            .tail(20)
            .rename(columns={"prob_home":"P(Home)","y_homewin":"Home Win"})
            .to_html(index=False),
        "</body></html>"
    ]
    (outdir / "backtest_report.html").write_text("\n".join(html), encoding="utf-8")
    print(f"[BACKTEST] wrote {outdir / 'backtest_report.html'}")

if __name__ == "__main__":
    main()