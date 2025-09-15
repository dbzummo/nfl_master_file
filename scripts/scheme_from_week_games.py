#!/usr/bin/env python3
import argparse
import pathlib
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True)
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--games", default="out/ingest/week_games.csv")
    ap.add_argument("--out", default="out/scheme_features_week.csv")
    ap.add_argument("--source", default="seed", help="value for the `source` column")
    args = ap.parse_args()

    p = pathlib.Path(args.games)
    if not p.exists():
        raise SystemExit(f"[scheme-from-games][ERR] missing {p}")

    g = pd.read_csv(p)
    req = {"date","away_team","home_team"}
    if g.empty or not req.issubset(g.columns):
        raise SystemExit("[scheme-from-games][ERR] week_games.csv missing required columns or is empty")

    # normalize date -> YYYY-MM-DD
    g["date"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d")

    rows = []
    # exactly 3 features per (date,team)
    FEATURES = (
        ("scheme_run_pct",  0.50),
        ("scheme_pass_pct", 0.50),
        ("scheme_confidence", ""),  # must be empty string, not NaN
    )

    for _, r in g.iterrows():
        away = str(r["away_team"]).strip().upper()
        home = str(r["home_team"]).strip().upper()
        date = str(r["date"]).strip()
        for team in (away, home):
            if not team:
                continue
            for feat, val in FEATURES:
                rows.append({
                    "date": date,
                    "team": team,
                    "feature": feat,
                    "value": val,
                    "source": args.source,
                })

    out = pd.DataFrame(rows, columns=["date","team","feature","value","source"])

    # Force scheme_confidence to empty string (guard against NaN coercion)
    mask_conf = out["feature"].eq("scheme_confidence")
    out.loc[mask_conf, "value"] = out.loc[mask_conf, "value"].astype(str).fillna("")

    # Sort & write
    out.sort_values(["date","team","feature"], inplace=True)
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)

    uniq = out[["date","team"]].drop_duplicates().shape[0]
    print(f"[scheme-from-games][ok] wrote {args.out} rows={len(out)} | unique (date,team)={uniq} | source={args.source}")

if __name__ == "__main__":
    main()