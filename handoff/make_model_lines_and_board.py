#!/usr/bin/env python3
import pandas as pd, pathlib, math, sys

BOARD_HTML = pathlib.Path("reports/board_week.html")

NORM = {
    "LAR":"LA", "STL":"LA", "SD":"LAC", "OAK":"LV",
    "WSH":"WAS", "JAX":"JAX", "NO":"NO",
}

def norm_abbr(df):
    for c in ("home_abbr","away_abbr"):
        if c in df.columns:
            df[c] = df[c].replace(NORM)
    return df

def load_csv(path):
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[FATAL] failed to read {path}: {e}", file=sys.stderr)
        sys.exit(2)
    return df

def log(s): print(s, flush=True)

def main():
    elo  = norm_abbr(load_csv("out/week_with_elo.csv"))
    mkt  = norm_abbr(load_csv("out/week_with_market.csv"))

    need_elo = {"home_abbr","away_abbr","p_home_model"}
    need_mkt = {"home_abbr","away_abbr","market_p_home"}
    if not need_elo.issubset(elo.columns):
        missing = sorted(list(need_elo - set(elo.columns)))
        print(f"[FATAL] week_with_elo.csv missing cols {missing}", file=sys.stderr); sys.exit(2)
    if not need_mkt.issubset(mkt.columns):
        missing = sorted(list(need_mkt - set(mkt.columns)))
        print(f"[FATAL] week_with_market.csv missing cols {missing}", file=sys.stderr); sys.exit(2)

    # Merge ONLY on teams; timestamps vary across feeds and cause false mismatches
    merged = (elo.merge(mkt[["home_abbr","away_abbr","market_p_home"]],
                        on=["home_abbr","away_abbr"], how="inner").copy())

    # Edge = model - market
    merged["edge"] = merged["p_home_model"] - merged["market_p_home"]

    # Light sanity
    k = merged["home_abbr"] + "@" + merged["away_abbr"]
    log(f"[OK] model_board rows={len(merged)} unique_pairs={k.nunique()}")
    nulls = merged["edge"].isna().sum()
    if nulls: log(f"[WARN] {nulls} edges are null; check inputs")

    # Persist CSV
    out_csv = pathlib.Path("out/model_board.csv")
    cols = [c for c in ["home_abbr","away_abbr","p_home_model","market_p_home","edge"] if c in merged.columns]
    merged[cols].to_csv(out_csv, index=False)
    log(f"[OK] Wrote {out_csv} (rows={len(merged)})")

    # Minimal HTML render
    html = """<!doctype html><html><head><meta charset="utf-8"><title>Week Board</title>
<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}
table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:6px}
th{background:#f2f2f2;text-align:left} .neg{color:#b00020} .pos{color:#006400}
</style></head><body><h2>Model vs Market</h2><table><thead><tr>
<th>Home</th><th>Away</th><th>Model P(Home)</th><th>Market P(Home)</th><th>Edge</th>
</tr></thead><tbody>
"""
    for _,r in merged[cols].sort_values("edge", ascending=False).iterrows():
        edge = r["edge"]
        cls = "pos" if edge>=0 else "neg"
        html += f"<tr><td>{r['home_abbr']}</td><td>{r['away_abbr']}</td>" \
                f"<td>{r['p_home_model']:.3f}</td><td>{r['market_p_home']:.3f}</td>" \
                f"<td class='{cls}'>{edge:.3f}</td></tr>\n"
    html += "</tbody></table></body></html>"
    BOARD_HTML.parent.mkdir(parents=True, exist_ok=True)
    BOARD_HTML.write_text(html, encoding="utf-8")
    log(f"[OK] Rendered HTML board -> {BOARD_HTML}")

if __name__ == "__main__":
    main()
