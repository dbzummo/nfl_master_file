#!/usr/bin/env python3
import os, math, json
from pathlib import Path
import pandas as pd
import numpy as np

BOARD_CSV  = Path("out/model_board.csv")
BOARD_HTML = Path("reports/board_week.html")
CALIB_PATH = Path("out/calibration/model_line_calibration.json")  # {"a":..., "b":...}

NORM = {"LAR":"LA","STL":"LA","SD":"LAC","OAK":"LV","WSH":"WAS","JAC":"JAX","NO":"NO"}

def log(msg): print(msg)

def norm_abbr(df):
    for c in ("home_abbr","away_abbr"):
        if c in df.columns: df[c] = df[c].replace(NORM)
    return df

def pick_base_prob(df):
    for nm in ("p_home_cal_iso","p_home_cal_platt","elo_exp_home","p_home_raw","p_home_model"):
        if nm in df.columns: return nm
    raise SystemExit("[FATAL] No base probability column found (looked for p_home_cal_iso, p_home_cal_platt, elo_exp_home, p_home_raw, p_home_model)")

def load_week_with_market():
    wk = pd.read_csv("out/week_with_elo.csv")
    mk = pd.read_csv("out/week_with_market.csv")[["home_abbr","away_abbr","market_p_home"]]
    wk = norm_abbr(wk); mk = norm_abbr(mk)
    merged = wk.merge(mk, on=["home_abbr","away_abbr"], how="inner")
    base_col = pick_base_prob(merged)
    merged["p_home_model"] = merged[base_col].astype(float)
    return merged

def injuries_blend(merged):
    """Compute pre/post injury (raw), and dp."""
    merged = merged.copy()
    merged["p_home_pre_injury"]   = np.nan
    merged["p_home_post_injury_raw"] = np.nan
    merged["dp_injury_raw"]       = np.nan
    inj_path = Path("out/injury_adjustments.csv")
    if not inj_path.exists():
        return merged

    try:
        inj = pd.read_csv(inj_path)[["home_abbr","away_abbr","elo_delta_home","elo_delta_away"]]
        merged = merged.merge(inj, on=["home_abbr","away_abbr"], how="left")
        merged[["elo_delta_home","elo_delta_away"]] = merged[["elo_delta_home","elo_delta_away"]].fillna(0.0)

        # Pre-injury prob (clip), convert to logit
        prev_p = merged["p_home_model"].astype(float).clip(1e-6, 1-1e-6)
        k = math.log(10.0)/400.0
        z0 = np.log(prev_p/(1-prev_p))
        z1 = z0 + k*(merged["elo_delta_home"].astype(float).values - merged["elo_delta_away"].astype(float).values)
        post_raw = 1/(1+np.exp(-z1))

        merged["p_home_pre_injury"]     = prev_p.values
        merged["p_home_post_injury_raw"]= post_raw
        merged["dp_injury_raw"]         = merged["p_home_post_injury_raw"] - merged["p_home_pre_injury"]
    except Exception as e:
        print(f"[WARN] injuries blending failed: {e}")
    return merged

def apply_calibration_after_injuries(merged):
    """Apply Platt-style calibration (on logit) if out/calibration/model_line_calibration.json exists."""
    merged = merged.copy()
    merged["p_home_post_injury_cal"] = np.nan
    if not CALIB_PATH.exists():
        # fall back: use raw (already in p_home_post_injury_raw or pre-injury if injuries absent)
        if "p_home_post_injury_raw" in merged.columns and merged["p_home_post_injury_raw"].notna().any():
            merged["p_home_model"] = merged["p_home_post_injury_raw"]
        # else leave p_home_model as it was
        merged["calibration_used"] = 0
        return merged

    try:
        cal = json.loads(CALIB_PATH.read_text(encoding="utf-8"))
        a = float(cal["a"]); b = float(cal["b"])

        # sanity gate: only apply if mapping is monotone-increasing and non-collapsing
        def _logit(p):
            import numpy as _np
            p = _np.clip(p, 1e-6, 1-1e-6)
            return _np.log(p/(1-p))
        def _cal(pp):
            import numpy as _np
            z = _logit(pp)
            z2 = a*z + b
            return 1/(1+_np.exp(-z2))
        probe = [0.2, 0.5, 0.8]
        outp  = [float(_cal(x)) for x in probe]
        monotone = (outp[0] < outp[1] < outp[2])
        collapsed = (max(outp) - min(outp) < 0.10)  # everything squashed near 0.5
        if not monotone or collapsed:
            print("[WARN] Calibration mapping failed sanity (monotone:", monotone, "collapsed:", collapsed, ") — using raw post-injury.")
            # fall back to raw
            if "p_home_post_injury_raw" in merged.columns and merged["p_home_post_injury_raw"].notna().any():
                merged["p_home_model"] = merged["p_home_post_injury_raw"]
            merged["calibration_used"] = 0
            return merged

        # choose the best "post-injury" to calibrate; if injuries missing, calibrate current p_home_model
        if "p_home_post_injury_raw" in merged.columns and merged["p_home_post_injury_raw"].notna().any():
            base = merged["p_home_post_injury_raw"].astype(float).clip(1e-6, 1-1e-6)
        else:
            base = merged["p_home_model"].astype(float).clip(1e-6, 1-1e-6)
        z = np.log(base/(1-base))
        p_cal = 1/(1+np.exp(-(a*z + b)))
        merged["p_home_post_injury_cal"] = p_cal
        merged["p_home_model"] = merged["p_home_post_injury_cal"]
        merged["calibration_used"] = 1
    except Exception as e:
        print(f"[WARN] calibration apply failed: {e}")
        # fall back to raw
        if "p_home_post_injury_raw" in merged.columns and merged["p_home_post_injury_raw"].notna().any():
            merged["p_home_model"] = merged["p_home_post_injury_raw"]
        merged["calibration_used"] = 0
    return merged

def write_board(merged):
    merged = merged.copy()
    merged["edge"] = merged["p_home_model"].astype(float) - merged["market_p_home"].astype(float)

    # Ensure schema (stable columns)
    for c in ("p_home_pre_injury","p_home_post_injury_raw","p_home_post_injury_cal","dp_injury_raw","calibration_used"):
        if c not in merged.columns: merged[c] = np.nan

    cols = [
        "home_abbr","away_abbr",
        "p_home_pre_injury","p_home_post_injury_raw","p_home_post_injury_cal","dp_injury_raw",
        "p_home_model","market_p_home","edge","calibration_used"
    ]
    merged[cols].to_csv(BOARD_CSV, index=False)
    print(f"[OK] Wrote {BOARD_CSV} (rows={len(merged)})")

    # HTML
    def fmt(x):
        try: return f"{float(x):.3f}"
        except: return ""
    html = [
        "<html><head><style>",
        "body{font-family:-apple-system,Segoe UI,Arial;margin:16px}",
        "table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px 8px}",
        "td.pos{color:#0a0}td.neg{color:#b00}.muted{color:#666;font-size:12px}",
        "</style></head><body>"
    ]
    # Provenance banner
    inj_ts = ""
    inj_jsons = sorted(Path("raw/msf/injuries").glob("*.json"))
    if inj_jsons: inj_ts = inj_jsons[-1].name.replace(".json","")
    html.append(f"<div class='muted'>calibration_used={int(merged['calibration_used'].fillna(0).max())} | injuries_snapshot={inj_ts}</div>")
    html.append("<h2>Model vs Market</h2>")
    html.append("<table><thead><tr>"
                "<th>home</th><th>away</th>"
                "<th>p_pre</th><th>p_post_raw</th><th>p_post_cal</th><th>dp_raw</th>"
                "<th>p_model</th><th>market</th><th>edge</th>"
                "</tr></thead><tbody>")
    for _, r in merged.iterrows():
        edge = float(r["edge"])
        cls  = "pos" if edge>=0 else "neg"
        html.append(
            "<tr>"
            f"<td>{r['home_abbr']}</td><td>{r['away_abbr']}</td>"
            f"<td>{fmt(r['p_home_pre_injury'])}</td>"
            f"<td>{fmt(r['p_home_post_injury_raw'])}</td>"
            f"<td>{fmt(r['p_home_post_injury_cal'])}</td>"
            f"<td>{fmt(r['dp_injury_raw'])}</td>"
            f"<td>{fmt(r['p_home_model'])}</td>"
            f"<td>{fmt(r['market_p_home'])}</td>"
            f"<td class='{cls}'>{fmt(edge)}</td>"
            "</tr>"
        )
    html.append("</tbody></table></body></html>")
    BOARD_HTML.parent.mkdir(parents=True, exist_ok=True)
    BOARD_HTML.write_text("".join(html), encoding="utf-8")
    print(f"[OK] Rendered HTML board -> {BOARD_HTML}")

def main():
    merged = load_week_with_market()
    merged = injuries_blend(merged)               # Elo → injuries
    merged = apply_calibration_after_injuries(merged)  # → calibration (if present)
    write_board(merged)

if __name__ == "__main__":
    main()
