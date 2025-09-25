#!/usr/bin/env python3
import os, json, math, glob, sys, traceback
import numpy as np
import pandas as pd

OK   = "[OK]"
WARN = "[WARN]"
INFO = "[INFO]"

P_OUT  = "out/calibration/model_line_calibration.json"
P_META = "out/calibration/meta.json"
P_SAMP = "out/calibration/train_sample.csv"

def _logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1-1e-6)
    return np.log(p/(1-p))

def _inv(z):
    z = np.asarray(z, float)
    return 1/(1+np.exp(-z))

def _logloss(p, y):
    p = np.clip(np.asarray(p, float), 1e-9, 1-1e-9)
    y = np.asarray(y, int)
    return float(-(y*np.log(p) + (1-y)*np.log(1-p)).mean())

def _write_json_identity(n, reason):
    os.makedirs(os.path.dirname(P_OUT), exist_ok=True)
    with open(P_OUT, "w") as f:
        json.dump({"a": 1.0, "b": 0.0, "n": int(n), "reason": reason}, f)
    print(OK, f"Wrote identity calibration -> {P_OUT} (reason: {reason})")

def _safe_write_meta(df_like, a, b, base_ll, cal_ll):
    os.makedirs(os.path.dirname(P_META), exist_ok=True)
    meta = {
        "n_rows": int(len(df_like)) if df_like is not None else 0,
        "a": a if (a is None or isinstance(a, (int,float))) else float(a),
        "b": b if (b is None or isinstance(b, (int,float))) else float(b),
        "base_logloss": base_ll if base_ll is None else float(base_ll),
        "cal_logloss":  cal_ll  if cal_ll  is None else float(cal_ll),
        "hist_glob": os.environ.get("CAL_TRAIN_HISTORY_GLOB", "history/season_20*_from_site.csv"),
        "season_start": int(os.environ.get("CAL_TRAIN_START_SEASON", 0)),
        "season_end":   int(os.environ.get("CAL_TRAIN_END_SEASON", 9999)),
    }
    try:
        if df_like is not None and "date" in df_like.columns:
            dd = pd.to_datetime(df_like["date"], errors="coerce").dropna()
            if len(dd):
                meta["date_min"] = dd.min().strftime("%Y-%m-%d")
                meta["date_max"] = dd.max().strftime("%Y-%m-%d")
        with open(P_META, "w") as fh:
            json.dump(meta, fh)
        print(OK, f"Wrote {P_META}")
    except Exception as e:
        print(WARN, f"Could not write calibration meta: {e}")

def _safe_write_sample(p, y, df_like=None):
    try:
        os.makedirs(os.path.dirname(P_SAMP), exist_ok=True)
        df = pd.DataFrame({"p": np.asarray(p, float), "y": np.asarray(y, int)}) if len(p) == len(y) else pd.DataFrame({"p":[], "y":[]})
        df.to_csv(P_SAMP, index=False)
        print(OK, f"Wrote {P_SAMP} rows={len(df)}")
    except Exception as e:
        print(WARN, f"Could not write calibration train_sample: {e}")

def _derive_prob(df):
    """Return model home-win probability p from flexible aliases or Elo pre."""
    # direct prob aliases (ordered preference)
    prob_aliases = [
        "p_home_model","p_model","p_home_pre_injury","p",
        "home_prob","home_win_prob","model_prob","pred_home","pred"
    ]
    for col in prob_aliases:
        if col in df.columns and df[col].notna().any():
            p = pd.to_numeric(df[col], errors="coerce")
            if p.notna().sum() > 0:
                return p.clip(1e-6, 1-1e-6).values

    # derive from Elo pre (aliases)
    home_alias = next((c for c in ["elo_pre_home","home_elo_pre","elo_home_pre","home_elo","elo_home"] if c in df.columns), None)
    away_alias = next((c for c in ["elo_pre_away","away_elo_pre","elo_away_pre","away_elo","elo_away"] if c in df.columns), None)
    if home_alias and away_alias:
        k = math.log(10)/400.0
        eh = pd.to_numeric(df[home_alias], errors="coerce")
        ea = pd.to_numeric(df[away_alias], errors="coerce")
        z  = k*(eh - ea)
        return _inv(z).clip(1e-6, 1-1e-6).values

    raise RuntimeError("No usable prob columns and no elo_pre_* (or aliases) to derive prob.")

def _derive_label(df):
    """Return y = 1 if home team won, else 0, from scores or outcome flags."""
    # score pairs
    score_pairs = [
        ("home_score","away_score"),
        ("home_points","away_points"),
        ("home_final","away_final"),
        ("score_home","score_away"),
        ("home_pts","away_pts"),
    ]
    for hs_col, as_col in score_pairs:
        if {hs_col, as_col}.issubset(df.columns):
            hs = pd.to_numeric(df[hs_col], errors="coerce")
            aw = pd.to_numeric(df[as_col], errors="coerce")
            mask = hs.notna() & aw.notna()
            if mask.any():
                return (hs[mask] > aw[mask]).astype(int).values

    # binary outcome fallback
    for col in ["home_win","home_result","result","y","home_won"]:
        if col in df.columns and df[col].notna().any():
            ser = pd.to_numeric(df[col], errors="coerce")
            if ser.notna().sum() > 0:
                return ser.astype(int).values

    raise RuntimeError("No usable label columns (scores or binary outcome).")

def _irls_platt(z, y, lam=1e-6, max_iter=50, tol=1e-6):
    """Fit Platt scaling parameters (a,b) by IRLS on p = sigmoid(a*z + b)."""
    z = np.asarray(z, float)
    y = np.asarray(y, float)
    a, b = 1.0, 0.0
    X = np.stack([z, np.ones_like(z)], axis=1)   # [a, b]
    for _ in range(max_iter):
        f  = a*z + b
        p  = _inv(f)
        W  = p*(1-p) + 1e-9               # n×1
        # IRLS step: (X^T W X + λI) θ = X^T (W z + (y-p))
        # Here model is logistic; Newton step on (a,b)
        # Gradient/Hessian formulation:
        # H = X^T diag(W) X + lam*I ; g = X^T (y - p)
        XtW = X.T * W
        H   = XtW @ X
        H[0,0] += lam
        H[1,1] += lam
        g   = X.T @ (y - p)
        try:
            step = np.linalg.solve(H, g)
        except np.linalg.LinAlgError:
            break
        a_new, b_new = a + step[0], b + step[1]
        if max(abs(a_new-a), abs(b_new-b)) < tol:
            a, b = float(a_new), float(b_new)
            break
        a, b = float(a_new), float(b_new)
    return float(a), float(b)

def _load_history():
    glob_pat = os.environ.get("CAL_TRAIN_HISTORY_GLOB", "history/season_20*_from_site.csv")
    files = sorted(glob.glob(glob_pat))
    if not files:
        print(WARN, f"No files matched CAL_TRAIN_HISTORY_GLOB={glob_pat}")
        return pd.DataFrame()
    frames = []
    for fp in files:
        try:
            df = pd.read_csv(fp)
            df["__src"] = os.path.basename(fp)
            frames.append(df)
        except Exception as e:
            print(WARN, f"Could not read {fp}: {e}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
# -- assemble training frame start
    # Filters: valid p in (0,1), drop coinflips, binary y, no NaNs
    df = df.copy()
    if 'p' in df.columns:
        df = df[df['p'].between(0.0, 1.0, inclusive='neither')]
        df = df[df['p'] != 0.5]
    if 'y' in df.columns:
        df['y'] = df['y'].astype(float).round().astype(int)
        df = df[df['y'].isin([0,1])]
    df = df.dropna(subset=['p','y']).copy()
    if df.empty:
        print(WARN, "No training rows after filters; writing identity calibration.")
        _write_json_identity(0, "no_training_after_filters")
        _safe_write_meta(pd.DataFrame(), None, None, None, None)
        _safe_write_sample([], [], pd.DataFrame())
        return
    assert (df['p'] != 0.5).all(), "Found p==0.5 after filters"
    print(OK, f"Training rows after filters: {len(df)}")
# -- assemble training frame end

    ##__CAL_FILTERS__  (idempotent marker)
    # keep informative rows only
    df = df.copy()
    if 'p' in df.columns:
        # guard valid range & drop coin-flips
        df = df[df['p'].between(0.0, 1.0, inclusive='neither')]
        df = df[df['p'] != 0.5]
    if 'y' in df.columns:
        # coerce y to {0,1} and drop the rest
        df['y'] = df['y'].astype(float).round().astype(int)
        df = df[df['y'].isin([0,1])].copy()
    print("[INFO] calibration filtering -> rows:", len(df))

    # drop uninformative rows
    df = df[df['p'] != 0.5].copy()
    # optional season filter
    if "season" in df.columns:
        s0 = int(os.environ.get("CAL_TRAIN_START_SEASON", 0))
        s1 = int(os.environ.get("CAL_TRAIN_END_SEASON", 9999))
        df = df[(df["season"] >= s0) & (df["season"] <= s1)]
    return df

def main():
    os.makedirs(os.path.dirname(P_OUT), exist_ok=True)

    try:
        df = _load_history()
        if df.empty:
            _write_json_identity(0, "no_training_data")
            _safe_write_meta(df, None, None, None, None)
            _safe_write_sample([], [], df)
            return

        # derive p,y
        try:
            p = _derive_prob(df)
            y = _derive_label(df)
            # align lengths (in case of row drops inside label derivation)
            n = min(len(p), len(y))
            p, y = p[:n], y[:n]
        except Exception as e:
            print(WARN, f"Could not compute p/y: {e}")
            _write_json_identity(0, "no_training_data")
            _safe_write_meta(df, None, None, None, None)
            _safe_write_sample([], [], df)
            return

        # minimum rows
        min_rows = int(os.environ.get("MIN_CAL_ROWS", "500"))
        if n < min_rows:
            print(WARN, f"Only {n} training rows; using identity calibration.")
            _write_json_identity(n, "too_few_rows")
            _safe_write_meta(df.iloc[:n], None, None, None, None)
            _safe_write_sample(p, y, df.iloc[:n])
            return

        # fit Platt on logits
        z = _logit(p)
        a, b = _irls_platt(z, y, lam=1e-4)

        # safety gates
        test_p = np.linspace(0.05, 0.95, 19)
        pt = _inv(a*_logit(test_p) + b)
        monotone  = np.all(np.diff(pt) > 0)
        collapsed = (pt.max() - pt.min()) < float(os.environ.get("CAL_COLLAPSE_MIN_RANGE", "0.15"))
        if (not monotone) or collapsed or (a <= 0):
            _write_json_identity(n, f"unsafe_mapping(a={a:.4f},monotone={monotone},collapsed={collapsed})")
            _safe_write_meta(df.iloc[:n], None, None, None, None)
            _safe_write_sample(p, y, df.iloc[:n])
            return

        base_ll = _logloss(p, y)
        cal_ll  = _logloss(_inv(a*z + b), y)

        # no real improvement guard
        if cal_ll > base_ll - 0.002:
            _write_json_identity(n, f"no_improvement(base={base_ll:.4f},cal={cal_ll:.4f})")
            _safe_write_meta(df.iloc[:n], None, None, float(base_ll), float(cal_ll))
            _safe_write_sample(p, y, df.iloc[:n])
            return

        # write provenance first
        _safe_write_meta(df.iloc[:n], float(a), float(b), float(base_ll), float(cal_ll))
        _safe_write_sample(p, y, df.iloc[:n])

        # final params
        with open(P_OUT, "w") as f:
            json.dump({"a": float(a), "b": float(b), "n": int(n)}, f)
        print(OK, f"Platt fit: a={a:.4f}, b={b:.4f}, n={n}, base_ll={base_ll:.4f}, cal_ll={cal_ll:.4f}")
        print(OK, f"Wrote {P_OUT}")

    except Exception as e:
        print(WARN, f"Calibration fit failed: {e}")
        traceback.print_exc()
        _write_json_identity(0, "fit_exception")
        try:
            _safe_write_meta(pd.DataFrame(), None, None, None, None)
            _safe_write_sample([], [], pd.DataFrame())
        except Exception:
            pass

if __name__ == "__main__":
    main()
