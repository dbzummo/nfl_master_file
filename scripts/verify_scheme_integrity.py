#!/usr/bin/env python3
"""
verify_scheme_integrity.py

Compares out/scheme_features_week.csv to the CURRENT MSF baseline at
out/msf_details/msf_week.csv (or a path you pass via --msf). It no longer
relies on any hidden defaults that could point at an old week.

Usage:
  python3 scripts/verify_scheme_integrity.py --autofix --replace \
    [--scheme out/scheme_features_week.csv] \
    [--msf out/msf_details/msf_week.csv]

Env (optional):
  START=YYYYMMDD  END=YYYYMMDD   # filters the MSF baseline by date
"""
import argparse, os, sys, pathlib
import pandas as pd

DEF_SCHEME = "out/scheme_features_week.csv"
DEF_SCHEME_DEDUP = "out/scheme_features_week_dedup.csv"
DEF_MSF = "out/msf_details/msf_week.csv"

def die(msg, code=1):
    print(f"[verify][ERR] {msg}", file=sys.stderr); sys.exit(code)

def to_ymd(s):
    return pd.to_datetime(s).dt.strftime("%Y-%m-%d")

def load_scheme(path):
    if not pathlib.Path(path).exists():
        die(f"scheme file not found: {path}")
    df = pd.read_csv(path)
    req = {"date","team","feature","value"}
    if not req.issubset(df.columns):
        missing = sorted(req - set(df.columns))
        die(f"scheme file missing required columns: {missing}")
    # normalize
    df["date"] = to_ymd(df["date"])
    df["team"] = df["team"].astype(str).str.upper().str.strip()
    # tolerate missing 'source'
    if "source" not in df.columns:
        df["source"] = ""
    return df

def load_msf(path, start_env=None, end_env=None):
    p = pathlib.Path(path)
    if not p.exists():
        die(f"MSF baseline not found: {path}")
    df = pd.read_csv(p)
    need = {"date","away_team","home_team"}
    if not need.issubset(df.columns):
        die(f"MSF baseline missing columns {sorted(need - set(df.columns))} in {path}")
    # normalize + optional window filter
    df["date"] = to_ymd(df["date"])
    if start_env and end_env:
        s = pd.to_datetime(start_env, format="%Y%m%d", errors="coerce")
        e = pd.to_datetime(end_env,   format="%Y%m%d", errors="coerce")
        if s is not pd.NaT and e is not pd.NaT:
            df2 = df[(pd.to_datetime(df["date"])>=s) & (pd.to_datetime(df["date"])<=e)].copy()
            if not df2.empty:
                df = df2
    df["away_team"] = df["away_team"].astype(str).str.upper().str.strip()
    df["home_team"] = df["home_team"].astype(str).str.upper().str.strip()
    # explode into (date,team) pairs
    left  = df[["date","away_team"]].rename(columns={"away_team":"team"})
    right = df[["date","home_team"]].rename(columns={"home_team":"team"})
    keys = pd.concat([left,right], ignore_index=True).drop_duplicates()
    return keys, df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheme", default=DEF_SCHEME)
    ap.add_argument("--msf", default=DEF_MSF)
    ap.add_argument("--autofix", action="store_true")
    ap.add_argument("--replace", action="store_true",
                    help="When autofixing, also write the _dedup file in-place")
    args = ap.parse_args()

    start_env = os.environ.get("START")
    end_env   = os.environ.get("END")

    scheme = load_scheme(args.scheme)
    msf_keys, msf_week = load_msf(args.msf, start_env, end_env)

    # compute (date,team) key sets
    scheme_keys = scheme[["date","team"]].drop_duplicates()
    exp = set(map(tuple, msf_keys[["date","team"]].itertuples(index=False, name=None)))
    got = set(map(tuple, scheme_keys[["date","team"]].itertuples(index=False, name=None)))

    print(f"[verify] scheme rows={len(scheme)} | unique (date,team)={len(got)}")
    print("[verify] features per (date,team):")
    per = scheme.groupby(["date","team"]).size().value_counts().sort_index()
    for k,v in per.items():
        print(f"{k}\t{v}")

    # Core checks
    # duplicates
    dups = scheme.duplicated(["date","team","feature"]).sum()
    if dups:
        print(f"[verify][WARN] duplicate (date,team,feature) rows: {dups}")
    else:
        print("[verify] no duplicate (date,team,feature) rows")

    # overlap pbp/box (if present)
    if "source" in scheme.columns:
        both = (scheme.groupby(["date","team","feature"])["source"]
                     .nunique()
                     .reset_index(name="n")["n"].gt(1).sum())
        if both:
            print(f"[verify][WARN] PBP/BOX overlap rows: {both}")
        else:
            print("[verify] no PBP/BOX overlap")

    # exactly 3 per team/day?
    exact3 = (per.index.tolist()==[3] and per.iloc[0]==len(got))
    if exact3:
        print("[verify] exactly 3 features per (date,team)")
    else:
        print("[verify][WARN] not exactly 3 features per (date,team)")

    # compare keys to MSF baseline
    missing = sorted(exp - got)
    extra   = sorted(got - exp)

    if missing:
        print(f"[verify][FAIL] missing (date,team) keys in scheme (expected from {args.msf}): {len(missing)}")
        print("  sample missing:", ", ".join(map(str, missing[:10])))
    else:
        print("[verify] no missing (date,team) keys vs MSF baseline")

    if extra:
        print(f"[verify][WARN] scheme contains {len(extra)} extra (date,team) keys not in MSF baseline")
        print("  sample extras:", ", ".join(map(str, extra[:10])))
    else:
        print("[verify] no extra (date,team) keys vs MSF baseline")

    # Autofix: keep only rows that match the MSF baseline
    out_dedup = DEF_SCHEME_DEDUP
    if args.autofix:
        keep = scheme.merge(msf_keys, on=["date","team"], how="inner")
        keep = keep.drop_duplicates(["date","team","feature"]).copy()
        pathlib.Path(out_dedup).parent.mkdir(parents=True, exist_ok=True)
        keep.to_csv(out_dedup, index=False)
        print(f"[verify] wrote sanitized de-duped → {out_dedup} rows={len(keep)}")
        if args.replace:
            # overwrite the original file with the sanitized one
            keep.to_csv(args.scheme, index=False)
            print(f"[verify] replaced original scheme file with sanitized version.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
