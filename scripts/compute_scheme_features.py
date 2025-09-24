#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute weekly scheme features (pass/rush rate + total plays) per team/date.

Priority:
  1) Use PBP counts when available.
  2) For (date, team) not covered by PBP, fill from box (rush_att/pass_att if present, else 0).

Inputs:
  - out/msf_details/pbp_week.csv
      needs: date (or date_utc), offense_team (or offense), play_type
  - out/msf_details/boxscores_week.csv
      needs: date, team
      optional: rush_att, pass_att

Output:
  - out/scheme_features_week.csv with rows:
      date, team, feature in {scheme_pass_rate, scheme_rush_rate, scheme_plays},
      value, source in {pbp, box}, scheme_confidence in {low, med, high}
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

OUT_PATH = Path("out/scheme_features_week.csv")
PBP_PATH = Path("out/msf_details/pbp_week.csv")
BOX_PATH = Path("out/msf_details/boxscores_week.csv")

def _normalize_date(s: pd.Series) -> pd.Series:
    return s.astype(str).str[:10]

def _confidence_from_plays(tot_series: pd.Series) -> pd.Series:
    tot = pd.to_numeric(tot_series, errors="coerce").fillna(0)
    conf = pd.Series("low", index=tot.index)
    conf.loc[(tot >= 20) & (tot < 60)] = "med"
    conf.loc[tot >= 60] = "high"
    return conf

def _emit_scheme_rows(agg: pd.DataFrame, src: str) -> pd.DataFrame:
    # ensure columns exist + numeric
    for c in ("rush_att", "pass_att"):
        if c not in agg.columns:
            agg[c] = 0
        agg[c] = pd.to_numeric(agg[c], errors="coerce").fillna(0)

    tot = (agg["rush_att"] + agg["pass_att"]).astype(float)
    zero = (tot <= 0) | tot.isna()

    with pd.option_context("mode.use_inf_as_na", True):
        pass_rate = (agg["pass_att"].astype(float) / tot).where(~zero)
        rush_rate = (agg["rush_att"].astype(float) / tot).where(~zero)

    conf = _confidence_from_plays(tot)

    rows = []
    for i, r in agg.iterrows():
        d, t = r["date"], r["team"]
        rows.append({"date": d, "team": t, "feature": "scheme_pass_rate",
                     "value": pass_rate.iat[i], "source": src, "scheme_confidence": conf.iat[i]})
        rows.append({"date": d, "team": t, "feature": "scheme_rush_rate",
                     "value": rush_rate.iat[i], "source": src, "scheme_confidence": conf.iat[i]})
        rows.append({"date": d, "team": t, "feature": "scheme_plays",
                     "value": None if pd.isna(tot.iat[i]) else int(tot.iat[i]),
                     "source": src, "scheme_confidence": conf.iat[i]})
    return pd.DataFrame(rows)

def _pbp_agg() -> pd.DataFrame:
    if not PBP_PATH.exists():
        print("[scheme][PBP] missing pbp_week.csv")
        return pd.DataFrame()

    pbp = pd.read_csv(PBP_PATH)

    # date
    if "date" not in pbp.columns:
        if "date_utc" in pbp.columns:
            pbp["date"] = _normalize_date(pbp["date_utc"])
        else:
            print("[scheme][PBP] missing 'date'/'date_utc'")
            return pd.DataFrame()
    else:
        pbp["date"] = _normalize_date(pbp["date"])

    # offense team
    if "offense_team" not in pbp.columns and "offense" in pbp.columns:
        pbp = pbp.rename(columns={"offense": "offense_team"})
    if "offense_team" not in pbp.columns:
        print("[scheme][PBP] missing 'offense_team'/'offense'")
        return pd.DataFrame()

    if "play_type" not in pbp.columns:
        print("[scheme][PBP] missing 'play_type'")
        return pd.DataFrame()

    pbp["offense_team"] = pbp["offense_team"].astype(str).str.upper()
    pbp["play_type"] = pbp["play_type"].astype(str).str.upper()

    pbp_use = pbp[pbp["play_type"].isin(["RUSH", "PASS"])].copy()
    if pbp_use.empty:
        print("[scheme][PBP] no RUSH/PASS rows")
        return pd.DataFrame()

    counts = (
        pbp_use
        .groupby(["date", "offense_team", "play_type"])
        .size()
        .unstack(fill_value=0)
        .rename_axis(index={"offense_team": "team"}, columns=None)
        .reset_index()
    )
    if "RUSH" not in counts.columns: counts["RUSH"] = 0
    if "PASS" not in counts.columns: counts["PASS"] = 0

    agg = counts.rename(columns={"RUSH": "rush_att", "PASS": "pass_att"})
    agg["team"] = agg["team"].astype(str).str.upper()
    agg["date"] = _normalize_date(agg["date"])
    return agg[["date", "team", "rush_att", "pass_att"]]

def _box_agg() -> pd.DataFrame:
    if not BOX_PATH.exists():
        print("[scheme][box] missing boxscores_week.csv")
        return pd.DataFrame()

    box = pd.read_csv(BOX_PATH)
    need = {"date", "team"}
    if not need.issubset(box.columns):
        print("[scheme][box] needs columns: date, team")
        return pd.DataFrame()

    box["date"] = _normalize_date(box["date"])
    box["team"] = box["team"].astype(str).str.upper()

    for c in ("rush_att", "pass_att"):
        if c not in box.columns:
            box[c] = 0
        box[c] = pd.to_numeric(box[c], errors="coerce").fillna(0)

    agg = (
        box.groupby(["date", "team"], as_index=False)[["rush_att", "pass_att"]]
           .sum(min_count=1)
    )
    return agg[["date", "team", "rush_att", "pass_att"]]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--season", type=str, default=None)
    _ = parser.parse_args()  # args accepted for CLI symmetry

    pbp = _pbp_agg()
    box = _box_agg()

    have_pbp = not pbp.empty
    have_box = not box.empty

    out_parts = []

    if have_pbp:
        pbp_rows = _emit_scheme_rows(pbp.copy(), src="pbp")
        out_parts.append(pbp_rows)
        print(f"[scheme][PBP] ok rows={len(pbp_rows)}")

    if have_box:
        if have_pbp:
            # take only (date,team) not present in pbp
            key = ["date", "team"]
            pbp_keys = set(map(tuple, pbp[key].values.tolist()))
            box_missing = box[~box[key].apply(tuple, axis=1).isin(pbp_keys)].copy()
            if not box_missing.empty:
                box_rows = _emit_scheme_rows(box_missing, src="box")
                out_parts.append(box_rows)
                print(f"[scheme][box] filled rows={len(box_rows)} (pbp-missing only)")
            else:
                print("[scheme][box] nothing to fill; all covered by PBP.")
        else:
            box_rows = _emit_scheme_rows(box.copy(), src="box")
            out_parts.append(box_rows)
            print(f"[scheme][box] ok rows={len(box_rows)}")
    elif not have_pbp:
        # nothing available; write empty
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["date","team","feature","value","source","scheme_confidence"]).to_csv(OUT_PATH, index=False)
        print("[scheme][ok] wrote empty scheme_features_week.csv (no pbp/box)")
        return

    final = pd.concat(out_parts, ignore_index=True) if out_parts else pd.DataFrame(
        columns=["date","team","feature","value","source","scheme_confidence"]
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUT_PATH, index=False)

    src_summary = []
    if have_pbp: src_summary.append("pbp=Y")
    else: src_summary.append("pbp=N")
    if have_box: src_summary.append("box=Y")
    else: src_summary.append("box=N")
    print(f"[scheme][ok] wrote {OUT_PATH} rows={len(final)} sources: {', '.join(src_summary)}")

if __name__ == "__main__":
    main()
